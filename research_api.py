import os
import re
import time
import json
import asyncio
import hashlib
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote
import xml.etree.ElementTree as ET

import httpx
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

try:
    import akshare as ak
except Exception:
    ak = None

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("astock-research-api")

APP_VERSION = "1.1.0-rss-stable-policy-websearch"
DEFAULT_RSSHUB_BASE = os.getenv("RSSHUB_BASE", "https://rsshub.app").rstrip("/")
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "12"))
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "180"))
MAX_RSS_PER_SOURCE = int(os.getenv("MAX_RSS_PER_SOURCE", "20"))
MAX_CONCEPTS_PER_KEYWORD = int(os.getenv("MAX_CONCEPTS_PER_KEYWORD", "3"))
MAX_STOCKS_PER_CONCEPT = int(os.getenv("MAX_STOCKS_PER_CONCEPT", "40"))

TAVILY_API_KEY = os.getenv("TAVILY_API_KEY", "")
SERPER_API_KEY = os.getenv("SERPER_API_KEY", "")
EXA_API_KEY = os.getenv("EXA_API_KEY", "")

app = FastAPI(title="A股/港股智能投研资料聚合 API", version=APP_VERSION)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_cache: Dict[str, Tuple[float, Any]] = {}


class ResearchRequest(BaseModel):
    request_id: Optional[str] = None
    user_message: str = Field(..., description="用户在钉钉/企微/TG 发来的原始问题")
    intent: Dict[str, Any] = Field(default_factory=dict, description="第一轮 LLM 提取的意图 JSON")
    max_items: int = 60
    top_k: int = 30
    enable_tavily: bool = True
    enable_serper: bool = True
    enable_exa: bool = True
    enable_akshare: bool = True

    # 新架构：政策默认不走 RSSHub。政策由豆包 web_search / Tavily / Serper / Exa 定向搜官方站点。
    enable_policy_rss: bool = False

    # 可在请求体中临时覆盖 Hugging Face 环境变量 RSSHUB_BASE。
    rsshub_base: Optional[str] = None


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def normalize_text(s: Any, max_len: int = 2000) -> str:
    if s is None:
        return ""
    s = str(s)
    s = re.sub(r"<script[\s\S]*?</script>", " ", s, flags=re.I)
    s = re.sub(r"<style[\s\S]*?</style>", " ", s, flags=re.I)
    s = re.sub(r"<!\[CDATA\[([\s\S]*?)\]\]>", r"\1", s, flags=re.I)
    s = re.sub(r"<[^>]+>", " ", s)
    html_entities = {
        "&nbsp;": " ", "&#160;": " ", "&amp;": "&", "&lt;": "<", "&gt;": ">",
        "&quot;": '"', "&#39;": "'", "&ldquo;": "“", "&rdquo;": "”",
    }
    for k, v in html_entities.items():
        s = s.replace(k, v)
    s = re.sub(r"\s+", " ", s).strip()
    return s[:max_len]


def listify(x: Any) -> List[str]:
    if not x:
        return []
    if isinstance(x, list):
        return [normalize_text(i, 80) for i in x if normalize_text(i, 80)]
    if isinstance(x, str):
        parts = re.split(r"[,，、;；\s]+", x)
        return [p.strip() for p in parts if len(p.strip()) >= 2]
    return []


GENERIC_QUERY_WORDS = {
    "最近", "政策", "很多", "帮我", "分析", "看看", "一下", "核心", "供应链", "股票", "A股", "港股",
    "板块", "概念", "机会", "有哪些", "什么", "相关", "最新", "方向", "研究", "投研", "挖", "市场",
    "今天", "今日", "近期", "未来", "利好", "消息", "新闻", "公司", "企业", "行业", "产业", "情况",
}


def meaningful_keywords(keywords: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for k in keywords:
        k = normalize_text(k, 30)
        if not k:
            continue
        if k in GENERIC_QUERY_WORDS:
            continue
        if len(k) < 2:
            continue
        lk = k.lower()
        if lk in seen:
            continue
        seen.add(lk)
        out.append(k)
    return out[:12]


def extract_keywords(user_message: str, intent: Dict[str, Any]) -> List[str]:
    candidates: List[str] = []

    # 兼容豆包第一轮意图解析的各种字段。
    for key in [
        "keywords", "core_keywords", "concepts", "industry_chain_keywords", "search_keywords",
        "policy_queries", "news_queries", "stock_concept_queries", "company_queries",
        "supply_chain_segments",
    ]:
        candidates += listify(intent.get(key))

    # 兼容 industry_chain: [{segment, keywords}]
    for seg in intent.get("industry_chain", []) if isinstance(intent.get("industry_chain"), list) else []:
        if isinstance(seg, dict):
            candidates += listify(seg.get("segment"))
            candidates += listify(seg.get("keywords"))

    # fallback：从用户原文抽关键词，但会过滤泛词。
    for m in re.findall(r"[\u4e00-\u9fa5A-Za-z0-9]{2,16}", user_message):
        if m not in candidates:
            candidates.append(m)

    out = meaningful_keywords(candidates)
    # 如果第一轮 LLM 没给关键词，至少保留用户原文中的少量非泛词 token。
    if not out:
        fallback = []
        for m in re.findall(r"[\u4e00-\u9fa5A-Za-z0-9]{2,16}", user_message):
            if m not in GENERIC_QUERY_WORDS:
                fallback.append(m)
        out = meaningful_keywords(fallback)
    return out[:12]


def is_broad_scan_query(user_message: str, keywords: List[str]) -> bool:
    core_kws = meaningful_keywords(keywords)
    scan_words = [
        "热点板块", "市场热点", "今天A股", "今日A股", "盘面", "题材", "异动", "全市场",
        "最近有哪些方向", "扫一下市场", "最近什么板块", "有什么机会", "今天市场",
    ]
    return len(core_kws) <= 1 and any(w in user_message for w in scan_words)


def make_cache_key(prefix: str, payload: Any) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return prefix + ":" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def cache_get(key: str) -> Any:
    item = _cache.get(key)
    if not item:
        return None
    ts, data = item
    if time.time() - ts > CACHE_TTL_SECONDS:
        _cache.pop(key, None)
        return None
    return data


def cache_set(key: str, data: Any):
    _cache[key] = (time.time(), data)


async def safe_http_get_debug(client: httpx.AsyncClient, url: str, headers: Optional[Dict[str, str]] = None) -> Tuple[str, Dict[str, Any]]:
    key = make_cache_key("GET", {"url": url})
    cached = cache_get(key)
    if cached is not None:
        return cached.get("text", ""), cached.get("debug", {"cached": True, "url": url})

    debug = {
        "url": url,
        "ok": False,
        "status_code": None,
        "content_type": "",
        "text_len": 0,
        "error": "",
        "sample": "",
        "cached": False,
    }
    last_err: Optional[Exception] = None
    for i in range(3):
        try:
            r = await client.get(url, headers=headers, timeout=HTTP_TIMEOUT, follow_redirects=True)
            debug["status_code"] = r.status_code
            debug["content_type"] = r.headers.get("content-type", "")
            debug["text_len"] = len(r.text or "")
            debug["sample"] = normalize_text(r.text, 250)
            if r.status_code >= 400:
                debug["error"] = f"HTTP {r.status_code}"
                continue
            debug["ok"] = True
            data = {"text": r.text, "debug": debug}
            cache_set(key, data)
            return r.text, debug
        except Exception as e:
            last_err = e
            debug["error"] = str(e)
            await asyncio.sleep(0.5 * (i + 1))
    logger.warning("GET failed url=%s err=%s", url, last_err)
    return "", debug


async def safe_http_post_json(client: httpx.AsyncClient, url: str, payload: Dict[str, Any], headers: Dict[str, str]) -> Dict[str, Any]:
    key = make_cache_key("POST", {"url": url, "payload": payload})
    cached = cache_get(key)
    if cached is not None:
        return cached
    last_err = None
    for i in range(3):
        try:
            r = await client.post(url, json=payload, headers=headers, timeout=HTTP_TIMEOUT, follow_redirects=True)
            r.raise_for_status()
            data = r.json()
            cache_set(key, data)
            return data
        except Exception as e:
            last_err = e
            await asyncio.sleep(0.8 * (i + 1))
    logger.warning("POST failed url=%s err=%s", url, last_err)
    return {"error": str(last_err)}


def parse_rss(xml_text: str, source: str, source_type: str, source_url: str, limit: int = 20) -> List[Dict[str, Any]]:
    if not xml_text:
        return []
    if "<rss" not in xml_text and "<feed" not in xml_text and "<item" not in xml_text:
        return []

    items: List[Dict[str, Any]] = []
    evidence = "S" if source_type in ["exchange", "cls"] else "A"
    try:
        root = ET.fromstring(xml_text.encode("utf-8"))
        for item in root.findall(".//item")[:limit]:
            title = normalize_text(item.findtext("title"), 220)
            link = normalize_text(item.findtext("link"), 500)
            pub = normalize_text(
                item.findtext("pubDate") or item.findtext("date") or item.findtext("{http://purl.org/dc/elements/1.1/}date"),
                100,
            )
            desc = normalize_text(item.findtext("description") or item.findtext("encoded"), 900)
            if title:
                items.append({
                    "source": source,
                    "source_type": source_type,
                    "title": title,
                    "link": link,
                    "published_at": pub,
                    "content": desc,
                    "source_url": source_url,
                    "evidence_level": evidence,
                })

        ns = {"atom": "http://www.w3.org/2005/Atom"}
        for entry in root.findall(".//atom:entry", ns)[:limit]:
            title = normalize_text(entry.findtext("atom:title", default="", namespaces=ns), 220)
            link_el = entry.find("atom:link", ns)
            link = link_el.attrib.get("href", "") if link_el is not None else ""
            pub = normalize_text(entry.findtext("atom:updated", default="", namespaces=ns), 100)
            content = normalize_text(
                entry.findtext("atom:summary", default="", namespaces=ns) or entry.findtext("atom:content", default="", namespaces=ns),
                900,
            )
            if title:
                items.append({
                    "source": source,
                    "source_type": source_type,
                    "title": title,
                    "link": link,
                    "published_at": pub,
                    "content": content,
                    "source_url": source_url,
                    "evidence_level": evidence,
                })
    except Exception as e:
        logger.warning("RSS parse fallback source=%s err=%s", source, e)
        chunks = re.findall(r"<item[\s\S]*?</item>", xml_text, flags=re.I)[:limit]
        for ch in chunks:
            def tag(t: str, length: int = 1000) -> str:
                m = re.search(rf"<{t}[^>]*>([\s\S]*?)</{t}>", ch, flags=re.I)
                return normalize_text(m.group(1), length) if m else ""
            title = tag("title", 220)
            if title:
                items.append({
                    "source": source,
                    "source_type": source_type,
                    "title": title,
                    "link": tag("link", 500),
                    "published_at": tag("pubDate", 100),
                    "content": tag("description", 900),
                    "source_url": source_url,
                    "evidence_level": evidence,
                })
    return items[:limit]


def build_rss_routes(keywords: List[str], base: str, enable_policy_rss: bool = False) -> List[Dict[str, str]]:
    """RSSHub 只保留稳定、可定向的信息流。政策默认不抓，由豆包 web_search 定向搜索官方站点。"""
    routes = [
        ("财联社-全部电报", "cls", "/cls/telegraph"),
        ("财联社-港股", "cls", "/cls/telegraph/hk"),
        ("财联社-公司", "cls", "/cls/telegraph/announcement"),
        ("财联社-解读", "cls", "/cls/telegraph/explain"),
        ("财联社深度-股市", "cls", "/cls/depth/1003"),
        ("财联社深度-港股", "cls", "/cls/depth/1135"),
        ("财联社深度-公司", "cls", "/cls/depth/1005"),
        ("财联社深度-科创", "cls", "/cls/depth/1111"),
        ("上交所-监管问询", "exchange", "/sse/inquire"),
        ("上交所-科创板项目动态", "exchange", "/sse/renewal"),
        ("上交所-上市公司公告", "exchange", "/sse/disclosure"),
        ("深交所-创业板项目动态", "exchange", "/szse/projectdynamic"),
        ("深交所-问询函件", "exchange", "/szse/inquire"),
        ("深交所-最新规则", "exchange", "/szse/rule"),
    ]

    # 可选：调试用。默认关闭，避免无关政策占上下文。
    if enable_policy_rss:
        routes += [
            ("国务院-最新政策", "policy", "/gov/zhengce/zuixin"),
            ("国务院-最新文件", "policy", "/gov/zhengce/wenjian"),
            ("工信部-政策解读", "policy", "/gov/miit/zcjd"),
            ("工信部-政策文件", "policy", "/gov/miit/zcwj"),
        ]

    out = [{"name": name, "type": typ, "url": base + path} for name, typ, path in routes]

    # 东方财富是关键词搜索，保留。它比“最新政策列表”更适合主题查询。
    for kw in meaningful_keywords(keywords)[:6]:
        out.append({"name": f"东方财富搜索-{kw}", "type": "eastmoney", "url": base + "/eastmoney/search/" + quote(kw)})
    return out


def build_authority_url_map(keywords: List[str], base: str) -> List[Dict[str, Any]]:
    kws = meaningful_keywords(keywords)[:6]
    static_sources = [
        {"name": "财联社电报", "level": "S", "url": base + "/cls/telegraph", "use_for": "盘中快讯、题材催化、市场消息"},
        {"name": "财联社公司", "level": "S", "url": base + "/cls/telegraph/announcement", "use_for": "上市公司相关快讯"},
        {"name": "财联社解读", "level": "S", "url": base + "/cls/telegraph/explain", "use_for": "题材解读、产业链线索"},
        {"name": "财联社深度-股市", "level": "S", "url": base + "/cls/depth/1003", "use_for": "A股深度内容"},
        {"name": "上交所公告", "level": "S", "url": base + "/sse/disclosure", "use_for": "上市公司公告、监管信息"},
        {"name": "深交所问询", "level": "S", "url": base + "/szse/inquire", "use_for": "问询函、风险线索"},
    ]
    for kw in kws:
        static_sources.append({
            "name": f"东方财富搜索-{kw}",
            "level": "A",
            "url": base + "/eastmoney/search/" + quote(kw),
            "use_for": "市场资讯、概念热度、相关公司线索",
        })
    return static_sources


def build_policy_search_plan(user_message: str, intent: Dict[str, Any], keywords: List[str]) -> Dict[str, Any]:
    kws = meaningful_keywords(keywords)[:8]
    if not kws:
        kws = meaningful_keywords([user_message])[:3]

    core = " ".join(kws[:5])
    domains = [
        {"domain": "gov.cn", "name": "中国政府网/国务院", "level": "S"},
        {"domain": "miit.gov.cn", "name": "工信部", "level": "S"},
        {"domain": "ndrc.gov.cn", "name": "发改委", "level": "S"},
        {"domain": "mot.gov.cn", "name": "交通运输部", "level": "S"},
        {"domain": "csrc.gov.cn", "name": "证监会", "level": "S"},
        {"domain": "mof.gov.cn", "name": "财政部", "level": "S"},
    ]

    policy_words = ["政策", "规划", "通知", "意见", "行动方案", "实施方案", "试点", "产业", "高质量发展"]
    queries = []
    for d in domains[:5]:
        queries.append(f"site:{d['domain']} {core} {' '.join(policy_words[:4])}".strip())
    # 补充一个不限制域名但强政策词的查询，给豆包 web_search 兜底。
    queries.append(f"{core} {' '.join(policy_words)} 官方 政策 文件")

    return {
        "handled_by": "doubao_web_search_and_optional_ai_search",
        "reason": "政策类 RSSHub 路由容易返回无关最新政策、route empty 或 403；因此默认不进入 RSS 主资料池，由最终豆包节点使用 web_search 定向搜索官方域名。",
        "core_keywords": kws,
        "priority_domains": domains,
        "queries": queries,
        "verification_rules": [
            "优先采用官方原文，不优先采用媒体转载。",
            "必须标明政策标题、发布机构、发布日期、链接。",
            "没有找到官方原文时，只能标记为政策线索/待验证。",
            "不得编造政策名称、文件编号、发布日期。",
        ],
    }


def score_item(item: Dict[str, Any], keywords: List[str], user_message: str) -> float:
    title = item.get("title", "")
    content = item.get("content", "")
    text = (title + " " + content).lower()
    core_keywords = meaningful_keywords(keywords)
    matched_keywords = []
    score = 0.0

    for kw in core_keywords:
        lk = kw.lower()
        if lk and lk in text:
            matched_keywords.append(kw)
            score += 6
            if lk in title.lower():
                score += 6

    item["matched_keywords"] = matched_keywords
    item["topic_matched"] = bool(matched_keywords)

    hot_words = [
        "政策", "规划", "试点", "产业链", "国产替代", "卡脖子", "自主可控", "突破", "订单", "中标", "量产",
        "龙头", "市占率", "核心供应商", "高端装备", "人工智能", "低空经济", "人形机器人", "算力", "芯片",
        "半导体", "固态电池", "商业航天", "卫星", "数据要素", "脑机接口", "液冷", "光模块", "飞控", "碳纤维",
        "eVTOL", "无人机", "空管", "通航", "通信导航", "机器人", "AI", "军工", "低空", "新质生产力",
    ]
    for w in hot_words:
        if w.lower() in text:
            score += 1.2

    broad_scan = is_broad_scan_query(user_message, keywords)
    if item["topic_matched"] or broad_scan:
        level = item.get("evidence_level")
        if level == "S":
            score += 5
        elif level == "A":
            score += 3
        elif level == "B":
            score += 1

        st = item.get("source_type")
        if st in ["exchange", "cls"]:
            score += 3
        if st == "eastmoney":
            score += 2
        if "公告" in text or "问询" in text:
            score += 2
    else:
        # 明确主题查询时，未命中主题的资料降权，避免“来源很权威但完全不相关”混进来。
        if item.get("source_type") in ["policy", "exchange", "eastmoney"]:
            score -= 8

    return round(max(score, 0), 2)


def dedupe_and_rank(items: List[Dict[str, Any]], keywords: List[str], user_message: str, limit: int) -> List[Dict[str, Any]]:
    seen = set()
    out = []
    fallback = []
    core_keywords = meaningful_keywords(keywords)
    broad_scan = is_broad_scan_query(user_message, keywords)
    strict_topic_mode = len(core_keywords) >= 2 and not broad_scan

    for item in items:
        key_raw = item.get("link") or item.get("title") or json.dumps(item, ensure_ascii=False)[:120]
        key = hashlib.md5(key_raw.encode("utf-8")).hexdigest()
        if key in seen:
            continue
        seen.add(key)
        item["score"] = score_item(item, keywords, user_message)

        if strict_topic_mode:
            if item.get("topic_matched"):
                item["match_status"] = "topic_matched"
                out.append(item)
            else:
                item["match_status"] = "unmatched_fallback"
                fallback.append(item)
        else:
            item["match_status"] = "broad_scan_or_weak_topic"
            out.append(item)

    out.sort(key=lambda x: x.get("score", 0), reverse=True)
    if not out and fallback:
        fallback.sort(key=lambda x: x.get("score", 0), reverse=True)
        return fallback[:min(5, limit)]
    return out[:limit]


async def fetch_rss_bundle(
    client: httpx.AsyncClient,
    keywords: List[str],
    base: str,
    user_message: str,
    max_items: int,
    enable_policy_rss: bool = False,
) -> Dict[str, Any]:
    routes = build_rss_routes(keywords, base, enable_policy_rss=enable_policy_rss)
    grouped: Dict[str, Any] = {
        "cls_news": [],
        "policy": [],
        "exchange_announcements": [],
        "eastmoney": [],
        "_debug": [],
    }

    async def fetch_one(route: Dict[str, str]) -> Dict[str, Any]:
        text, dbg = await safe_http_get_debug(
            client,
            route["url"],
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 astock-agent/1.1",
                "Accept": "application/rss+xml, application/xml, text/xml, */*",
            },
        )
        parsed = parse_rss(text, route["name"], route["type"], route["url"], limit=MAX_RSS_PER_SOURCE)
        dbg.update({
            "name": route["name"],
            "type": route["type"],
            "parsed_count": len(parsed),
            "rss_ok": len(parsed) > 0,
        })
        if len(parsed) == 0 and not dbg.get("error"):
            dbg["error"] = "Fetched but parsed 0 valid RSS items. It may be an RSSHub error page, empty feed, or non-RSS content."
        return {"route": route, "items": parsed, "debug": dbg}

    results = await asyncio.gather(*[fetch_one(r) for r in routes])
    for res in results:
        route = res["route"]
        parsed = res["items"]
        grouped["_debug"].append(res["debug"])
        if route["type"] == "cls":
            grouped["cls_news"].extend(parsed)
        elif route["type"] == "policy":
            grouped["policy"].extend(parsed)
        elif route["type"] == "exchange":
            grouped["exchange_announcements"].extend(parsed)
        elif route["type"] == "eastmoney":
            grouped["eastmoney"].extend(parsed)

    grouped["cls_news"] = dedupe_and_rank(grouped["cls_news"], keywords, user_message, max(12, max_items // 3))
    grouped["exchange_announcements"] = dedupe_and_rank(grouped["exchange_announcements"], keywords, user_message, max(8, max_items // 5))
    grouped["eastmoney"] = dedupe_and_rank(grouped["eastmoney"], keywords, user_message, max(10, max_items // 5))
    grouped["policy"] = dedupe_and_rank(grouped["policy"], keywords, user_message, max(5, max_items // 8)) if enable_policy_rss else []
    return grouped


async def tavily_search(client: httpx.AsyncClient, query: str) -> List[Dict[str, Any]]:
    if not TAVILY_API_KEY:
        return []
    payload = {
        "api_key": TAVILY_API_KEY,
        "query": query,
        "search_depth": "advanced",
        "include_answer": True,
        "include_raw_content": False,
        "max_results": 8,
    }
    data = await safe_http_post_json(client, "https://api.tavily.com/search", payload, {"Content-Type": "application/json"})
    results = []
    for r in data.get("results", [])[:8]:
        results.append({
            "source": "Tavily",
            "source_type": "ai_search",
            "title": normalize_text(r.get("title"), 220),
            "link": r.get("url", ""),
            "content": normalize_text(r.get("content"), 1000),
            "published_at": r.get("published_date") or "",
            "evidence_level": "B",
        })
    if data.get("answer"):
        results.insert(0, {
            "source": "Tavily-answer",
            "source_type": "ai_search_summary",
            "title": "Tavily 综合摘要",
            "link": "",
            "content": normalize_text(data.get("answer"), 1200),
            "published_at": "",
            "evidence_level": "B",
        })
    return results


async def serper_search(client: httpx.AsyncClient, query: str) -> List[Dict[str, Any]]:
    if not SERPER_API_KEY:
        return []
    payload = {"q": query, "gl": "cn", "hl": "zh-cn", "num": 10}
    data = await safe_http_post_json(client, "https://google.serper.dev/news", payload, {
        "Content-Type": "application/json",
        "X-API-KEY": SERPER_API_KEY,
    })
    results = []
    for r in data.get("news", [])[:10]:
        results.append({
            "source": "Serper-News",
            "source_type": "ai_search",
            "title": normalize_text(r.get("title"), 220),
            "link": r.get("link", ""),
            "content": normalize_text(r.get("snippet"), 900),
            "published_at": r.get("date") or "",
            "evidence_level": "B",
        })
    return results


async def exa_search(client: httpx.AsyncClient, query: str) -> List[Dict[str, Any]]:
    if not EXA_API_KEY:
        return []
    payload = {"query": query, "numResults": 8, "useAutoprompt": True}
    data = await safe_http_post_json(client, "https://api.exa.ai/search", payload, {
        "Content-Type": "application/json",
        "x-api-key": EXA_API_KEY,
    })
    results = []
    for r in data.get("results", [])[:8]:
        results.append({
            "source": "Exa",
            "source_type": "ai_search",
            "title": normalize_text(r.get("title"), 220),
            "link": r.get("url", ""),
            "content": normalize_text(r.get("text") or r.get("summary"), 900),
            "published_at": r.get("publishedDate") or "",
            "evidence_level": "B",
        })
    return results


async def run_ai_searches(client: httpx.AsyncClient, keywords: List[str], user_message: str, req: ResearchRequest) -> Dict[str, List[Dict[str, Any]]]:
    core = " ".join(meaningful_keywords(keywords)[:8])
    q = (
        f"{user_message}\n关键词：{core}\n"
        "A股 港股 产业链 核心供应商 卡脖子 国产替代 高市占率 公司公告 最新产业催化"
    )
    tasks = []
    names = []
    if req.enable_tavily:
        tasks.append(tavily_search(client, q)); names.append("tavily")
    if req.enable_serper:
        tasks.append(serper_search(client, q)); names.append("serper")
    if req.enable_exa:
        tasks.append(exa_search(client, q)); names.append("exa")
    results = await asyncio.gather(*tasks) if tasks else []
    return {name: dedupe_and_rank(items, keywords, user_message, 10) for name, items in zip(names, results)}


def df_records(df: pd.DataFrame, limit: int) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    df = df.copy()
    df = df.replace({pd.NA: None})
    df = df.where(pd.notnull(df), None)
    preferred = [
        "代码", "名称", "最新价", "涨跌幅", "成交额", "总市值", "流通市值", "市盈率-动态", "市净率", "换手率",
        "板块名称", "序号", "股票代码", "股票简称",
    ]
    cols = [c for c in preferred if c in df.columns]
    if cols:
        df = df[cols + [c for c in df.columns if c not in cols][:8]]
    return df.head(limit).to_dict(orient="records")


def akshare_bundle(keywords: List[str]) -> Dict[str, Any]:
    if ak is None:
        return {"available": False, "error": "akshare is not installed or import failed", "concept_stocks": [], "market_snapshot": []}
    key = make_cache_key("akshare", {"keywords": meaningful_keywords(keywords)[:8]})
    cached = cache_get(key)
    if cached is not None:
        return cached

    result: Dict[str, Any] = {"available": True, "concept_matches": [], "concept_stocks": [], "market_snapshot": []}
    try:
        try:
            spot = ak.stock_zh_a_spot_em()
            for col in ["成交额", "涨跌幅"]:
                if col in spot.columns:
                    spot[col] = pd.to_numeric(spot[col], errors="coerce")
            spot_sorted = spot.sort_values("成交额", ascending=False) if "成交额" in spot.columns else spot
            result["market_snapshot"] = df_records(spot_sorted, 40)
        except Exception as e:
            result["market_snapshot_error"] = str(e)

        try:
            concepts = ak.stock_board_concept_name_em()
            concepts_str = concepts.astype(str)
            matched_rows = []
            seen_names = set()
            for kw in meaningful_keywords(keywords)[:8]:
                mask = concepts_str.apply(lambda row: row.str.contains(re.escape(kw), case=False, na=False).any(), axis=1)
                sub = concepts[mask].head(MAX_CONCEPTS_PER_KEYWORD)
                for _, row in sub.iterrows():
                    name = str(row.get("板块名称") or row.get("名称") or row.iloc[1])
                    if name and name not in seen_names:
                        seen_names.add(name)
                        matched_rows.append((kw, name, row.to_dict()))
            for kw, concept_name, rowdict in matched_rows[:12]:
                result["concept_matches"].append({"keyword": kw, "concept_name": concept_name, "raw": rowdict})
                try:
                    cons = ak.stock_board_concept_cons_em(symbol=concept_name)
                    recs = df_records(cons, MAX_STOCKS_PER_CONCEPT)
                    for r in recs:
                        r["matched_keyword"] = kw
                        r["concept_name"] = concept_name
                        r["evidence_level"] = "S"
                    result["concept_stocks"].extend(recs)
                except Exception as e:
                    result.setdefault("concept_errors", []).append({"concept_name": concept_name, "error": str(e)})
        except Exception as e:
            result["concept_match_error"] = str(e)
    except Exception as e:
        result["error"] = str(e)

    cache_set(key, result)
    return result


def build_structured_context(
    req: ResearchRequest,
    keywords: List[str],
    rss: Dict[str, Any],
    search: Dict[str, Any],
    ak_data: Dict[str, Any],
    base: str,
) -> Dict[str, Any]:
    policy_search_plan = build_policy_search_plan(req.user_message, req.intent, keywords)
    return {
        "data_time": now_iso(),
        "evidence_rules": {
            "S": "n8n抓取的官方/专业数据：交易所公告、财联社、AkShare行情/概念/财务，以及豆包web_search核验到的官方政策原文",
            "A": "n8n抓取的东方财富等公开财经源",
            "B": "Tavily/Serper/Exa/豆包web_search等联网公开网页补充",
            "C": "模型自身历史知识，仅能作为背景，不能作为事实依据",
            "conflict_rule": "S级 > A级 > B级 > C级；冲突时以更高等级证据为准，低等级信息标记为待验证或冲突线索",
        },
        "user_message": req.user_message,
        "intent": req.intent,
        "keywords": keywords,
        "professional_sources": {
            # 政策 RSS 默认不塞入，避免无关政策污染上下文。
            "policy": rss.get("policy", []),
            "cls_news": rss.get("cls_news", []),
            "exchange_announcements": rss.get("exchange_announcements", []),
            "eastmoney": rss.get("eastmoney", []),
            "akshare": ak_data,
        },
        "policy_search_plan": policy_search_plan,
        "authority_url_map": build_authority_url_map(keywords, base),
        "rsshub_debug": rss.get("_debug", []),
        "search_sources": search,
        "source_health": {
            "rsshub_base": base,
            "rss_policy_mode": "disabled_by_default_use_doubao_web_search" if not req.enable_policy_rss else "enabled_optional_debug",
            "akshare_available": bool(ak_data.get("available")),
            "tavily_enabled": bool(TAVILY_API_KEY and req.enable_tavily),
            "serper_enabled": bool(SERPER_API_KEY and req.enable_serper),
            "exa_enabled": bool(EXA_API_KEY and req.enable_exa),
        },
    }


@app.get("/")
async def root():
    return {
        "name": "A股/港股智能投研资料聚合 API",
        "status": "running",
        "version": APP_VERSION,
        "docs": "/docs",
        "health": "/health",
        "research": "/research",
        "rsshub_base": DEFAULT_RSSHUB_BASE,
        "policy_strategy": "政策源默认不走 RSSHub；由最终豆包节点使用 web_search 定向搜索 gov.cn / miit.gov.cn / ndrc.gov.cn 等官方域名。",
    }


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "version": APP_VERSION,
        "time": now_iso(),
        "rsshub_base": DEFAULT_RSSHUB_BASE,
        "akshare_available": ak is not None,
    }


@app.post("/research")
async def research(req: ResearchRequest):
    if not req.user_message.strip():
        raise HTTPException(status_code=400, detail="user_message is required")

    keywords = extract_keywords(req.user_message, req.intent)
    base = (req.rsshub_base or DEFAULT_RSSHUB_BASE).rstrip("/")

    async with httpx.AsyncClient() as client:
        rss_task = fetch_rss_bundle(
            client,
            keywords,
            base,
            req.user_message,
            req.max_items,
            enable_policy_rss=req.enable_policy_rss,
        )
        search_task = run_ai_searches(client, keywords, req.user_message, req)
        rss, search = await asyncio.gather(rss_task, search_task)

    ak_data = akshare_bundle(keywords) if req.enable_akshare else {"available": False, "disabled": True}
    structured_context = build_structured_context(req, keywords, rss, search, ak_data, base)

    compact = {
        "data_time": structured_context["data_time"],
        "keywords": keywords,
        "source_counts": {
            "policy_rss": len(rss.get("policy", [])),
            "cls_news": len(rss.get("cls_news", [])),
            "exchange_announcements": len(rss.get("exchange_announcements", [])),
            "eastmoney": len(rss.get("eastmoney", [])),
            "tavily": len(search.get("tavily", [])),
            "serper": len(search.get("serper", [])),
            "exa": len(search.get("exa", [])),
            "concept_matches": len(ak_data.get("concept_matches", [])) if isinstance(ak_data, dict) else 0,
            "concept_stocks": len(ak_data.get("concept_stocks", [])) if isinstance(ak_data, dict) else 0,
            "market_snapshot": len(ak_data.get("market_snapshot", [])) if isinstance(ak_data, dict) else 0,
        },
        "news_and_market_first": {
            "cls_top": rss.get("cls_news", [])[:18],
            "eastmoney_top": rss.get("eastmoney", [])[:12],
            "exchange_top": rss.get("exchange_announcements", [])[:10],
        },
        "policy_search_plan": structured_context["policy_search_plan"],
        "authority_url_map": structured_context["authority_url_map"],
        "ai_search_top": {
            "tavily": search.get("tavily", [])[:8],
            "serper": search.get("serper", [])[:8],
            "exa": search.get("exa", [])[:8],
        },
        "stock_data_auxiliary": {
            "concept_matches": ak_data.get("concept_matches", [])[:10] if isinstance(ak_data, dict) else [],
            "concept_stocks_top": ak_data.get("concept_stocks", [])[:35] if isinstance(ak_data, dict) else [],
            "market_snapshot_top": ak_data.get("market_snapshot", [])[:10] if isinstance(ak_data, dict) else [],
        },
        "rsshub_debug_top": rss.get("_debug", [])[:20],
        "source_health": structured_context["source_health"],
        "evidence_rules": structured_context["evidence_rules"],
    }

    return {
        "ok": True,
        "request_id": req.request_id,
        "version": APP_VERSION,
        "keywords": keywords,
        "structured_context": structured_context,
        "compact_context": compact,
    }

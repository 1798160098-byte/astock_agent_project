import os
import re
import time
import json
import math
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
except Exception:  # akshare can fail during build/import in some environments
    ak = None

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("astock-research-api")

APP_VERSION = "1.0.0-production-v1"
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
    rsshub_base: Optional[str] = None


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def normalize_text(s: Any, max_len: int = 2000) -> str:
    if s is None:
        return ""
    s = str(s)
    s = re.sub(r"<script[\s\S]*?</script>", " ", s, flags=re.I)
    s = re.sub(r"<style[\s\S]*?</style>", " ", s, flags=re.I)
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"&nbsp;|&#160;", " ", s)
    s = re.sub(r"&amp;", "&", s)
    s = re.sub(r"&lt;", "<", s)
    s = re.sub(r"&gt;", ">", s)
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


def extract_keywords(user_message: str, intent: Dict[str, Any]) -> List[str]:
    candidates: List[str] = []
    for key in ["keywords", "core_keywords", "concepts", "industry_chain_keywords", "search_keywords"]:
        candidates += listify(intent.get(key))
    for seg in intent.get("industry_chain", []) if isinstance(intent.get("industry_chain"), list) else []:
        if isinstance(seg, dict):
            candidates += listify(seg.get("segment"))
            candidates += listify(seg.get("keywords"))
    # fallback: pull Chinese/English keyword-like tokens from user message
    for m in re.findall(r"[\u4e00-\u9fa5A-Za-z0-9]{2,12}", user_message):
        if m not in candidates and len(m) >= 2:
            candidates.append(m)
    stop = {"帮我", "分析", "一下", "最近", "这个", "什么", "哪些", "有没有", "看看", "股票", "A股", "港股"}
    out = []
    seen = set()
    for k in candidates:
        k = normalize_text(k, 30)
        if not k or k in stop or k.lower() in seen:
            continue
        seen.add(k.lower())
        out.append(k)
    return out[:12]


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


async def safe_http_get(client: httpx.AsyncClient, url: str, headers: Optional[Dict[str, str]] = None) -> str:
    key = make_cache_key("GET", {"url": url})
    cached = cache_get(key)
    if cached is not None:
        return cached
    last_err = None
    for i in range(3):
        try:
            r = await client.get(url, headers=headers, timeout=HTTP_TIMEOUT, follow_redirects=True)
            r.raise_for_status()
            text = r.text
            cache_set(key, text)
            return text
        except Exception as e:
            last_err = e
            await asyncio.sleep(0.5 * (i + 1))
    logger.warning("GET failed url=%s err=%s", url, last_err)
    return ""


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
    items: List[Dict[str, Any]] = []
    try:
        root = ET.fromstring(xml_text.encode("utf-8"))
        # RSS item
        for item in root.findall(".//item")[:limit]:
            title = normalize_text(item.findtext("title"), 200)
            link = normalize_text(item.findtext("link"), 500)
            pub = normalize_text(item.findtext("pubDate") or item.findtext("date") or item.findtext("{http://purl.org/dc/elements/1.1/}date"), 80)
            desc = normalize_text(item.findtext("description") or item.findtext("encoded"), 1000)
            if title:
                items.append({
                    "source": source,
                    "source_type": source_type,
                    "title": title,
                    "link": link,
                    "published_at": pub,
                    "content": desc,
                    "source_url": source_url,
                    "evidence_level": "S" if source_type in ["policy", "exchange", "cls"] else "A",
                })
        # Atom fallback
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        for entry in root.findall(".//atom:entry", ns)[:limit]:
            title = normalize_text(entry.findtext("atom:title", default="", namespaces=ns), 200)
            link_el = entry.find("atom:link", ns)
            link = link_el.attrib.get("href", "") if link_el is not None else ""
            pub = normalize_text(entry.findtext("atom:updated", default="", namespaces=ns), 80)
            content = normalize_text(entry.findtext("atom:summary", default="", namespaces=ns) or entry.findtext("atom:content", default="", namespaces=ns), 1000)
            if title:
                items.append({
                    "source": source,
                    "source_type": source_type,
                    "title": title,
                    "link": link,
                    "published_at": pub,
                    "content": content,
                    "source_url": source_url,
                    "evidence_level": "S" if source_type in ["policy", "exchange", "cls"] else "A",
                })
    except Exception as e:
        # Regex fallback; useful if RSS has invalid XML chars
        logger.warning("RSS parse fallback source=%s err=%s", source, e)
        chunks = re.findall(r"<item[\s\S]*?</item>", xml_text, flags=re.I)[:limit]
        for ch in chunks:
            def tag(t: str) -> str:
                m = re.search(rf"<{t}[^>]*>([\s\S]*?)</{t}>", ch, flags=re.I)
                return normalize_text(m.group(1), 1000) if m else ""
            title = tag("title")
            if title:
                items.append({
                    "source": source,
                    "source_type": source_type,
                    "title": title,
                    "link": tag("link"),
                    "published_at": tag("pubDate"),
                    "content": tag("description"),
                    "source_url": source_url,
                    "evidence_level": "S" if source_type in ["policy", "exchange", "cls"] else "A",
                })
    return items[:limit]


def build_rss_routes(keywords: List[str], base: str) -> List[Dict[str, str]]:
    routes = [
        ("财联社-全部电报", "cls", "/cls/telegraph"),
        ("财联社-港股", "cls", "/cls/telegraph/hk"),
        ("财联社-公司", "cls", "/cls/telegraph/announcement"),
        ("财联社-解读", "cls", "/cls/telegraph/explain"),
        ("财联社深度-股市", "cls", "/cls/depth/1003"),
        ("财联社深度-港股", "cls", "/cls/depth/1135"),
        ("财联社深度-公司", "cls", "/cls/depth/1005"),
        ("财联社深度-科创", "cls", "/cls/depth/1111"),
        ("国务院-最新政策", "policy", "/gov/zhengce/zuixin"),
        ("国务院-最新文件", "policy", "/gov/zhengce/wenjian"),
        ("工信部-政策解读", "policy", "/gov/miit/zcjd"),
        ("工信部-政策文件", "policy", "/gov/miit/zcwj"),
        ("上交所-监管问询", "exchange", "/sse/inquire"),
        ("上交所-科创板项目动态", "exchange", "/sse/renewal"),
        ("上交所-上市公司公告", "exchange", "/sse/disclosure"),
        ("深交所-创业板项目动态", "exchange", "/szse/projectdynamic"),
        ("深交所-问询函件", "exchange", "/szse/inquire"),
        ("深交所-最新规则", "exchange", "/szse/rule"),
    ]
    out = [{"name": name, "type": typ, "url": base + path} for name, typ, path in routes]
    # keyword-based 东方财富 search
    for kw in keywords[:6]:
        out.append({"name": f"东方财富搜索-{kw}", "type": "eastmoney", "url": base + "/eastmoney/search/" + quote(kw)})
    return out


def score_item(item: Dict[str, Any], keywords: List[str], user_message: str) -> float:
    text = (item.get("title", "") + " " + item.get("content", "")).lower()
    score = 0.0
    for kw in keywords:
        if not kw:
            continue
        lk = kw.lower()
        if lk in text:
            score += 4
            if lk in item.get("title", "").lower():
                score += 4
    hot_words = [
        "政策", "规划", "试点", "产业链", "国产替代", "卡脖子", "自主可控", "突破", "订单", "中标", "量产", "龙头", "市占率", "核心供应商", "高端装备", "人工智能", "低空经济", "人形机器人", "算力", "芯片", "半导体", "固态电池", "商业航天", "卫星", "数据要素", "脑机接口", "液冷", "光模块", "飞控", "碳纤维"
    ]
    for w in hot_words:
        if w.lower() in text:
            score += 1.5
    level = item.get("evidence_level")
    if level == "S":
        score += 5
    elif level == "A":
        score += 3
    elif level == "B":
        score += 1
    # source boost
    st = item.get("source_type")
    if st in ["policy", "exchange", "cls"]:
        score += 3
    if "公告" in text or "问询" in text:
        score += 2
    return score


def dedupe_and_rank(items: List[Dict[str, Any]], keywords: List[str], user_message: str, limit: int) -> List[Dict[str, Any]]:
    seen = set()
    out = []
    for item in items:
        key_raw = item.get("link") or item.get("title") or json.dumps(item, ensure_ascii=False)[:120]
        key = hashlib.md5(key_raw.encode("utf-8")).hexdigest()
        if key in seen:
            continue
        seen.add(key)
        item["score"] = round(score_item(item, keywords, user_message), 2)
        out.append(item)
    out.sort(key=lambda x: x.get("score", 0), reverse=True)
    return out[:limit]


async def fetch_rss_bundle(client: httpx.AsyncClient, keywords: List[str], base: str, user_message: str, max_items: int) -> Dict[str, List[Dict[str, Any]]]:
    routes = build_rss_routes(keywords, base)
    tasks = [safe_http_get(client, r["url"], headers={"User-Agent": "Mozilla/5.0 astock-agent/1.0"}) for r in routes]
    texts = await asyncio.gather(*tasks)
    grouped = {"cls_news": [], "policy": [], "exchange_announcements": [], "eastmoney": []}
    for route, xml in zip(routes, texts):
        parsed = parse_rss(xml, route["name"], route["type"], route["url"], limit=MAX_RSS_PER_SOURCE)
        if route["type"] == "cls":
            grouped["cls_news"].extend(parsed)
        elif route["type"] == "policy":
            grouped["policy"].extend(parsed)
        elif route["type"] == "exchange":
            grouped["exchange_announcements"].extend(parsed)
        elif route["type"] == "eastmoney":
            grouped["eastmoney"].extend(parsed)
    for k in grouped:
        grouped[k] = dedupe_and_rank(grouped[k], keywords, user_message, max(8, max_items // 4))
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
            "title": normalize_text(r.get("title"), 200),
            "link": r.get("url", ""),
            "content": normalize_text(r.get("content"), 1200),
            "published_at": r.get("published_date") or "",
            "evidence_level": "B",
        })
    if data.get("answer"):
        results.insert(0, {
            "source": "Tavily-answer",
            "source_type": "ai_search_summary",
            "title": "Tavily 综合摘要",
            "link": "",
            "content": normalize_text(data.get("answer"), 1500),
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
            "title": normalize_text(r.get("title"), 200),
            "link": r.get("link", ""),
            "content": normalize_text(r.get("snippet"), 1000),
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
            "title": normalize_text(r.get("title"), 200),
            "link": r.get("url", ""),
            "content": normalize_text(r.get("text") or r.get("summary"), 1000),
            "published_at": r.get("publishedDate") or "",
            "evidence_level": "B",
        })
    return results


async def run_ai_searches(client: httpx.AsyncClient, keywords: List[str], user_message: str, req: ResearchRequest) -> Dict[str, List[Dict[str, Any]]]:
    q = f"{user_message}\n关键词：{' '.join(keywords[:8])}\nA股 港股 产业链 核心供应商 卡脖子 国产替代 高市占率 公司公告 最新政策"
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
    # Keep only useful columns if present
    preferred = [
        "代码", "名称", "最新价", "涨跌幅", "成交额", "总市值", "流通市值", "市盈率-动态", "市净率", "换手率", "板块名称", "序号", "股票代码", "股票简称"
    ]
    cols = [c for c in preferred if c in df.columns]
    if cols:
        df = df[cols + [c for c in df.columns if c not in cols][:8]]
    return df.head(limit).to_dict(orient="records")


def akshare_bundle(keywords: List[str]) -> Dict[str, Any]:
    if ak is None:
        return {"available": False, "error": "akshare is not installed or import failed", "concept_stocks": [], "market_snapshot": []}
    key = make_cache_key("akshare", {"keywords": keywords[:8]})
    cached = cache_get(key)
    if cached is not None:
        return cached
    result = {"available": True, "concept_matches": [], "concept_stocks": [], "market_snapshot": []}
    try:
        # Full market snapshot can be large. Keep top active rows and use concept codes to enrich later.
        try:
            spot = ak.stock_zh_a_spot_em()
            # keep high turnover / high move context
            for col in ["成交额", "涨跌幅"]:
                if col in spot.columns:
                    spot[col] = pd.to_numeric(spot[col], errors="coerce")
            if "成交额" in spot.columns:
                spot_sorted = spot.sort_values("成交额", ascending=False)
            else:
                spot_sorted = spot
            result["market_snapshot"] = df_records(spot_sorted, 80)
        except Exception as e:
            result["market_snapshot_error"] = str(e)

        try:
            concepts = ak.stock_board_concept_name_em()
            concepts_str = concepts.astype(str)
            matched_rows = []
            seen_names = set()
            for kw in keywords[:8]:
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


def build_structured_context(req: ResearchRequest, keywords: List[str], rss: Dict[str, Any], search: Dict[str, Any], ak_data: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "data_time": now_iso(),
        "evidence_rules": {
            "S": "n8n抓取的官方/专业数据：政策源、交易所公告、财联社、AkShare行情/概念/财务",
            "A": "n8n抓取的权威财经媒体或东方财富等公开财经源",
            "B": "Tavily/Serper/Exa/豆包web_search等联网公开网页补充",
            "C": "模型自身历史知识，仅能作为背景，不能作为事实依据",
            "conflict_rule": "S级 > A级 > B级 > C级；冲突时以更高等级证据为准，低等级信息标记为待验证或冲突线索",
        },
        "user_message": req.user_message,
        "intent": req.intent,
        "keywords": keywords,
        "professional_sources": {
            "policy": rss.get("policy", []),
            "cls_news": rss.get("cls_news", []),
            "exchange_announcements": rss.get("exchange_announcements", []),
            "eastmoney": rss.get("eastmoney", []),
            "akshare": ak_data,
        },
        "search_sources": search,
        "source_health": {
            "rsshub_base": req.rsshub_base or DEFAULT_RSSHUB_BASE,
            "akshare_available": bool(ak_data.get("available")),
            "tavily_enabled": bool(TAVILY_API_KEY and req.enable_tavily),
            "serper_enabled": bool(SERPER_API_KEY and req.enable_serper),
            "exa_enabled": bool(EXA_API_KEY and req.enable_exa),
        }
    }


@app.get("/health")
async def health():
    return {"status": "ok", "version": APP_VERSION, "time": now_iso(), "akshare_available": ak is not None}


@app.post("/research")
async def research(req: ResearchRequest):
    if not req.user_message.strip():
        raise HTTPException(status_code=400, detail="user_message is required")
    keywords = extract_keywords(req.user_message, req.intent)
    base = (req.rsshub_base or DEFAULT_RSSHUB_BASE).rstrip("/")
    async with httpx.AsyncClient() as client:
        rss_task = fetch_rss_bundle(client, keywords, base, req.user_message, req.max_items)
        search_task = run_ai_searches(client, keywords, req.user_message, req)
        rss, search = await asyncio.gather(rss_task, search_task)
    ak_data = akshare_bundle(keywords) if req.enable_akshare else {"available": False, "disabled": True}
    structured_context = build_structured_context(req, keywords, rss, search, ak_data)
    # Build token-friendly summary string for n8n/LLM input
    compact = {
        "data_time": structured_context["data_time"],
        "keywords": keywords,
        "policy_top": rss.get("policy", [])[:8],
        "cls_top": rss.get("cls_news", [])[:12],
        "exchange_top": rss.get("exchange_announcements", [])[:8],
        "eastmoney_top": rss.get("eastmoney", [])[:8],
        "concept_matches": ak_data.get("concept_matches", [])[:10] if isinstance(ak_data, dict) else [],
        "concept_stocks_top": ak_data.get("concept_stocks", [])[:80] if isinstance(ak_data, dict) else [],
        "market_snapshot_top": ak_data.get("market_snapshot", [])[:40] if isinstance(ak_data, dict) else [],
        "search_top": {
            "tavily": search.get("tavily", [])[:6],
            "serper": search.get("serper", [])[:6],
            "exa": search.get("exa", [])[:6],
        },
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

import json, uuid

def nid(): return str(uuid.uuid4())

normalize_js = r'''
function getEnv(name, fallback = '') {
  try {
    if (typeof $env !== 'undefined' && $env && $env[name]) return $env[name];
  } catch (e) {}
  try {
    if (typeof process !== 'undefined' && process.env && process.env[name]) return process.env[name];
  } catch (e) {}
  return fallback;
}

const input = $input.first().json;
const body = input.body || input;
const headers = input.headers || {};

function clean(s) {
  return String(s || '').replace(/\s+/g, ' ').trim();
}

let userMessage = '';
if (body.text && typeof body.text === 'object' && body.text.content) userMessage = body.text.content;
else if (body.text && typeof body.text === 'string') userMessage = body.text;
else if (body.content) userMessage = body.content;
else if (body.msg) userMessage = body.msg;
else if (body.message) userMessage = body.message;
else if (body.prompt) userMessage = body.prompt;
else userMessage = JSON.stringify(body).slice(0, 2000);

// 去掉 @机器人 的噪声
userMessage = clean(userMessage.replace(/@[^\s]+/g, ''));

const requestId = `${Date.now()}_${Math.random().toString(16).slice(2)}`;
const sessionWebhook = body.sessionWebhook || body.sessionWebhookUrl || '';
const fallbackWebhook = getEnv('DINGTALK_WEBHOOK_URL', '');
const replyWebhook = sessionWebhook || fallbackWebhook;

return [{
  json: {
    request_id: requestId,
    user_message: userMessage,
    sender_nick: body.senderNick || body.senderStaffId || body.senderId || '',
    conversation_id: body.conversationId || '',
    session_webhook: sessionWebhook,
    reply_webhook: replyWebhook,
    raw_body: body,
    raw_headers: headers,
    config: {
      ark_base_url: getEnv('ARK_BASE_URL', 'https://ark.cn-beijing.volces.com'),
      ark_api_key: getEnv('ARK_API_KEY', ''),
      doubao_model: getEnv('DOUBAO_MODEL', 'doubao-seed-2-0-pro-260215'),
      research_api_base: getEnv('RESEARCH_API_BASE', ''),
      enable_doubao_web_search: getEnv('ENABLE_DOUBAO_WEB_SEARCH', 'true') !== 'false'
    }
  }
}];
'''

ack_body_expr = "={{ JSON.stringify({ msgtype: 'text', text: { content: '收到，已进入A股/港股投研分析流程。这个过程会抓取财联社、政策、公告、行情、搜索结果，稍等我推送完整报告。' } }) }}"

intent_body_expr = r'''={{ JSON.stringify({
  model: $json.config.doubao_model,
  stream: false,
  input: [
    {
      role: 'system',
      content: [{
        type: 'input_text',
        text: `你是A股/港股投研Agent的“意图解析器”。只输出JSON，不要输出Markdown。
任务：从用户消息中提取主题、关键词、产业链环节、需要抓取的数据源、是否涉及A股/港股/政策/公司公告/实时行情。
要求：
1. keywords 给出 5-12 个短关键词。
2. industry_chain 按上游/中游/下游/应用端或技术环节输出。
3. data_need 只能从 policy, cls_news, exchange_announcement, eastmoney, akshare_market, akshare_concept, ai_search, web_search 中选择。
4. 不要编造股票代码。`
      }]
    },
    {
      role: 'user',
      content: [{ type: 'input_text', text: $json.user_message }]
    }
  ]
}) }}'''

parse_intent_js = r'''
const original = $node['Normalize DingTalk Input'].json;
const res = $input.first().json;

function extractText(obj) {
  if (!obj) return '';
  if (typeof obj.output_text === 'string') return obj.output_text;
  if (Array.isArray(obj.output)) {
    let parts = [];
    for (const o of obj.output) {
      if (Array.isArray(o.content)) {
        for (const c of o.content) {
          if (typeof c.text === 'string') parts.push(c.text);
          if (typeof c.output_text === 'string') parts.push(c.output_text);
        }
      }
      if (typeof o.text === 'string') parts.push(o.text);
    }
    if (parts.length) return parts.join('\n');
  }
  if (obj.choices?.[0]?.message?.content) return obj.choices[0].message.content;
  return JSON.stringify(obj);
}

function parseJsonLoose(text) {
  let t = String(text || '').trim();
  t = t.replace(/^```json\s*/i, '').replace(/^```\s*/i, '').replace(/```$/i, '').trim();
  try { return JSON.parse(t); } catch (e) {}
  const m = t.match(/\{[\s\S]*\}/);
  if (m) {
    try { return JSON.parse(m[0]); } catch (e) {}
  }
  return {
    keywords: original.user_message.match(/[\u4e00-\u9fa5A-Za-z0-9]{2,12}/g)?.slice(0, 10) || [],
    industry_chain: [],
    data_need: ['policy','cls_news','exchange_announcement','eastmoney','akshare_market','akshare_concept','ai_search','web_search'],
    parse_warning: 'intent_json_parse_failed',
    raw_text: text.slice(0, 2000)
  };
}

const intentText = extractText(res);
const intent = parseJsonLoose(intentText);
return [{ json: { ...original, intent, intent_raw: intentText } }];
'''

research_body_expr = r'''={{ JSON.stringify({
  request_id: $json.request_id,
  user_message: $json.user_message,
  intent: $json.intent,
  max_items: 80,
  top_k: 40,
  enable_tavily: true,
  enable_serper: true,
  enable_exa: true,
  enable_akshare: true
}) }}'''

merge_research_js = r'''
const original = $node['Parse Intent JSON'].json;
const research = $input.first().json;
return [{ json: { ...original, research } }];
'''

final_body_expr = r'''={{ JSON.stringify({
  model: $json.config.doubao_model,
  stream: false,
  tools: $json.config.enable_doubao_web_search ? [{ type: 'web_search', max_keyword: 4, limit: 10 }] : [],
  input: [
    {
      role: 'system',
      content: [{
        type: 'input_text',
        text: `你是一名资深A股/港股产业链投研分析师，同时也是严谨的事实审稿人。
你的目标：根据用户消息，结合n8n提供的专业数据，生成“研究候选池报告”。

【最高规则：证据优先级】
S级：n8n提供的专业数据，包括政策文件、交易所公告、公司公告、财联社、AkShare行情/概念/财务数据。
A级：n8n提供的东方财富、权威财经媒体等公开财经源。
B级：Tavily/Serper/Exa/豆包web_search等联网公开网页补充。
C级：你的历史知识，只能作为背景。
当S/A级数据与web_search冲突时，必须以n8n数据为准，并在“信息冲突与待验证”中说明。

【禁止事项】
1. 禁止编造股票代码、财务数据、市占率、客户、订单、供应链关系。
2. 禁止输出“保证上涨、买入、卖出、目标价”。
3. 如果数据不足，必须写“待验证”，不要强行下结论。
4. 不要把普通网页传闻当成确定事实。

【输出要求】
必须用中文Markdown输出，结构如下：
# A股/港股智能投研候选池报告
## 1. 核心结论
## 2. 事件/政策/消息解读
## 3. 产业链拆解
## 4. 候选股票池表格
表格列：公司名称 | 股票代码 | 市场 | 所属环节 | 核心逻辑 | 证据等级 | 可信度评分(1-5) | 主要风险
## 5. 重点公司深挖
## 6. 信息冲突与待验证
表格列：信息点 | n8n专业数据 | web_search/网页补充 | 是否冲突 | 处理方式
## 7. 风险提示
## 8. 后续跟踪清单

候选池必须优先选择：产业链关键环节、卡脖子技术、国产替代、高市占率、核心材料/设备/零部件/软件/工艺公司。`
      }]
    },
    {
      role: 'user',
      content: [{
        type: 'input_text',
        text: `用户原始问题：\n${$json.user_message}\n\n【第一轮意图解析】\n${JSON.stringify($json.intent, null, 2)}\n\n【n8n专业数据，最高优先级】\n${JSON.stringify($json.research.compact_context || $json.research.structured_context || $json.research, null, 2)}\n\n请必要时使用web_search补充，但最终判断必须服从证据优先级。`
      }]
    }
  ]
}) }}'''

format_final_js = r'''
const res = $input.first().json;
const original = $node['Merge Research Context'].json;

function extractText(obj) {
  if (!obj) return '';
  if (typeof obj.output_text === 'string') return obj.output_text;
  if (Array.isArray(obj.output)) {
    let parts = [];
    for (const o of obj.output) {
      if (Array.isArray(o.content)) {
        for (const c of o.content) {
          if (typeof c.text === 'string') parts.push(c.text);
          if (typeof c.output_text === 'string') parts.push(c.output_text);
        }
      }
      if (typeof o.text === 'string') parts.push(o.text);
    }
    if (parts.length) return parts.join('\n');
  }
  if (obj.choices?.[0]?.message?.content) return obj.choices[0].message.content;
  return JSON.stringify(obj, null, 2);
}

let report = extractText(res).trim();
if (!report) report = '分析失败：模型没有返回有效文本。请检查ARK_API_KEY、模型ID、Responses API权限和Research API。';

// 钉钉Markdown消息长度别太夸张。超过时自动截断，完整结果仍可在n8n执行记录里看。
const maxLen = 14500;
let shown = report;
if (shown.length > maxLen) {
  shown = shown.slice(0, maxLen) + '\n\n---\n内容较长，已截断。完整报告请在 n8n 执行记录查看。';
}

const title = 'A股/港股智能投研报告';
const webhook = original.reply_webhook;
return [{
  json: {
    request_id: original.request_id,
    reply_webhook: webhook,
    report,
    dingtalk_body: {
      msgtype: 'markdown',
      markdown: {
        title,
        text: shown
      }
    }
  }
}];
'''

workflow = {
    "name": "A股港股智能投研Agent - DingTalk + Doubao + n8n + Research API",
    "nodes": [
        {
            "parameters": {"httpMethod": "POST", "path": "ding-astock-agent", "responseMode": "onReceived", "options": {"responseCode": 200, "responseData": "success"}},
            "id": nid(), "name": "DingTalk Webhook", "type": "n8n-nodes-base.webhook", "typeVersion": 2,
            "position": [-980, 0], "webhookId": "ding-astock-agent"
        },
        {
            "parameters": {"jsCode": normalize_js},
            "id": nid(), "name": "Normalize DingTalk Input", "type": "n8n-nodes-base.code", "typeVersion": 2,
            "position": [-760, 0]
        },
        {
            "parameters": {
                "method": "POST",
                "url": "={{$json.reply_webhook}}",
                "sendHeaders": True,
                "headerParameters": {"parameters": [{"name": "Content-Type", "value": "application/json"}]},
                "sendBody": True,
                "specifyBody": "json",
                "jsonBody": ack_body_expr,
                "options": {"timeout": 10000}
            },
            "id": nid(), "name": "DingTalk Ack", "type": "n8n-nodes-base.httpRequest", "typeVersion": 4.2,
            "position": [-540, -140], "continueOnFail": True
        },
        {
            "parameters": {
                "method": "POST",
                "url": "={{$json.config.ark_base_url + '/api/v3/responses'}}",
                "sendHeaders": True,
                "headerParameters": {"parameters": [
                    {"name": "Authorization", "value": "=Bearer {{$json.config.ark_api_key}}"},
                    {"name": "Content-Type", "value": "application/json"}
                ]},
                "sendBody": True,
                "specifyBody": "json",
                "jsonBody": intent_body_expr,
                "options": {"timeout": 60000}
            },
            "id": nid(), "name": "Doubao Intent Extract", "type": "n8n-nodes-base.httpRequest", "typeVersion": 4.2,
            "position": [-540, 80], "retryOnFail": True, "maxTries": 3, "waitBetweenTries": 3000
        },
        {
            "parameters": {"jsCode": parse_intent_js},
            "id": nid(), "name": "Parse Intent JSON", "type": "n8n-nodes-base.code", "typeVersion": 2,
            "position": [-320, 80]
        },
        {
            "parameters": {
                "method": "POST",
                "url": "={{$json.config.research_api_base.replace(/\\/$/, '') + '/research'}}",
                "sendHeaders": True,
                "headerParameters": {"parameters": [{"name": "Content-Type", "value": "application/json"}]},
                "sendBody": True,
                "specifyBody": "json",
                "jsonBody": research_body_expr,
                "options": {"timeout": 180000}
            },
            "id": nid(), "name": "Research API - RSSHub Search AkShare", "type": "n8n-nodes-base.httpRequest", "typeVersion": 4.2,
            "position": [-100, 80], "retryOnFail": True, "maxTries": 2, "waitBetweenTries": 5000
        },
        {
            "parameters": {"jsCode": merge_research_js},
            "id": nid(), "name": "Merge Research Context", "type": "n8n-nodes-base.code", "typeVersion": 2,
            "position": [120, 80]
        },
        {
            "parameters": {
                "method": "POST",
                "url": "={{$json.config.ark_base_url + '/api/v3/responses'}}",
                "sendHeaders": True,
                "headerParameters": {"parameters": [
                    {"name": "Authorization", "value": "=Bearer {{$json.config.ark_api_key}}"},
                    {"name": "Content-Type", "value": "application/json"}
                ]},
                "sendBody": True,
                "specifyBody": "json",
                "jsonBody": final_body_expr,
                "options": {"timeout": 300000}
            },
            "id": nid(), "name": "Doubao Final Report with Web Search", "type": "n8n-nodes-base.httpRequest", "typeVersion": 4.2,
            "position": [340, 80], "retryOnFail": True, "maxTries": 2, "waitBetweenTries": 5000
        },
        {
            "parameters": {"jsCode": format_final_js},
            "id": nid(), "name": "Format DingTalk Markdown", "type": "n8n-nodes-base.code", "typeVersion": 2,
            "position": [560, 80]
        },
        {
            "parameters": {
                "method": "POST",
                "url": "={{$json.reply_webhook}}",
                "sendHeaders": True,
                "headerParameters": {"parameters": [{"name": "Content-Type", "value": "application/json"}]},
                "sendBody": True,
                "specifyBody": "json",
                "jsonBody": "={{ JSON.stringify($json.dingtalk_body) }}",
                "options": {"timeout": 30000}
            },
            "id": nid(), "name": "Send DingTalk Final Report", "type": "n8n-nodes-base.httpRequest", "typeVersion": 4.2,
            "position": [780, 80], "retryOnFail": True, "maxTries": 2, "waitBetweenTries": 3000
        }
    ],
    "connections": {
        "DingTalk Webhook": {"main": [[{"node": "Normalize DingTalk Input", "type": "main", "index": 0}]]},
        "Normalize DingTalk Input": {"main": [[{"node": "DingTalk Ack", "type": "main", "index": 0}, {"node": "Doubao Intent Extract", "type": "main", "index": 0}]]},
        "Doubao Intent Extract": {"main": [[{"node": "Parse Intent JSON", "type": "main", "index": 0}]]},
        "Parse Intent JSON": {"main": [[{"node": "Research API - RSSHub Search AkShare", "type": "main", "index": 0}]]},
        "Research API - RSSHub Search AkShare": {"main": [[{"node": "Merge Research Context", "type": "main", "index": 0}]]},
        "Merge Research Context": {"main": [[{"node": "Doubao Final Report with Web Search", "type": "main", "index": 0}]]},
        "Doubao Final Report with Web Search": {"main": [[{"node": "Format DingTalk Markdown", "type": "main", "index": 0}]]},
        "Format DingTalk Markdown": {"main": [[{"node": "Send DingTalk Final Report", "type": "main", "index": 0}]]}
    },
    "pinData": {},
    "settings": {"executionOrder": "v1", "timezone": "Asia/Shanghai", "saveExecutionProgress": True, "saveDataErrorExecution": "all", "saveDataSuccessExecution": "all"},
    "staticData": None,
    "tags": [],
    "triggerCount": 1,
    "updatedAt": "2026-05-03T00:00:00.000Z",
    "versionId": str(uuid.uuid4())
}

with open('/mnt/data/astock_agent_project/n8n_astock_agent_workflow.json','w', encoding='utf-8') as f:
    json.dump(workflow, f, ensure_ascii=False, indent=2)

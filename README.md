# A股/港股智能投研 Agent：n8n + 豆包 + RSSHub + AkShare + AI Search

## 1. 文件说明

- `n8n_astock_agent_workflow.json`：n8n 可导入工作流。
- `research_api.py`：资料聚合服务，负责 RSSHub、Tavily、Serper、Exa、AkShare。
- `Dockerfile`：Hugging Face Spaces / VPS Docker 部署。
- `docker-compose.yml`：VPS 一键部署示例。
- `requirements.txt`：Python 依赖。

## 2. n8n 环境变量

建议在 n8n 容器或服务器环境变量里配置：

```bash
ARK_API_KEY="你的火山方舟API Key"
ARK_BASE_URL="https://ark.cn-beijing.volces.com"
DOUBAO_MODEL="doubao-seed-2-0-pro-260215"
ENABLE_DOUBAO_WEB_SEARCH="true"
RESEARCH_API_BASE="https://你的Research-API地址"
DINGTALK_WEBHOOK_URL="你的钉钉机器人推送webhook，可选。如果钉钉回调里有sessionWebhook，会优先使用sessionWebhook"
```

## 3. Research API 环境变量

```bash
RSSHUB_BASE="https://rsshub.app"
TAVILY_API_KEY="可选"
SERPER_API_KEY="可选"
EXA_API_KEY="可选"
CACHE_TTL_SECONDS="180"
HTTP_TIMEOUT="12"
LOG_LEVEL="INFO"
```

## 4. 部署 Research API

### VPS / 本地 Docker

```bash
docker compose up -d --build
curl http://你的服务器IP:7860/health
```

### Hugging Face Spaces

1. 新建 Space，选择 Docker。
2. 上传 `research_api.py`、`requirements.txt`、`Dockerfile`。
3. 在 Space Settings → Variables 配置 API Key。
4. 访问：`https://用户名-项目名.hf.space/health`。
5. 将该地址填入 n8n 的 `RESEARCH_API_BASE`。

## 5. n8n 导入

n8n 官方支持 JSON 方式导入/导出工作流。打开 n8n：

```text
Workflow 页面 → 右上角三个点 → Import from File → 选择 n8n_astock_agent_workflow.json
```

然后检查：

1. Webhook URL 是否替换到钉钉机器人回调地址。
2. 环境变量是否生效。
3. `Research API - RSSHub Search AkShare` 节点是否能访问你的 Research API。
4. `Doubao Final Report with Web Search` 是否能正常调用火山方舟 Responses API。

## 6. 设计原则

- n8n 专业数据为主证据。
- 豆包 web_search 为补充证据。
- 冲突时按 S/A/B/C 证据等级处理。
- 输出候选股票池，不输出买卖点、目标价、保证上涨。

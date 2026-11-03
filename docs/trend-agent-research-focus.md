# Agent 关注清单

公开数据分支 `agent-data` 新增 `agent/v1/focus.json`，作为 GPT 日报固定读取入口。
新生成的日报、7日、30日快照同时内嵌 `research_focus`；旧的冻结归档保持原样。
`focus.json` 不依赖股市收盘就能发布，语义内容不变时保留原始 `generated_at`，重复发布不新增提交。
它随现有快照发布流程刷新，不是网页 List/Holds 编辑后的实时推送。
日报原 220 KB 的本地保护门槛调整为 350 KB，周/月由 200 KB 调整为 300 KB；
关注清单独立上限 100 KB。超限仍拒绝发布，不截断候选或降低数据质量门槛。

服务器配置 `data/agent_focus_config.json`，参照 `config/agent_focus.example.json`：
开启 `enabled` 并显式指定唯一账户 `username`。不扫描或合并其他账户。
主账户的 List 来源为 `data/watchlist.json`，其他账户取既有按用户存储的文件。
Holds 只读取与该账户 UI 相同 chat_id 的 ACTIVE 行，不导出平仓记录。
两类来源按市场和规范化代码合并，导出中不标记来源、持仓状态、数量或各来源支数。

字段只允许市场、代码、公司名、资产类型、行业、主营业务、产品和资料时间/质量。
用户名、chat_id、成本、仓位、盈亏、交易日期、自由文本理由/标签均不导出。
公司简介从本地资料表读取，不触发网络或 LLM；缺失保留 null，历史简介不视为最新消息。
缺少来源和真实空清单分开标记，不把抓取失败显示为“没有关注标的”。

可单独发布：

```sh
python3 scripts/publish_agent_snapshot.py --root /root/Trade_db --repo /root/trend_agent_public --focus-only
```

GPT 日报建议补充指令：

> 先读取 jaydu08/Trade_db 的 agent-data 分支 agent/v1/focus.json；若该次市场快照
> 带 research_focus，按 generated_at 使用更新的一份，避免重复处理。对 items 中
> 的美股、港股、A股检索最新重大变动，遵守 research_instructions，统一中文、附
> 来源及披露/生效时间。优先报告增发、减持、解禁、业绩/指引、管理层变动、并购、
> 监管和指数调整风险；仅报告重大新事件和实质进展。清单不代表真实持仓。
> 对照本任务可访问的近期日报合并转载；无法读取历史时说明去重范围，不宣称
> 已与 TG 或其他任务完成去重。清单为空时明确说明，不擅自换成其他用户的清单。

该版本不建立资讯采集、模型分析或 TG 推送任务。`recent_news_dedup.status=not_available`
说明尚无导出的跨报告/TG事件送达台账。定时研究仍需使用该聊天实际可访问的搜索和历史输出。

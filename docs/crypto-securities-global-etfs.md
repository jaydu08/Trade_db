# 股票类加密资产与全球ETF扩展

币安现货与合约页新增资产分组：加密、股票/ETF、商品/汇率、全部。
加密、证券各按24小时成交额独立取前150，商品/汇率另取前30；现有TG信号与推送不改变。
证券组不使用BTC/ETH相对强度筛选。历史不足/陈旧行情仍不进入对应周期上涨榜。

期货接受官方 `PERPETUAL` 与 `TRADIFI_PERPETUAL`，用 exchangeInfo 的 underlyingType
区分 COIN、EQUITY/HK_EQUITY/CN_EQUITY/KR_EQUITY、INDEX、PREMARKET、COMMODITY、FX。
bStocks 来自币安公开网页产品目录 `get-products` 的准确交易对 `bStocks` 标签，
不按B后缀或股票式ticker猜测。目录不可用且无已取目录时拒绝未核实分类。
ETF身份优先产品名称及已核实ETF代码；未细分基金的传统资产保留“证券永续”标签。
上市前资产标明上市前，不能视为已上市股票或现股所有权。

“现货”标记查完整TRADING USDT现货目录，不依赖本榜前150；股票永续关联 bStocks
时显示“bStock现货”，两者是不同产品。明确排除同名加密币冒充股票现货。
原加密Agent导出仍使用独立的crypto_selected_symbols，避免证券挤占原加密候选池。

全球方向性池由10扩到58：美股11行业、医药/半导体/软件/安全/军工/能源等主题，
全球区域宽基，10个A股ETF，7个港股上市ETF。上市地与投资敞口分开标注。
“当日”是最近完整交易日相对前一个交易日的未复权收盘涨跌，不是盘中涨跌，也不是总回报。
3/7/30日继续自然日锚定。A/H与美国按各自时区排除尚未结束交易日。
单一行情刷新失败保留该标的旧值并标记待更新，不刷新其观察日期。

来源：

- https://www.binance.com/en/academy/articles/how-to-trade-stock-perpetual-contracts-on-binance
- https://www.binance.com/en/support/faq/detail/f0c03cd6509a4085b4cce1636f16be38
- https://www.ssga.com/us/en/individual/capabilities/equities/sector-investing/sector-and-industry-etfs
- https://www.ishares.com/us/products/etf-investments
- https://www.globalxetfs.com.hk/content/files/25-01-FundMap_GXHK_A4_EN.pdf
- 日线行情：新浪，经AkShare现有API；A股/港股上市ETF使用本地市场日线。

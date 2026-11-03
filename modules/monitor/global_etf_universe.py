"""Directional ETF identities. Listing currency is distinct from exposure region."""
SPDR = 'https://www.ssga.com/us/en/individual/capabilities/equities/sector-investing/sector-and-industry-etfs'
ISHARES = 'https://www.ishares.com/us/products/etf-investments'
CN_FUND = 'https://fundf10.eastmoney.com/{symbol}.html'
GXHK = 'https://www.globalxetfs.com.hk/funds/'

EXTRA_GLOBAL_ASSETS = {}
ASSET_META = {}


def add(symbol, name, group, market='US', url=ISHARES, kind='ETF 代理'):
    key = symbol if market == 'US' else market + '.' + symbol
    EXTRA_GLOBAL_ASSETS[key] = (name, group, kind, url)
    ASSET_META[key] = dict(listing_market=market, currency={'US': 'USD', 'CN': 'CNY', 'HK': 'HKD'}[market], symbol=symbol)


for symbol, name in [('XLK', '美国科技'), ('XLF', '美国金融'), ('XLV', '美国医疗保健'),
                     ('XLI', '美国工业'), ('XLY', '美国可选消费'), ('XLP', '美国必需消费'),
                     ('XLB', '美国材料'), ('XLU', '美国公用事业'), ('XLRE', '美国房地产'),
                     ('XLC', '美国通信服务')]:
    add(symbol, name, '美股板块', url=SPDR)
for symbol, name, url in [
    ('SMH', '半导体产业链', 'https://www.vaneck.com/us/en/investments/semiconductor-etf-smh/'),
    ('IGV', '美国软件', ISHARES), ('CIBR', '网络安全', 'https://www.ftportfolios.com/'),
    ('ITA', '美国航空航天与国防', ISHARES), ('ICLN', '全球清洁能源', ISHARES),
    ('TAN', '全球太阳能', 'https://www.invesco.com/us/financial-products/etfs'),
    ('URNM', '铀矿产业', 'https://sprottetfs.com/urnm-sprott-uranium-miners-etf/')]:
    add(symbol, name, '美股主题', url=url)
for symbol, name in [('SPY', '标普500'), ('QQQ', '纳斯达克100'), ('IWM', '美国小盘股'),
                     ('ACWI', '全球股票'), ('VGK', '欧洲'), ('EWU', '英国'),
                     ('EWY', '韩国'), ('EWT', '中国台湾'), ('EWA', '澳大利亚'),
                     ('EWC', '加拿大'), ('EWW', '墨西哥'), ('EIDO', '印度尼西亚'),
                     ('THD', '泰国'), ('EZA', '南非')]:
    add(symbol, name, '全球区域', url='https://investor.vanguard.com/investment-products/etfs' if symbol == 'VGK' else ISHARES)
for symbol, name in [('510300', '沪深300'), ('510500', '中证500'), ('159915', '创业板'),
                     ('588000', '科创50'), ('512480', 'A股半导体'), ('512010', 'A股医药'),
                     ('512660', 'A股军工'), ('512800', 'A股银行'), ('515030', '新能源车'),
                     ('515790', '光伏产业')]:
    add(symbol, name, 'A股板块', 'CN', CN_FUND.format(symbol=symbol), '本地ETF')
for symbol, name, url in [
    ('02800', '恒生指数', 'https://www.trahk.com.hk/'),
    ('02828', '恒生中国企业', 'https://www.hangsenginvestment.com/'),
    ('03033', '恒生科技', 'https://www.csopasset.com/'),
    ('03191', '中国半导体', GXHK + 'global-x-china-semiconductor-etf/'),
    ('02845', '中国电动车及电池', GXHK + 'global-x-china-electric-vehicle-and-battery-etf/'),
    ('02820', '中国生物科技', GXHK + 'global-x-china-biotech-etf/'),
    ('02807', '中国机器人与人工智能', GXHK + 'global-x-china-robotics-and-ai-etf/')]:
    add(symbol, name, '港股上市ETF', 'HK', url, '本地ETF')

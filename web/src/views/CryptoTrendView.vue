<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref, watch } from 'vue'
import api from '../api'

type Market = 'spot' | 'futures' | 'onchain'
type SortKey = 'period_change' | 'price' | 'volume_24h' | 'relative_change_percent' | 'hourly_volume_ratio' | 'depth_impact_percent' | 'oi_change_percent' | 'oi_notional' | 'funding_rate_percent' | 'liquidity' | 'market_cap' | 'fdv' | 'age_hours' | 'updated'
const market = ref<Market>('spot')
const period = ref('12h')
const query = ref('')
const pushed = ref(false)
const sortKey = ref<SortKey>('period_change')
const sortDirection = ref<'asc' | 'desc'>('desc')
const loading = ref(false)
const error = ref('')
const payload = ref<any>({ items: [], review_candidates: [], health: {}, counts: {} })
const selected = ref<any>(null)
const page = ref(1)
const chain = ref('all')
const periods = computed(() => market.value === 'onchain' ? ['24h', '3d', '7d', '30d'] : ['12h', '3d', '7d', '30d'])
const labels: Record<string, string> = { '12h': '12小时', '24h': '24小时', '3d': '3天', '7d': '7天', '30d': '30天' }
const risks: Record<string, string> = { pass: '筛查通过', review: '需复核', unknown: '待核验', blocked: '已拦截', all: '全部' }
const sortLabels: Record<SortKey, string> = { period_change: '周期涨幅', price: '现价', volume_24h: '24h成交额', relative_change_percent: '相对强度', hourly_volume_ratio: '近1h放量', depth_impact_percent: '$5k盘口冲击', oi_change_percent: 'OI数量变化', oi_notional: 'OI金额', funding_rate_percent: '费率', liquidity: '流动性', market_cap: '市值', fdv: 'FDV', age_hours: '交易池年龄', updated: '行情更新时间' }
function sortHeader(key: SortKey) {
  if (sortKey.value === key) sortDirection.value = sortDirection.value === 'desc' ? 'asc' : 'desc'
  else { sortKey.value = key; sortDirection.value = key === 'depth_impact_percent' ? 'asc' : 'desc' }
  page.value = 1
}
function sortIcon(key: SortKey): string { return sortKey.value === key ? sortDirection.value === 'desc' ? ' ↓' : ' ↑' : ' ↕' }
const rows = computed(() => {
  const result = (payload.value.items || []).filter((r: any) =>
    (chain.value === 'all' || r.chain === chain.value) && !r.stale && r.history_available && Number(r.period_change) > 0)
  return [...result].sort((a: any, b: any) => {
    const av = a[sortKey.value] == null ? null : Number(a[sortKey.value])
    const bv = b[sortKey.value] == null ? null : Number(b[sortKey.value])
    if (av == null || !Number.isFinite(av)) return bv == null || !Number.isFinite(bv) ? b.period_change - a.period_change : 1
    if (bv == null) return -1
    if (!Number.isFinite(bv)) return -1
    return (sortDirection.value === 'desc' ? bv - av : av - bv) || b.period_change - a.period_change || a.symbol.localeCompare(b.symbol)
  })
})
const visible = computed(() => rows.value.slice((page.value - 1) * 25, page.value * 25).map((r: any, i: number) => ({ ...r, display_rank: (page.value - 1) * 25 + i + 1 })))
const reviewRows = computed(() => (payload.value.review_candidates || []).filter((r: any) => chain.value === 'all' || r.chain === chain.value).slice(0, 10))
const chains = computed(() => [...new Set<string>([...(payload.value.items || []), ...(payload.value.review_candidates || [])].map((r: any) => r.chain).filter(Boolean))])
let timer: ReturnType<typeof setInterval> | undefined
let serial = 0
async function fetchData() {
  const request = ++serial
  loading.value = true
  error.value = ''
  try {
    const res = await api.get('/crypto/trend', { params: { market: market.value, period: period.value, risk: 'pass', q: query.value, pushed_only: market.value === 'onchain' ? false : pushed.value } })
    if (request === serial) payload.value = res.data
  } catch (e: any) {
    if (request === serial) error.value = e?.response?.data?.detail || '数据读取失败，显示的旧数据不可视为最新结果'
  } finally { if (request === serial) loading.value = false }
}
watch(market, () => { period.value = market.value === 'onchain' ? '24h' : '12h'; chain.value = 'all'; selected.value = null; pushed.value = false; sortKey.value = 'period_change'; sortDirection.value = 'desc' })
watch([market, period, pushed], () => { page.value = 1; payload.value = { items: [], review_candidates: [], health: {}, counts: {} }; fetchData() })
watch(chain, () => { page.value = 1 })
watch(rows, (current) => { page.value = Math.min(page.value, Math.max(1, Math.ceil(current.length / 25))) })
onMounted(() => { fetchData(); timer = setInterval(fetchData, 60000) })
onUnmounted(() => { serial++; if (timer) clearInterval(timer) })
function money(v: any): string {
  if (v === null || v === undefined) return '—'
  const n = Number(v)
  return Math.abs(n) >= 1e9 ? '$' + (n / 1e9).toFixed(2) + 'B' : Math.abs(n) >= 1e6 ? '$' + (n / 1e6).toFixed(2) + 'M' : '$' + n.toLocaleString('en-US', { maximumSignificantDigits: 7 })
}
function pct(v: any): string { return v === null || v === undefined ? '—' : (v >= 0 ? '+' : '') + Number(v).toFixed(2) + '%' }
function ratio(v: any): string { return v === null || v === undefined ? '—' : Number(v).toFixed(2) + '×' }
function timeText(v: any): string { return v ? new Date(Number(v) * 1000).toLocaleString('zh-CN', { hour12: false }) : '—' }
function ageText(v: any): string {
  if (v == null || !Number.isFinite(Number(v))) return '—'
  const hours = Number(v)
  return hours < 24 ? hours.toFixed(1) + '小时' : (hours / 24).toFixed(1) + '天'
}
function url(r: any): string {
  return r.market === 'onchain' ? 'https://dexscreener.com/' + encodeURIComponent(r.chain) + '/' + encodeURIComponent(r.pair || r.address)
    : r.market === 'futures' ? 'https://www.binance.com/en/futures/' + encodeURIComponent(r.symbol)
    : 'https://www.binance.com/en/trade/' + encodeURIComponent(r.symbol.replace(/USDT$/, '_USDT'))
}
</script>

<template>
  <section class="crypto-view">
    <div class="intro"><div><h2>Crypto Trend</h2><p>全天候行情 · 多周期比较 · 合约风险证据</p></div><router-link to="/trend">股票 Trend ↗</router-link></div>
    <div class="controls">
      <div class="segments"><button v-for="m in (['spot', 'futures', 'onchain'] as Market[])" :key="m" :class="{ active: market === m }" @click="market = m">{{ { spot: '币安现货', futures: '币安合约', onchain: '链上' }[m] }}</button></div>
      <div class="segments period"><button v-for="p in periods" :key="p" :class="{ active: period === p }" @click="period = p">{{ labels[p] }}</button></div>
      <button @click="fetchData" :disabled="loading">{{ loading ? '读取中…' : '刷新' }}</button>
    </div>
    <div class="status-row"><span v-for="(h, name) in payload.health" :key="name" :class="{ bad: h.stale || h.status !== 'ok' }">{{ name }} · {{ h.stale ? '数据过期' : h.status }} · {{ timeText(h.updated) }}<template v-if="h.count !== undefined"> · {{ h.count }}/{{ h.available ?? h.count }}</template></span></div>
    <div v-if="error" class="error">{{ error }}</div>
    <div class="notice">{{ payload.warning }}<span v-if="market === 'onchain'"> 主榜仅显示筛查通过且上涨的标的；下方至多展示 10 个有交易和流动性基础、但仍需复核的候选。复核分数只表示关注优先级，不表示合约安全。年龄与 TG 一样按交易池创建时间计算，不是合约部署时间。项目链接为资料线索，未经官方认证。</span><span> 行情榜独立于 TG 告警。</span></div>
    <div class="filters">
      <template v-if="market === 'onchain'"><span class="screening-counts">全库筛查：通过 {{ payload.counts.pass || 0 }} · 需复核 {{ payload.counts.review || 0 }} · 待核验 {{ payload.counts.unknown || 0 }} · 拦截 {{ payload.counts.blocked || 0 }}</span><select v-model="chain"><option value="all">全部链</option><option v-for="c in chains" :key="c">{{ c }}</option></select></template>
      <input v-model="query" placeholder="代码 / 名称 / 合约地址" @keyup.enter="page = 1; fetchData()" aria-label="搜索代码或合约" />
      <button @click="page = 1; fetchData()">搜索</button><label v-if="market !== 'onchain'"><input type="checkbox" v-model="pushed" />仅看曾推送</label>
    </div>
    <p class="count">{{ rows.length }} 个{{ market === 'onchain' ? '筛查通过且' : '' }}上涨标的 · {{ labels[period] }}涨幅 &gt; 0 · 按{{ sortLabels[sortKey] }}{{ sortDirection === 'desc' ? '从高到低' : '从低到高' }}排序 · 点击表头切换 · 历史不足或价格过期不显示 · 页面每分钟刷新</p>
      <div class="table-scroll"><table><thead><tr><th>#</th><th>标的 / 身份</th><th v-if="market === 'onchain'">项目 / 叙事</th><th><button class="sort-header" @click="sortHeader('price')">现价{{ sortIcon('price') }}</button></th><th><button class="sort-header" @click="sortHeader('period_change')">{{ labels[period] }}涨幅{{ sortIcon('period_change') }}</button></th><th><button class="sort-header" @click="sortHeader('volume_24h')">24h成交额{{ sortIcon('volume_24h') }}</button></th><template v-if="market !== 'onchain'"><th><button class="sort-header" @click="sortHeader('relative_change_percent')">相对强度{{ sortIcon('relative_change_percent') }}</button></th><th><button class="sort-header" @click="sortHeader('hourly_volume_ratio')">近1h放量{{ sortIcon('hourly_volume_ratio') }}</button></th><th><button class="sort-header" @click="sortHeader('depth_impact_percent')">$5k盘口冲击{{ sortIcon('depth_impact_percent') }}</button></th></template><template v-if="market === 'futures'"><th><button class="sort-header" @click="sortHeader('oi_change_percent')">OI数量变化{{ sortIcon('oi_change_percent') }}</button></th><th><button class="sort-header" @click="sortHeader('oi_notional')">OI金额{{ sortIcon('oi_notional') }}</button></th><th><button class="sort-header" @click="sortHeader('funding_rate_percent')">费率{{ sortIcon('funding_rate_percent') }}</button></th></template><template v-if="market === 'onchain'"><th><button class="sort-header" @click="sortHeader('liquidity')">流动性{{ sortIcon('liquidity') }}</button></th><th><button class="sort-header" @click="sortHeader('market_cap')">市值{{ sortIcon('market_cap') }}</button> / <button class="sort-header" @click="sortHeader('fdv')">FDV{{ sortIcon('fdv') }}</button></th><th><button class="sort-header" @click="sortHeader('age_hours')">交易池年龄{{ sortIcon('age_hours') }}</button></th><th>合约筛查</th></template><th><template v-if="market !== 'onchain'">告警 / </template><button class="sort-header" @click="sortHeader('updated')">更新时间{{ sortIcon('updated') }}</button></th></tr></thead>
      <tbody><tr v-for="r in visible" :key="r.id"><td>{{ r.display_rank ?? '—' }}</td><td><a :href="url(r)" target="_blank" rel="noopener noreferrer">{{ r.symbol }} ↗</a><small>{{ r.name }}</small><template v-if="r.address"><small>{{ r.chain }} · {{ r.dex }}</small><code :title="r.address">{{ r.address }}</code><small class="bad" v-if="r.same_name_count > 1">同名 {{ r.same_name_count }} 个合约，请核对 CA</small></template></td><td v-if="market === 'onchain'" class="story"><div>{{ r.narrative || '暂无项目简介' }}</div><small>{{ r.narrative_source || '尚无叙事资料' }} · {{ r.profile_source || '链接未经官方核验' }}</small><div class="story-links"><a v-if="r.website" :href="r.website" target="_blank" rel="noopener noreferrer">网站 ↗</a><a v-if="r.twitter" :href="r.twitter" target="_blank" rel="noopener noreferrer">X ↗</a><span v-if="!r.website && !r.twitter">暂无网站 / X 线索</span></div></td><td>{{ money(r.price) }}<small v-if="r.stale" class="bad">价格已过期</small></td><td :class="r.period_change >= 0 ? 'up' : 'down'">{{ pct(r.period_change) }}<small v-if="!r.history_available">未满周期 / 历史缺失</small><small v-else :title="timeText(r.baseline_at)">{{ r.return_source === 'DexScreener 24h' ? '供应商24h口径' : '小时级历史基准' }}</small></td><td>{{ money(r.volume_24h) }}</td><template v-if="market !== 'onchain'"><td :class="r.relative_change_percent >= 0 ? 'up' : 'down'">{{ pct(r.relative_change_percent) }}<small>同期减 BTC/ETH 等权涨幅</small></td><td>{{ ratio(r.hourly_volume_ratio) }}<small>对比过去14天小时中位数</small></td><td>{{ r.depth_impact_percent == null ? '—' : Number(r.depth_impact_percent).toFixed(2) + '%' }}<small>{{ market === 'spot' ? '现货买入' : '永续卖出' }} · 100档</small></td></template>
      <template v-if="market === 'futures'"><td>{{ pct(r.oi_change_percent) }}<small :title="timeText(r.oi_asof)">数量口径 · 2h采样</small></td><td>{{ money(r.oi_notional) }}</td><td>{{ r.funding_rate_percent == null ? '—' : Number(r.funding_rate_percent).toFixed(4) + '%' }}<small>/ {{ r.funding_interval_hours }}h</small></td></template>
      <template v-if="market === 'onchain'"><td>{{ money(r.liquidity) }}</td><td>{{ money(r.market_cap) }}<small>FDV {{ money(r.fdv) }}</small></td><td>{{ ageText(r.age_hours) }}<small :title="r.pair_created_at">{{ r.age_source || '交易池口径' }} · 非合约年龄</small></td><td><button class="risk" :class="r.security.status" @click="selected = r">{{ risks[r.security.status] }} ⓘ</button><small>{{ timeText(r.security.checked_at) }}</small></td></template>
      <td><template v-if="r.market !== 'onchain'">{{ r.has_alert ? '曾推送' : '无推送记录' }}<small>{{ r.alert_note }}</small><small v-if="r.last_alert_at">告警 {{ timeText(r.last_alert_at) }}</small></template><small>行情 {{ timeText(r.updated) }}</small></td></tr>
      <tr v-if="!visible.length"><td colspan="15" class="empty">{{ loading ? '正在读取…' : market === 'onchain' ? '本周期暂无筛查通过的上涨标的；下方可看限量优先复核候选。' : '本周期暂无符合筛选条件的上涨标的。' }}</td></tr></tbody></table></div>
    <div class="pager"><button :disabled="page <= 1" @click="page--">上一页</button><span>{{ page }} / {{ Math.max(1, Math.ceil(rows.length / 25)) }}</span><button :disabled="page * 25 >= rows.length" @click="page++">下一页</button></div>
    <section v-if="market === 'onchain'" class="review-section">
      <h3>优先复核 <small>最多 10 个 · 当前 {{ payload.review_eligible_count || 0 }} 个达到基础门槛，展示 {{ reviewRows.length }} 个</small></h3>
      <p class="review-explain">仅含以太坊 / Base / BSC，且周期上涨、价格未过期、24h 成交额 ≥ $200k、流动性 ≥ $50k、市值达到各链基础线（ETH $1.5M / Base $300k / BSC $2M）。分数 = 涨幅 30 分 + 成交额 35 分 + 流动性 35 分，各项封顶；仅用于复核排序，不是安全分或买入建议。Solana / Robinhood 等暂不支持同等双源合约检测的 {{ payload.unsupported_count || 0 }} 个待核验标的，不进入候选。</p>
      <div class="table-scroll"><table><thead><tr><th>#</th><th>代币 / 合约</th><th>复核优先级</th><th>{{ labels[period] }}涨幅</th><th>24h成交额</th><th>流动性</th><th>成交额 / 流动性</th><th>市值</th><th>交易池年龄</th><th>筛查依据</th></tr></thead>
        <tbody><tr v-for="(r, i) in reviewRows" :key="r.id"><td>{{ i + 1 }}</td><td><a :href="url(r)" target="_blank" rel="noopener noreferrer">{{ r.symbol }} ↗</a><small>{{ r.chain }} · {{ r.name }}</small><code :title="r.address">{{ r.address }}</code><small v-if="r.same_name_count > 1" class="bad">同名 {{ r.same_name_count }} 个合约</small><div class="story-links"><a v-if="r.website" :href="r.website" target="_blank" rel="noopener noreferrer">网站 ↗</a><a v-if="r.twitter" :href="r.twitter" target="_blank" rel="noopener noreferrer">X ↗</a></div></td><td><strong>{{ r.review_priority.score }} / 100</strong><small>涨幅 {{ r.review_priority.momentum_points }} · 成交 {{ r.review_priority.volume_points }} · 流动性 {{ r.review_priority.liquidity_points }}</small><small>关注优先级，非安全分</small></td><td class="up">{{ pct(r.period_change) }}</td><td>{{ money(r.volume_24h) }}</td><td>{{ money(r.liquidity) }}</td><td :class="{ bad: r.review_priority.turnover_warning }">{{ ratio(r.review_priority.volume_liquidity_ratio) }}<small v-if="r.review_priority.turnover_warning">异常高，需排查刷量</small></td><td>{{ money(r.market_cap) }}</td><td>{{ ageText(r.age_hours) }}<small>{{ r.age_source || '交易池口径' }} · 非合约年龄</small></td><td><button class="risk" :class="r.security.status" @click="selected = r">{{ risks[r.security.status] }} ⓘ</button><small>{{ (r.security.reasons || []).slice(0, 2).join('；') }}</small></td></tr>
        <tr v-if="!reviewRows.length"><td colspan="10" class="empty">当前没有同时满足成交额、流动性、市值和上涨要求的优先复核候选。</td></tr></tbody></table></div>
    </section>
    <div v-if="selected" class="overlay" @click.self="selected = null"><section class="detail" role="dialog" aria-modal="true" aria-label="合约风险证据"><button class="close" @click="selected = null">关闭 ×</button><h3>{{ selected.symbol }} · {{ selected.security.label }}</h3><p>{{ selected.chain }}</p><code>{{ selected.address }}</code><p>定价/检测池：<code>{{ selected.pair }}</code></p><p>检测时间：{{ timeText(selected.security.checked_at) }}；有效至 {{ timeText(selected.security.expires_at) }}</p><ul><li v-for="reason in selected.security.reasons" :key="reason">{{ reason }}</li></ul><p>持有人卖出样本：成功 {{ selected.security.holder_successful ?? '未知' }} / 失败 {{ selected.security.holder_failed ?? '未知' }}</p><p>买税 {{ selected.security.buy_tax_percent ?? '未知' }}% · 卖税 {{ selected.security.sell_tax_percent ?? '未知' }}%</p><p>{{ selected.security.identity }}</p><p>{{ selected.security.coverage }}</p><p class="notice">{{ selected.security.limitations }}。管理员可在检测后更改条件；一次模拟成功不能保证所有钱包或金额都能卖出。</p><a href="https://docs.honeypot.is/ishoneypot" target="_blank" rel="noopener noreferrer">检测口径说明 ↗</a></section></div>
  </section>
</template>

<style scoped>
.crypto-view{color:var(--text);max-width:1600px}.intro{display:flex;align-items:center;justify-content:space-between;margin-bottom:24px}.intro h2{font-size:26px;margin:0 0 8px}.intro p,.count{color:#787873;font-size:13px}.controls,.filters,.segments,.pager{display:flex;align-items:center;gap:8px;flex-wrap:wrap}.controls{margin-bottom:16px;gap:20px}button,select,input{font:inherit;font-size:13px;border:1px solid #ddded8;background:#fff;padding:8px 12px;border-radius:7px}button{cursor:pointer}button:disabled{opacity:.45;cursor:default}button.active{background:#222b25;color:white;border-color:#222b25}.period button.active{background:#edf1ec;color:#24332a;border-color:#b6c4b5}.filters{margin:18px 0}.filters label{font-size:13px;display:flex;align-items:center;gap:5px}.status-row{display:flex;flex-wrap:wrap;gap:8px;font-size:11px;color:#60715e}.status-row span{background:#f1f4ef;border-radius:5px;padding:6px}.notice{font-size:12px;line-height:1.7;padding:12px;background:#f6f4ed;border-radius:7px;margin-top:12px;color:#71664c}.error{padding:12px;background:#fff0ef;color:#a53731}.table-scroll{overflow-x:auto;border:1px solid #e4e5df;border-radius:9px}table{width:100%;border-collapse:collapse;font-size:13px;white-space:nowrap}th{text-align:left;background:#f5f6f2;color:#73756c;font-size:12px;font-weight:500;padding:13px}.sort-header{border:0;background:transparent;padding:0;color:inherit;font-size:inherit;font-weight:inherit;white-space:nowrap}.sort-header:hover{color:#24533c;text-decoration:underline}.sort-header:focus-visible{outline:2px solid #24533c;outline-offset:3px}td{border-top:1px solid #eee;padding:13px;vertical-align:top}td.story{min-width:230px;max-width:290px;white-space:normal;line-height:1.5}.story-links{display:flex;gap:12px;margin-top:7px;font-size:12px}.story-links span{color:#85877e}small{display:block;font-size:11px;color:#85877e;margin-top:5px}code{font-size:10px;display:block;max-width:230px;overflow:hidden;text-overflow:ellipsis;margin-top:5px}a{color:#24533c;text-decoration:none;font-weight:600}.up{color:#258158}.down,.bad{color:#b84742!important}.risk{padding:5px 8px;font-size:12px}.risk.pass{color:#2e7851;background:#edf7ed}.risk.blocked{color:#ad3131;background:#fff0ef}.risk.review{color:#986300;background:#fff7e6}.empty{text-align:center;padding:45px 20px;color:#797c73;white-space:normal}.pager{justify-content:flex-end;margin-top:15px;font-size:12px}.overlay{position:fixed;inset:0;background:#0005;z-index:100;display:flex;align-items:center;justify-content:center;padding:20px}.detail{background:white;border-radius:12px;padding:28px;width:620px;max-height:85vh;overflow:auto;line-height:1.6}.detail code{max-width:100%;word-break:break-all;white-space:normal}.close{float:right}.detail li{margin-bottom:7px;font-size:13px}@media(max-width:700px){.intro{align-items:flex-start}.intro h2{font-size:21px}.controls{gap:10px}.detail{padding:18px}td,th{padding:10px}}
.screening-counts{font-size:12px;color:#647164;margin-right:6px}.review-section{margin-top:34px}.review-section h3{font-size:18px;margin:0 0 9px}.review-section h3 small{display:inline;font-weight:400;margin-left:10px}.review-explain{font-size:12px;line-height:1.65;color:#71664c;margin:0 0 14px;max-width:1150px}.review-section td{white-space:normal;min-width:86px}.review-section td:nth-child(2){min-width:185px;max-width:235px}.review-section td:nth-child(3){min-width:190px}.review-section td:last-child{min-width:160px;max-width:250px}.review-section strong{font-size:16px;color:#24533c}
</style>

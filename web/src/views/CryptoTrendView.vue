<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref, watch } from 'vue'
import api from '../api'

type Market = 'spot' | 'futures' | 'onchain'
const market = ref<Market>('spot')
const period = ref('12h')
const risk = ref('pass')
const query = ref('')
const pushed = ref(false)
const loading = ref(false)
const error = ref('')
const payload = ref<any>({ items: [], health: {}, counts: {} })
const selected = ref<any>(null)
const page = ref(1)
const chain = ref('all')
const periods = computed(() => market.value === 'onchain' ? ['24h', '3d', '7d', '30d'] : ['12h', '3d', '7d', '30d'])
const labels: Record<string, string> = { '12h': '12小时', '24h': '24小时', '3d': '3天', '7d': '7天', '30d': '30天' }
const risks: Record<string, string> = { pass: '筛查通过', review: '需复核', unknown: '待核验', blocked: '已拦截', all: '全部' }
const rows = computed(() => (payload.value.items || []).filter((r: any) => chain.value === 'all' || r.chain === chain.value))
const visible = computed(() => rows.value.slice((page.value - 1) * 25, page.value * 25))
const chains = computed(() => [...new Set<string>((payload.value.items || []).map((r: any) => r.chain).filter(Boolean))])
let timer: ReturnType<typeof setInterval> | undefined
let serial = 0
async function fetchData() {
  const request = ++serial
  loading.value = true
  error.value = ''
  try {
    const res = await api.get('/crypto/trend', { params: { market: market.value, period: period.value, risk: risk.value, q: query.value, pushed_only: pushed.value } })
    if (request === serial) payload.value = res.data
  } catch (e: any) {
    if (request === serial) error.value = e?.response?.data?.detail || '数据读取失败，显示的旧数据不可视为最新结果'
  } finally { if (request === serial) loading.value = false }
}
watch(market, () => { period.value = market.value === 'onchain' ? '24h' : '12h'; chain.value = 'all'; selected.value = null })
watch([market, period, risk, pushed], () => { page.value = 1; payload.value = { items: [], health: {}, counts: {} }; fetchData() })
watch(chain, () => { page.value = 1 })
onMounted(() => { fetchData(); timer = setInterval(fetchData, 60000) })
onUnmounted(() => { serial++; if (timer) clearInterval(timer) })
function money(v: any): string {
  if (v === null || v === undefined) return '—'
  const n = Number(v)
  return Math.abs(n) >= 1e9 ? '$' + (n / 1e9).toFixed(2) + 'B' : Math.abs(n) >= 1e6 ? '$' + (n / 1e6).toFixed(2) + 'M' : '$' + n.toLocaleString('en-US', { maximumSignificantDigits: 7 })
}
function pct(v: any): string { return v === null || v === undefined ? '—' : (v >= 0 ? '+' : '') + Number(v).toFixed(2) + '%' }
function timeText(v: any): string { return v ? new Date(Number(v) * 1000).toLocaleString('zh-CN', { hour12: false }) : '—' }
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
    <div class="notice">{{ payload.warning }}<span v-if="market === 'onchain'"> 筛查通过＝当前双源证据满足门槛；需复核＝存在可疑管理权限等；待核验＝检测不完整或已过期。点击状态可查看依据。项目链接为资料线索，未经官方认证。</span><span> 行情榜独立于 TG 告警；“曾推送”仅标记历史推送。</span></div>
    <div class="filters">
      <template v-if="market === 'onchain'"><button v-for="(name, value) in risks" :key="value" :class="{ active: risk === value }" @click="risk = value">{{ name }} <small v-if="value !== 'all'">{{ payload.counts[value] || 0 }}</small></button><select v-model="chain"><option value="all">全部链</option><option v-for="c in chains" :key="c">{{ c }}</option></select></template>
      <input v-model="query" placeholder="代码 / 名称 / 合约地址" @keyup.enter="page = 1; fetchData()" aria-label="搜索代码或合约" />
      <button @click="page = 1; fetchData()">搜索</button><label><input type="checkbox" v-model="pushed" />仅看曾推送</label>
    </div>
    <p class="count">{{ rows.length }} 个结果 · 按{{ labels[period] }}涨幅排序 · 历史不足/价格过期不排名 · 页面每分钟刷新</p>
      <div class="table-scroll"><table><thead><tr><th>#</th><th>标的 / 身份</th><th v-if="market === 'onchain'">项目 / 叙事</th><th>现价</th><th>{{ labels[period] }}涨幅</th><th>24h成交额</th><template v-if="market === 'futures'"><th>OI数量变化</th><th>OI金额</th><th>费率</th></template><template v-if="market === 'onchain'"><th>流动性</th><th>市值 / FDV</th><th>合约筛查</th></template><th>告警 / 更新时间</th></tr></thead>
      <tbody><tr v-for="r in visible" :key="r.id"><td>{{ r.rank ?? '—' }}</td><td><a :href="url(r)" target="_blank" rel="noopener noreferrer">{{ r.symbol }} ↗</a><small>{{ r.name }}</small><template v-if="r.address"><small>{{ r.chain }} · {{ r.dex }}</small><code :title="r.address">{{ r.address }}</code><small class="bad" v-if="r.same_name_count > 1">同名 {{ r.same_name_count }} 个合约，请核对 CA</small></template></td><td v-if="market === 'onchain'" class="story"><div>{{ r.narrative || '暂无项目简介' }}</div><small>{{ r.narrative_source || '尚无叙事资料' }} · {{ r.profile_source || '链接未经官方核验' }}</small><div class="story-links"><a v-if="r.website" :href="r.website" target="_blank" rel="noopener noreferrer">网站 ↗</a><a v-if="r.twitter" :href="r.twitter" target="_blank" rel="noopener noreferrer">X ↗</a><span v-if="!r.website && !r.twitter">暂无网站 / X 线索</span></div></td><td>{{ money(r.price) }}<small v-if="r.stale" class="bad">价格已过期</small></td><td :class="r.period_change >= 0 ? 'up' : 'down'">{{ pct(r.period_change) }}<small v-if="!r.history_available">未满周期 / 历史缺失</small><small v-else :title="timeText(r.baseline_at)">{{ r.return_source === 'DexScreener 24h' ? '供应商24h口径' : '小时级历史基准' }}</small></td><td>{{ money(r.volume_24h) }}</td>
      <template v-if="market === 'futures'"><td>{{ pct(r.oi_change_percent) }}<small :title="timeText(r.oi_asof)">数量口径 · 2h采样</small></td><td>{{ money(r.oi_notional) }}</td><td>{{ r.funding_rate_percent == null ? '—' : Number(r.funding_rate_percent).toFixed(4) + '%' }}<small>/ {{ r.funding_interval_hours }}h</small></td></template>
      <template v-if="market === 'onchain'"><td>{{ money(r.liquidity) }}</td><td>{{ money(r.market_cap) }}<small>FDV {{ money(r.fdv) }}</small></td><td><button class="risk" :class="r.security.status" @click="selected = r">{{ risks[r.security.status] }} ⓘ</button><small>{{ timeText(r.security.checked_at) }}</small></td></template>
      <td>{{ r.has_alert ? '曾推送' : '未推送' }}<small>{{ r.alert_note }}</small><small v-if="r.last_alert_at">告警 {{ timeText(r.last_alert_at) }}</small><small>行情 {{ timeText(r.updated) }}</small></td></tr>
      <tr v-if="!visible.length"><td colspan="10" class="empty">{{ loading ? '正在读取…' : market === 'onchain' && risk === 'pass' ? '暂无仍在有效期内的通过结果。可切换「待核验 / 需复核 / 已拦截」查看原因。' : '暂无匹配数据，采集和历史回补将在后台继续。' }}</td></tr></tbody></table></div>
    <div class="pager"><button :disabled="page <= 1" @click="page--">上一页</button><span>{{ page }} / {{ Math.max(1, Math.ceil(rows.length / 25)) }}</span><button :disabled="page * 25 >= rows.length" @click="page++">下一页</button></div>
    <div v-if="selected" class="overlay" @click.self="selected = null"><section class="detail" role="dialog" aria-modal="true" aria-label="合约风险证据"><button class="close" @click="selected = null">关闭 ×</button><h3>{{ selected.symbol }} · {{ selected.security.label }}</h3><p>{{ selected.chain }}</p><code>{{ selected.address }}</code><p>定价/检测池：<code>{{ selected.pair }}</code></p><p>检测时间：{{ timeText(selected.security.checked_at) }}；有效至 {{ timeText(selected.security.expires_at) }}</p><ul><li v-for="reason in selected.security.reasons" :key="reason">{{ reason }}</li></ul><p>持有人卖出样本：成功 {{ selected.security.holder_successful ?? '未知' }} / 失败 {{ selected.security.holder_failed ?? '未知' }}</p><p>买税 {{ selected.security.buy_tax_percent ?? '未知' }}% · 卖税 {{ selected.security.sell_tax_percent ?? '未知' }}%</p><p>{{ selected.security.identity }}</p><p>{{ selected.security.coverage }}</p><p class="notice">{{ selected.security.limitations }}。管理员可在检测后更改条件；一次模拟成功不能保证所有钱包或金额都能卖出。</p><a href="https://docs.honeypot.is/ishoneypot" target="_blank" rel="noopener noreferrer">检测口径说明 ↗</a></section></div>
  </section>
</template>

<style scoped>
.crypto-view{color:var(--text);max-width:1600px}.intro{display:flex;align-items:center;justify-content:space-between;margin-bottom:24px}.intro h2{font-size:26px;margin:0 0 8px}.intro p,.count{color:#787873;font-size:13px}.controls,.filters,.segments,.pager{display:flex;align-items:center;gap:8px;flex-wrap:wrap}.controls{margin-bottom:16px;gap:20px}button,select,input{font:inherit;font-size:13px;border:1px solid #ddded8;background:#fff;padding:8px 12px;border-radius:7px}button{cursor:pointer}button:disabled{opacity:.45;cursor:default}button.active{background:#222b25;color:white;border-color:#222b25}.period button.active{background:#edf1ec;color:#24332a;border-color:#b6c4b5}.filters{margin:18px 0}.filters label{font-size:13px;display:flex;align-items:center;gap:5px}.status-row{display:flex;flex-wrap:wrap;gap:8px;font-size:11px;color:#60715e}.status-row span{background:#f1f4ef;border-radius:5px;padding:6px}.notice{font-size:12px;line-height:1.7;padding:12px;background:#f6f4ed;border-radius:7px;margin-top:12px;color:#71664c}.error{padding:12px;background:#fff0ef;color:#a53731}.table-scroll{overflow-x:auto;border:1px solid #e4e5df;border-radius:9px}table{width:100%;border-collapse:collapse;font-size:13px;white-space:nowrap}th{text-align:left;background:#f5f6f2;color:#73756c;font-size:12px;font-weight:500;padding:13px}td{border-top:1px solid #eee;padding:13px;vertical-align:top}td.story{min-width:230px;max-width:290px;white-space:normal;line-height:1.5}.story-links{display:flex;gap:12px;margin-top:7px;font-size:12px}.story-links span{color:#85877e}small{display:block;font-size:11px;color:#85877e;margin-top:5px}code{font-size:10px;display:block;max-width:230px;overflow:hidden;text-overflow:ellipsis;margin-top:5px}a{color:#24533c;text-decoration:none;font-weight:600}.up{color:#258158}.down,.bad{color:#b84742!important}.risk{padding:5px 8px;font-size:12px}.risk.pass{color:#2e7851;background:#edf7ed}.risk.blocked{color:#ad3131;background:#fff0ef}.risk.review{color:#986300;background:#fff7e6}.empty{text-align:center;padding:45px 20px;color:#797c73;white-space:normal}.pager{justify-content:flex-end;margin-top:15px;font-size:12px}.overlay{position:fixed;inset:0;background:#0005;z-index:100;display:flex;align-items:center;justify-content:center;padding:20px}.detail{background:white;border-radius:12px;padding:28px;width:620px;max-height:85vh;overflow:auto;line-height:1.6}.detail code{max-width:100%;word-break:break-all;white-space:normal}.close{float:right}.detail li{margin-bottom:7px;font-size:13px}@media(max-width:700px){.intro{align-items:flex-start}.intro h2{font-size:21px}.controls{gap:10px}.detail{padding:18px}td,th{padding:10px}}
</style>

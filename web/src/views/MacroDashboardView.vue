<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from 'vue'
import api from '../api'

const payload = ref<any>({ macro: [], futures: [], movers: [], errors: {}, notes: [] })
const loading = ref(false)
const error = ref('')
const category = ref('全部')
let timer: ReturnType<typeof setInterval> | undefined
let serial = 0

const macroOrder = ['DFEDTARU', 'DGS2', 'DGS10', 'DFII10', 'T10YIE', 'DTWEXBGS']
const futureOrder = ['CL', 'OIL', 'GC', 'SI', 'HG']
const macroRows = computed(() => macroOrder.map(id => (payload.value.macro || []).find((r: any) => r.id === id)).filter(Boolean))
const futureRows = computed(() => futureOrder.map(id => (payload.value.futures || []).find((r: any) => r.id === id)).filter(Boolean))
const categories = computed(() => ['全部', ...new Set<string>((payload.value.movers || []).map((r: any) => r.category).filter(Boolean))])
const movers = computed(() => (payload.value.movers || []).filter((r: any) => category.value === '全部' || r.category === category.value))
const ten = computed(() => macroRows.value.find((r: any) => r.id === 'DGS10'))
const two = computed(() => macroRows.value.find((r: any) => r.id === 'DGS2'))
const curve = computed(() => ten.value && two.value ? Number(ten.value.value) - Number(two.value.value) : null)

async function fetchData() {
  const request = ++serial
  loading.value = true
  error.value = ''
  try {
    const response = await api.get('/macro/dashboard')
    if (request === serial) payload.value = response.data
  } catch (e: any) {
    if (request === serial) error.value = e?.response?.data?.detail || '宏观数据读取失败；旧快照不可视为最新行情'
  } finally { if (request === serial) loading.value = false }
}
onMounted(() => { fetchData(); timer = setInterval(fetchData, 60000) })
onUnmounted(() => { serial++; if (timer) clearInterval(timer) })

function number(value: any, digits = 2): string {
  const n = Number(value)
  return value == null || !Number.isFinite(n) ? '—' : n.toLocaleString('en-US', { maximumFractionDigits: digits })
}
function signed(value: any, digits = 2): string {
  const n = Number(value)
  return value == null || !Number.isFinite(n) ? '—' : (n > 0 ? '+' : '') + n.toFixed(digits)
}
function status(asof: string, daily = false): string {
  if (!asof) return '时间未知'
  const hours = (Date.now() - new Date(asof).getTime()) / 3600000
  if (!Number.isFinite(hours)) return '时间未知'
  return hours > (daily ? 24 * 7 : 3) ? '已收盘 / 延迟' : '最新快照'
}
function timeText(seconds: any): string { return seconds ? new Date(Number(seconds) * 1000).toLocaleString('zh-CN', { hour12: false }) : '尚无快照' }
function asofText(value: string): string { return value ? value.replace('T', ' ').slice(0, 19) : '—' }
</script>

<template>
  <section class="macro-page">
    <div class="heading"><div><h2>宏观与大宗</h2><p>利率 · 美元 · 外盘期货 · 国内商品异动</p></div><button @click="fetchData" :disabled="loading">{{ loading ? '读取中…' : '刷新页面' }}</button></div>
    <div v-if="error" class="error">{{ error }}</div>
    <p class="disclaimer">不同数据的交易时间和发布频率不同，请看每张卡片的时间戳。外盘期货为第三方行情快照，可能延迟；日度宏观指标不能当作盘中报价。页面刷新不会强制向上游重复抓取。</p>

    <div class="section-head"><h3>利率与美元</h3><span>FRED 日度发布 · 快照 {{ timeText(payload.macro_checked_at) }}</span></div>
    <div class="card-grid">
      <a v-for="r in macroRows" :key="r.id" class="metric-card" :href="r.url" target="_blank" rel="noopener noreferrer">
        <div class="card-top"><span>{{ r.name }}</span><span class="status">{{ status(r.asof, true) }}</span></div>
        <strong>{{ number(r.value, r.id === 'DTWEXBGS' ? 2 : 3) }} <em>{{ r.unit }}</em></strong>
        <div class="card-foot"><span :class="Number(r.change) >= 0 ? 'up' : 'down'">{{ signed(r.change, 3) }} {{ r.unit === '%' ? '百分点' : '点' }}</span><span>截至 {{ r.asof }}</span></div>
      </a>
      <div v-if="!macroRows.length" class="empty-card">宏观数据尚未取得，保留空状态，不显示猜测报价。</div>
    </div>
    <div class="context-line"><span>10Y－2Y 利差：<b>{{ signed(curve, 3) }} 个百分点</b></span><span>10Y 盈亏平衡通胀率＝名义 10Y－实际 10Y；并非未来 CPI 预测。</span><a href="https://www.cmegroup.com/fedwatch" target="_blank" rel="noopener noreferrer">CME FedWatch ↗</a></div>

    <div class="section-head"><h3>外盘商品期货</h3><span>精选报价 · 快照 {{ timeText(payload.futures_checked_at) }}</span></div>
    <div class="card-grid futures">
      <a v-for="r in futureRows" :key="r.id" class="metric-card" :href="r.url" target="_blank" rel="noopener noreferrer">
        <div class="card-top"><span>{{ r.name }}</span><span class="status" :class="{ delayed: status(r.asof) !== '最新快照' }">{{ status(r.asof) }}</span></div>
        <strong>{{ number(r.value, 3) }} <em>{{ r.unit }}</em></strong>
        <div class="card-foot"><span :class="Number(r.change_pct) >= 0 ? 'up' : 'down'">{{ signed(r.change_pct) }}%</span><span>{{ asofText(r.asof) }}</span></div>
      </a>
      <div v-if="!futureRows.length" class="empty-card">外盘报价尚未取得。</div>
    </div>
    <p class="source-note">外盘为新浪期货行情经 AkShare 获取的报价，并非交易所直连；WTI、Brent、COMEX 金银铜分别显示原始报价口径。周末或休市时会保留最后一笔并标为延迟。</p>

    <div class="section-head movers-head"><div><h3>国内商品异动</h3><span>复用 Trade_db 已保存的 CF 期货日线 · 最近记录 {{ payload.mover_date || '—' }}</span></div><select v-model="category" aria-label="筛选商品板块"><option v-for="c in categories" :key="c">{{ c }}</option></select></div>
    <div class="table-scroll"><table><thead><tr><th>商品</th><th>板块</th><th>主力合约</th><th>记录价格</th><th>当日涨跌</th><th>来源</th></tr></thead><tbody>
      <tr v-for="r in movers" :key="r.symbol"><td><strong>{{ r.name }}</strong></td><td>{{ r.category }}</td><td>{{ r.symbol }}</td><td>{{ number(r.price, 3) }}</td><td :class="Number(r.change_pct) >= 0 ? 'up' : 'down'">{{ signed(r.change_pct) }}%</td><td>{{ r.from_daily_monitor ? '大宗日报领涨' : '趋势池日线' }}</td></tr>
      <tr v-if="!movers.length"><td colspan="6" class="empty-row">最近记录日暂无涨跌幅达到 1.5% 的商品，或日线尚未更新。</td></tr>
    </tbody></table></div>
    <p class="source-note">这是现有监控和趋势池的异动摘录，最多 20 条，不代表全市场排名；主力合约换月可能造成价格不连续。<router-link to="/trend">查看股票 Trend ↗</router-link></p>
    <p v-if="Object.keys(payload.errors || {}).length" class="error">上游本轮更新失败：{{ Object.keys(payload.errors).join('、') }}。页面保留上次成功快照，请核对各项时间。</p>
  </section>
</template>

<style scoped>
.macro-page{max-width:1540px;color:var(--text)}.heading,.section-head,.card-top,.card-foot,.context-line{display:flex;align-items:center;justify-content:space-between;gap:12px}.heading{margin-bottom:20px}.heading h2{font-size:26px;margin:0 0 6px}.heading p{color:#747a71;font-size:13px;margin:0}.heading button,.movers-head select{border:1px solid #d8ded5;border-radius:7px;padding:8px 12px;background:white;font:inherit;font-size:13px;cursor:pointer}.heading button:disabled{opacity:.5}.disclaimer{background:#f4f5ee;border-left:3px solid #7c9a79;border-radius:5px;padding:11px 14px;color:#687269;font-size:12px;line-height:1.7;margin:0 0 28px}.section-head{margin:25px 0 12px}.section-head h3{font-size:18px;margin:0}.section-head span{font-size:12px;color:#7a817a}.card-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:12px}.metric-card,.empty-card{display:block;border:1px solid #e1e5dc;border-radius:10px;background:#fff;padding:16px;color:inherit;text-decoration:none;min-height:132px}.metric-card:hover{border-color:#91aa94;box-shadow:0 3px 13px #1b3b2310}.card-top{align-items:flex-start;font-size:13px;color:#59665a;min-height:33px}.status{font-size:10px;color:#5d8467;white-space:nowrap}.status.delayed{color:#a17a40}.metric-card strong{display:block;font-size:24px;margin:9px 0 14px;letter-spacing:-.03em}.metric-card em{font-size:11px;font-style:normal;color:#80897f;font-weight:400;letter-spacing:0}.card-foot{font-size:11px;color:#8b9188}.up{color:#18805a!important}.down{color:#b44945!important}.empty-card{font-size:13px;color:#8b9188;display:flex;align-items:center}.context-line{justify-content:flex-start;flex-wrap:wrap;background:#f7f8f4;padding:12px 14px;border-radius:7px;font-size:12px;color:#687268;margin-top:14px}.context-line b{color:#253b2d}.context-line a,.source-note a{color:#245a3b;text-decoration:none}.source-note{font-size:12px;line-height:1.7;color:#818780;margin:12px 0 25px}.movers-head{align-items:end}.movers-head span{display:block;margin-top:5px}.table-scroll{overflow-x:auto;border:1px solid #e1e5dc;border-radius:9px}table{border-collapse:collapse;width:100%;font-size:13px;white-space:nowrap}th{text-align:left;color:#727d70;background:#f4f6f1;font-weight:500;padding:12px}td{border-top:1px solid #ebeee9;padding:13px}td strong{font-weight:600}.empty-row{text-align:center;padding:35px;color:#92978f}.error{margin:12px 0;padding:11px 14px;background:#fff0ee;color:#ae423b;border-radius:6px;font-size:12px}@media(max-width:700px){.heading h2{font-size:22px}.card-grid{grid-template-columns:repeat(auto-fit,minmax(165px,1fr))}.metric-card{padding:13px}.metric-card strong{font-size:20px}.section-head{align-items:flex-start;flex-direction:column}.context-line{align-items:flex-start;flex-direction:column}}
</style>

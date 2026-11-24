<script setup lang="ts">
import { computed, ref } from 'vue'
import api from '../api'
const props = defineProps<{ payload?: any }>()
const mode = ref('focus'), market = ref('all'), expanded = ref(true)
const selected = ref<any>(null), busy = ref(false), error = ref(''), eventRows = ref<any[]>([]), research = ref<any[]>([])
const horizon = ref(180), period = ref('all'), sampling = ref('primary'), sort = ref('date')
const pickedEvent = ref<any>(null)
let request = 0
const data = computed(() => props.payload || { markets: [], signals: [], quality: 'missing' })
const rows = computed(() => (data.value.signals || []).filter((s: any) =>
  (market.value === 'all' || s.market === market.value) &&
  (mode.value === 'all' || mode.value === 'focus' && s.tier !== '对照规则' || mode.value === 'new' && s.new_event === true ||
   mode.value === 'ongoing' && s.state === true && s.new_event !== true || mode.value === 'error' && s.quality !== 'ok')))
const qualityNames: Record<string, string> = { ok: '已核验', pending: '待发布', stale: '已过期', missing: '缺数据', invalid: '无效', insufficient_history: '历史不足', source_conflict: '来源冲突', partial: '部分可用', not_applicable: '不适用' }
const marketNames: Record<string, string> = { CYB: '创业板指', CSI300: '沪深300', HSI: '恒生指数', HSTECH: '恒生科技', BTC: 'BTC', ETH: 'ETH', SPX: '标普500', NDX: '纳斯达克100', SOX: '费城半导体' }
const metricNames: Record<string, string> = { iv: '隐波', iv_q90_1y: '隐波前一年90分位阈值', rv_20: 'RV20', rv_q90_1y: 'RV前一年90分位阈值', ret_20: '20日收益', close: '收盘', ma_20: 'MA20', ma_200: 'MA200', prior_20_close_high: '前20日收盘高点', rsi_14: 'RSI14', drawdown_1y: '一年回撤', previous_rsi_14: '前日RSI', previous_close: '前日收盘', previous_ma_20: '前日MA20', previous_ma_200: '前日MA200', fng_delayed: '延迟恐贪', previous_fng_delayed: '前日延迟恐贪', fng_prior30_min: '此前30日恐贪最低' }
function n(v: any, digits = 2): string { return v == null || !Number.isFinite(Number(v)) ? '—' : Number(v).toLocaleString('zh-CN', { maximumFractionDigits: digits }) }
function pct(v: any) { return v == null ? '—' : `${n(Number(v) * 100)}%` }
function vol(v: any) { return v == null ? '—' : `${n(v)}%` }
function feature(v: any, unit: string) { return v == null ? '—' : unit === 'ratio' ? pct(v) : n(v) + (unit === 'volatility_pct' ? '%' : '') }
function q(v: any) { return qualityNames[v] || '未知' }
function when(v: any) { return v ? String(v).replace('T', ' ').slice(0, 19) : '未取得' }
function evaluation(e: any) { return [...(e.forward || []), ...(e.historical_evaluations || [])].find((v: any) => v.horizon === Number(horizon.value)) }
const filteredEvents = computed(() => eventRows.value.filter(e => e.market === selected.value?.market && (!selected.value?.signal || e.signal_id === selected.value.signal)).sort((a, b) => sort.value === 'mae' ? (evaluation(a)?.mae ?? Infinity) - (evaluation(b)?.mae ?? Infinity) : b.session_date.localeCompare(a.session_date)))
const evidenceRows = computed(() => research.value.filter(r => r.market === selected.value?.market && (!selected.value?.signal || r.signal_id === selected.value.signal) && r.holding_period === Number(horizon.value) && r.period === period.value && (sampling.value === 'primary' ? r.sampling_interval === (r.market === 'BTC' || r.market === 'ETH' ? 181 : 127) : r.sampling_interval === (r.market === 'BTC' || r.market === 'ETH' ? 91 : 64))))
const selectedMarket = computed(() => (data.value.markets || []).find((m: any) => m.market === selected.value?.market))
const chart = computed(() => selectedMarket.value?.chart || [])
function xy(i: number, price: number) { const values = chart.value.map((p: any) => p.close); const lo = Math.min(...values), hi = Math.max(...values); return [12 + i * 876 / Math.max(1, values.length - 1), 136 - (price - lo) * 120 / Math.max(0.0000001, hi - lo)] }
const line = computed(() => chart.value.map((p: any, i: number) => xy(i, p.close).join(',')).join(' '))
const dots = computed(() => filteredEvents.value.flatMap(e => { const i = chart.value.findIndex((p: any) => p.date === e.session_date); return i < 0 ? [] : [{ ...e, x: xy(i, chart.value[i].close)[0], y: xy(i, chart.value[i].close)[1] }] }))
async function details(code: string, signal = '') {
  selected.value = { market: code, signal }; horizon.value = ['BTC', 'ETH'].includes(code) ? 180 : 126
  pickedEvent.value = null; error.value = ''; eventRows.value = []; research.value = []; busy.value = true
  const current = ++request
  try { const [events, ev] = await Promise.all([api.get('/quant/events', { params: { market: code, limit: 300 } }), api.get('/quant/evidence', { params: { market: code } })]); if (current === request) { eventRows.value = events.data.events || []; research.value = ev.data.records || [] } }
  catch { if (current === request) error.value = '事件或研究证据暂不可用；不影响上方其他市场数据。' }
  finally { if (current === request) busy.value = false }
}
</script>

<template>
  <section class="quant" aria-label="市场状态与量化信号">
    <header class="q-head"><div><h3>市场状态与量化信号 <span class="badge">研究监控</span></h3><p>{{ data.markets?.length || 0 }}个市场对象 · A/H/美股/加密 · 收盘确认 · 历史证据与上线后评价分开</p></div><button @click="expanded = !expanded">{{ expanded ? '收起' : '展开' }}</button></header>
    <template v-if="expanded">
      <p class="notice">不是买入建议，不自动下单或配仓。半年胜率不代表明日或下周上涨概率；高胜率可能伴随严重浮亏。RV 是实际波动率，不是 VIX。</p>
      <div v-if="!data.markets?.length" class="empty">量化数据尚未初始化。其他宏观数据继续正常展示。</div>
      <div class="q-grid">
        <article v-for="m in data.markets" :key="m.market" class="market-card">
          <div class="q-row"><b>{{ m.name }}</b><span :class="['badge', m.quality !== 'ok' && 'warn']">{{ q(m.quality) }}</span></div>
          <strong>{{ n(m.price) }} <small>{{ m.kind === 'SPOT_USD' ? 'USD 现货' : '价格指数' }}</small></strong>
          <div class="metrics"><span>20日收益 <b>{{ pct(m.features?.ret_20) }}</b></span><span>RSI14 <b>{{ n(m.features?.rsi_14) }}</b></span><span>RV20 <b>{{ vol(m.features?.rv_20) }}</b></span><span>MA20 / MA200 <b>{{ n(m.features?.ma_20) }} / {{ n(m.features?.ma_200) }}</b></span><span>RV前一年90分位阈值 <b>{{ vol(m.features?.rv_q90_1y) }}</b></span><span>RV当前历史百分位 <b>{{ pct(m.features?.rv_percentile_1y) }}</b></span></div>
          <p class="iv-label">{{ m.iv_name }}</p>
          <template v-if="m.iv_applicable !== false"><div class="q-row"><span>{{ vol(m.iv_last_value) }} <small>截至 {{ m.iv_last_session || '未知' }}</small></span><span :class="['badge', m.iv_quality !== 'ok' && 'warn']">{{ q(m.iv_quality) }}</span></div>
          <p class="iv-label">IV当日百分位 {{ pct(m.features?.iv_percentile_1y) }} · 前一年90分位阈值 {{ vol(m.features?.iv_q90_1y) }}</p></template>
          <p v-else class="note">使用板块自身RV独立检验，不套用VIX/VXN胜率；未设置隐波规则。</p>
          <footer>价格交易日 {{ m.session_date || '未知' }} · {{ m.price_source || '来源未知' }}<br v-if="m.iv_applicable !== false"><template v-if="m.iv_applicable !== false">隐波有效覆盖 {{ n((m.windows?.iv_coverage ?? 0) * 100, 1) }}% / {{ m.windows?.required_year_bars }} 根</template><template v-if="m.research_coverage"><br>冻结研究：价格 {{ m.research_coverage.price_start }} 起<template v-if="m.iv_applicable !== false">；隐波 {{ m.research_coverage.iv_start }} 起</template></template></footer>
          <p v-if="m.price_feed?.source_switch" class="warning">已换源：重叠区间已核验，实时数据版本与冻结研究分别记录。</p><p v-if="m.missing_reason" class="warning">{{ m.missing_reason }}</p><button class="text-button" @click="details(m.market)">查看指标、事件与证据 →</button>
        </article>
      </div>
      <div class="toolbar"><button v-for="f in [{id:'focus',name:'重点跟踪'},{id:'new',name:'新触发'},{id:'ongoing',name:'持续成立'},{id:'error',name:'数据异常'},{id:'all',name:'全部规则'}]" :key="f.id" :class="{active:mode===f.id}" @click="mode=f.id">{{ f.name }}</button><select v-model="market" aria-label="量化市场筛选"><option value="all">全部市场</option><option v-for="(name,code) in marketNames" :key="code" :value="code">{{ name }}</option></select></div>
      <div class="signals">
        <article v-for="s in rows" :key="`${s.market}:${s.signal_id}`" class="signal-card">
          <div class="q-row"><b>{{ marketNames[s.market] }} · {{ s.label }}</b><span :class="['badge', s.state === null && 'warn']">{{ s.state === true ? '成立' : s.state === false ? '未成立' : '不可判定' }}</span></div>
          <p><span class="badge">{{ s.tier }}</span> <span v-if="s.new_event === true" class="badge">本交易日新事件</span> <span v-if="s.event_start_uncertain" class="warning">起点不确定</span></p>
          <div class="thresholds"><div v-for="(t,i) in s.thresholds" :key="i">{{ metricNames[t.metric] || t.metric }} <b>{{ feature(t.value,t.unit) }}</b> {{ t.operator }} {{ t.threshold_metric ? (metricNames[t.threshold_metric] || t.threshold_metric) : '' }} {{ feature(t.threshold,t.unit) }} <small>差值 {{ feature(t.distance,t.unit) }}（非触发概率）</small></div></div>
          <p v-if="s.state === true">持续 {{ s.episode_duration_bars }} 根日K · 起于 {{ s.episode_start }}</p><p v-if="s.state === null" class="warning">{{ q(s.quality) }}：{{ s.missing_reason }}</p>
          <div v-if="s.evidence" class="evidence-line"><b>{{ s.evidence.wins }}/{{ s.evidence.completed_events }}</b> 历史完成样本 · 持有 {{ s.evidence.holding_period }} {{ s.evidence.holding_unit === 'UTC days' ? '天' : '交易日' }}<br>Wilson 95%：{{ n(s.evidence.confidence_interval?.[0]) }}%–{{ n(s.evidence.confidence_interval?.[1]) }}%<br>中位净收益 {{ n(s.evidence.median_return_pct) }}% · 最坏 MAE <b class="risk">{{ n(s.evidence.worst_mae_pct) }}%</b><br>匹配状态基准 {{ n(s.evidence.matched_state_baseline?.win_rate_pct) }}% · 无条件基准 {{ n(s.evidence.unconditional_baseline?.win_rate_pct) }}%</div>
          <p v-else>历史证据待复现或没有完成样本，不展示估算胜率。</p>
          <footer>交易日 {{ s.session_date }} · {{ s.formula_version }}<br>研究 {{ s.research_version || '待复现' }}</footer><button class="text-button" @click="details(s.market,s.signal_id)">历史事件与完整证据 →</button>
        </article>
      </div>
      <p v-if="!rows.length" class="empty">本筛选下没有信号。请查看“数据异常”，缺失不等于未触发。</p>
      <div v-if="selected" class="detail-panel">
        <div class="q-row"><h4>{{ marketNames[selected.market] }} · {{ selected.signal || '全部规则' }}</h4><button @click="selected=null; request++">关闭详情</button></div>
        <p v-if="busy">读取本地事件与研究证据…</p><p v-if="error" class="warning">{{ error }}</p>
        <details v-if="selectedMarket"><summary>来源切换、可用时间与窗口覆盖</summary><pre>{{ JSON.stringify({price_feed:selectedMarket.price_feed,iv_source:selectedMarket.iv_source,windows:selectedMarket.windows,bar_end_at:selectedMarket.bar_end_at,available_at:selectedMarket.available_at,fetched_at:selectedMarket.fetched_at},null,2) }}</pre></details>
        <div v-if="chart.length" class="chart"><svg viewBox="0 0 900 150" role="img" aria-label="已完成日线与历史事件"><polyline :points="line" fill="none" stroke="#416f50" stroke-width="2"/><circle v-for="e in dots" :key="e.id" :cx="e.x" :cy="e.y" r="4" fill="#bb8144" tabindex="0" @click="pickedEvent=e" @keydown.enter="pickedEvent=e"><title>{{ e.session_date }} {{ e.signal_id }} {{ e.origin }}</title></circle></svg><small>{{ chart[0]?.date }} — {{ chart[chart.length-1]?.date }} · 最近180根已完成日K；圆点为所选规则事件，点击查看当时证据。</small></div>
        <div class="toolbar"><select v-model="horizon" aria-label="信号持有期"><option v-for="h in (['BTC','ETH'].includes(selected.market) ? [30,90,180] : [21,63,126])" :key="h" :value="h">持有 {{ h }} {{ ['BTC','ETH'].includes(selected.market) ? 'UTC天' : '交易日' }}</option></select><select v-model="period" aria-label="研究历史分段"><option value="all">全历史</option><option value="pre2023">2023年前（剔除跨界退出）</option><option value="2023plus">2023年以来（非前瞻）</option></select><select v-model="sampling" aria-label="回测取样间隔"><option value="primary">主取样：半年冷却</option><option value="sensitivity">敏感性：三月冷却</option></select></div>
        <h4>研究证据（仅历史，不是前瞻）</h4><div class="scroll"><table><thead><tr><th>规则</th><th>赢/完成</th><th>Wilson 95%</th><th>均值/中位</th><th>最差期末/MAE</th><th>无条件/匹配基准</th><th>取样间隔</th><th>评价类型</th></tr></thead><tbody><tr v-for="r in evidenceRows" :key="`${r.signal_id}:${r.holding_period}:${r.period}`"><td>{{ r.signal_id }}</td><td>{{ r.wins }}/{{ r.completed_events }}</td><td>{{ n(r.confidence_interval[0]) }}%–{{ n(r.confidence_interval[1]) }}%</td><td>{{ n(r.mean_return_pct) }}% / {{ n(r.median_return_pct) }}%</td><td class="risk">{{ n(r.worst_terminal_return_pct) }}% / {{ n(r.worst_mae_pct) }}%</td><td>{{ n(r.unconditional_baseline?.win_rate_pct) }}% / {{ n(r.matched_state_baseline?.win_rate_pct) }}%</td><td>{{ r.sampling_interval }}根 {{ r.overlapping_windows ? '⚠窗口重叠' : '主窗口不重叠' }}</td><td>{{ r.validation_type === 'historical_full_sample' ? '全样本历史' : '历史分段诊断' }}</td></tr><tr v-if="!evidenceRows.length"><td colspan="8">没有本条件下的完成研究样本。</td></tr></tbody></table></div>
        <div class="q-row"><h4>事件详情</h4><select v-model="sort" aria-label="事件排序"><option value="date">最新事件</option><option value="mae">最坏MAE优先</option></select></div><p class="note">这里记录全部有效状态变化，不套用回测冷却。历史事件详情不是去重研究样本；未到期不计完成胜率。</p>
        <div class="scroll"><table><thead><tr><th>信号日/规则</th><th>入场/退出日</th><th>状态</th><th>期末净收益/MAE</th><th>类型</th><th>修订</th></tr></thead><tbody><tr v-for="e in filteredEvents.slice(0,100)" :key="e.id" tabindex="0" @click="pickedEvent=e" @keydown.enter="pickedEvent=e"><td>{{ e.session_date }}<br>{{ e.signal_id }}</td><td>{{ evaluation(e)?.entry_date || '待下一根完成K线' }}<br>{{ evaluation(e)?.exit_date || '未到期' }}</td><td>{{ e.status === 'revised_out' ? '修订后撤销（保留记录）' : evaluation(e)?.status === 'completed' ? '已完成' : '评价中' }}</td><td>{{ pct(evaluation(e)?.net_return) }} / <span class="risk">{{ pct(evaluation(e)?.mae) }}</span></td><td>{{ e.origin === 'prospective' ? '上线后前瞻' : '历史回补' }}</td><td>{{ e.revision_count }} 次</td></tr></tbody></table></div>
        <details v-if="pickedEvent" open><summary>{{ pickedEvent.session_date }} · 当时特征、来源与版本</summary><pre>{{ JSON.stringify(pickedEvent,null,2) }}</pre></details>
      </div>
      <div class="forward"><b>上线后前瞻验证</b><p>启动 {{ when(data.launched_at) }} UTC · 已完成 {{ data.forward?.completed_evaluations || 0 }} 项期限评价，评价中 {{ data.forward?.evaluating_count || 0 }} 项。未到期不计胜率；不同市场、公式及期限分别统计，不合并成一个“系统胜率”。</p><p v-for="(g,i) in data.forward?.groups || []" :key="i">{{ g.market }} {{ g.signal_id }} · {{ g.horizon }}根 · {{ g.wins }}/{{ g.completed }} 完成样本，{{ g.evaluating }} 评价中</p></div>
      <details class="provenance"><summary>数据来源、公式与限制</summary><p>研究截止 {{ data.research_cutoff }}；{{ data.research_version }}；{{ data.formula_version }}。当前计算 {{ when(data.generated_at) }} UTC。恐贪仅用于 BTC，按冻结口径延迟完整一天。</p><p v-for="note in data.notes" :key="note">{{ note }}</p><p v-for="(problem,key) in data.source_errors || {}" :key="key" class="warning">{{ key }}：{{ (problem as any).error_type }} · {{ (problem as any).reason }}</p></details>
    </template>
  </section>
</template>

<style scoped>
.quant{margin:24px 0 32px;color:#283e2d}.q-head,.q-row{display:flex;align-items:center;justify-content:space-between;gap:12px}.q-head h3{margin:0;font-size:19px}.q-head p,.note,footer,.iv-label{color:#7c8679;font-size:12px;line-height:1.65}.badge{font-size:11px;padding:3px 7px;border-radius:5px;background:#edf3e9;color:#456341;font-weight:400}.warn{background:#fff3de;color:#936b2e}.notice{font-size:12px;line-height:1.7;padding:12px 15px;background:#f5f5ee;border-left:3px solid #c0ab77;color:#75684c}.q-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px}.market-card,.signal-card{border:1px solid #dfe5db;border-radius:9px;padding:16px;background:#fff;min-width:0}.market-card strong{display:block;font-size:25px;margin:12px 0}.market-card small{font-size:11px;font-weight:400;color:#818b7e}.metrics{display:grid;grid-template-columns:1fr 1fr;gap:8px;font-size:12px;color:#6e7c69}.metrics b{display:block;font-weight:500;color:#324a35;margin-top:2px}.iv-label{margin:14px 0 6px}.market-card footer,.signal-card footer{margin-top:12px;font-size:11px}.toolbar{display:flex;gap:7px;flex-wrap:wrap;margin:15px 0}button,select{background:white;border:1px solid #d8ded2;border-radius:6px;padding:6px 10px;color:#44533e;font:inherit;font-size:12px;cursor:pointer}.active{background:#2e4934;color:white}.text-button{border:none;padding:10px 0 0;color:#386d44}.signals{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}.signal-card b{font-size:13px}.signal-card p{font-size:12px;color:#7b8576}.thresholds{font-size:12px;line-height:1.9}.thresholds small{display:block;color:#7e8877;font-size:11px}.evidence-line{margin-top:12px;padding:10px;background:#f6f8f3;font-size:12px;line-height:1.9}.risk{color:#ac5246}.warning{font-size:12px;color:#a07537;overflow-wrap:anywhere}.empty{font-size:13px;padding:20px;color:#8d9686}.detail-panel{margin-top:16px;padding:16px;border:1px solid #ccd8c5;border-radius:9px;background:#fafbf7}.detail-panel h4{font-size:14px;margin:14px 0}.scroll{overflow:auto;border:1px solid #e0e6d9;border-radius:6px;max-height:480px}table{width:100%;border-collapse:collapse;white-space:nowrap;font-size:12px}td,th{padding:10px;text-align:left;border-bottom:1px solid #e5e9df}th{background:#edf2e8;position:sticky;top:0}.scroll tbody tr{cursor:pointer}.scroll tbody tr:hover{background:#f0f4eb}.chart{margin-top:15px;background:#fff;padding:10px;border:1px solid #e2e8dc;border-radius:7px}.chart svg{width:100%;max-height:180px}.chart circle{cursor:pointer}.chart small{font-size:11px;color:#7d8874}.forward{margin-top:16px;padding:14px;background:#f1f5ec;font-size:12px;line-height:1.7}.provenance{margin-top:12px;font-size:12px;color:#7c8674;line-height:1.7}pre{font-size:11px;max-height:320px;overflow:auto;white-space:pre-wrap;word-break:break-word;background:#eef1e9;padding:12px}summary{cursor:pointer;font-size:12px;margin-top:12px}@media(max-width:800px){.q-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.signals{grid-template-columns:1fr}}@media(max-width:500px){.q-grid{grid-template-columns:1fr}.q-head{align-items:flex-start}.q-head h3{font-size:17px}.q-row{flex-wrap:wrap}}
</style>

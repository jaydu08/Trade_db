<script setup lang="ts">
import { ref, onMounted, watch, computed } from 'vue'
import { ElMessage } from 'element-plus'
import api from '../api'

interface CapitalSignalItem {
  label: string
  value: string
  direction?: string
  score?: number
  tooltip?: string
  asof?: string
}

interface CapitalSignalCoverage {
  status?: string
  text?: string
  reason?: string
  item_count?: number
  latest_asof?: string
}

interface TrendItem {
  rank: number
  symbol: string
  name: string
  market: string
  price: number
  period_change: number
  return_20d: number
  return_60d: number
  amount: number
  score: number
  catalyst_tags: string
  days_on_list: number
  market_cap: number
  inst_factor: number
  inst_label: string
  inst_change_pp: number
  inst_delta_abs: number
  inst_delta_pct: number
  inst_start_value: number
  inst_end_value: number
  inst_start_date: string
  inst_end_date: string
  inst_metric_unit: string
  inst_date: string
  inst_text: string
  inst_source: string
  inst_direction: string
  capital_signal_items: CapitalSignalItem[]
  capital_signal_score: number
  capital_signal_coverage: CapitalSignalCoverage
  signal_strength: number
  price_date: string
}

const markets = ['CN', 'HK', 'US']
const daysOptions = [3, 7, 14, 30, 60, 90, 180]
const activeMode = ref<'hot' | 'slow'>('hot')
const activeMarket = ref('CN')
const activeDays = ref(7)
const loading = ref(false)
const allData = ref<Record<string, TrendItem[]>>({})

const pageSize = ref(20)
const currentPage = ref(1)
const sortProp = ref<string>('')
const sortOrder = ref<'ascending' | 'descending' | ''>('')

async function fetchTrend() {
  loading.value = true
  try {
    const endpoint = activeMode.value === 'slow' ? '/trend/slow' : '/trend'
    const params = activeMode.value === 'slow' ? { limit: 100 } : { days: activeDays.value, limit: 100 }
    const res = await api.get(endpoint, { params })
    const marketsData: Record<string, TrendItem[]> = {}
    const raw = res.data.markets || {}
    for (const [mkt, items] of Object.entries(raw) as [string, any[]][]) {
      marketsData[mkt] = (items || []).map((item: any, idx: number) => ({
        rank: idx + 1,
        symbol: item.symbol || '',
        name: item.name || '',
        market: mkt,
        price: item.price || 0,
        period_change: activeMode.value === 'slow' ? (item.return_60d || item.return_pct || 0) : (item.return_pct || 0),
        return_20d: item.return_20d || 0,
        return_60d: item.return_60d || item.return_pct || 0,
        amount: item.amount || 0,
        score: item.trend_score || 0,
        catalyst_tags: item.catalyst_tags || '',
        days_on_list: item.days_on_list || 0,
        market_cap: item.market_cap || 0,
        inst_factor: item.inst_factor || 0,
        inst_label: item.inst_label || "",
        inst_change_pp: item.inst_change_pp || 0,
        inst_delta_abs: item.inst_delta_abs || item.inst_change_pp || 0,
        inst_delta_pct: item.inst_delta_pct || 0,
        inst_start_value: item.inst_start_value || 0,
        inst_end_value: item.inst_end_value || 0,
        inst_start_date: item.inst_start_date || "",
        inst_end_date: item.inst_end_date || item.inst_date || "",
        inst_metric_unit: item.inst_metric_unit || "percentage_point",
        inst_date: item.inst_date || "",
        inst_text: item.inst_text || "",
        inst_source: item.inst_source || "",
        inst_direction: item.inst_direction || "",
        capital_signal_items: Array.isArray(item.capital_signal_items) ? item.capital_signal_items : [],
        capital_signal_score: item.capital_signal_score || 0,
        capital_signal_coverage: item.capital_signal_coverage || {},
        signal_strength: item.signal_strength || 0,
        price_date: item.price_date || '',
      }))
    }
    allData.value = marketsData
  } catch (e: any) {
    ElMessage.error(e?.response?.data?.detail || '加载趋势数据失败')
  } finally {
    loading.value = false
  }
}

const currentItems = computed<TrendItem[]>(() => allData.value[activeMarket.value] || [])

const sortedItems = computed<TrendItem[]>(() => {
  const list = [...currentItems.value]
  if (!sortProp.value || !sortOrder.value) return list

  const factor = sortOrder.value === 'ascending' ? 1 : -1
  const prop = sortProp.value as keyof TrendItem
  list.sort((a, b) => {
    const va = Number((a as any)[prop] ?? 0)
    const vb = Number((b as any)[prop] ?? 0)
    if (va === vb) return 0
    return va > vb ? factor : -factor
  })
  return list
})

const totalItems = computed(() => sortedItems.value.length)

const pagedItems = computed<TrendItem[]>(() => {
  const start = (currentPage.value - 1) * pageSize.value
  const end = start + pageSize.value
  return sortedItems.value.slice(start, end)
})

watch(activeMarket, () => {
  currentPage.value = 1
  sortProp.value = ''
  sortOrder.value = ''
})

watch(activeDays, () => {
  if (activeMode.value !== 'hot') return
  currentPage.value = 1
  fetchTrend()
})

watch(activeMode, () => {
  currentPage.value = 1
  sortProp.value = ''
  sortOrder.value = ''
  fetchTrend()
})

watch(pageSize, () => {
  currentPage.value = 1
})

function pctClass(v: number) {
  return v > 0 ? 'pct-up' : v < 0 ? 'pct-down' : ''
}

function scoreStars(score: number): number {
  if (score >= 50) return 5
  if (score >= 30) return 4
  if (score >= 15) return 3
  if (score >= 5) return 2
  if (score > 0) return 1
  return 0
}

function formatCap(v: number, market: string): string {
  if (!v) return '-'
  if (market === 'US') {
    if (v >= 10000) return (v / 10000).toFixed(2) + '万亿美元'
    if (v >= 1000) return v.toFixed(0) + '亿美元'
    if (v >= 100) return v.toFixed(1) + '亿美元'
    return v.toFixed(2) + '亿美元'
  }
  if (v >= 10000) return (v / 10000).toFixed(2) + '万亿'
  if (v >= 1000) return v.toFixed(0) + '亿'
  if (v >= 100) return v.toFixed(1) + '亿'
  return v.toFixed(2) + '亿'
}


function formatAmount(v: number, market: string): string {
  if (!v) return '-'
  const unit = market === 'US' ? '美元' : market === 'HK' ? '港币' : '元'
  if (v >= 1_0000_0000) return (v / 1_0000_0000).toFixed(1) + '亿' + unit
  if (v >= 1_0000_0000 / 10) return (v / 1_0000_0000).toFixed(2) + '亿' + unit
  if (v >= 1_0000) return (v / 1_0000).toFixed(0) + '万' + unit
  return v.toFixed(0) + unit
}

function formatDate(v: string): string {
  if (!v) return '-'
  return String(v).slice(0, 10)
}

function chipItems(row: TrendItem): CapitalSignalItem[] {
  const items = Array.isArray(row.capital_signal_items) ? row.capital_signal_items : []
  if (items.length) return items
  if (row.inst_text) {
    const label = row.inst_label || (row.market === "US" ? "内部人" : row.market === "HK" ? "南向" : "北向")
    const delta = Number(row.inst_delta_abs || row.inst_change_pp || 0)
    const value = Math.abs(delta) >= 0.005 ? formatSigned(delta, row.inst_metric_unit === "percent" ? 3 : 2) + "%" : "持平"
    return [{ label, value, direction: row.inst_direction, tooltip: row.inst_text }]
  }
  return []
}

function visibleChipItems(row: TrendItem): CapitalSignalItem[] {
  return chipItems(row).slice(0, 2)
}

function hiddenChipCount(row: TrendItem): number {
  return Math.max(0, chipItems(row).length - visibleChipItems(row).length)
}

function coverageClass(row: TrendItem): string {
  const status = String(row.capital_signal_coverage?.status || '')
  if (status === 'complete') return 'coverage-complete'
  if (status === 'partial') return 'coverage-partial'
  if (status === 'missing') return 'coverage-missing'
  return 'coverage-unknown'
}

function coverageText(row: TrendItem): string {
  return row.capital_signal_coverage?.text || (chipItems(row).length ? '部分' : '缺失')
}

function coverageTooltip(row: TrendItem): string {
  const reason = row.capital_signal_coverage?.reason || ''
  const latest = row.capital_signal_coverage?.latest_asof || ''
  const chip = chipTooltip(row)
  return [chip, reason, latest ? `最新披露: ${latest}` : ''].filter(Boolean).join(' / ')
}

function formatSigned(v: number, digits = 2): string {
  const n = Number(v || 0)
  return `${n > 0 ? "+" : ""}${n.toFixed(digits)}`
}

function chipClass(item: CapitalSignalItem): string {
  const dir = String(item.direction || "")
  const score = Number(item.score || 0)
  if (dir.includes("增") || dir.includes("流入") || dir.includes("集中") || score > 0) return "chip-up"
  if (dir.includes("减") || dir.includes("流出") || dir.includes("分散") || score < 0) return "chip-down"
  return "chip-flat"
}

function chipTooltip(row: TrendItem): string {
  const parts = chipItems(row)
    .map((item) => item.tooltip || `${item.label}${item.value}${item.asof ? ` (${item.asof})` : ""}`)
    .filter(Boolean)
  if (!parts.length && row.inst_text) parts.push(row.inst_text)
  return parts.join(" / ")
}

function handleSortChange(payload: any) {
  const prop = payload?.prop
  const order = payload?.order
  sortProp.value = (prop as string) || ''
  sortOrder.value = (order as any) || ''
  currentPage.value = 1
}

onMounted(fetchTrend)
</script>

<template>
  <div class="trend-view">
    <div class="toolbar">
      <div class="mode-group">
        <button class="mode-btn" :class="{ active: activeMode === 'hot' }" @click="activeMode = 'hot'">热门趋势</button>
        <button class="mode-btn" :class="{ active: activeMode === 'slow' }" @click="activeMode = 'slow'">慢趋势机构票</button>
      </div>
      <div class="tab-group">
        <button
          v-for="m in markets" :key="m"
          class="tab-btn" :class="{ active: activeMarket === m }"
          @click="activeMarket = m"
        >{{ m }}</button>
      </div>
      <div v-if="activeMode === 'hot'" class="days-group">
        <button
          v-for="d in daysOptions" :key="d"
          class="days-btn" :class="{ active: activeDays === d }"
          @click="activeDays = d"
        >{{ d }}天</button>
      </div>
      <div v-else class="slow-criteria">20日≥12% · 60日≥30% · 高市值/高成交额</div>
      <div class="meta-text">{{ activeMode === 'slow' ? '慢趋势候选' : '共' }} {{ totalItems }} 个标的</div>
    </div>

    <el-table :data="pagedItems" v-loading="loading" size="small" @sort-change="handleSortChange">
      <el-table-column prop="rank" label="#" width="46" />
      <el-table-column prop="symbol" label="代码" width="98" />
      <el-table-column prop="name" label="名称" width="110" />
      <el-table-column label="现价" width="78">
        <template #default="{ row }">{{ row.price ? row.price.toFixed(2) : '-' }}</template>
      </el-table-column>
      <el-table-column prop="period_change" :label="activeMode === 'slow' ? '60日涨幅' : 'N日涨幅'" width="96" sortable="custom">
        <template #default="{ row }">
          <span :class="pctClass(row.period_change)">
            {{ row.period_change > 0 ? '+' : '' }}{{ row.period_change.toFixed(1) }}%
          </span>
        </template>
      </el-table-column>
      <el-table-column v-if="activeMode === 'slow'" prop="return_20d" label="20日涨幅" width="96" sortable="custom">
        <template #default="{ row }">
          <span :class="pctClass(row.return_20d)">
            {{ row.return_20d > 0 ? '+' : '' }}{{ row.return_20d.toFixed(1) }}%
          </span>
        </template>
      </el-table-column>
      <el-table-column prop="market_cap" label="市值" width="118" sortable="custom">
        <template #default="{ row }">{{ formatCap(row.market_cap, row.market) }}</template>
      </el-table-column>
      <el-table-column v-if="activeMode === 'slow'" prop="amount" label="成交额" width="116" sortable="custom">
        <template #default="{ row }">{{ formatAmount(row.amount, row.market) }}</template>
      </el-table-column>
      <el-table-column prop="capital_signal_score" label="筹码" width="236" sortable="custom">
        <template #default="{ row }">
          <el-tooltip :content="coverageTooltip(row)" placement="top" :disabled="!coverageTooltip(row)">
            <div v-if="chipItems(row).length" class="chip-wrap">
              <span v-for="item in visibleChipItems(row)" :key="`${item.label}-${item.value}`" class="capital-chip" :class="chipClass(item)">
                <span class="chip-label">{{ item.label }}</span><span>{{ item.value }}</span>
              </span>
              <span v-if="hiddenChipCount(row)" class="chip-more">+{{ hiddenChipCount(row) }}</span>
              <span class="coverage-dot" :class="coverageClass(row)">{{ coverageText(row) }}</span>
            </div>
            <span v-else class="coverage-empty" :class="coverageClass(row)">{{ coverageText(row) }}</span>
          </el-tooltip>
        </template>
      </el-table-column>
      <el-table-column label="催化标签" min-width="168">
        <template #default="{ row }">
          <span class="catalyst-text">{{ row.catalyst_tags || "-" }}</span>
        </template>
      </el-table-column>
      <el-table-column prop="days_on_list" label="上榜天数" width="88" sortable="custom">
        <template #default="{ row }">{{ row.days_on_list }}d</template>
      </el-table-column>
      <el-table-column prop="score" label="评分" width="110" sortable="custom">
        <template #default="{ row }">
          <div class="score-wrap">
            <span class="stars">{{ "★".repeat(scoreStars(row.score)) }}{{ "☆".repeat(5 - scoreStars(row.score)) }}</span>
            <span class="score-num">{{ row.score.toFixed(1) }}</span>
          </div>
        </template>
      </el-table-column>
      <el-table-column label="价格日期" width="112">
        <template #default="{ row }">{{ formatDate(row.price_date) }}</template>
      </el-table-column>
    </el-table>

    <div class="pager-wrap">
      <el-pagination
        v-model:current-page="currentPage"
        v-model:page-size="pageSize"
        :page-sizes="[20, 50, 100]"
        layout="total, sizes, prev, pager, next"
        :total="totalItems"
        background
      />
    </div>
  </div>
</template>

<style scoped>
.trend-view { max-width: 1440px; }
.toolbar {
  display: flex;
  align-items: center;
  gap: 24px;
  margin-bottom: 16px;
}
.mode-group, .tab-group, .days-group {
  display: flex;
  gap: 2px;
  background: var(--bg-hover);
  border-radius: var(--radius);
  padding: 2px;
}
.mode-btn, .tab-btn, .days-btn {
  padding: 5px 12px;
  font-size: 13px;
  border-radius: 4px;
  color: var(--text-secondary);
  transition: all 0.15s;
}
.mode-btn.active, .tab-btn.active, .days-btn.active {
  background: white;
  color: var(--text);
  font-weight: 500;
  box-shadow: 0 1px 2px rgba(0,0,0,0.06);
}
.meta-text {
  font-size: 12px;
  color: var(--text-secondary);
}
.slow-criteria {
  padding: 4px 10px;
  border-radius: 999px;
  font-size: 12px;
  font-weight: 600;
  color: #8a4b00;
  background: rgba(245, 158, 11, 0.12);
  white-space: nowrap;
}
.catalyst-text {
  font-size: 12px;
  color: var(--text-secondary);
}
.chip-wrap {
  display: flex;
  align-items: center;
  gap: 6px;
  min-width: 0;
}
.capital-chip {
  display: inline-flex;
  align-items: center;
  gap: 2px;
  max-width: 104px;
  padding: 2px 6px;
  border-radius: 999px;
  font-size: 11px;
  font-weight: 600;
  line-height: 1.25;
  white-space: nowrap;
}
.chip-label {
  opacity: 0.82;
}
.chip-up {
  color: #b42318;
  background: rgba(180, 35, 24, 0.08);
}
.chip-down {
  color: #027a48;
  background: rgba(2, 122, 72, 0.08);
}
.chip-flat {
  color: var(--text-secondary);
  background: var(--bg-hover);
}
.chip-more {
  display: inline-flex;
  align-items: center;
  padding: 2px 5px;
  border-radius: 999px;
  font-size: 11px;
  font-weight: 600;
  color: var(--text-secondary);
  background: rgba(120, 119, 116, 0.12);
}
.inst-empty {
  font-size: 12px;
  color: var(--text-secondary);
}
.coverage-dot {
  display: inline-flex;
  align-items: center;
  padding: 2px 5px;
  border-radius: 999px;
  font-size: 10px;
  font-weight: 700;
  white-space: nowrap;
}
.coverage-empty {
  display: inline-flex;
  align-items: center;
  padding: 2px 7px;
  border-radius: 999px;
  font-size: 11px;
  font-weight: 700;
}
.coverage-complete {
  color: #155eef;
  background: rgba(21, 94, 239, 0.08);
}
.coverage-partial {
  color: #b54708;
  background: rgba(181, 71, 8, 0.10);
}
.coverage-missing, .coverage-unknown {
  color: var(--text-secondary);
  background: rgba(120, 119, 116, 0.12);
}
.score-wrap {
  display: flex;
  align-items: center;
  gap: 6px;
}
.stars {
  font-size: 13px;
  color: #F2C94C;
  letter-spacing: 0;
}
.score-num {
  font-size: 12px;
  color: var(--text-secondary);
}
.pager-wrap {
  margin-top: 12px;
  display: flex;
  justify-content: flex-end;
}
</style>

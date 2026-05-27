<script setup lang="ts">
import { ref, onMounted, watch, computed } from "vue"
import { ElMessage } from "element-plus"
import api from "../api"

interface HeatmapItem {
  rank: number
  symbol: string
  raw_symbol?: string
  name: string
  close: number
  change_pct: number
  amount: number
  market_cap: number
  catalyst_tags: string
  industry_label?: string
  trend_score?: number
  signal_strength?: number
  days_on_list?: number
  price_date?: string
}

const markets = ["CN", "HK", "US"]
const trendWindows = [1, 3, 7, 30, 90, 180]
const activeMarket = ref("CN")
const selectedDate = ref("")
const tradingDays = ref<string[]>([])
const items = ref<HeatmapItem[]>([])
const trendItems = ref<HeatmapItem[]>([])
const trendDays = ref(1)
const loading = ref(false)
const trendLoading = ref(false)

const trendQuota = computed(() => activeMarket.value === "HK" ? 7 : 10)
const quotaText = computed(() => activeMarket.value === "HK" ? "Top7" : "Top10")
const trendQuotaRule = computed(() => {
  if (activeMarket.value === "CN") return "3 Large + 4 Mid + 3 Small"
  if (activeMarket.value === "HK") return "3 Large + 3 Mid + 1 Small"
  return "3 Large + 5 Mid + 2 Small"
})

async function fetchTradingDays(month?: string) {
  const m = month || new Date().toISOString().slice(0, 7)
  try {
    const res = await api.get("/trading-days", { params: { month: m } })
    tradingDays.value = res.data.days || []
    if (!selectedDate.value && tradingDays.value.length) {
      selectedDate.value = tradingDays.value[tradingDays.value.length - 1]
    }
  } catch {}
}

async function fetchHeatmap() {
  if (!selectedDate.value) return
  loading.value = true
  try {
    const res = await api.get("/heatmap", {
      params: { date: selectedDate.value, market: activeMarket.value }
    })
    items.value = res.data.items || []
  } catch {
    ElMessage.error("加载Heatmap日榜失败")
  } finally {
    loading.value = false
  }
}

async function fetchTrendPush() {
  if (!selectedDate.value) return
  trendLoading.value = true
  try {
    const res = await api.get("/heatmap/trend-push", {
      params: { date: selectedDate.value, days: trendDays.value, market: activeMarket.value },
      timeout: 60000,
    })
    trendItems.value = res.data.items || []
  } catch (e: any) {
    trendItems.value = []
    ElMessage.error(e?.response?.data?.detail || "加载Trend算法榜失败")
  } finally {
    trendLoading.value = false
  }
}

function fetchBoth() {
  fetchHeatmap()
  fetchTrendPush()
}

function isDisabledDate(date: Date) {
  const dateStr = date.toISOString().slice(0, 10)
  return !tradingDays.value.includes(dateStr)
}

function formatAmount(v: number, market = activeMarket.value) {
  if (!v) return "-"
  const unit = market === "US" ? "美元" : market === "HK" ? "港币" : "元"
  if (v >= 1e8) return (v / 1e8).toFixed(1) + "亿" + unit
  if (v >= 1e4) return (v / 1e4).toFixed(0) + "万" + unit
  return v.toFixed(0) + unit
}

function formatCap(v: number, market = activeMarket.value) {
  if (!v) return "-"
  const unit = market === "US" ? "亿美元" : market === "HK" ? "亿港币" : "亿"
  if (v >= 10000) return (v / 10000).toFixed(2) + "万" + unit
  if (v >= 1000) return v.toFixed(0) + unit
  if (v >= 100) return v.toFixed(1) + unit
  return v.toFixed(2) + unit
}

function pctClass(v: number) {
  return v > 0 ? "pct-up" : v < 0 ? "pct-down" : ""
}

function formatPct(v: number) {
  return `${v > 0 ? "+" : ""}${Number(v || 0).toFixed(2)}%`
}

function formatDate(v?: string) {
  return v ? String(v).slice(0, 10) : "-"
}

function rowTags(row: HeatmapItem) {
  return row.catalyst_tags || row.industry_label || "-"
}

function handleDateChange(val: string) {
  selectedDate.value = val
  fetchBoth()
}

onMounted(async () => {
  await fetchTradingDays()
  fetchBoth()
})

watch(activeMarket, fetchBoth)
watch(trendDays, fetchTrendPush)
</script>

<template>
  <div class="heatmap-view">
    <div class="toolbar">
      <div class="tab-group market-tabs">
        <button
          v-for="m in markets" :key="m"
          class="tab-btn" :class="{ active: activeMarket === m }"
          @click="activeMarket = m"
        >{{ m }}</button>
      </div>
      <el-date-picker
        :model-value="selectedDate"
        type="date"
        format="YYYY-MM-DD"
        value-format="YYYY-MM-DD"
        placeholder="选择日期"
        size="small"
        :disabled-date="isDisabledDate"
        @update:model-value="handleDateChange"
        style="width: 150px"
      />
      <div class="days-group">
        <span class="days-label">Trend窗口</span>
        <button
          v-for="d in trendWindows"
          :key="d"
          class="days-btn"
          :class="{ active: trendDays === d }"
          @click="trendDays = d"
        >{{ d }}天</button>
      </div>
    </div>

    <div class="compare-grid">
      <section class="panel-card heatmap-panel">
        <div class="panel-head">
          <div>
            <h2>Heatmap日榜</h2>
            <p>{{ selectedDate || "-" }} · 实际TG日榜落库</p>
          </div>
          <span class="count-pill">{{ items.length }}只</span>
        </div>

        <el-table :data="items" v-loading="loading" size="small" class="compact-table">
          <el-table-column prop="rank" label="#" width="42" align="center" />
          <el-table-column label="标的" min-width="138" show-overflow-tooltip>
            <template #default="{ row }">
              <div class="stock-cell">
                <span class="symbol">{{ row.symbol }}</span>
                <span class="name">{{ row.name }}</span>
              </div>
            </template>
          </el-table-column>
          <el-table-column label="涨幅" width="78" align="right">
            <template #default="{ row }">
              <span :class="pctClass(row.change_pct)">{{ formatPct(row.change_pct) }}</span>
            </template>
          </el-table-column>
          <el-table-column label="成交/市值" width="122" align="right">
            <template #default="{ row }">
              <div class="metric-stack">
                <span>{{ formatAmount(row.amount, activeMarket) }}</span>
                <span>{{ formatCap(row.market_cap, activeMarket) }}</span>
              </div>
            </template>
          </el-table-column>
          <el-table-column label="催化标签" min-width="150" show-overflow-tooltip>
            <template #default="{ row }">
              <span class="catalyst-text">{{ rowTags(row) }}</span>
            </template>
          </el-table-column>
        </el-table>
      </section>

      <section class="panel-card trend-panel">
        <div class="panel-head">
          <div>
            <h2>Trend算法榜</h2>
            <p>{{ selectedDate || "-" }} · {{ trendDays }}天窗口 · {{ quotaText }} · {{ trendQuotaRule }}</p>
          </div>
          <span class="count-pill accent">{{ trendItems.length }}/{{ trendQuota }}</span>
        </div>

        <el-table :data="trendItems" v-loading="trendLoading" size="small" class="compact-table">
          <el-table-column prop="rank" label="#" width="42" align="center" />
          <el-table-column label="标的" min-width="138" show-overflow-tooltip>
            <template #default="{ row }">
              <div class="stock-cell">
                <span class="symbol">{{ row.symbol }}</span>
                <span class="name">{{ row.name }}</span>
              </div>
            </template>
          </el-table-column>
          <el-table-column :label="`${trendDays}天`" width="78" align="right">
            <template #default="{ row }">
              <span :class="pctClass(row.change_pct)">{{ formatPct(row.change_pct) }}</span>
            </template>
          </el-table-column>
          <el-table-column label="市值/分数" width="122" align="right">
            <template #default="{ row }">
              <div class="metric-stack">
                <span>{{ formatCap(row.market_cap, activeMarket) }}</span>
                <span>分 {{ row.trend_score ? row.trend_score.toFixed(1) : "-" }}</span>
              </div>
            </template>
          </el-table-column>
          <el-table-column label="主线/价格日" min-width="164" show-overflow-tooltip>
            <template #default="{ row }">
              <div class="trend-meta">
                <span class="catalyst-text">{{ rowTags(row) }}</span>
                <span>{{ formatDate(row.price_date) }} · {{ row.days_on_list || 1 }}d</span>
              </div>
            </template>
          </el-table-column>
        </el-table>
      </section>
    </div>
  </div>
</template>

<style scoped>
.heatmap-view { max-width: 1560px; }
.toolbar {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 16px;
  flex-wrap: wrap;
}
.tab-group, .days-group {
  display: flex;
  align-items: center;
  gap: 2px;
  background: var(--bg-hover);
  border-radius: var(--radius);
  padding: 2px;
}
.tab-btn, .days-btn {
  padding: 5px 11px;
  font-size: 13px;
  border-radius: 4px;
  color: var(--text-secondary);
  transition: all 0.15s;
}
.tab-btn.active, .days-btn.active {
  background: white;
  color: var(--text);
  font-weight: 600;
  box-shadow: 0 1px 2px rgba(0,0,0,0.06);
}
.days-label {
  padding: 0 8px;
  font-size: 12px;
  color: var(--text-secondary);
  font-weight: 700;
}
.compare-grid {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
  gap: 16px;
  align-items: start;
}
.panel-card {
  min-width: 0;
  padding: 14px;
  border: 1px solid var(--border);
  border-radius: 14px;
  background: linear-gradient(180deg, #fff 0%, #fbfbfa 100%);
  box-shadow: 0 1px 2px rgba(0,0,0,0.03);
}
.trend-panel {
  background: linear-gradient(180deg, #fffaf0 0%, #fbfbfa 88%);
}
.panel-head {
  min-height: 46px;
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 12px;
}
.panel-head h2 {
  margin: 0 0 4px;
  font-size: 16px;
  line-height: 1.25;
}
.panel-head p {
  margin: 0;
  font-size: 12px;
  color: var(--text-secondary);
}
.count-pill {
  flex: 0 0 auto;
  padding: 3px 8px;
  border-radius: 999px;
  font-size: 12px;
  font-weight: 700;
  color: var(--text-secondary);
  background: var(--bg-hover);
}
.count-pill.accent {
  color: #8a4b00;
  background: rgba(245, 158, 11, 0.14);
}
.compact-table :deep(.el-table__cell) { padding: 7px 0; }
.stock-cell, .metric-stack, .trend-meta {
  display: flex;
  flex-direction: column;
  gap: 2px;
  min-width: 0;
}
.symbol {
  font-weight: 700;
  color: var(--text);
  line-height: 1.15;
}
.name, .metric-stack span:nth-child(2), .trend-meta span:nth-child(2) {
  font-size: 12px;
  color: var(--text-secondary);
  line-height: 1.2;
}
.catalyst-text {
  font-size: 12px;
  color: var(--text-secondary);
}
@media (max-width: 1240px) {
  .compare-grid { grid-template-columns: 1fr; }
}
@media (max-width: 760px) {
  .heatmap-view { max-width: 100%; }
  .toolbar { gap: 8px; }
  .tab-btn, .days-btn { padding: 5px 8px; }
  .days-group { max-width: 100%; overflow-x: auto; }
}
</style>

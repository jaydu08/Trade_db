<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import api from '../api'

type HoldMode = 'active' | 'history'

interface TradeItem {
  id: number
  symbol: string
  name: string
  market: string
  entry_price: number
  entry_date: string
  target_days: number | null
  entry_reason: string
  days_held: number
  hold_days?: number
  status?: string
  exit_price?: number | null
  exit_date?: string
  pnl_pct: number | null
  review_status?: string
  review_attempts?: number
  review_source?: string
  last_reviewed_at?: string
  review_text: string
  review_error?: string
}

interface HistorySummary {
  total: number
  wins: number
  losses: number
  win_rate: number
  avg_pnl: number
  avg_hold_days: number
  best_trade: TradeItem | null
  worst_trade: TradeItem | null
}

const activeMode = ref<HoldMode>('active')
const items = ref<TradeItem[]>([])
const historyItems = ref<TradeItem[]>([])
const historySummary = ref<HistorySummary>({
  total: 0,
  wins: 0,
  losses: 0,
  win_rate: 0,
  avg_pnl: 0,
  avg_hold_days: 0,
  best_trade: null,
  worst_trade: null,
})

const loading = ref(false)
const historyLoading = ref(false)
const historyLoaded = ref(false)
const buyDialogVisible = ref(false)
const buyForm = ref({ symbol: '', market: '', target_days: 7, reason: '' })
const reviewTarget = ref<{ mode: HoldMode; id: number } | null>(null)

const historyPage = ref(1)
const historyPageSize = ref(20)
const historyTotal = ref(0)
const historyMarket = ref('')
const historyReviewStatus = ref('')
const historySymbol = ref('')
const historySort = ref('exit_date_desc')

async function fetchHolds() {
  loading.value = true
  try {
    const res = await api.get('/holds')
    items.value = res.data.items || []
  } catch {
    ElMessage.error('加载持仓失败')
  } finally {
    loading.value = false
  }
}

async function fetchHistory() {
  historyLoading.value = true
  try {
    const res = await api.get('/trades/history', {
      params: {
        page: historyPage.value,
        page_size: historyPageSize.value,
        market: historyMarket.value,
        symbol: historySymbol.value,
        review_status: historyReviewStatus.value,
        sort: historySort.value,
      },
    })
    historyItems.value = res.data.items || []
    historyTotal.value = res.data.total || 0
    historySummary.value = res.data.summary || historySummary.value
    historyLoaded.value = true
  } catch {
    ElMessage.error('加载历史交易失败')
  } finally {
    historyLoading.value = false
  }
}

async function handleBuy() {
  if (!buyForm.value.symbol) return
  try {
    const res = await api.post('/trade/buy', buyForm.value)
    ElMessage.success(res.data.message || '建仓成功')
    buyDialogVisible.value = false
    buyForm.value = { symbol: '', market: '', target_days: 7, reason: '' }
    fetchHolds()
  } catch (e: any) {
    ElMessage.error(e.response?.data?.detail || '建仓失败')
  }
}

async function handleSell(item: TradeItem) {
  try {
    await ElMessageBox.confirm(`确认平仓 ${item.name || item.symbol}?`, '平仓')
    const res = await api.post('/trade/sell', { symbol: item.symbol, market: item.market })
    ElMessage.success(res.data.message || '平仓成功')
    fetchHolds()
    if (historyLoaded.value) fetchHistory()
  } catch {}
}

function switchMode(mode: HoldMode) {
  activeMode.value = mode
}

function applyHistoryFilters() {
  historyPage.value = 1
  fetchHistory()
}

function resetHistoryFilters() {
  historyMarket.value = ''
  historyReviewStatus.value = ''
  historySymbol.value = ''
  historySort.value = 'exit_date_desc'
  historyPage.value = 1
  fetchHistory()
}

function handleHistorySortChange(payload: any) {
  const prop = payload?.prop || 'exit_date'
  const order = payload?.order || 'descending'
  const suffix = order === 'ascending' ? 'asc' : 'desc'
  if (prop === 'pnl_pct') historySort.value = `pnl_${suffix}`
  else if (prop === 'hold_days') historySort.value = `hold_days_${suffix}`
  else if (prop === 'entry_date') historySort.value = `entry_date_${suffix}`
  else historySort.value = `exit_date_${suffix}`
  historyPage.value = 1
  fetchHistory()
}

function toggleReview(mode: HoldMode, id: number) {
  if (reviewTarget.value?.mode === mode && reviewTarget.value?.id === id) {
    reviewTarget.value = null
  } else {
    reviewTarget.value = { mode, id }
  }
}

const currentReviewItem = computed(() => {
  if (!reviewTarget.value) return null
  const source = reviewTarget.value.mode === 'history' ? historyItems.value : items.value
  return source.find((item) => item.id === reviewTarget.value?.id) || null
})

function pctClass(pct: number | null | undefined) {
  if (!pct) return ''
  return pct > 0 ? 'pct-up' : pct < 0 ? 'pct-down' : ''
}

function formatPct(pct: number | null | undefined): string {
  if (pct == null) return '-'
  return `${pct > 0 ? '+' : ''}${Number(pct).toFixed(2)}%`
}

function formatPrice(v: number | null | undefined): string {
  if (v == null || Number.isNaN(Number(v))) return '-'
  return Number(v).toFixed(2)
}

function formatDate(v: string | undefined): string {
  if (!v) return '-'
  return String(v).slice(0, 10)
}

function reviewText(status?: string): string {
  const s = String(status || 'PENDING').toUpperCase()
  if (s === 'DONE') return '已复盘'
  if (s === 'FAILED') return '失败'
  return '待复盘'
}

function reviewClass(status?: string): string {
  const s = String(status || 'PENDING').toUpperCase()
  if (s === 'DONE') return 'review-done'
  if (s === 'FAILED') return 'review-failed'
  return 'review-pending'
}

watch(activeMode, (mode) => {
  reviewTarget.value = null
  if (mode === 'history' && !historyLoaded.value) fetchHistory()
})

watch([historyPage, historyPageSize], () => {
  if (activeMode.value === 'history') fetchHistory()
})

onMounted(fetchHolds)
</script>

<template>
  <div class="holds-view">
    <div class="toolbar">
      <div class="mode-group">
        <button class="mode-btn" :class="{ active: activeMode === 'active' }" @click="switchMode('active')">当前持仓</button>
        <button class="mode-btn" :class="{ active: activeMode === 'history' }" @click="switchMode('history')">历史交易</button>
      </div>
      <el-button type="primary" size="small" @click="buyDialogVisible = true">+ 建仓</el-button>
    </div>

    <template v-if="activeMode === 'active'">
      <el-table :data="items" v-loading="loading" size="small" empty-text="暂无当前持仓">
        <el-table-column prop="symbol" label="代码" width="86" fixed />
        <el-table-column prop="name" label="名称" width="110" fixed />
        <el-table-column prop="market" label="市场" width="58" />
        <el-table-column label="建仓价" width="86" align="right">
          <template #default="{ row }">{{ formatPrice(row.entry_price) }}</template>
        </el-table-column>
        <el-table-column label="盈亏%" width="86" align="right" sortable prop="pnl_pct">
          <template #default="{ row }">
            <span :class="pctClass(row.pnl_pct)">{{ formatPct(row.pnl_pct) }}</span>
          </template>
        </el-table-column>
        <el-table-column label="持仓" width="88">
          <template #default="{ row }">
            <span>{{ row.days_held }}d</span>
            <span class="target-days"> / {{ row.target_days || '∞' }}d</span>
          </template>
        </el-table-column>
        <el-table-column prop="entry_date" label="建仓日" width="104" />
        <el-table-column label="理由" min-width="180">
          <template #default="{ row }"><span class="reason-text">{{ row.entry_reason || '-' }}</span></template>
        </el-table-column>
        <el-table-column label="操作" width="110" fixed="right">
          <template #default="{ row }">
            <button class="action-btn sell" @click="handleSell(row)">平仓</button>
            <button v-if="row.review_text" class="action-btn" @click="toggleReview('active', row.id)">
              {{ reviewTarget?.mode === 'active' && reviewTarget?.id === row.id ? '收起' : '复盘' }}
            </button>
          </template>
        </el-table-column>
      </el-table>
    </template>

    <template v-else>
      <div class="summary-grid">
        <div class="summary-card">
          <span class="summary-label">历史交易</span>
          <strong>{{ historySummary.total }}</strong>
        </div>
        <div class="summary-card">
          <span class="summary-label">胜率</span>
          <strong>{{ historySummary.win_rate.toFixed(1) }}%</strong>
        </div>
        <div class="summary-card">
          <span class="summary-label">平均盈亏</span>
          <strong :class="pctClass(historySummary.avg_pnl)">{{ formatPct(historySummary.avg_pnl) }}</strong>
        </div>
        <div class="summary-card">
          <span class="summary-label">平均持有</span>
          <strong>{{ historySummary.avg_hold_days }}d</strong>
        </div>
      </div>

      <div class="history-filters">
        <el-input
          v-model="historySymbol"
          size="small"
          clearable
          placeholder="代码/名称"
          class="symbol-filter"
          @keyup.enter="applyHistoryFilters"
          @clear="applyHistoryFilters"
        />
        <el-select v-model="historyMarket" size="small" placeholder="市场" clearable class="tiny-select" @change="applyHistoryFilters">
          <el-option label="A股" value="CN" />
          <el-option label="港股" value="HK" />
          <el-option label="美股" value="US" />
          <el-option label="商品" value="CF" />
        </el-select>
        <el-select v-model="historyReviewStatus" size="small" placeholder="复盘状态" clearable class="status-select" @change="applyHistoryFilters">
          <el-option label="待复盘" value="PENDING" />
          <el-option label="已复盘" value="DONE" />
          <el-option label="失败" value="FAILED" />
        </el-select>
        <el-button size="small" @click="applyHistoryFilters">查询</el-button>
        <el-button size="small" @click="resetHistoryFilters">重置</el-button>
      </div>

      <el-table
        :data="historyItems"
        v-loading="historyLoading"
        size="small"
        empty-text="暂无历史交易"
        @sort-change="handleHistorySortChange"
      >
        <el-table-column prop="symbol" label="代码" width="86" fixed />
        <el-table-column prop="name" label="名称" width="110" fixed />
        <el-table-column prop="market" label="市场" width="58" />
        <el-table-column prop="entry_date" label="建仓日" width="104" sortable="custom">
          <template #default="{ row }">{{ formatDate(row.entry_date) }}</template>
        </el-table-column>
        <el-table-column prop="exit_date" label="平仓日" width="104" sortable="custom">
          <template #default="{ row }">{{ formatDate(row.exit_date) }}</template>
        </el-table-column>
        <el-table-column prop="hold_days" label="持有" width="74" sortable="custom">
          <template #default="{ row }">{{ row.hold_days ?? row.days_held }}d</template>
        </el-table-column>
        <el-table-column label="买入" width="84" align="right">
          <template #default="{ row }">{{ formatPrice(row.entry_price) }}</template>
        </el-table-column>
        <el-table-column label="卖出" width="84" align="right">
          <template #default="{ row }">{{ formatPrice(row.exit_price) }}</template>
        </el-table-column>
        <el-table-column prop="pnl_pct" label="盈亏" width="92" align="right" sortable="custom">
          <template #default="{ row }"><span :class="pctClass(row.pnl_pct)">{{ formatPct(row.pnl_pct) }}</span></template>
        </el-table-column>
        <el-table-column label="复盘" width="90">
          <template #default="{ row }">
            <span class="review-badge" :class="reviewClass(row.review_status)">{{ reviewText(row.review_status) }}</span>
          </template>
        </el-table-column>
        <el-table-column label="理由" min-width="180">
          <template #default="{ row }"><span class="reason-text">{{ row.entry_reason || '-' }}</span></template>
        </el-table-column>
        <el-table-column label="操作" width="92" fixed="right">
          <template #default="{ row }">
            <button v-if="row.review_text || row.review_error" class="action-btn" @click="toggleReview('history', row.id)">
              {{ reviewTarget?.mode === 'history' && reviewTarget?.id === row.id ? '收起' : '详情' }}
            </button>
            <span v-else class="muted-text">-</span>
          </template>
        </el-table-column>
      </el-table>

      <div class="pager-wrap">
        <el-pagination
          v-model:current-page="historyPage"
          v-model:page-size="historyPageSize"
          :page-sizes="[10, 20, 50, 100]"
          layout="total, sizes, prev, pager, next"
          :total="historyTotal"
          background
        />
      </div>
    </template>

    <div v-if="currentReviewItem" class="review-panel">
      <div class="review-title">
        <span>{{ currentReviewItem.name || currentReviewItem.symbol }} 复盘</span>
        <span class="review-badge" :class="reviewClass(currentReviewItem.review_status)">{{ reviewText(currentReviewItem.review_status) }}</span>
      </div>
      <pre class="review-text">{{ currentReviewItem.review_text || currentReviewItem.review_error || '暂无复盘内容' }}</pre>
    </div>

    <el-dialog v-model="buyDialogVisible" title="模拟建仓" width="380px">
      <div class="add-form">
        <div class="form-row">
          <label>标的代码</label>
          <el-input v-model="buyForm.symbol" placeholder="如 NVDA / 600519" size="small" />
        </div>
        <div class="form-row">
          <label>目标天数</label>
          <el-input-number v-model="buyForm.target_days" :min="1" :max="90" size="small" />
        </div>
        <div class="form-row">
          <label>建仓理由</label>
          <el-input v-model="buyForm.reason" type="textarea" :rows="2" placeholder="可选" size="small" />
        </div>
      </div>
      <template #footer>
        <el-button size="small" @click="buyDialogVisible = false">取消</el-button>
        <el-button type="primary" size="small" @click="handleBuy">确认建仓</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<style scoped>
.holds-view { max-width: 1280px; }
.toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 16px;
}
.mode-group {
  display: flex;
  gap: 2px;
  padding: 2px;
  border-radius: var(--radius);
  background: var(--bg-hover);
}
.mode-btn {
  padding: 5px 12px;
  font-size: 13px;
  border-radius: 4px;
  color: var(--text-secondary);
  transition: all 0.15s;
}
.mode-btn.active {
  background: #fff;
  color: var(--text);
  font-weight: 600;
  box-shadow: 0 1px 2px rgba(0,0,0,0.06);
}
.summary-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(130px, 1fr));
  gap: 10px;
  margin-bottom: 14px;
}
.summary-card {
  padding: 12px 14px;
  border: 1px solid var(--border);
  border-radius: 10px;
  background: linear-gradient(180deg, #fff 0%, #fbfbfa 100%);
}
.summary-label {
  display: block;
  margin-bottom: 4px;
  font-size: 12px;
  color: var(--text-secondary);
}
.summary-card strong {
  font-size: 20px;
  letter-spacing: -0.02em;
}
.history-filters {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 12px;
}
.symbol-filter { width: 160px; }
.tiny-select { width: 96px; }
.status-select { width: 116px; }
.reason-text { font-size: 12px; color: var(--text-secondary); }
.target-days, .muted-text { color: var(--text-secondary); font-size: 11px; }
.action-btn {
  font-size: 12px;
  color: var(--accent);
  padding: 2px 6px;
  border-radius: 4px;
  margin-right: 4px;
}
.action-btn:hover { background: var(--bg-hover); }
.action-btn.sell { color: var(--red); }
.action-btn.sell:hover { background: #FEF1F1; }
.review-badge {
  display: inline-flex;
  align-items: center;
  padding: 2px 7px;
  border-radius: 999px;
  font-size: 11px;
  font-weight: 700;
  white-space: nowrap;
}
.review-done { color: #155eef; background: rgba(21, 94, 239, 0.08); }
.review-pending { color: #b54708; background: rgba(181, 71, 8, 0.10); }
.review-failed { color: #b42318; background: rgba(180, 35, 24, 0.08); }
.review-panel {
  margin-top: 16px;
  padding: 16px;
  background: var(--bg-hover);
  border-radius: var(--radius);
}
.review-title {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 10px;
  font-size: 13px;
  font-weight: 600;
}
.review-text {
  margin: 0;
  font-size: 13px;
  white-space: pre-wrap;
  line-height: 1.6;
}
.pager-wrap {
  display: flex;
  justify-content: flex-end;
  margin-top: 12px;
}
.add-form { display: flex; flex-direction: column; gap: 16px; }
.form-row { display: flex; flex-direction: column; gap: 4px; }
.form-row label { font-size: 12px; color: var(--text-secondary); font-weight: 500; }
.pct-up { color: var(--red); }
.pct-down { color: var(--green); }
@media (max-width: 860px) {
  .summary-grid { grid-template-columns: repeat(2, minmax(120px, 1fr)); }
  .history-filters { flex-wrap: wrap; }
}
</style>

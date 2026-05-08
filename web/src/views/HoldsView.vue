<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import api from '../api'

interface HoldItem {
  id: number
  symbol: string
  name: string
  market: string
  entry_price: number
  entry_date: string
  target_days: number | null
  entry_reason: string
  days_held: number
  pnl_pct: number | null
  review_text: string
}

const items = ref<HoldItem[]>([])
const loading = ref(false)
const buyDialogVisible = ref(false)
const buyForm = ref({ symbol: '', market: '', target_days: 7, reason: '' })
const expandedRow = ref<number | null>(null)

async function fetchHolds() {
  loading.value = true
  try {
    const res = await api.get('/holds')
    items.value = res.data.items
  } catch {
    ElMessage.error('加载失败')
  } finally {
    loading.value = false
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

async function handleSell(item: HoldItem) {
  try {
    await ElMessageBox.confirm(`确认平仓 ${item.name || item.symbol}?`, '平仓')
    const res = await api.post('/trade/sell', { symbol: item.symbol, market: item.market })
    ElMessage.success(res.data.message || '平仓成功')
    fetchHolds()
  } catch {}
}

function toggleExpand(id: number) {
  expandedRow.value = expandedRow.value === id ? null : id
}

function pctClass(pct: number | null) {
  if (!pct) return ''
  return pct > 0 ? 'pct-up' : pct < 0 ? 'pct-down' : ''
}

onMounted(fetchHolds)
</script>

<template>
  <div class="holds-view">
    <div class="toolbar">
      <el-button type="primary" size="small" @click="buyDialogVisible = true">
        + 建仓
      </el-button>
    </div>

    <el-table :data="items" v-loading="loading" size="small">
      <el-table-column prop="symbol" label="代码" width="80" fixed />
      <el-table-column prop="name" label="名称" width="100" fixed />
      <el-table-column prop="market" label="市场" width="55" />
      <el-table-column label="建仓价" width="80" align="right">
        <template #default="{ row }">{{ row.entry_price.toFixed(2) }}</template>
      </el-table-column>
      <el-table-column label="盈亏%" width="80" align="right" sortable prop="pnl_pct">
        <template #default="{ row }">
          <span :class="pctClass(row.pnl_pct)">
            {{ row.pnl_pct != null ? (row.pnl_pct > 0 ? '+' : '') + row.pnl_pct.toFixed(2) + '%' : '-' }}
          </span>
        </template>
      </el-table-column>
      <el-table-column label="持仓" width="85">
        <template #default="{ row }">
          <span>{{ row.days_held }}d</span>
          <span class="target-days"> / {{ row.target_days || '∞' }}d</span>
        </template>
      </el-table-column>
      <el-table-column prop="entry_date" label="建仓日" width="100" />
      <el-table-column label="理由" min-width="160">
        <template #default="{ row }">
          <span class="reason-text">{{ row.entry_reason || '-' }}</span>
        </template>
      </el-table-column>
      <el-table-column label="操作" width="100" fixed="right">
        <template #default="{ row }">
          <button class="action-btn sell" @click="handleSell(row)">平仓</button>
          <button v-if="row.review_text" class="action-btn" @click="toggleExpand(row.id)">
            {{ expandedRow === row.id ? '收起' : '复盘' }}
          </button>
        </template>
      </el-table-column>
    </el-table>

    <!-- Expanded Review -->
    <div v-if="expandedRow" class="review-panel">
      <pre class="review-text">{{ items.find(i => i.id === expandedRow)?.review_text }}</pre>
    </div>

    <!-- Buy Dialog -->
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
.holds-view { max-width: 1100px; }
.toolbar { margin-bottom: 16px; }
.reason-text { font-size: 12px; color: var(--text-secondary); }
.target-days { color: var(--text-secondary); font-size: 11px; }
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
.review-panel {
  margin-top: 16px;
  padding: 16px;
  background: var(--bg-hover);
  border-radius: var(--radius);
}
.review-text {
  font-size: 13px;
  white-space: pre-wrap;
  line-height: 1.6;
}
.add-form { display: flex; flex-direction: column; gap: 16px; }
.form-row { display: flex; flex-direction: column; gap: 4px; }
.form-row label { font-size: 12px; color: var(--text-secondary); font-weight: 500; }
</style>

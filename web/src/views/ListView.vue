<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import api from '../api'

interface WatchItem {
  key: string
  symbol: string
  name: string
  market: string
  added_at: string
  tags: string
  price: number
  day_change: number
  amount: number
  turnover_rate: number
  market_cap: number
  float_cap: number
  entry_price: number
  total_change: number
  max_drawdown: number
}

const items = ref<WatchItem[]>([])
const loading = ref(false)
const addDialogVisible = ref(false)
const addForm = ref({ symbol: '', market: 'CN', name: '' })

async function fetchList() {
  loading.value = true
  try {
    const res = await api.get('/watchlist')
    items.value = res.data.items
  } catch (e) {
    ElMessage.error('加载失败')
  } finally {
    loading.value = false
  }
}

async function handleAdd() {
  if (!addForm.value.symbol) return
  try {
    await api.post('/watchlist', addForm.value)
    ElMessage.success('添加成功')
    addDialogVisible.value = false
    addForm.value = { symbol: '', market: 'CN', name: '' }
    fetchList()
  } catch (e: any) {
    ElMessage.error(e.response?.data?.detail || '添加失败')
  }
}

async function handleDelete(item: WatchItem) {
  try {
    await ElMessageBox.confirm(`确认删除 ${item.name || item.symbol}?`, '删除')
    await api.delete(`/watchlist/${encodeURIComponent(item.key)}`)
    ElMessage.success('已删除')
    fetchList()
  } catch {}
}

async function handleTagSave(item: WatchItem, newTags: string) {
  try {
    await api.patch(`/watchlist/${encodeURIComponent(item.key)}/tags`, { tags: newTags })
    item.tags = newTags
  } catch {
    ElMessage.error('标签保存失败')
  }
}

const editingTag = ref<string | null>(null)
const editTagValue = ref('')

function startEditTag(item: WatchItem) {
  editingTag.value = item.key
  editTagValue.value = item.tags || ''
}

function saveTag(item: WatchItem) {
  handleTagSave(item, editTagValue.value)
  editingTag.value = null
}

function marketColor(market: string) {
  if (market === 'CN') return '#E8453C'
  if (market === 'US') return '#2F80ED'
  if (market === 'HK') return '#F2994A'
  return '#787774'
}

function pctClass(v: number) {
  return v > 0 ? 'pct-up' : v < 0 ? 'pct-down' : ''
}

function formatAmount(v: number) {
  if (!v) return '-'
  if (v >= 1e8) return (v / 1e8).toFixed(1) + '亿'
  if (v >= 1e4) return (v / 1e4).toFixed(0) + '万'
  return v.toFixed(0)
}

function formatCap(v: number, market: string) {
  if (!v) return '-'
  if (market === 'US') return v.toFixed(1) + '亿$'
  if (market === 'HK') return v.toFixed(1) + '亿港'
  return v.toFixed(0) + '亿'
}

onMounted(fetchList)
</script>

<template>
  <div class="list-view">
    <div class="toolbar">
      <el-button type="primary" size="small" @click="addDialogVisible = true">
        + 添加监控
      </el-button>
      <el-button size="small" @click="fetchList" :loading="loading">刷新</el-button>
    </div>

    <el-table :data="items" v-loading="loading" size="small" :default-sort="{ prop: 'day_change', order: 'descending' }">
      <el-table-column prop="symbol" label="代码" width="80" fixed />
      <el-table-column prop="name" label="名称" width="90" fixed />
      <el-table-column label="市场" width="55">
        <template #default="{ row }">
          <span class="market-tag" :style="{ color: marketColor(row.market) }">{{ row.market }}</span>
        </template>
      </el-table-column>
      <el-table-column label="现价" width="75" align="right">
        <template #default="{ row }">{{ row.price ? row.price.toFixed(2) : '-' }}</template>
      </el-table-column>
      <el-table-column label="当日涨幅" width="80" align="right" sortable prop="day_change">
        <template #default="{ row }">
          <span :class="pctClass(row.day_change)">
            {{ row.day_change ? (row.day_change > 0 ? '+' : '') + row.day_change.toFixed(2) + '%' : '-' }}
          </span>
        </template>
      </el-table-column>
      <el-table-column label="至今涨幅" width="80" align="right" sortable prop="total_change">
        <template #default="{ row }">
          <span :class="pctClass(row.total_change)">
            {{ row.total_change ? (row.total_change > 0 ? '+' : '') + row.total_change.toFixed(1) + '%' : '-' }}
          </span>
        </template>
      </el-table-column>
      <el-table-column label="最大回撤" width="80" align="right" sortable prop="max_drawdown">
        <template #default="{ row }">
          <span class="pct-down">{{ row.max_drawdown ? '-' + row.max_drawdown.toFixed(1) + '%' : '-' }}</span>
        </template>
      </el-table-column>
      <el-table-column label="成交额" width="80" align="right" sortable prop="amount">
        <template #default="{ row }">{{ formatAmount(row.amount) }}</template>
      </el-table-column>
      <el-table-column label="换手率" width="70" align="right" sortable prop="turnover_rate">
        <template #default="{ row }">{{ row.turnover_rate ? row.turnover_rate.toFixed(2) + '%' : '-' }}</template>
      </el-table-column>
      <el-table-column label="市值" width="80" align="right" sortable prop="market_cap">
        <template #default="{ row }">{{ formatCap(row.market_cap, row.market) }}</template>
      </el-table-column>
      <el-table-column label="流通" width="75" align="right">
        <template #default="{ row }">{{ row.float_cap ? formatCap(row.float_cap, row.market) : '-' }}</template>
      </el-table-column>
      <el-table-column label="赛道题材" min-width="140">
        <template #default="{ row }">
          <div v-if="editingTag === row.key" class="tag-edit">
            <input v-model="editTagValue" class="tag-input" @keyup.enter="saveTag(row)" @blur="saveTag(row)" autofocus />
          </div>
          <div v-else class="tag-display" @click="startEditTag(row)">
            <span v-if="row.tags" class="tag-text">{{ row.tags }}</span>
            <span v-else class="tag-placeholder">点击添加</span>
          </div>
        </template>
      </el-table-column>
      <el-table-column prop="added_at" label="加入日期" width="100" />
      <el-table-column label="操作" width="55" fixed="right">
        <template #default="{ row }">
          <button class="del-btn" @click="handleDelete(row)">删除</button>
        </template>
      </el-table-column>
    </el-table>

    <!-- Add Dialog -->
    <el-dialog v-model="addDialogVisible" title="添加监控" width="360px">
      <div class="add-form">
        <div class="form-row">
          <label>市场</label>
          <el-radio-group v-model="addForm.market" size="small">
            <el-radio-button value="CN">A股</el-radio-button>
            <el-radio-button value="HK">港股</el-radio-button>
            <el-radio-button value="US">美股</el-radio-button>
          </el-radio-group>
        </div>
        <div class="form-row">
          <label>代码</label>
          <el-input v-model="addForm.symbol" placeholder="如 600519" size="small" />
        </div>
        <div class="form-row">
          <label>名称</label>
          <el-input v-model="addForm.name" placeholder="如 贵州茅台 (可选)" size="small" />
        </div>
      </div>
      <template #footer>
        <el-button size="small" @click="addDialogVisible = false">取消</el-button>
        <el-button type="primary" size="small" @click="handleAdd">添加</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<style scoped>
.list-view { max-width: 1400px; }
.toolbar { margin-bottom: 16px; display: flex; gap: 8px; }
.market-tag { font-size: 12px; font-weight: 600; }
.tag-display { cursor: pointer; padding: 2px 0; min-height: 24px; display: flex; align-items: center; }
.tag-text { font-size: 12px; background: var(--bg-hover); padding: 2px 8px; border-radius: 4px; }
.tag-placeholder { font-size: 11px; color: var(--text-secondary); opacity: 0.4; }
.tag-edit { display: flex; }
.tag-input { width: 100%; padding: 3px 8px; border: 1px solid var(--accent); border-radius: 4px; font-size: 12px; outline: none; }
.del-btn { font-size: 11px; color: var(--text-secondary); padding: 2px 6px; border-radius: 4px; }
.del-btn:hover { color: var(--red); background: #FEF1F1; }
.add-form { display: flex; flex-direction: column; gap: 16px; }
.form-row { display: flex; flex-direction: column; gap: 4px; }
.form-row label { font-size: 12px; color: var(--text-secondary); font-weight: 500; }
</style>

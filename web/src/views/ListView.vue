<script setup lang="ts">
import { ref, computed, onMounted, watch } from 'vue'
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
  news_summary: string
}

interface SearchResult {
  symbol: string
  name: string
  market: string
}

const items = ref<WatchItem[]>([])
const loading = ref(false)
const addDialogVisible = ref(false)
const addForm = ref({ symbol: '', market: 'CN', name: '' })
const searchResults = ref<SearchResult[]>([])
const searchLoading = ref(false)
const searchQuery = ref('')
const sortKey = ref('added_at')
const sortOrder = ref<'asc' | 'desc'>('desc')

// All known tags for reuse
const allTags = computed(() => {
  const tagSet = new Set<string>()
  items.value.forEach(item => {
    if (item.tags) {
      item.tags.split(',').forEach(t => {
        const trimmed = t.trim()
        if (trimmed) tagSet.add(trimmed)
      })
    }
  })
  return Array.from(tagSet)
})

const sortedItems = computed(() => {
  const list = [...items.value]
  list.sort((a, b) => {
    let va: any = (a as any)[sortKey.value]
    let vb: any = (b as any)[sortKey.value]
    if (typeof va === 'string') {
      const cmp = va.localeCompare(vb)
      return sortOrder.value === 'asc' ? cmp : -cmp
    }
    va = va || 0
    vb = vb || 0
    return sortOrder.value === 'asc' ? va - vb : vb - va
  })
  return list
})

function toggleSort(key: string) {
  if (sortKey.value === key) {
    sortOrder.value = sortOrder.value === 'asc' ? 'desc' : 'asc'
  } else {
    sortKey.value = key
    sortOrder.value = 'desc'
  }
}

function sortIcon(key: string) {
  if (sortKey.value !== key) return '↕'
  return sortOrder.value === 'asc' ? '↑' : '↓'
}

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

let searchTimer: any = null
async function handleSearch(query: string) {
  searchQuery.value = query
  if (!query || query.length < 1) {
    searchResults.value = []
    return
  }
  clearTimeout(searchTimer)
  searchTimer = setTimeout(async () => {
    searchLoading.value = true
    try {
      const res = await api.get('/search-stock', { params: { q: query, market: addForm.value.market } })
      searchResults.value = res.data.results
    } catch { searchResults.value = [] }
    finally { searchLoading.value = false }
  }, 300)
}

function selectSearchResult(r: SearchResult) {
  addForm.value.symbol = r.symbol
  addForm.value.name = r.name
  addForm.value.market = r.market
  searchResults.value = []
  searchQuery.value = ''
}

async function handleAdd() {
  if (!addForm.value.symbol) return
  try {
    await api.post('/watchlist', addForm.value)
    ElMessage.success('添加成功')
    addDialogVisible.value = false
    addForm.value = { symbol: '', market: 'CN', name: '' }
    searchResults.value = []
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

function addExistingTag(tag: string) {
  if (editTagValue.value) {
    editTagValue.value += ',' + tag
  } else {
    editTagValue.value = tag
  }
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

function formatCap(total: number, float_cap: number) {
  // Smart format: 22.72亿 / 132.5亿 / 5.42万亿 / 4785亿
  function fmt(v: number): string {
    if (!v) return '-'
    if (v >= 10000) return (v / 10000).toFixed(2) + '万亿'
    if (v >= 1000) return v.toFixed(0) + '亿'
    if (v >= 100) return v.toFixed(1) + '亿'
    return v.toFixed(2) + '亿'
  }
  const t = fmt(total)
  const f = fmt(float_cap)
  if (t === '-' && f === '-') return '-'
  if (f === '-' || t === f) return t
  return `${t} / ${f}`
}

function formatDate(d: string) {
  if (!d) return '-'
  // Extract YYYY-MM-DD and convert to YYYY/MM/DD
  const dateStr = d.substring(0, 10)
  return dateStr.replace(/-/g, '/')
}

onMounted(fetchList)
</script>

<template>
  <div class="list-view">
    <div class="toolbar">
      <el-button type="primary" size="small" @click="addDialogVisible = true">+ 添加监控</el-button>
      <el-button size="small" @click="fetchList" :loading="loading">刷新</el-button>
    </div>

    <div class="table-wrapper">
      <table class="watch-table">
        <thead>
          <tr>
            <th class="sortable" @click="toggleSort('added_at')">日期 <span class="sort-icon">{{ sortIcon('added_at') }}</span></th>
            <th>代码</th>
            <th>名称</th>
            <th class="sortable" @click="toggleSort('market')">市场 <span class="sort-icon">{{ sortIcon('market') }}</span></th>
            <th class="num sortable" @click="toggleSort('price')">现价 <span class="sort-icon">{{ sortIcon('price') }}</span></th>
            <th class="num sortable" @click="toggleSort('day_change')">当日涨幅 <span class="sort-icon">{{ sortIcon('day_change') }}</span></th>
            <th class="num sortable" @click="toggleSort('total_change')">至今涨幅 <span class="sort-icon">{{ sortIcon('total_change') }}</span></th>
            <th class="num sortable" @click="toggleSort('max_drawdown')">最大回撤 <span class="sort-icon">{{ sortIcon('max_drawdown') }}</span></th>
            <th class="num sortable" @click="toggleSort('amount')">成交额 <span class="sort-icon">{{ sortIcon('amount') }}</span></th>
            <th class="num sortable" @click="toggleSort('turnover_rate')">换手率 <span class="sort-icon">{{ sortIcon('turnover_rate') }}</span></th>
            <th class="num sortable" @click="toggleSort('market_cap')">市值/流通 <span class="sort-icon">{{ sortIcon('market_cap') }}</span></th>
            <th>赛道题材</th>
            <th>最新新闻</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="row in sortedItems" :key="row.key">
            <td class="date-cell">{{ formatDate(row.added_at) }}</td>
            <td class="code-cell">{{ row.symbol }}</td>
            <td class="name-cell">{{ row.name || '-' }}</td>
            <td><span class="market-tag" :style="{ color: marketColor(row.market) }">{{ row.market }}</span></td>
            <td class="num">{{ row.price ? row.price.toFixed(2) : '-' }}</td>
            <td class="num">
              <span :class="pctClass(row.day_change)">
                {{ row.day_change ? (row.day_change > 0 ? '+' : '') + row.day_change.toFixed(2) + '%' : '-' }}
              </span>
            </td>
            <td class="num">
              <span :class="pctClass(row.total_change)">
                {{ row.total_change ? (row.total_change > 0 ? '+' : '') + row.total_change.toFixed(1) + '%' : '-' }}
              </span>
            </td>
            <td class="num">
              <span class="pct-down">{{ row.max_drawdown ? '-' + row.max_drawdown.toFixed(1) + '%' : '-' }}</span>
            </td>
            <td class="num">{{ formatAmount(row.amount) }}</td>
            <td class="num">{{ row.turnover_rate ? row.turnover_rate.toFixed(2) + '%' : '-' }}</td>
            <td class="num cap-cell">{{ formatCap(row.market_cap, row.float_cap) }}</td>
            <td class="tag-cell">
              <div v-if="editingTag === row.key" class="tag-edit">
                <input v-model="editTagValue" class="tag-input" @keyup.enter="saveTag(row)" @blur="saveTag(row)" autofocus />
                <div v-if="allTags.length" class="tag-suggest">
                  <span v-for="t in allTags" :key="t" class="tag-chip" @mousedown.prevent="addExistingTag(t)">{{ t }}</span>
                </div>
              </div>
              <div v-else class="tag-display" @click="startEditTag(row)">
                <span v-if="row.tags" class="tag-badges">
                  <span v-for="t in row.tags.split(',')" :key="t" class="tag-badge">{{ t.trim() }}</span>
                </span>
                <span v-else class="tag-placeholder">+</span>
              </div>
            </td>
            <td class="news-cell">
              <span class="news-text">{{ row.news_summary || '-' }}</span>
            </td>
            <td>
              <button class="del-btn" @click="handleDelete(row)">删除</button>
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <!-- Add Dialog -->
    <el-dialog v-model="addDialogVisible" title="添加监控" width="400px">
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
          <label>搜索 (代码/中文名)</label>
          <el-input
            v-model="searchQuery"
            placeholder="输入代码或中文名搜索"
            size="small"
            @input="handleSearch"
            clearable
          />
          <div v-if="searchResults.length" class="search-dropdown">
            <div
              v-for="r in searchResults"
              :key="r.market + ':' + r.symbol"
              class="search-item"
              @click="selectSearchResult(r)"
            >
              <span class="sr-symbol">{{ r.symbol }}</span>
              <span class="sr-name">{{ r.name }}</span>
              <span class="sr-market" :style="{ color: marketColor(r.market) }">{{ r.market }}</span>
            </div>
          </div>
        </div>
        <div class="form-row">
          <label>代码</label>
          <el-input v-model="addForm.symbol" placeholder="如 600519 / AAPL" size="small" />
        </div>
        <div class="form-row">
          <label>名称 (可选，留空自动获取)</label>
          <el-input v-model="addForm.name" placeholder="自动获取" size="small" />
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
.list-view { width: 100%; }
.toolbar { margin-bottom: 12px; display: flex; gap: 8px; }
.table-wrapper { overflow-x: auto; }
.watch-table { width: 100%; border-collapse: collapse; font-size: 12px; line-height: 1.4; }
.watch-table th, .watch-table td { padding: 6px 6px; text-align: left; white-space: nowrap; border-bottom: 1px solid var(--border, #E9E9E7); }
.watch-table th { font-size: 11px; color: var(--text-secondary, #787774); font-weight: 500; user-select: none; padding: 5px 6px; }
.watch-table th.sortable { cursor: pointer; }
.watch-table th.sortable:hover { color: var(--text, #37352F); }
.sort-icon { font-size: 9px; opacity: 0.4; margin-left: 1px; }
.watch-table tbody tr:hover { background: var(--bg-hover, #F7F6F3); }
.num { text-align: right !important; font-variant-numeric: tabular-nums; }
.date-cell { color: var(--text-secondary, #787774); font-size: 11px; }
.code-cell { font-weight: 600; }
.name-cell { max-width: 72px; overflow: hidden; text-overflow: ellipsis; }
.cap-cell { font-size: 11px; }
.market-tag { font-size: 11px; font-weight: 600; letter-spacing: -0.5px; }
.tag-cell { min-width: 60px; max-width: 160px; }
.tag-display { cursor: pointer; display: flex; flex-wrap: wrap; gap: 2px; align-items: center; min-height: 20px; }
.tag-badges { display: flex; flex-wrap: wrap; gap: 2px; }
.tag-badge { font-size: 10px; background: var(--bg-hover, #F7F6F3); padding: 1px 5px; border-radius: 3px; color: var(--text, #37352F); }
.tag-placeholder { font-size: 11px; color: var(--text-secondary, #787774); opacity: 0.3; }
.tag-edit { position: relative; }
.tag-input { width: 110px; padding: 2px 5px; border: 1px solid var(--accent, #2F80ED); border-radius: 4px; font-size: 11px; outline: none; }
.tag-suggest { position: absolute; top: 24px; left: 0; background: #fff; border: 1px solid var(--border, #E9E9E7); border-radius: 6px; padding: 4px; z-index: 10; display: flex; flex-wrap: wrap; gap: 3px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); }
.tag-chip { font-size: 10px; padding: 2px 6px; border-radius: 4px; background: #F0EFED; cursor: pointer; }
.tag-chip:hover { background: #E3E2E0; }
.news-cell { max-width: 140px; overflow: hidden; text-overflow: ellipsis; }
.news-text { font-size: 11px; color: var(--text-secondary, #787774); }
.del-btn { font-size: 11px; color: var(--text-secondary, #787774); padding: 1px 4px; border-radius: 3px; border: none; background: none; cursor: pointer; }
.del-btn:hover { color: var(--red, #EB5757); background: #FEF1F1; }
.add-form { display: flex; flex-direction: column; gap: 14px; }
.form-row { display: flex; flex-direction: column; gap: 4px; position: relative; }
.form-row label { font-size: 12px; color: var(--text-secondary, #787774); font-weight: 500; }
.search-dropdown { position: absolute; top: 56px; left: 0; right: 0; background: #fff; border: 1px solid var(--border, #E9E9E7); border-radius: 8px; max-height: 200px; overflow-y: auto; z-index: 100; box-shadow: 0 4px 12px rgba(0,0,0,0.1); }
.search-item { padding: 8px 12px; cursor: pointer; display: flex; gap: 8px; align-items: center; font-size: 13px; }
.search-item:hover { background: var(--bg-hover, #F7F6F3); }
.sr-symbol { font-weight: 600; min-width: 60px; }
.sr-name { flex: 1; color: var(--text-secondary, #787774); }
.sr-market { font-size: 11px; font-weight: 600; }
</style>

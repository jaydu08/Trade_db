<script setup lang="ts">
import { ref, onMounted, watch, computed } from 'vue'
import { ElMessage } from 'element-plus'
import api from '../api'

interface TrendItem {
  rank: number
  symbol: string
  name: string
  market: string
  price: number
  period_change: number
  score: number
  catalyst_tags: string
  days_on_list: number
  market_cap: number
  signal_strength: number
  price_date: string
}

const markets = ['CN', 'HK', 'US']
const daysOptions = [3, 7, 14, 30, 60, 90, 180]
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
    const res = await api.get('/trend', { params: { days: activeDays.value, limit: 100 } })
    const marketsData: Record<string, TrendItem[]> = {}
    const raw = res.data.markets || {}
    for (const [mkt, items] of Object.entries(raw) as [string, any[]][]) {
      marketsData[mkt] = (items || []).map((item: any, idx: number) => ({
        rank: idx + 1,
        symbol: item.symbol || '',
        name: item.name || '',
        market: mkt,
        price: item.price || 0,
        period_change: item.return_pct || 0,
        score: item.trend_score || 0,
        catalyst_tags: item.catalyst_tags || '',
        days_on_list: item.days_on_list || 0,
        market_cap: item.market_cap || 0,
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
  currentPage.value = 1
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

function formatDate(v: string): string {
  if (!v) return '-'
  return String(v).slice(0, 10)
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
      <div class="tab-group">
        <button
          v-for="m in markets" :key="m"
          class="tab-btn" :class="{ active: activeMarket === m }"
          @click="activeMarket = m"
        >{{ m }}</button>
      </div>
      <div class="days-group">
        <button
          v-for="d in daysOptions" :key="d"
          class="days-btn" :class="{ active: activeDays === d }"
          @click="activeDays = d"
        >{{ d }}天</button>
      </div>
      <div class="meta-text">共 {{ totalItems }} 个标的</div>
    </div>

    <el-table :data="pagedItems" v-loading="loading" size="small" @sort-change="handleSortChange">
      <el-table-column prop="rank" label="#" width="46" />
      <el-table-column prop="symbol" label="代码" width="98" />
      <el-table-column prop="name" label="名称" width="110" />
      <el-table-column label="现价" width="86">
        <template #default="{ row }">{{ row.price ? row.price.toFixed(2) : '-' }}</template>
      </el-table-column>
      <el-table-column prop="period_change" label="N日涨幅" width="96" sortable="custom">
        <template #default="{ row }">
          <span :class="pctClass(row.period_change)">
            {{ row.period_change > 0 ? '+' : '' }}{{ row.period_change.toFixed(1) }}%
          </span>
        </template>
      </el-table-column>
      <el-table-column prop="score" label="评分" width="110" sortable="custom">
        <template #default="{ row }">
          <div class="score-wrap">
            <span class="stars">{{ '★'.repeat(scoreStars(row.score)) }}{{ '☆'.repeat(5 - scoreStars(row.score)) }}</span>
            <span class="score-num">{{ row.score.toFixed(1) }}</span>
          </div>
        </template>
      </el-table-column>
      <el-table-column prop="market_cap" label="市值" width="120" sortable="custom">
        <template #default="{ row }">{{ formatCap(row.market_cap, row.market) }}</template>
      </el-table-column>
      <el-table-column label="催化标签" min-width="220">
        <template #default="{ row }">
          <span class="catalyst-text">{{ row.catalyst_tags || '-' }}</span>
        </template>
      </el-table-column>
      <el-table-column prop="days_on_list" label="上榜天数" width="88" sortable="custom">
        <template #default="{ row }">{{ row.days_on_list }}d</template>
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
.trend-view { max-width: 1320px; }
.toolbar {
  display: flex;
  align-items: center;
  gap: 24px;
  margin-bottom: 16px;
}
.tab-group, .days-group {
  display: flex;
  gap: 2px;
  background: var(--bg-hover);
  border-radius: var(--radius);
  padding: 2px;
}
.tab-btn, .days-btn {
  padding: 5px 12px;
  font-size: 13px;
  border-radius: 4px;
  color: var(--text-secondary);
  transition: all 0.15s;
}
.tab-btn.active, .days-btn.active {
  background: white;
  color: var(--text);
  font-weight: 500;
  box-shadow: 0 1px 2px rgba(0,0,0,0.06);
}
.meta-text {
  font-size: 12px;
  color: var(--text-secondary);
}
.catalyst-text {
  font-size: 12px;
  color: var(--text-secondary);
}
.score-wrap {
  display: flex;
  align-items: center;
  gap: 6px;
}
.stars {
  font-size: 13px;
  color: #F2C94C;
  letter-spacing: -1px;
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

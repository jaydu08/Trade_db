<script setup lang="ts">
import { ref, onMounted, watch } from 'vue'
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
}

const markets = ['CN', 'HK', 'US']
const daysOptions = [3, 5, 7, 14]
const activeMarket = ref('CN')
const activeDays = ref(7)
const loading = ref(false)
const allData = ref<Record<string, TrendItem[]>>({})

async function fetchTrend() {
  loading.value = true
  try {
    const res = await api.get('/trend', { params: { days: activeDays.value } })
    // Transform response
    const marketsData: Record<string, TrendItem[]> = {}
    const raw = res.data.markets || {}
    for (const [mkt, items] of Object.entries(raw) as [string, any[]][]) {
      marketsData[mkt] = (items || []).map((item: any, idx: number) => ({
        rank: idx + 1,
        symbol: item.symbol || '',
        name: item.name || '',
        market: mkt,
        price: item.price || 0,
        period_change: item.period_change || item.return_pct || 0,
        score: item.score || item.total_score || 0,
        catalyst_tags: item.catalyst_tags || item.reason || '',
        days_on_list: item.days_on_list || item.pool_days || 0,
      }))
    }
    allData.value = marketsData
  } catch {
    ElMessage.error('加载趋势数据失败')
  } finally {
    loading.value = false
  }
}

const currentItems = ref<TrendItem[]>([])
watch([activeMarket, allData], () => {
  currentItems.value = allData.value[activeMarket.value] || []
}, { immediate: true })

function pctClass(v: number) {
  return v > 0 ? 'pct-up' : v < 0 ? 'pct-down' : ''
}

onMounted(fetchTrend)
watch(activeDays, fetchTrend)
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
    </div>

    <el-table :data="currentItems" v-loading="loading" size="small">
      <el-table-column prop="rank" label="#" width="40" />
      <el-table-column prop="symbol" label="代码" width="90" />
      <el-table-column prop="name" label="名称" width="120" />
      <el-table-column label="现价" width="90">
        <template #default="{ row }">{{ row.price ? row.price.toFixed(2) : '-' }}</template>
      </el-table-column>
      <el-table-column label="N日涨幅" width="90">
        <template #default="{ row }">
          <span :class="pctClass(row.period_change)">
            {{ row.period_change > 0 ? '+' : '' }}{{ row.period_change.toFixed(1) }}%
          </span>
        </template>
      </el-table-column>
      <el-table-column label="评分" width="70">
        <template #default="{ row }">{{ row.score.toFixed(2) }}</template>
      </el-table-column>
      <el-table-column label="马甲" min-width="160">
        <template #default="{ row }">
          <span class="catalyst-text">{{ row.catalyst_tags || '-' }}</span>
        </template>
      </el-table-column>
      <el-table-column label="上榜天数" width="80">
        <template #default="{ row }">{{ row.days_on_list }}d</template>
      </el-table-column>
    </el-table>
  </div>
</template>

<style scoped>
.trend-view { max-width: 1100px; }
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
.catalyst-text {
  font-size: 12px;
  color: var(--text-secondary);
}
</style>

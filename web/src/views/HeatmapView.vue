<script setup lang="ts">
import { ref, onMounted, watch } from 'vue'
import { ElMessage } from 'element-plus'
import api from '../api'

interface HeatmapItem {
  rank: number
  symbol: string
  name: string
  close: number
  change_pct: number
  amount: number
  market_cap: number
  catalyst_tags: string
}

const markets = ['CN', 'HK', 'US']
const activeMarket = ref('CN')
const selectedDate = ref('')
const tradingDays = ref<string[]>([])
const items = ref<HeatmapItem[]>([])
const loading = ref(false)

async function fetchTradingDays(month?: string) {
  const m = month || new Date().toISOString().slice(0, 7)
  try {
    const res = await api.get('/trading-days', { params: { month: m } })
    tradingDays.value = res.data.days || []
    // Auto-select latest day
    if (!selectedDate.value && tradingDays.value.length) {
      selectedDate.value = tradingDays.value[tradingDays.value.length - 1]
    }
  } catch {}
}

async function fetchHeatmap() {
  if (!selectedDate.value) return
  loading.value = true
  try {
    const res = await api.get('/heatmap', {
      params: { date: selectedDate.value, market: activeMarket.value }
    })
    items.value = res.data.items || []
  } catch {
    ElMessage.error('加载热榜失败')
  } finally {
    loading.value = false
  }
}

function isDisabledDate(date: Date) {
  const dateStr = date.toISOString().slice(0, 10)
  return !tradingDays.value.includes(dateStr)
}

function formatAmount(v: number) {
  if (!v) return '-'
  if (v >= 1e8) return (v / 1e8).toFixed(1) + '亿'
  if (v >= 1e4) return (v / 1e4).toFixed(0) + '万'
  return v.toFixed(0)
}

function formatCap(v: number) {
  if (!v) return '-'
  if (v >= 10000) return (v / 10000).toFixed(2) + '万亿'
  if (v >= 1000) return v.toFixed(0) + '亿'
  if (v >= 100) return v.toFixed(1) + '亿'
  return v.toFixed(2) + '亿'
}

function pctClass(v: number) {
  return v > 0 ? 'pct-up' : v < 0 ? 'pct-down' : ''
}

function handleDateChange(val: string) {
  selectedDate.value = val
  fetchHeatmap()
}

onMounted(async () => {
  await fetchTradingDays()
  fetchHeatmap()
})

watch(activeMarket, fetchHeatmap)
</script>

<template>
  <div class="heatmap-view">
    <div class="toolbar">
      <div class="tab-group">
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
    </div>

    <el-table :data="items" v-loading="loading" size="small">
      <el-table-column prop="rank" label="#" width="40" />
      <el-table-column prop="symbol" label="代码" width="90" />
      <el-table-column prop="name" label="名称" width="120" />
      <el-table-column label="收盘价" width="90">
        <template #default="{ row }">{{ row.close ? row.close.toFixed(2) : '-' }}</template>
      </el-table-column>
      <el-table-column label="涨幅" width="80">
        <template #default="{ row }">
          <span :class="pctClass(row.change_pct)">
            {{ row.change_pct > 0 ? '+' : '' }}{{ row.change_pct.toFixed(2) }}%
          </span>
        </template>
      </el-table-column>
      <el-table-column label="成交额" width="90">
        <template #default="{ row }">{{ formatAmount(row.amount) }}</template>
      </el-table-column>
      <el-table-column label="市值" width="90">
        <template #default="{ row }">{{ formatCap(row.market_cap) }}</template>
      </el-table-column>
      <el-table-column label="马甲" min-width="180">
        <template #default="{ row }">
          <span class="catalyst-text">{{ row.catalyst_tags || '-' }}</span>
        </template>
      </el-table-column>
    </el-table>
  </div>
</template>

<style scoped>
.heatmap-view { max-width: 1100px; }
.toolbar {
  display: flex;
  align-items: center;
  gap: 16px;
  margin-bottom: 16px;
}
.tab-group {
  display: flex;
  gap: 2px;
  background: var(--bg-hover);
  border-radius: var(--radius);
  padding: 2px;
}
.tab-btn {
  padding: 5px 12px;
  font-size: 13px;
  border-radius: 4px;
  color: var(--text-secondary);
  transition: all 0.15s;
}
.tab-btn.active {
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

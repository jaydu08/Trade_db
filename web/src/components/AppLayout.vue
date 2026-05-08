<script setup lang="ts">
import { useRoute, useRouter } from 'vue-router'
import { computed } from 'vue'
import { useAuthStore } from '../stores/auth'

const route = useRoute()
const router = useRouter()
const auth = useAuthStore()

const navItems = [
  { path: '/list', label: 'List', icon: '📋' },
  { path: '/holds', label: 'Holds', icon: '💼' },
  { path: '/trend', label: 'Trend', icon: '📈' },
  { path: '/heatmap', label: 'Heatmap', icon: '🔥' },
]

const pageTitle = computed(() => {
  const item = navItems.find(n => n.path === route.path)
  return item?.label || ''
})

function handleLogout() {
  auth.logout()
  router.push('/login')
}
</script>

<template>
  <div class="layout">
    <aside class="sidebar">
      <div class="sidebar-header">
        <span class="logo">T</span>
        <span class="logo-text">Trade DB</span>
      </div>
      <nav class="nav-list">
        <router-link
          v-for="item in navItems"
          :key="item.path"
          :to="item.path"
          class="nav-item"
          :class="{ active: route.path === item.path }"
        >
          <span class="nav-icon">{{ item.icon }}</span>
          <span class="nav-label">{{ item.label }}</span>
        </router-link>
      </nav>
      <div class="sidebar-footer">
        <span class="user-name">{{ auth.username }}</span>
        <button class="logout-btn" @click="handleLogout">退出</button>
      </div>
    </aside>
    <main class="main-content">
      <header class="top-bar">
        <h1 class="page-title">{{ pageTitle }}</h1>
      </header>
      <div class="content-area">
        <slot />
      </div>
    </main>
  </div>
</template>

<style scoped>
.layout {
  display: flex;
  min-height: 100vh;
}

.sidebar {
  width: var(--nav-width);
  border-right: 1px solid var(--border);
  display: flex;
  flex-direction: column;
  padding: 16px 12px;
  position: fixed;
  top: 0;
  left: 0;
  bottom: 0;
  background: #FBFBFA;
}

.sidebar-header {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 4px 8px;
  margin-bottom: 20px;
}

.logo {
  width: 24px;
  height: 24px;
  background: var(--text);
  color: white;
  border-radius: 4px;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 12px;
  font-weight: 700;
}

.logo-text {
  font-weight: 600;
  font-size: 14px;
}

.nav-list {
  flex: 1;
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.nav-item {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 6px 10px;
  border-radius: var(--radius);
  color: var(--text-secondary);
  text-decoration: none;
  font-size: 14px;
  transition: background 0.15s;
}

.nav-item:hover {
  background: var(--bg-hover);
  color: var(--text);
}

.nav-item.active {
  background: var(--bg-hover);
  color: var(--text);
  font-weight: 500;
}

.nav-icon {
  font-size: 16px;
  width: 22px;
  text-align: center;
}

.sidebar-footer {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 8px 10px;
  border-top: 1px solid var(--border);
  margin-top: 8px;
}

.user-name {
  font-size: 13px;
  color: var(--text-secondary);
}

.logout-btn {
  font-size: 12px;
  color: var(--text-secondary);
  padding: 4px 8px;
  border-radius: 4px;
}

.logout-btn:hover {
  background: var(--bg-hover);
  color: var(--red);
}

.main-content {
  flex: 1;
  margin-left: var(--nav-width);
  display: flex;
  flex-direction: column;
}

.top-bar {
  padding: 24px 40px 0;
}

.page-title {
  font-size: 24px;
  font-weight: 700;
  color: var(--text);
}

.content-area {
  padding: 24px 40px;
  flex: 1;
}
</style>

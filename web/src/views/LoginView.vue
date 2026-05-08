<script setup lang="ts">
import { ref } from 'vue'
import { useRouter } from 'vue-router'
import { useAuthStore } from '../stores/auth'
import api from '../api'

const router = useRouter()
const auth = useAuthStore()
const username = ref('')
const loading = ref(false)
const error = ref('')

async function handleLogin() {
  if (!username.value.trim()) return
  loading.value = true
  error.value = ''
  try {
    const res = await api.post('/login', { username: username.value.trim() })
    auth.setAuth(res.data.token, res.data.username)
    router.push('/list')
  } catch (e: any) {
    error.value = e.response?.data?.detail || '登录失败'
  } finally {
    loading.value = false
  }
}
</script>

<template>
  <div class="login-page">
    <div class="login-card">
      <div class="login-logo">
        <span class="logo-icon">T</span>
      </div>
      <h1 class="login-title">Trade DB</h1>
      <p class="login-subtitle">输入用户名登录</p>
      <form @submit.prevent="handleLogin" class="login-form">
        <input
          v-model="username"
          type="text"
          placeholder="用户名"
          class="login-input"
          autofocus
        />
        <button type="submit" class="login-btn" :disabled="loading">
          {{ loading ? '...' : '登录' }}
        </button>
        <p v-if="error" class="login-error">{{ error }}</p>
      </form>
    </div>
  </div>
</template>

<style scoped>
.login-page {
  min-height: 100vh;
  display: flex;
  align-items: center;
  justify-content: center;
  background: #FBFBFA;
}

.login-card {
  text-align: center;
  width: 320px;
}

.login-logo {
  margin-bottom: 16px;
}

.logo-icon {
  display: inline-flex;
  width: 40px;
  height: 40px;
  background: var(--text);
  color: white;
  border-radius: 8px;
  align-items: center;
  justify-content: center;
  font-size: 18px;
  font-weight: 700;
}

.login-title {
  font-size: 24px;
  font-weight: 700;
  margin-bottom: 4px;
}

.login-subtitle {
  color: var(--text-secondary);
  font-size: 14px;
  margin-bottom: 32px;
}

.login-form {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.login-input {
  width: 100%;
  padding: 10px 14px;
  border: 1px solid var(--border);
  border-radius: var(--radius);
  font-size: 14px;
  outline: none;
  transition: border-color 0.2s;
}

.login-input:focus {
  border-color: var(--accent);
}

.login-btn {
  width: 100%;
  padding: 10px;
  background: var(--text);
  color: white;
  border-radius: var(--radius);
  font-size: 14px;
  font-weight: 500;
  transition: opacity 0.2s;
}

.login-btn:hover {
  opacity: 0.85;
}

.login-btn:disabled {
  opacity: 0.5;
}

.login-error {
  color: var(--red);
  font-size: 13px;
}
</style>

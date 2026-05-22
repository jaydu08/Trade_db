import { defineStore } from 'pinia'
import { ref, computed } from 'vue'

export const useAuthStore = defineStore('auth', () => {
  const token = ref(localStorage.getItem('token') || '')
  const username = ref(localStorage.getItem('username') || '')
  const role = ref(localStorage.getItem('role') || 'user')
  const chatId = ref(Number(localStorage.getItem('chat_id') || '0') || 0)

  const isLoggedIn = computed(() => !!token.value)

  function setAuth(t: string, u: string, r = 'user', c = 0) {
    token.value = t
    username.value = u
    role.value = r
    chatId.value = Number(c) || 0
    localStorage.setItem('token', t)
    localStorage.setItem('username', u)
    localStorage.setItem('role', role.value)
    localStorage.setItem('chat_id', String(chatId.value))
  }

  function logout() {
    token.value = ''
    username.value = ''
    role.value = 'user'
    chatId.value = 0
    localStorage.removeItem('token')
    localStorage.removeItem('username')
    localStorage.removeItem('role')
    localStorage.removeItem('chat_id')
  }

  return { token, username, role, chatId, isLoggedIn, setAuth, logout }
})

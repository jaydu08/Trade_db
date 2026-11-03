<script setup lang="ts">
import { useRoute } from 'vue-router'
import { computed } from 'vue'
import { useAuthStore } from './stores/auth'
import AppLayout from './components/AppLayout.vue'

const route = useRoute()
const auth = useAuthStore()
const showLayout = computed(() => route.path !== '/login' && auth.isLoggedIn)
</script>

<template>
  <AppLayout v-if="showLayout">
    <router-view />
  </AppLayout>
  <router-view v-else />
</template>

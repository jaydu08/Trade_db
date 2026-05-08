import { createRouter, createWebHistory } from 'vue-router'
import { useAuthStore } from '../stores/auth'

const routes = [
  {
    path: '/login',
    name: 'Login',
    component: () => import('../views/LoginView.vue'),
  },
  {
    path: '/',
    redirect: '/list',
  },
  {
    path: '/list',
    name: 'List',
    component: () => import('../views/ListView.vue'),
    meta: { requiresAuth: true },
  },
  {
    path: '/holds',
    name: 'Holds',
    component: () => import('../views/HoldsView.vue'),
    meta: { requiresAuth: true },
  },
  {
    path: '/trend',
    name: 'Trend',
    component: () => import('../views/TrendView.vue'),
    meta: { requiresAuth: true },
  },
  {
    path: '/heatmap',
    name: 'Heatmap',
    component: () => import('../views/HeatmapView.vue'),
    meta: { requiresAuth: true },
  },
]

const router = createRouter({
  history: createWebHistory(),
  routes,
})

router.beforeEach((to, _from, next) => {
  const auth = useAuthStore()
  if (to.meta.requiresAuth && !auth.isLoggedIn) {
    next('/login')
  } else if (to.path === '/login' && auth.isLoggedIn) {
    next('/list')
  } else {
    next()
  }
})

export default router

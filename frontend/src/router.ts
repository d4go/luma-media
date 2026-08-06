import { createRouter, createWebHistory } from 'vue-router'

export const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'dashboard', component: () => import('./views/DashboardView.vue'), meta: { title: '概览' } },
    { path: '/folders', name: 'folders', component: () => import('./views/FoldersView.vue'), meta: { title: '媒体目录' } },
    { path: '/tasks', name: 'tasks', component: () => import('./views/TasksView.vue'), meta: { title: '任务中心' } },
    { path: '/media', name: 'media', component: () => import('./views/MediaView.vue'), meta: { title: '媒体库' } },
    { path: '/settings', name: 'settings', component: () => import('./views/SettingsView.vue'), meta: { title: '系统设置' } },
  ],
})

router.afterEach((to) => {
  document.title = `${String(to.meta.title)} | Luma Media`
})


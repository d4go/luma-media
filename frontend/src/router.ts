import { createRouter, createWebHistory } from 'vue-router'

export const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'dashboard', component: () => import('./views/DashboardView.vue'), meta: { title: 'Dashboard' } },
    { path: '/discovery', name: 'discovery', component: () => import('./views/DiscoveryView.vue'), meta: { title: '资源发现' } },
    { path: '/downloads', name: 'downloads', component: () => import('./views/DownloadsView.vue'), meta: { title: '下载中心' } },
    { path: '/folders', name: 'folders', component: () => import('./views/FoldersView.vue'), meta: { title: '媒体目录' } },
    { path: '/tasks', name: 'tasks', component: () => import('./views/TasksView.vue'), meta: { title: '任务中心' } },
    { path: '/media', name: 'media', component: () => import('./views/MediaView.vue'), meta: { title: '媒体库' } },
    { path: '/automation', name: 'automation', component: () => import('./views/AutomationView.vue'), meta: { title: '自动化规则' } },
    { path: '/crawlers', redirect: '/automation' },
    { path: '/settings', name: 'settings', component: () => import('./views/SettingsView.vue'), meta: { title: '系统设置' } },
  ],
})

router.afterEach((to) => {
  document.title = `${String(to.meta.title)} | Luma`
})


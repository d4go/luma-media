import { createRouter, createWebHistory } from 'vue-router'

export const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'home', component: () => import('./views/HomeView.vue'), meta: { title: '首页' } },
    { path: '/resources', name: 'resources', component: () => import('./views/ResourcesView.vue'), meta: { title: '资源' } },
    { path: '/media/:id', name: 'media-detail', component: () => import('./views/MediaDetailView.vue'), meta: { title: '媒体详情' } },
    { path: '/actors/:id', name: 'actor-detail', component: () => import('./views/ActorDetailView.vue'), meta: { title: '演员详情' } },
    { path: '/downloads', name: 'downloads', component: () => import('./views/AcquisitionsView.vue'), meta: { title: '下载' } },
    { path: '/acquisitions/:id', name: 'acquisition-detail', component: () => import('./views/AcquisitionDetailView.vue'), meta: { title: '获取详情' } },
    { path: '/library', name: 'library', component: () => import('./views/LibraryView.vue'), meta: { title: '媒体库' } },
    { path: '/library/:id', name: 'library-detail', component: () => import('./views/LibraryDetailView.vue'), meta: { title: '媒体库详情' } },
    { path: '/automation', name: 'automation', component: () => import('./views/AutomationProductView.vue'), meta: { title: '自动化' } },
    { path: '/settings', name: 'settings', component: () => import('./views/ProductSettingsView.vue'), meta: { title: '设置' } },
    { path: '/settings/legacy/folders', component: () => import('./views/FoldersView.vue'), meta: { title: '媒体目录诊断' } },
    { path: '/settings/legacy/tasks', component: () => import('./views/TasksView.vue'), meta: { title: '刮削任务诊断' } },
    { path: '/settings/legacy/crawlers', component: () => import('./views/AutomationView.vue'), meta: { title: '脚本诊断' } },
    { path: '/discovery', redirect: '/resources' }, { path: '/media', redirect: '/library' }, { path: '/tasks', redirect: '/settings/legacy/tasks' }, { path: '/folders', redirect: '/settings/legacy/folders' }, { path: '/crawlers', redirect: '/settings/legacy/crawlers' },
  ],
})
router.afterEach(to => { document.title = `${String(to.meta.title)} | Luma` })

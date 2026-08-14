<script setup lang="ts">
import { computed, h, onMounted, onUnmounted, provide, ref, watch } from 'vue'
import { darkTheme, dateZhCN, NButton, NConfigProvider, NDialogProvider, NIcon, NInput, NLoadingBarProvider, NMenu, NMessageProvider, NTooltip, zhCN, type GlobalThemeOverrides, type MenuOption } from 'naive-ui'
import { RouterLink, useRoute, useRouter } from 'vue-router'
import { IconAutomation, IconBooks, IconDownload, IconHome, IconList, IconMenu2, IconMoon, IconRefresh, IconSearch, IconSettings, IconSparkles, IconSun, IconX } from '@tabler/icons-vue'
import { api } from './api'
import { serviceStatusKey } from './service-status'
import type { ServiceHealth, ServiceStatus } from './types'

const route = useRoute()
const router = useRouter()
const mobileOpen = ref(false)
const searchText = ref('')
const refreshing = ref(false)
const status = ref<ServiceStatus | null>(null)
const prefersDark = window.matchMedia('(prefers-color-scheme: dark)')
const storedTheme = localStorage.getItem('luma-theme')
const isDark = ref(storedTheme ? storedTheme === 'dark' : prefersDark.matches)
let statusTimer: number | undefined

const renderIcon = (icon: typeof IconHome) => () => h(NIcon, null, { default: () => h(icon) })
const renderLink = (label: string, to: string) => () => h(RouterLink, { to }, { default: () => label })
const menuOptions: MenuOption[] = [
  { label: renderLink('首页', '/'), key: '/', icon: renderIcon(IconHome) },
  { label: renderLink('资源', '/resources'), key: '/resources', icon: renderIcon(IconSparkles) },
  { label: renderLink('下载', '/downloads'), key: '/downloads', icon: renderIcon(IconDownload) },
  { label: renderLink('任务', '/tasks'), key: '/tasks', icon: renderIcon(IconList) },
  { label: renderLink('媒体库', '/library'), key: '/library', icon: renderIcon(IconBooks) },
  { label: renderLink('自动化', '/automation'), key: '/automation', icon: renderIcon(IconAutomation) },
  { label: renderLink('设置', '/settings'), key: '/settings', icon: renderIcon(IconSettings) },
]
const activeKey = computed(() => {
  const path = route.path
  if (path === '/') return '/'
  if (path.startsWith('/media/') || path.startsWith('/actors/')) return '/resources'
  if (path.startsWith('/acquisitions/')) return '/downloads'
  if (path.startsWith('/library')) return '/library'
  return `/${path.split('/')[1]}`
})
const checkedAt = computed(() => status.value?.checkedAt
  ? new Date(`${status.value.checkedAt}Z`).toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit', second: '2-digit' })
  : '等待检查')
const services = computed(() => [
  { key: 'luma', name: 'Luma', health: status.value?.luma },
  { key: 'metatube', name: 'MetaTube', health: status.value?.metaTube },
  { key: 'qbittorrent', name: 'qBittorrent', health: status.value?.qbittorrent },
])
const themeOverrides = computed<GlobalThemeOverrides>(() => ({
  common: {
    primaryColor: isDark.value ? '#2997ff' : '#0071e3',
    primaryColorHover: isDark.value ? '#4ba5ff' : '#0077ed',
    primaryColorPressed: isDark.value ? '#1f7fdd' : '#0060c9',
    primaryColorSuppl: isDark.value ? '#2997ff' : '#0071e3',
    infoColor: isDark.value ? '#2997ff' : '#0071e3',
    borderRadius: '10px',
    borderRadiusSmall: '8px',
    fontFamily: '-apple-system, BlinkMacSystemFont, "SF Pro Text", "Segoe UI", "PingFang SC", "Hiragino Sans GB", "Microsoft YaHei", sans-serif',
  },
  Button: { fontWeight: '600', borderRadiusMedium: '10px' }, Card: { borderRadius: '14px' },
  Menu: isDark.value
    ? { itemTextColor: '#a1a1a6', itemTextColorHover: '#f5f5f7', itemTextColorActive: '#ffffff', itemIconColor: '#86868b', itemIconColorHover: '#f5f5f7', itemIconColorActive: '#ffffff', itemColorHover: 'rgba(255,255,255,.08)', itemColorActive: 'rgba(41,151,255,.28)', itemColorActiveHover: 'rgba(41,151,255,.32)', itemBorderRadius: '10px' }
    : { itemTextColor: '#6e6e73', itemTextColorHover: '#1d1d1f', itemTextColorActive: '#0071e3', itemIconColor: '#86868b', itemIconColorHover: '#3a3a3c', itemIconColorActive: '#0071e3', itemColorHover: 'rgba(0,0,0,.05)', itemColorActive: 'rgba(0,113,227,.12)', itemColorActiveHover: 'rgba(0,113,227,.16)', itemBorderRadius: '10px' },
}))

function unavailable(message: string): ServiceHealth { return { connected: false, message, latencyMs: null } }
async function refreshStatus() {
  if (refreshing.value) return
  refreshing.value = true
  try { status.value = await api.serviceStatus() }
  catch (reason) {
    const message = reason instanceof Error ? reason.message : '服务状态请求失败'
    status.value = { luma: unavailable(message), metaTube: unavailable('Luma 不可达'), qbittorrent: unavailable('Luma 不可达'), checkedAt: new Date().toISOString() }
  } finally { refreshing.value = false }
}
function submitSearch() {
  const query = searchText.value.trim()
  if (query) router.push({ path: '/resources', query: { q: query } })
}
function toggleTheme() { isDark.value = !isDark.value; localStorage.setItem('luma-theme', isDark.value ? 'dark' : 'light') }
function onSystemTheme(event: MediaQueryListEvent) { if (!localStorage.getItem('luma-theme')) isDark.value = event.matches }

provide(serviceStatusKey, { status, refreshing, refresh: refreshStatus })
watch(isDark, dark => document.body.classList.toggle('luma-dark', dark), { immediate: true })
watch(() => route.path, () => { mobileOpen.value = false })
onMounted(() => { prefersDark.addEventListener('change', onSystemTheme); refreshStatus(); statusTimer = window.setInterval(refreshStatus, 10000) })
onUnmounted(() => { prefersDark.removeEventListener('change', onSystemTheme); window.clearInterval(statusTimer); document.body.classList.remove('luma-dark') })
</script>

<template>
  <n-config-provider :theme="isDark ? darkTheme : null" :theme-overrides="themeOverrides" :locale="zhCN" :date-locale="dateZhCN">
    <n-message-provider><n-dialog-provider><n-loading-bar-provider>
      <div class="app-shell">
        <aside class="sidebar">
          <RouterLink to="/" class="brand" aria-label="Luma 首页"><span class="brand-mark">L</span><span class="brand-copy"><strong>Luma</strong><small>MEDIA ORCHESTRATOR</small></span></RouterLink>
          <nav class="primary-nav" aria-label="主要导航"><n-menu :value="activeKey" :options="menuOptions" /></nav>
          <section class="service-rack" aria-label="服务状态">
            <div class="service-rack-head"><span>服务状态</span><n-button text :loading="refreshing" aria-label="刷新服务状态" @click="refreshStatus"><template #icon><IconRefresh :size="15" /></template></n-button></div>
            <n-tooltip v-for="service in services" :key="service.key" trigger="hover">
              <template #trigger><div class="service-row"><i class="status-dot" :class="{ online: service.health?.connected, waiting: !service.health }" /><span>{{ service.name }}</span><small>{{ !service.health ? '检查中' : service.health.connected ? '在线' : '离线' }}</small></div></template>
              {{ service.health?.message ?? '正在检查连接' }}
            </n-tooltip>
            <span class="service-checked">{{ checkedAt }} · 每 10 秒刷新</span>
          </section>
        </aside>

        <div class="mobile-backdrop" :class="{ visible: mobileOpen }" @click="mobileOpen = false" />
        <aside class="mobile-sidebar" :class="{ visible: mobileOpen }">
          <div class="mobile-sidebar-head"><span class="brand compact"><span class="brand-mark">L</span><span class="brand-copy"><strong>Luma</strong></span></span><n-button quaternary circle aria-label="关闭菜单" @click="mobileOpen = false"><template #icon><IconX /></template></n-button></div>
          <n-menu :value="activeKey" :options="menuOptions" />
        </aside>

        <main class="main-panel">
          <header class="topbar">
            <n-button class="mobile-menu" quaternary circle aria-label="打开菜单" @click="mobileOpen = true"><template #icon><IconMenu2 /></template></n-button>
            <div class="global-search"><IconSearch :size="17" /><n-input v-model:value="searchText" :bordered="false" clearable placeholder="搜索番号、标题或演员" aria-label="全局搜索" @keyup.enter="submitSearch" /></div>
            <span class="topbar-service"><i class="status-dot" :class="{ online: status?.luma.connected }" />{{ status?.luma.connected ? '系统正常' : '连接异常' }}</span>
            <n-button quaternary circle :aria-label="isDark ? '切换到浅色模式' : '切换到深色模式'" @click="toggleTheme"><template #icon><IconSun v-if="isDark" /><IconMoon v-else /></template></n-button>
          </header>
          <div class="page-container"><router-view /></div>
        </main>
      </div>
    </n-loading-bar-provider></n-dialog-provider></n-message-provider>
  </n-config-provider>
</template>

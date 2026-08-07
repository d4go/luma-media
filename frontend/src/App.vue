<script setup lang="ts">
import { computed, h, onMounted, onUnmounted, provide, ref, watch } from 'vue'
import {
  darkTheme, dateZhCN, NButton, NConfigProvider, NDialogProvider, NIcon, NLoadingBarProvider,
  NMenu, NMessageProvider, NTooltip, type GlobalThemeOverrides, type MenuOption,
  zhCN,
} from 'naive-ui'
import { RouterLink, useRoute } from 'vue-router'
import {
  IconActivityHeartbeat, IconAutomation, IconBinoculars, IconDownload,
  IconGauge, IconListCheck, IconMenu2, IconMoon, IconMovie, IconRefresh, IconSettings,
  IconSun, IconX,
} from '@tabler/icons-vue'
import { api } from './api'
import { serviceStatusKey } from './service-status'
import type { ServiceHealth, ServiceStatus } from './types'

const route = useRoute()
const mobileOpen = ref(false)
const refreshing = ref(false)
const status = ref<ServiceStatus | null>(null)
const prefersDark = window.matchMedia('(prefers-color-scheme: dark)')
const storedTheme = localStorage.getItem('luma-theme')
const isDark = ref(storedTheme ? storedTheme === 'dark' : prefersDark.matches)
let statusTimer: number | undefined

const renderIcon = (icon: typeof IconGauge) => () => h(NIcon, null, { default: () => h(icon) })
const renderLink = (label: string, to: string) => () => h(RouterLink, { to }, { default: () => label })
const menuOptions: MenuOption[] = [
  { label: renderLink('Dashboard', '/'), key: '/', icon: renderIcon(IconGauge) },
  { label: renderLink('资源发现', '/discovery'), key: '/discovery', icon: renderIcon(IconBinoculars) },
  { label: renderLink('下载中心', '/downloads'), key: '/downloads', icon: renderIcon(IconDownload) },
  { label: renderLink('任务中心', '/tasks'), key: '/tasks', icon: renderIcon(IconListCheck) },
  { label: renderLink('媒体库', '/media'), key: '/media', icon: renderIcon(IconMovie) },
  { label: renderLink('自动化规则', '/automation'), key: '/automation', icon: renderIcon(IconAutomation) },
  { label: renderLink('系统设置', '/settings'), key: '/settings', icon: renderIcon(IconSettings) },
]
const activeKey = computed(() => route.path)
const checkedAt = computed(() => {
  if (!status.value?.checkedAt) return '等待首次检查'
  return `更新于 ${new Date(`${status.value.checkedAt}Z`).toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit', second: '2-digit' })}`
})
const services = computed(() => [
  { key: 'luma', name: 'Luma', health: status.value?.luma },
  { key: 'metatube', name: 'MetaTube', health: status.value?.metaTube },
  { key: 'qbittorrent', name: 'qBittorrent', health: status.value?.qbittorrent },
])

const themeOverrides: GlobalThemeOverrides = {
  common: {
    primaryColor: '#e45735', primaryColorHover: '#ef6845', primaryColorPressed: '#c9472a',
    primaryColorSuppl: '#e45735', infoColor: '#e45735', borderRadius: '10px', borderRadiusSmall: '8px',
    fontFamily: 'Inter, "IBM Plex Sans", "PingFang SC", "Microsoft YaHei", sans-serif',
  },
  Button: { fontWeight: '650', borderRadiusMedium: '10px' },
  Card: { borderRadius: '14px' },
  Menu: {
    itemTextColor: '#a7abb2', itemTextColorHover: '#f5f6f7', itemTextColorActive: '#ffffff',
    itemIconColor: '#7f848d', itemIconColorHover: '#ffffff', itemIconColorActive: '#ffffff',
    itemColorHover: 'rgba(255,255,255,.07)', itemColorActive: 'rgba(228,87,53,.2)',
    itemColorActiveHover: 'rgba(228,87,53,.25)', itemBorderRadius: '9px',
  },
}

function unavailable(message: string): ServiceHealth {
  return { connected: false, message, latencyMs: null }
}

async function refreshStatus() {
  if (refreshing.value) return
  refreshing.value = true
  try {
    status.value = await api.serviceStatus()
  } catch (reason) {
    const message = reason instanceof Error ? reason.message : '服务状态请求失败'
    status.value = {
      luma: unavailable(message), metaTube: unavailable('Luma 不可达，无法检查 MetaTube'),
      qbittorrent: unavailable('Luma 不可达，无法检查 qBittorrent'), checkedAt: new Date().toISOString(),
    }
  } finally {
    refreshing.value = false
  }
}

function toggleTheme() {
  isDark.value = !isDark.value
  localStorage.setItem('luma-theme', isDark.value ? 'dark' : 'light')
}

function onSystemTheme(event: MediaQueryListEvent) {
  if (!localStorage.getItem('luma-theme')) isDark.value = event.matches
}

provide(serviceStatusKey, { status, refreshing, refresh: refreshStatus })
watch(isDark, (dark) => document.body.classList.toggle('luma-dark', dark), { immediate: true })
watch(() => route.path, () => { mobileOpen.value = false })
onMounted(() => {
  prefersDark.addEventListener('change', onSystemTheme)
  refreshStatus()
  statusTimer = window.setInterval(refreshStatus, 10000)
})
onUnmounted(() => {
  prefersDark.removeEventListener('change', onSystemTheme)
  window.clearInterval(statusTimer)
  document.body.classList.remove('luma-dark')
})
</script>

<template>
  <n-config-provider :theme="isDark ? darkTheme : null" :theme-overrides="themeOverrides" :locale="zhCN" :date-locale="dateZhCN">
    <n-message-provider>
      <n-dialog-provider>
        <n-loading-bar-provider>
          <div class="app-shell" :class="{ 'is-dark': isDark }">
            <aside class="sidebar">
              <div class="brand">
                <div class="brand-mark" aria-hidden="true"><span>L</span></div>
                <div class="brand-copy"><strong>Luma</strong><span>MEDIA AUTOMATION</span></div>
              </div>
              <nav class="primary-nav" aria-label="主要导航">
                <n-menu :value="activeKey" :options="menuOptions" @update:value="mobileOpen = false" />
              </nav>
              <section class="service-rack" aria-label="服务状态">
                <div class="service-rack-head">
                  <span>服务状态</span>
                  <n-button text :loading="refreshing" aria-label="刷新服务状态" @click="refreshStatus">
                    <template #icon><IconRefresh :size="15" /></template>
                  </n-button>
                </div>
                <n-tooltip v-for="service in services" :key="service.key" trigger="hover">
                  <template #trigger>
                    <div class="service-row">
                      <span class="status-dot" :class="{ online: service.health?.connected, waiting: !service.health }" />
                      <span>{{ service.name }}</span>
                      <small>{{ !service.health ? '检查中' : service.health.connected ? '在线' : '离线' }}</small>
                    </div>
                  </template>
                  {{ service.health?.message ?? '正在检查连接' }}
                </n-tooltip>
                <span class="service-checked">{{ checkedAt }} · 每 10 秒刷新</span>
              </section>
            </aside>

            <div class="mobile-backdrop" :class="{ visible: mobileOpen }" @click="mobileOpen = false" />
            <aside class="mobile-sidebar" :class="{ visible: mobileOpen }">
              <div class="mobile-sidebar-head">
                <div class="brand compact"><div class="brand-mark"><span>L</span></div><div class="brand-copy"><strong>Luma</strong></div></div>
                <n-button quaternary circle aria-label="关闭菜单" @click="mobileOpen = false"><template #icon><IconX /></template></n-button>
              </div>
              <n-menu :value="activeKey" :options="menuOptions" @update:value="mobileOpen = false" />
              <div class="mobile-service-summary">
                <span v-for="service in services" :key="service.key" class="mobile-service-item">
                  <i class="status-dot" :class="{ online: service.health?.connected, waiting: !service.health }" />{{ service.name }}
                </span>
              </div>
            </aside>

            <main class="main-panel">
              <header class="topbar">
                <n-button class="mobile-menu" quaternary circle aria-label="打开菜单" @click="mobileOpen = true"><template #icon><IconMenu2 /></template></n-button>
                <IconActivityHeartbeat :size="17" class="topbar-glyph" />
                <span class="topbar-title">{{ route.meta.title }}</span>
                <span class="topbar-status">{{ checkedAt }}</span>
                <n-button quaternary circle :aria-label="isDark ? '切换到浅色模式' : '切换到深色模式'" @click="toggleTheme">
                  <template #icon><IconSun v-if="isDark" /><IconMoon v-else /></template>
                </n-button>
              </header>
              <div class="page-container"><router-view /></div>
            </main>
          </div>
        </n-loading-bar-provider>
      </n-dialog-provider>
    </n-message-provider>
  </n-config-provider>
</template>

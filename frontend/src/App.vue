<script setup lang="ts">
import { computed, h, onMounted, onUnmounted, ref } from 'vue'
import {
  darkTheme,
  NButton,
  NConfigProvider,
  NDialogProvider,
  NIcon,
  NLoadingBarProvider,
  NMenu,
  NMessageProvider,
  NTag,
  type GlobalThemeOverrides,
  type MenuOption,
} from 'naive-ui'
import { RouterLink, useRoute } from 'vue-router'
import {
  IconFolders, IconLayoutDashboard, IconListCheck, IconMenu2, IconMoon,
  IconMovie, IconSettings, IconSun, IconX,
} from '@tabler/icons-vue'

const route = useRoute()
const mobileOpen = ref(false)
const prefersDark = window.matchMedia('(prefers-color-scheme: dark)')
const storedTheme = localStorage.getItem('luma-theme')
const isDark = ref(storedTheme ? storedTheme === 'dark' : prefersDark.matches)

const renderIcon = (icon: typeof IconLayoutDashboard) => () => h(NIcon, null, { default: () => h(icon) })
const renderLink = (label: string, to: string) => () => h(RouterLink, { to }, { default: () => label })
const menuOptions: MenuOption[] = [
  { label: renderLink('概览', '/'), key: '/', icon: renderIcon(IconLayoutDashboard) },
  { label: renderLink('媒体目录', '/folders'), key: '/folders', icon: renderIcon(IconFolders) },
  { label: renderLink('任务中心', '/tasks'), key: '/tasks', icon: renderIcon(IconListCheck) },
  { label: renderLink('媒体库', '/media'), key: '/media', icon: renderIcon(IconMovie) },
  { label: renderLink('系统设置', '/settings'), key: '/settings', icon: renderIcon(IconSettings) },
]
const activeKey = computed(() => route.path)

const themeOverrides: GlobalThemeOverrides = {
  common: {
    primaryColor: '#2f7d64', primaryColorHover: '#3c9075', primaryColorPressed: '#286b56',
    primaryColorSuppl: '#2f7d64', borderRadius: '10px', borderRadiusSmall: '8px',
    fontFamily: '"Avenir Next", Avenir, "Segoe UI", "Noto Sans SC", sans-serif',
  },
  Button: { fontWeight: '600' },
  Card: { borderRadius: '14px' },
}

function toggleTheme() {
  isDark.value = !isDark.value
  localStorage.setItem('luma-theme', isDark.value ? 'dark' : 'light')
}

function onSystemTheme(event: MediaQueryListEvent) {
  if (!localStorage.getItem('luma-theme')) isDark.value = event.matches
}

onMounted(() => prefersDark.addEventListener('change', onSystemTheme))
onUnmounted(() => prefersDark.removeEventListener('change', onSystemTheme))
</script>

<template>
  <n-config-provider :theme="isDark ? darkTheme : null" :theme-overrides="themeOverrides">
    <n-message-provider>
      <n-dialog-provider>
        <n-loading-bar-provider>
          <div class="app-shell" :class="{ 'is-dark': isDark }">
            <aside class="sidebar">
              <div class="brand">
                <div class="brand-mark"><IconMovie :size="24" :stroke-width="1.8" /></div>
                <div><strong>Luma Media</strong><span>媒体管理中心</span></div>
              </div>
              <n-menu :value="activeKey" :options="menuOptions" @update:value="mobileOpen = false" />
              <div class="sidebar-footer">
                <span>服务状态</span>
                <n-tag type="success" size="small" :bordered="false">运行正常</n-tag>
              </div>
            </aside>

            <div class="mobile-backdrop" :class="{ visible: mobileOpen }" @click="mobileOpen = false" />
            <aside class="mobile-sidebar" :class="{ visible: mobileOpen }">
              <div class="mobile-sidebar-head">
                <div class="brand compact"><div class="brand-mark"><IconMovie :size="22" /></div><strong>Luma Media</strong></div>
                <n-button quaternary circle aria-label="关闭菜单" @click="mobileOpen = false"><template #icon><IconX /></template></n-button>
              </div>
              <n-menu :value="activeKey" :options="menuOptions" @update:value="mobileOpen = false" />
            </aside>

            <main class="main-panel">
              <div class="topbar">
                <n-button class="mobile-menu" quaternary circle aria-label="打开菜单" @click="mobileOpen = true"><template #icon><IconMenu2 /></template></n-button>
                <span class="topbar-title">{{ route.meta.title }}</span>
                <n-button quaternary circle :aria-label="isDark ? '切换到浅色模式' : '切换到深色模式'" @click="toggleTheme">
                  <template #icon><IconSun v-if="isDark" /><IconMoon v-else /></template>
                </n-button>
              </div>
              <div class="page-container"><router-view /></div>
            </main>
          </div>
        </n-loading-bar-provider>
      </n-dialog-provider>
    </n-message-provider>
  </n-config-provider>
</template>

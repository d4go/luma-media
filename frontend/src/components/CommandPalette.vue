<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { IconAutomation, IconBooks, IconDownload, IconHome, IconList, IconMoon, IconRefresh, IconSearch, IconSettings, IconSparkles, IconSun } from '@tabler/icons-vue'

const props = defineProps<{ show: boolean; isDark: boolean }>()
const emit = defineEmits<{
  (e: 'close'): void
  (e: 'toggle-theme'): void
  (e: 'refresh-status'): void
}>()

const router = useRouter()
const query = ref('')
const activeIndex = ref(0)
const inputEl = ref<HTMLInputElement | null>(null)

interface Command {
  key: string
  label: string
  hint?: string
  icon: unknown
  run: () => void
}

const commands = computed<Command[]>(() => {
  const q = query.value.trim().toLocaleLowerCase()
  const go = (path: string) => { router.push(path) }
  const nav: Command[] = [
    { key: 'home', label: '首页', hint: '/', icon: IconHome, run: () => go('/') },
    { key: 'resources', label: '资源', hint: '/resources', icon: IconSparkles, run: () => go('/resources') },
    { key: 'downloads', label: '下载', hint: '/downloads', icon: IconDownload, run: () => go('/downloads') },
    { key: 'tasks', label: '任务', hint: '/tasks', icon: IconList, run: () => go('/tasks') },
    { key: 'library', label: '媒体库', hint: '/library', icon: IconBooks, run: () => go('/library') },
    { key: 'automation', label: '自动化', hint: '/automation', icon: IconAutomation, run: () => go('/automation') },
    { key: 'settings', label: '设置', hint: '/settings', icon: IconSettings, run: () => go('/settings') },
  ]
  const actions: Command[] = [
    { key: 'theme', label: props.isDark ? '切换到浅色模式' : '切换到深色模式', icon: props.isDark ? IconSun : IconMoon, run: () => emit('toggle-theme') },
    { key: 'refresh', label: '刷新服务状态', icon: IconRefresh, run: () => emit('refresh-status') },
  ]
  const search: Command[] = q
    ? [{ key: 'search', label: `搜索资源：“${query.value.trim()}”`, icon: IconSearch, run: () => go(`/resources?q=${encodeURIComponent(query.value.trim())}`) }]
    : []
  const filtered = q ? nav.filter(item => item.label.toLocaleLowerCase().includes(q) || item.key.includes(q)) : nav
  return [...search, ...filtered, ...(q ? [] : actions)]
})

function execute(command: Command) {
  command.run()
  emit('close')
}

function onKeydown(event: KeyboardEvent) {
  if (!props.show) return
  if (event.key === 'Escape') {
    event.preventDefault()
    emit('close')
  } else if (event.key === 'ArrowDown') {
    event.preventDefault()
    activeIndex.value = Math.min(activeIndex.value + 1, commands.value.length - 1)
  } else if (event.key === 'ArrowUp') {
    event.preventDefault()
    activeIndex.value = Math.max(activeIndex.value - 1, 0)
  } else if (event.key === 'Enter') {
    event.preventDefault()
    const command = commands.value[activeIndex.value]
    if (command) execute(command)
  }
}

watch(() => props.show, open => {
  if (!open) return
  query.value = ''
  activeIndex.value = 0
  requestAnimationFrame(() => inputEl.value?.focus())
})
watch(commands, () => { activeIndex.value = 0 })
onMounted(() => window.addEventListener('keydown', onKeydown))
onUnmounted(() => window.removeEventListener('keydown', onKeydown))
</script>

<template>
  <transition name="palette">
    <div v-if="show" class="command-palette" role="dialog" aria-modal="true" @click.self="emit('close')">
      <div class="command-palette-panel">
        <div class="command-input">
          <IconSearch :size="18" />
          <input ref="inputEl" v-model="query" type="text" placeholder="搜索资源，或输入命令" aria-label="命令面板搜索" />
        </div>
        <div class="command-results">
          <button v-for="(command, index) in commands" :key="command.key" type="button" class="command-row" :class="{ active: index === activeIndex }" @mouseenter="activeIndex = index" @click="execute(command)">
            <span class="command-row-icon"><component :is="command.icon" :size="17" /></span>
            <span class="command-row-label">{{ command.label }}</span>
            <span v-if="command.hint" class="command-row-hint">{{ command.hint }}</span>
          </button>
          <div v-if="!commands.length" class="command-empty">没有匹配的入口</div>
        </div>
        <footer class="command-foot"><span>↑↓ 选择</span><span>回车 打开</span><span>esc 关闭</span></footer>
      </div>
    </div>
  </transition>
</template>

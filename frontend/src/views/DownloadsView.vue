<script setup lang="ts">
import { computed, h, onMounted, onUnmounted, ref } from 'vue'
import { NButton, NDataTable, NPopconfirm, NProgress, NTag, type DataTableColumns, useMessage } from 'naive-ui'
import { IconPlayerPause, IconPlayerPlay, IconRefresh, IconTrash } from '@tabler/icons-vue'
import PageHeader from '../components/PageHeader.vue'
import EmptyState from '../components/EmptyState.vue'
import { api } from '../api'
import type { DownloadItem } from '../types'

const message = useMessage()
const loading = ref(true)
const downloads = ref<DownloadItem[]>([])
const actingHash = ref('')
let poller: number | undefined

const activeCount = computed(() => downloads.value.filter(item => !isPaused(item) && item.progress < 1).length)
const totalDownloadSpeed = computed(() => downloads.value.reduce((total, item) => total + item.downloadSpeed, 0))
const totalUploadSpeed = computed(() => downloads.value.reduce((total, item) => total + item.uploadSpeed, 0))

function isPaused(item: DownloadItem) { return item.state.toLocaleLowerCase().includes('paused') }
function formatBytes(value: number) {
  if (!Number.isFinite(value) || value <= 0) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  const index = Math.min(Math.floor(Math.log(value) / Math.log(1024)), units.length - 1)
  return `${(value / 1024 ** index).toFixed(index > 1 ? 1 : 0)} ${units[index]}`
}
function formatSpeed(value: number) { return value > 0 ? `${formatBytes(value)}/s` : '0 B/s' }
function formatEta(value: number) {
  if (value <= 0 || value >= 8_640_000) return '未知'
  const hours = Math.floor(value / 3600)
  const minutes = Math.floor((value % 3600) / 60)
  if (hours) return `${hours} 小时 ${minutes} 分`
  return `${Math.max(1, minutes)} 分钟`
}
function stateLabel(item: DownloadItem) {
  const state = item.state.toLocaleLowerCase()
  if (state.includes('error') || state.includes('missing')) return '异常'
  if (isPaused(item)) return '已暂停'
  if (item.progress >= 1 || state.includes('upload')) return '做种中'
  if (state.includes('stalled')) return '等待数据'
  if (state.includes('check')) return '校验中'
  if (state.includes('queue')) return '排队中'
  return '下载中'
}
function stateType(item: DownloadItem) {
  const label = stateLabel(item)
  return label === '异常' ? 'error' : label === '已暂停' ? 'warning' : label === '做种中' ? 'success' : label === '下载中' ? 'info' : 'default'
}

const columns: DataTableColumns<DownloadItem> = [
  { title: '下载任务', key: 'name', minWidth: 280, render: row => h('div', { class: 'resource-cell' }, [
    h('strong', row.name), h('span', { class: 'mono-value', title: row.savePath }, row.savePath),
  ]) },
  { title: '大小', key: 'size', width: 105, render: row => formatBytes(row.size) },
  { title: '进度', key: 'progress', minWidth: 180, render: row => h('div', { class: 'download-progress' }, [
    h(NProgress, { type: 'line', percentage: Math.round(row.progress * 1000) / 10, height: 5, showIndicator: false, processing: stateLabel(row) === '下载中' }),
    h('span', `${Math.round(row.progress * 1000) / 10}%`),
  ]) },
  { title: '速度', key: 'speed', width: 150, render: row => h('div', { class: 'speed-stack' }, [h('span', `↓ ${formatSpeed(row.downloadSpeed)}`), h('small', `↑ ${formatSpeed(row.uploadSpeed)}`)]) },
  { title: '剩余时间', key: 'eta', width: 110, render: row => formatEta(row.eta) },
  { title: '状态', key: 'state', width: 100, render: row => h(NTag, { size: 'small', bordered: false, type: stateType(row) }, { default: () => stateLabel(row) }) },
  { title: '操作', key: 'actions', width: 150, align: 'right', render: row => h('div', { class: 'row-actions' }, [
    isPaused(row)
      ? h(NButton, { quaternary: true, circle: true, title: '继续', loading: actingHash.value === row.hash, onClick: () => resume(row) }, { icon: () => h(IconPlayerPlay) })
      : h(NButton, { quaternary: true, circle: true, title: '暂停', loading: actingHash.value === row.hash, onClick: () => pause(row) }, { icon: () => h(IconPlayerPause) }),
    h(NPopconfirm, { onPositiveClick: () => remove(row) }, {
      trigger: () => h(NButton, { quaternary: true, circle: true, type: 'error', title: '移除任务' }, { icon: () => h(IconTrash) }),
      default: () => '仅从 qBittorrent 移除任务，不删除已下载文件。',
    }),
  ]) },
]

async function load(silent = false) {
  if (!silent) loading.value = true
  try { downloads.value = await api.downloads() }
  catch (reason) { if (!silent) message.error(reason instanceof Error ? reason.message : '下载任务加载失败') }
  finally { loading.value = false }
}
async function pause(item: DownloadItem) {
  actingHash.value = item.hash
  try { await api.pauseDownload(item.hash); message.success('下载已暂停'); await load(true) }
  catch (reason) { message.error(reason instanceof Error ? reason.message : '暂停失败') }
  finally { actingHash.value = '' }
}
async function resume(item: DownloadItem) {
  actingHash.value = item.hash
  try { await api.resumeDownload(item.hash); message.success('下载已继续'); await load(true) }
  catch (reason) { message.error(reason instanceof Error ? reason.message : '继续失败') }
  finally { actingHash.value = '' }
}
async function remove(item: DownloadItem) {
  actingHash.value = item.hash
  try { await api.removeDownload(item.hash); message.success('下载任务已移除'); await load(true) }
  catch (reason) { message.error(reason instanceof Error ? reason.message : '移除失败') }
  finally { actingHash.value = '' }
}

onMounted(() => { load(); poller = window.setInterval(() => load(true), 5000) })
onUnmounted(() => window.clearInterval(poller))
</script>

<template>
  <PageHeader title="下载中心" description="直接管理 qBittorrent 下载生命周期，查看速度、进度与任务状态。">
    <n-button secondary :loading="loading" @click="load()"><template #icon><IconRefresh /></template>刷新</n-button>
  </PageHeader>
  <section class="download-summary" aria-label="下载实时状态">
    <div><span>活动任务</span><strong>{{ activeCount }}</strong></div>
    <div><span>下载速度</span><strong>{{ formatSpeed(totalDownloadSpeed) }}</strong></div>
    <div><span>上传速度</span><strong>{{ formatSpeed(totalUploadSpeed) }}</strong></div>
    <div><span>全部任务</span><strong>{{ downloads.length }}</strong></div>
  </section>
  <section class="panel">
    <EmptyState v-if="!loading && !downloads.length" title="下载队列为空" description="从资源发现页面选择候选资源并加入下载。">
      <n-button type="primary" tag="a" href="/discovery">前往资源发现</n-button>
    </EmptyState>
    <div v-else class="table-wrap"><n-data-table :loading="loading" :columns="columns" :data="downloads" :row-key="(row: DownloadItem) => row.hash" :bordered="false" :single-line="false" /></div>
  </section>
</template>

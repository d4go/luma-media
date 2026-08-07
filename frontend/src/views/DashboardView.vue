<script setup lang="ts">
import { computed, h, onMounted, onUnmounted, ref } from 'vue'
import { NAlert, NButton, NDataTable, NProgress, NSkeleton, NTag, type DataTableColumns, useMessage } from 'naive-ui'
import { IconCircleCheck, IconCircleX, IconListCheck, IconMovie } from '@tabler/icons-vue'
import { RouterLink } from 'vue-router'
import PageHeader from '../components/PageHeader.vue'
import EmptyState from '../components/EmptyState.vue'
import { api } from '../api'
import { formatDate, statusLabel, statusType, taskTypeLabel } from '../format'
import type { DashboardStats, Task } from '../types'

const message = useMessage()
const loading = ref(true)
const error = ref('')
const stats = ref<DashboardStats>({ mediaCount: 0, taskCount: 0, successCount: 0, failedCount: 0, recentActivity: [] })
let timer: number | undefined

const metrics = computed(() => [
  { label: '媒体文件', value: stats.value.mediaCount, icon: IconMovie },
  { label: '逻辑任务', value: stats.value.taskCount, icon: IconListCheck },
  { label: '成功任务', value: stats.value.successCount, icon: IconCircleCheck },
  { label: '失败任务', value: stats.value.failedCount, icon: IconCircleX },
])

const columns: DataTableColumns<Task> = [
  { title: '任务', key: 'id', width: 90, render: (row) => h(RouterLink, { class: 'resource-link', to: { path: '/tasks', query: { taskId: row.id } } }, { default: () => `#${row.id}` }) },
  { title: '关联资源', key: 'resource', minWidth: 200, render: (row) => row.media?.title ?? row.folder?.name ?? '关联资源已删除' },
  { title: '类型', key: 'taskType', render: (row) => taskTypeLabel[row.taskType] },
  { title: '状态', key: 'status', render: (row) => h(NTag, { type: statusType[row.status], bordered: false, size: 'small' }, { default: () => statusLabel[row.status] }) },
  { title: '进度', key: 'progress', render: (row) => h(NProgress, { type: 'line', percentage: row.progress, height: 5, showIndicator: false, processing: row.status === 'running' }) },
  { title: '最近执行', key: 'updatedAt', render: (row) => formatDate(row.updatedAt) },
]

async function load(silent = false) {
  if (!silent) loading.value = true
  error.value = ''
  try { stats.value = await api.dashboard() }
  catch (reason) {
    error.value = reason instanceof Error ? reason.message : '无法连接到服务'
    if (silent) message.error(error.value)
  } finally { loading.value = false }
}

onMounted(() => {
  load()
  timer = window.setInterval(() => load(true), 10000)
})
onUnmounted(() => window.clearInterval(timer))
</script>

<template>
  <PageHeader title="媒体概览" description="查看媒体索引与后台任务的当前状态。">
    <n-button secondary :loading="loading" @click="load()">刷新数据</n-button>
  </PageHeader>

  <n-alert v-if="error" type="error" title="数据加载失败" style="margin-bottom: 18px">
    {{ error }}
  </n-alert>

  <div class="metric-grid">
    <n-skeleton v-if="loading" v-for="index in 4" :key="index" height="136px" :sharp="false" />
    <article v-else v-for="metric in metrics" :key="metric.label" class="metric-card">
      <div class="metric-top"><span>{{ metric.label }}</span><span class="metric-icon"><component :is="metric.icon" :size="19" :stroke-width="1.8" /></span></div>
      <span class="metric-value">{{ metric.value.toLocaleString('zh-CN') }}</span>
    </article>
  </div>

  <section class="panel">
    <div class="panel-heading"><h2>最近活动</h2><n-button text type="primary" tag="a" href="/tasks">查看全部</n-button></div>
    <div v-if="loading" style="padding: 20px"><n-skeleton text :repeat="6" /></div>
    <EmptyState v-else-if="!stats.recentActivity.length" title="还没有任务记录" description="添加媒体目录并开始扫描后，活动会显示在这里。">
      <n-button type="primary" tag="a" href="/folders">添加目录</n-button>
    </EmptyState>
    <div v-else class="table-wrap"><n-data-table :columns="columns" :data="stats.recentActivity" :bordered="false" :single-line="false" /></div>
  </section>
</template>

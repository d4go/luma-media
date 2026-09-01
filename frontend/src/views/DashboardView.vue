<script setup lang="ts">
import { computed, inject, onMounted, ref } from 'vue'
import { NButton, NSkeleton, NTag, useMessage } from 'naive-ui'
import { RouterLink } from 'vue-router'
import {
  IconAlertTriangle, IconArrowRight, IconBinoculars, IconDownload, IconLibrary,
  IconRefresh, IconSparkles,
} from '@tabler/icons-vue'
import PageHeader from '../components/PageHeader.vue'
import { api } from '../api'
import { formatDate, statusLabel, statusType, taskTypeLabel } from '../format'
import { serviceStatusKey } from '../service-status'
import type { DashboardStats } from '../types'
import PaginationBar from '../components/PaginationBar.vue'

const message = useMessage()
const serviceContext = inject(serviceStatusKey)
const loading = ref(true)
const stats = ref<DashboardStats | null>(null)
const page = ref(1)
const pageSize = ref(8)

const metrics = computed(() => [
  { label: '发现资源', value: stats.value?.candidateCount ?? 0, hint: '候选资源', icon: IconBinoculars, to: '/discovery' },
  { label: '下载任务', value: stats.value?.downloadCount ?? 0, hint: 'qBittorrent', icon: IconDownload, to: '/downloads' },
  { label: '待刮削', value: stats.value?.pendingScrapeCount ?? 0, hint: '等待元数据', icon: IconSparkles, to: '/media' },
  { label: '异常任务', value: stats.value?.failedCount ?? 0, hint: '需要处理', icon: IconAlertTriangle, to: '/tasks?status=failed' },
])

const pipeline = computed(() => [
  { name: '资源发现', detail: `${stats.value?.candidateCount ?? 0} 个候选`, to: '/discovery', ready: true },
  { name: '下载调度', detail: `${stats.value?.downloadCount ?? 0} 个任务`, to: '/downloads', ready: Boolean(serviceContext?.status.value?.qbittorrent.connected) },
  { name: '媒体处理', detail: '目录扫描与整理', to: '/tasks', ready: Boolean(serviceContext?.status.value?.luma.connected) },
  { name: '元数据刮削', detail: `${stats.value?.pendingScrapeCount ?? 0} 个待处理`, to: '/media', ready: Boolean(serviceContext?.status.value?.metaTube.connected) },
  { name: '媒体库', detail: `${stats.value?.mediaCount ?? 0} 个媒体`, to: '/media', ready: true },
])

async function load() {
  loading.value = true
  try {
    stats.value = await api.dashboard(page.value, pageSize.value)
    await serviceContext?.refresh()
  } catch (reason) {
    message.error(reason instanceof Error ? reason.message : 'Dashboard 加载失败')
  } finally {
    loading.value = false
  }
}

function changePage(value: number) { page.value = value; load() }
function changePageSize(value: number) { pageSize.value = value; page.value = 1; load() }

onMounted(load)
</script>

<template>
  <PageHeader title="Dashboard" description="从资源发现到媒体入库，查看整条自动化链路的实时状态。">
    <n-button secondary :loading="loading" @click="load"><template #icon><IconRefresh /></template>刷新</n-button>
    <n-button type="primary" tag="div"><RouterLink class="button-link" to="/automation">新建规则</RouterLink></n-button>
  </PageHeader>

  <section class="orchestration-metrics" aria-label="媒体自动化概览">
    <RouterLink v-for="metric in metrics" :key="metric.label" :to="metric.to" class="orchestration-metric">
      <span class="metric-icon"><component :is="metric.icon" :size="18" /></span>
      <span class="metric-copy"><small>{{ metric.label }}</small><strong>{{ loading ? '···' : metric.value }}</strong><em>{{ metric.hint }}</em></span>
      <IconArrowRight :size="16" class="metric-arrow" />
    </RouterLink>
  </section>

  <section class="panel lifecycle-panel">
    <div class="panel-heading lifecycle-heading">
      <div><h2>媒体生命周期</h2><p>每个模块只处理自己的职责，状态由 Luma 统一编排。</p></div>
      <n-tag :bordered="false" :type="serviceContext?.status.value?.luma.connected ? 'success' : 'error'">
        {{ serviceContext?.status.value?.luma.connected ? '编排中心在线' : '编排中心离线' }}
      </n-tag>
    </div>
    <div class="lifecycle-track">
      <template v-for="(stage, index) in pipeline" :key="stage.name">
        <RouterLink :to="stage.to" class="lifecycle-stage">
          <span class="lifecycle-state" :class="{ ready: stage.ready }" />
          <strong>{{ stage.name }}</strong>
          <small>{{ stage.detail }}</small>
        </RouterLink>
        <IconArrowRight v-if="index < pipeline.length - 1" :size="17" class="lifecycle-arrow" />
      </template>
    </div>
  </section>

  <div class="dashboard-grid">
    <section class="panel activity-panel">
      <div class="panel-heading"><h2>最近任务</h2><RouterLink class="text-link" to="/tasks">查看全部</RouterLink></div>
      <div v-if="loading" class="skeleton-block"><n-skeleton text :repeat="6" /></div>
      <div v-else-if="!stats?.recentActivity.items.length" class="compact-empty">还没有任务记录</div>
      <ol v-else class="activity-list">
        <li v-for="task in stats.recentActivity.items" :key="task.id">
          <span class="activity-id">#{{ task.id }}</span>
          <div><strong>{{ task.media?.title ?? task.folder?.name ?? '系统任务' }}</strong><small>{{ taskTypeLabel[task.taskType] ?? task.taskType }} · {{ formatDate(task.updatedAt) }}</small></div>
          <n-tag size="small" :bordered="false" :type="statusType[task.status]">{{ statusLabel[task.status] }}</n-tag>
        </li>
      </ol>
      <PaginationBar v-if="stats && stats.recentActivity.total > 0" :page="page" :page-size="pageSize" :total="stats.recentActivity.total" :page-sizes="[8, 10, 20]" @update:page="changePage" @update:page-size="changePageSize" />
    </section>

    <aside class="panel autopilot-panel">
      <div class="panel-heading"><h2>自动驾驶状态</h2></div>
      <div class="autopilot-copy">
        <IconLibrary :size="32" :stroke-width="1.5" />
        <strong>{{ stats?.mediaCount ?? 0 }} 个媒体已进入索引</strong>
        <p>配置资源规则后，Luma 会持续执行发现、下载、处理和刮削。</p>
      </div>
      <div class="autopilot-links">
        <RouterLink to="/automation">管理自动化规则 <IconArrowRight :size="15" /></RouterLink>
        <RouterLink to="/folders">配置媒体目录 <IconArrowRight :size="15" /></RouterLink>
        <RouterLink to="/settings">检查外部服务 <IconArrowRight :size="15" /></RouterLink>
      </div>
    </aside>
  </div>
</template>

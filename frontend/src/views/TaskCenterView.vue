<script setup lang="ts">
import { computed, h, onMounted, onUnmounted, reactive, ref } from 'vue'
import {
  NButton, NDataTable, NDrawer, NDrawerContent, NInput, NModal, NSelect, NSpin, NTag,
  useDialog, useMessage, type DataTableColumns,
} from 'naive-ui'
import {
  IconHistory, IconPlayerPause, IconPlayerPlay, IconPlus, IconRefresh,
  IconRotateClockwise, IconX,
} from '@tabler/icons-vue'
import PageHeader from '../components/PageHeader.vue'
import EmptyState from '../components/EmptyState.vue'
import PaginationBar from '../components/PaginationBar.vue'
import { api } from '../api'
import { formatDate } from '../format'
import type { JobEvent, JobItem, JobItemStatus, JobRun, JobRunDetail, JobStatus, ProviderConfig } from '../types'

const message = useMessage()
const dialog = useDialog()
const loading = ref(true)
const status = ref('')
const page = ref(1)
const pageSize = ref(20)
const runs = ref<{ items: JobRun[]; total: number; page: number; pageSize: number; totalPages: number }>({ items: [], total: 0, page: 1, pageSize: 20, totalPages: 0 })

const detailOpen = ref(false)
const detailLoading = ref(false)
const detail = ref<JobRunDetail | null>(null)
const itemStatus = ref('')
const itemPage = ref(1)
const itemPageSize = ref(20)
const items = ref<{ items: JobItem[]; total: number; page: number; pageSize: number; totalPages: number }>({ items: [], total: 0, page: 1, pageSize: 20, totalPages: 0 })
const eventPage = ref(1)
const eventPageSize = ref(20)
const events = ref<{ items: JobEvent[]; total: number; page: number; pageSize: number; totalPages: number }>({ items: [], total: 0, page: 1, pageSize: 20, totalPages: 0 })
const drawerWidth = ref(Math.min(780, window.innerWidth))

const createOpen = ref(false)
const creating = ref(false)
const providers = ref<ProviderConfig[]>([])
const createForm = reactive({ providerKey: '', mode: 'incremental', from: '', to: '', includeResources: true })
let poller: number | undefined

const statusType: Record<JobStatus, 'default' | 'info' | 'success' | 'warning' | 'error'> = {
  pending: 'default', running: 'info', pausing: 'warning', paused: 'warning',
  cancelling: 'warning', cancelled: 'warning', success: 'success', failed: 'error',
}
const statusLabel: Record<JobStatus, string> = {
  pending: '等待', running: '运行中', pausing: '暂停中', paused: '已暂停',
  cancelling: '取消中', cancelled: '已取消', success: '已完成', failed: '失败',
}
const itemStatusType: Record<JobItemStatus, 'default' | 'info' | 'success' | 'warning' | 'error'> = {
  pending: 'default', running: 'info', success: 'success', failed: 'error', skipped: 'warning', cancelled: 'warning',
}
const itemStatusLabel: Record<JobItemStatus, string> = {
  pending: '等待', running: '执行中', success: '成功', failed: '失败', skipped: '跳过', cancelled: '已取消',
}
const sourceProviders = computed(() => providers.value.filter(provider => provider.type === 'source'))
const providerOptions = computed(() => sourceProviders.value.map(provider => ({ label: `${provider.displayName}（${provider.key}）`, value: provider.key })))

const statusOptions = [
  { label: '全部', value: '' },
  { label: '运行中', value: 'running' },
  { label: '等待', value: 'pending' },
  { label: '已暂停', value: 'paused' },
  { label: '失败', value: 'failed' },
  { label: '已完成', value: 'success' },
]

function runTitle(run: JobRun) {
  const mode = run.config.mode === 'incremental' ? '增量同步' : '历史回填'
  return `${run.providerKey ?? run.jobType} · ${mode}`
}

function checkpointText(run: JobRun) {
  const checkpoint = run.checkpoint as { page?: number; nextUrl?: string | null } | null
  if (checkpoint?.page) return `第 ${checkpoint.page} 页`
  return run.progressCurrent > 0 ? `${run.progressCurrent} 项` : '尚未开始'
}

function progressText(run: JobRun) {
  if (run.progressTotal != null) return `${run.progressCurrent} / ${run.progressTotal}`
  return checkpointText(run)
}

function isTerminal(run: JobRun) {
  return ['success', 'failed', 'cancelled'].includes(run.status)
}

async function load(silent = false) {
  if (!silent) loading.value = true
  try {
    runs.value = await api.jobs(status.value, page.value, pageSize.value)
    if (detail.value && detailOpen.value) await refreshDetail()
  } catch (reason) {
    if (!silent) message.error(reason instanceof Error ? reason.message : '任务列表加载失败')
  } finally {
    loading.value = false
  }
}

function changePage(value: number) { page.value = value; load(true) }
function changePageSize(value: number) { pageSize.value = value; page.value = 1; load(true) }
function changeStatus(value: string) { status.value = value; page.value = 1; load(true) }

async function openDetail(id: number) {
  detailOpen.value = true
  detailLoading.value = true
  try {
    detail.value = await api.job(id)
    await Promise.all([loadItems(), loadEvents()])
  } catch (reason) {
    message.error(reason instanceof Error ? reason.message : '任务详情加载失败')
  } finally {
    detailLoading.value = false
  }
}

async function refreshDetail() {
  if (!detail.value) return
  try {
    detail.value = await api.job(detail.value.run.id)
    await Promise.all([loadItems(), loadEvents()])
  } catch {
    // keep the last known detail visible
  }
}

async function loadItems() {
  if (!detail.value) return
  items.value = await api.jobItems(detail.value.run.id, itemStatus.value, itemPage.value, itemPageSize.value)
}

async function loadEvents() {
  if (!detail.value) return
  events.value = await api.jobEvents(detail.value.run.id, eventPage.value, eventPageSize.value)
}

function changeItemPage(value: number) { itemPage.value = value; loadItems() }
function changeItemPageSize(value: number) { itemPageSize.value = value; itemPage.value = 1; loadItems() }
function changeItemStatus(value: string) { itemStatus.value = value; itemPage.value = 1; loadItems() }
function changeEventPage(value: number) { eventPage.value = value; loadEvents() }
function changeEventPageSize(value: number) { eventPageSize.value = value; eventPage.value = 1; loadEvents() }

async function act(run: JobRun, action: 'pause' | 'resume' | 'cancel' | 'retry') {
  try {
    const names = { pause: '已暂停', resume: '已继续', cancel: '已取消', retry: '已重试失败项' }
    if (action === 'pause') await api.pauseJob(run.id)
    else if (action === 'resume') await api.resumeJob(run.id)
    else if (action === 'cancel') await api.cancelJob(run.id)
    else await api.retryFailedJobs(run.id)
    message.success(names[action])
    await load(true)
  } catch (reason) {
    message.error(reason instanceof Error ? reason.message : '操作失败')
  }
}

function confirmCancel(run: JobRun) {
  dialog.warning({
    title: '取消任务',
    content: `确认取消“${runTitle(run)}”吗？已完成的页面会保留，任务进入已取消状态，之后可重试失败项。`,
    positiveText: '取消任务',
    negativeText: '返回',
    onPositiveClick: () => act(run, 'cancel'),
  })
}

function openCreate() {
  const today = new Date()
  const to = today.toISOString().slice(0, 10)
  const from = new Date(today.getTime() - 7 * 86400000).toISOString().slice(0, 10)
  Object.assign(createForm, { providerKey: sourceProviders.value[0]?.key ?? '', mode: 'incremental', from, to, includeResources: true })
  createOpen.value = true
}

async function createRun() {
  if (!createForm.providerKey) return message.warning('请选择数据源')
  if (!/^\d{4}-\d{2}-\d{2}$/.test(createForm.from) || !/^\d{4}-\d{2}-\d{2}$/.test(createForm.to)) return message.warning('日期格式应为 YYYY-MM-DD')
  if (createForm.from > createForm.to) return message.warning('开始日期不能晚于结束日期')
  creating.value = true
  try {
    const input = { providerKey: createForm.providerKey, from: createForm.from, to: createForm.to, includeResources: createForm.includeResources }
    const result = createForm.mode === 'incremental' ? await api.createIncrementalJob(input) : await api.createBootstrapJob(input)
    message.success(createForm.mode === 'incremental' ? '增量任务已创建' : '回填任务已创建')
    createOpen.value = false
    await load(true)
    await openDetail(result.run.id)
  } catch (reason) {
    message.error(reason instanceof Error ? reason.message : '创建任务失败')
  } finally {
    creating.value = false
  }
}

const itemColumns: DataTableColumns<JobItem> = [
  { title: '条目', key: 'itemKey', minWidth: 200, render: row => h('code', { class: 'mono-value' }, row.itemKey) },
  { title: '状态', key: 'status', width: 100, render: row => h(NTag, { size: 'small', bordered: false, type: itemStatusType[row.status] }, { default: () => itemStatusLabel[row.status] }) },
  { title: '重试', key: 'retryCount', width: 70, render: row => `${row.retryCount} 次` },
  { title: '错误', key: 'errorMessage', minWidth: 220, render: row => h('span', { class: row.errorMessage ? 'record-error inline-error' : 'muted-text' }, row.errorMessage ?? '无') },
  { title: '开始', key: 'startedAt', width: 150, render: row => row.startedAt ? formatDate(row.startedAt) : '—' },
]

async function loadProviders() {
  try {
    const result = await api.providers(1, 100)
    providers.value = result.items
  } catch {
    // provider list is optional for the create form
  }
}

onMounted(() => {
  loadProviders()
  load()
  poller = window.setInterval(() => load(true), 5000)
})
onUnmounted(() => window.clearInterval(poller))
</script>

<template>
  <PageHeader title="任务" description="所有回填、增量、扫描与维护工作都在这里统一可见、可控、可恢复。">
    <n-button type="primary" @click="openCreate"><template #icon><IconPlus /></template>新建任务</n-button>
  </PageHeader>

  <div class="toolbar">
    <n-select v-model:value="status" :options="statusOptions" class="status-filter" @update:value="changeStatus" />
    <n-button secondary :loading="loading" @click="load()"><template #icon><IconRefresh /></template>刷新</n-button>
  </div>

  <n-spin :show="loading">
    <section class="task-run-list">
      <article v-for="run in runs.items" :key="run.id" class="task-run-row" @click="openDetail(run.id)">
        <div class="task-run-main">
          <div class="task-run-title"><strong>{{ runTitle(run) }}</strong><n-tag size="small" :bordered="false" :type="statusType[run.status]">{{ statusLabel[run.status] }}</n-tag></div>
          <div class="task-run-meta">
            <span>#{{ run.id }}</span>
            <span>进度 {{ progressText(run) }}</span>
            <span>更新于 {{ formatDate(run.updatedAt) }}</span>
            <span v-if="run.errorMessage" class="record-error inline-error">{{ run.errorMessage }}</span>
          </div>
        </div>
        <div class="task-run-actions" @click.stop>
          <n-button v-if="['running', 'pending'].includes(run.status)" size="small" secondary @click="act(run, 'pause')"><template #icon><IconPlayerPause /></template>暂停</n-button>
          <n-button v-if="run.status === 'paused'" size="small" secondary type="primary" @click="act(run, 'resume')"><template #icon><IconPlayerPlay /></template>继续</n-button>
          <n-button v-if="run.status === 'failed'" size="small" secondary @click="act(run, 'retry')"><template #icon><IconRotateClockwise /></template>重试失败</n-button>
          <n-button v-if="!isTerminal(run)" size="small" quaternary type="error" @click="confirmCancel(run)"><template #icon><IconX /></template>取消</n-button>
        </div>
      </article>
      <EmptyState v-if="!loading && !runs.items.length" title="还没有任务" description="点击“新建任务”创建历史回填或增量同步。" />
    </section>
    <PaginationBar v-if="runs.total > 0" :page="page" :page-size="pageSize" :total="runs.total" @update:page="changePage" @update:page-size="changePageSize" />
  </n-spin>

  <n-drawer v-model:show="detailOpen" :width="drawerWidth" placement="right">
    <n-drawer-content title="任务详情" closable>
      <n-spin :show="detailLoading">
        <template v-if="detail">
          <div class="task-detail-head">
            <div>
              <span class="task-id">任务 #{{ detail.run.id }}</span>
              <h2>{{ runTitle(detail.run) }}</h2>
            </div>
            <n-tag :bordered="false" :type="statusType[detail.run.status]">{{ statusLabel[detail.run.status] }}</n-tag>
          </div>

          <div class="job-stats">
            <div><span>成功</span><strong>{{ detail.stats.success }}</strong></div>
            <div><span>失败</span><strong>{{ detail.stats.failed }}</strong></div>
            <div><span>等待</span><strong>{{ detail.stats.pending }}</strong></div>
            <div><span>执行中</span><strong>{{ detail.stats.running }}</strong></div>
            <div><span>跳过</span><strong>{{ detail.stats.skipped }}</strong></div>
            <div><span>已取消</span><strong>{{ detail.stats.cancelled }}</strong></div>
          </div>

          <dl class="detail-list task-summary">
            <dt>类型</dt><dd>{{ detail.run.config.mode === 'incremental' ? '增量同步' : '历史回填' }} · {{ detail.run.jobType }}</dd>
            <dt>数据源</dt><dd>{{ detail.run.providerKey ?? '—' }}</dd>
            <dt>范围</dt><dd>{{ detail.run.config.from ?? '—' }} → {{ detail.run.config.to ?? '—' }}</dd>
            <dt>Checkpoint</dt><dd class="mono-value">{{ checkpointText(detail.run) }}</dd>
            <dt>开始时间</dt><dd>{{ detail.run.startedAt ? formatDate(detail.run.startedAt) : '尚未开始' }}</dd>
            <dt>结束时间</dt><dd>{{ detail.run.finishedAt ? formatDate(detail.run.finishedAt) : '—' }}</dd>
            <dt>幂等键</dt><dd class="mono-value">{{ detail.run.idempotencyKey }}</dd>
          </dl>

          <div class="task-run-actions detail-actions">
            <n-button v-if="['running', 'pending'].includes(detail.run.status)" :loading="detailLoading" @click="act(detail.run, 'pause')"><template #icon><IconPlayerPause /></template>暂停</n-button>
            <n-button v-if="detail.run.status === 'paused'" type="primary" @click="act(detail.run, 'resume')"><template #icon><IconPlayerPlay /></template>继续</n-button>
            <n-button v-if="detail.run.status === 'failed'" @click="act(detail.run, 'retry')"><template #icon><IconRotateClockwise /></template>重试失败</n-button>
            <n-button v-if="!isTerminal(detail.run)" secondary type="error" @click="confirmCancel(detail.run)"><template #icon><IconX /></template>取消任务</n-button>
          </div>

          <section class="record-section">
            <div class="record-heading"><h3>执行条目</h3><n-select v-model:value="itemStatus" size="small" class="status-filter" :options="[{ label: '全部', value: '' }, ...Object.entries(itemStatusLabel).map(([value, label]) => ({ label, value }))]" @update:value="changeItemStatus" /></div>
            <div class="table-wrap"><n-data-table :columns="itemColumns" :data="items.items" :bordered="false" :single-line="false" /></div>
            <PaginationBar v-if="items.total > 0" :page="itemPage" :page-size="itemPageSize" :total="items.total" @update:page="changeItemPage" @update:page-size="changeItemPageSize" />
          </section>

          <section class="record-section">
            <div class="record-heading"><h3>事件</h3></div>
            <ol class="event-timeline task-events">
              <li v-for="event in events.items" :key="event.id"><i /><div><strong>{{ event.message }}</strong><span>{{ event.eventKey }}</span><small>{{ formatDate(event.createdAt) }}</small></div></li>
            </ol>
            <PaginationBar v-if="events.total > 0" :page="eventPage" :page-size="eventPageSize" :total="events.total" @update:page="changeEventPage" @update:page-size="changeEventPageSize" />
          </section>
        </template>
      </n-spin>
    </n-drawer-content>
  </n-drawer>

  <n-modal v-model:show="createOpen" preset="card" title="新建任务" :bordered="false" style="width:min(520px,calc(100vw - 32px))">
    <n-form label-placement="top">
      <n-form-item label="数据源" required>
        <n-select v-model:value="createForm.providerKey" :options="providerOptions" placeholder="选择内容来源" />
      </n-form-item>
      <n-form-item label="任务类型" required>
        <n-select v-model:value="createForm.mode" :options="[{ label: '增量同步（最近 7 天）', value: 'incremental' }, { label: '历史回填（指定范围）', value: 'bootstrap' }]" />
      </n-form-item>
      <div class="form-grid">
        <n-form-item label="开始日期"><n-input v-model:value="createForm.from" placeholder="YYYY-MM-DD" /></n-form-item>
        <n-form-item label="结束日期"><n-input v-model:value="createForm.to" placeholder="YYYY-MM-DD" /></n-form-item>
      </div>
      <n-form-item label="同步下载资源"><n-switch v-model:value="createForm.includeResources" /></n-form-item>
    </n-form>
    <template #footer><div class="modal-actions"><n-button @click="createOpen = false">取消</n-button><n-button type="primary" :loading="creating" @click="createRun"><template #icon><IconHistory /></template>创建任务</n-button></div></template>
  </n-modal>
</template>

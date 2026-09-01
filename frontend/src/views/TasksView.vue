<script setup lang="ts">
import { h, onMounted, onUnmounted, ref } from 'vue'
import {
  NButton, NDataTable, NDrawer, NDrawerContent, NProgress, NSelect, NSkeleton, NTag,
  type DataTableColumns, useMessage,
} from 'naive-ui'
import { IconEye, IconRefresh, IconRotateClockwise, IconX } from '@tabler/icons-vue'
import { RouterLink, useRoute } from 'vue-router'
import PageHeader from '../components/PageHeader.vue'
import EmptyState from '../components/EmptyState.vue'
import { api } from '../api'
import { formatDate, statusLabel, statusType, taskTypeLabel } from '../format'
import type { Paged, Task, TaskDetail, TaskStatus } from '../types'
import PaginationBar from '../components/PaginationBar.vue'

const message = useMessage()
const route = useRoute()
const tasks = ref<Paged<Task>>({ items: [], total: 0, page: 1, pageSize: 20, totalPages: 0 })
const loading = ref(true)
const detailLoading = ref(false)
const status = ref('')
const page = ref(1)
const pageSize = ref(20)
const recordPage = ref(1)
const recordPageSize = ref(20)
const checkedRowKeys = ref<Array<string | number>>([])
const batchAction = ref<'retry' | 'cancel' | ''>('')
const detail = ref<TaskDetail | null>(null)
const drawerOpen = ref(false)
const drawerWidth = ref(620)
let timer: number | undefined

function resourceCell(row: Task) {
  if (row.media) {
    return h('div', { class: 'resource-cell' }, [
      h(RouterLink, { class: 'resource-link', to: { path: '/media', query: { mediaId: row.media.id } } }, {
        default: () => row.media?.title,
      }),
      h('span', { class: 'resource-meta', title: row.media.path }, row.media.filename),
    ])
  }
  if (row.folder) {
    return h('div', { class: 'resource-cell' }, [
      h(RouterLink, { class: 'resource-link', to: '/folders' }, { default: () => row.folder?.name }),
      h('span', { class: 'resource-meta', title: row.folder.path }, row.folder.path),
    ])
  }
  return h('span', { class: 'muted-text' }, '关联资源已删除')
}

const columns: DataTableColumns<Task> = [
  { type: 'selection' },
  { title: '任务', key: 'id', width: 92, render: (row) => `#${row.id}` },
  { title: '关联媒体 / 目录', key: 'resource', minWidth: 260, render: resourceCell },
  { title: '类型', key: 'taskType', width: 112, render: (row) => taskTypeLabel[row.taskType] },
  {
    title: '当前状态', key: 'status', width: 106,
    render: (row) => h(NTag, { type: statusType[row.status], bordered: false, size: 'small' }, { default: () => statusLabel[row.status] }),
  },
  {
    title: '进度', key: 'progress', minWidth: 142,
    render: (row) => h('div', { class: 'status-cell' }, [
      h(NProgress, { type: 'line', percentage: row.progress, height: 4, showIndicator: false, processing: row.status === 'running' }),
      h('span', `${row.progress}%`),
    ]),
  },
  { title: '执行记录', key: 'recordCount', width: 92, render: (row) => `${row.recordCount} 次` },
  { title: '最近执行', key: 'updatedAt', width: 150, render: (row) => formatDate(row.updatedAt) },
  {
    title: '操作', key: 'actions', width: 160, align: 'right',
    render: (row) => h('div', { class: 'row-actions' }, [
      h(NButton, { size: 'small', quaternary: true, onClick: () => showDetail(row.id) }, { icon: () => h(IconEye), default: () => '详情' }),
      ...(['failed', 'cancelled'] as TaskStatus[]).includes(row.status)
        ? [h(NButton, { size: 'small', quaternary: true, type: 'primary', onClick: () => retry(row) }, { icon: () => h(IconRotateClockwise), default: () => '重试' })]
        : [],
      ...(['pending', 'running'] as TaskStatus[]).includes(row.status)
        ? [h(NButton, { size: 'small', quaternary: true, type: 'warning', onClick: () => cancel(row) }, { icon: () => h(IconX), default: () => '取消' })]
        : [],
    ]),
  },
]
const rowKey = (row: Task) => row.id

async function load(silent = false) {
  if (!silent) loading.value = true
  try { tasks.value = await api.tasks(status.value, page.value, pageSize.value) }
  catch (reason) { if (!silent) message.error(reason instanceof Error ? reason.message : '任务加载失败') }
  finally { loading.value = false }
}
async function showDetail(taskId: number) {
  drawerOpen.value = true
  detailLoading.value = true
  try { detail.value = await api.task(taskId, recordPage.value, recordPageSize.value) }
  catch (reason) { message.error(reason instanceof Error ? reason.message : '任务详情加载失败') }
  finally { detailLoading.value = false }
}
function changePage(value: number) { page.value = value; load() }
function changePageSize(value: number) { pageSize.value = value; page.value = 1; load() }
function changeRecordPage(value: number) { recordPage.value = value; if (detail.value) showDetail(detail.value.id) }
function changeRecordPageSize(value: number) { recordPageSize.value = value; recordPage.value = 1; if (detail.value) showDetail(detail.value.id) }
async function retry(task: Task) {
  try {
    const updated = await api.retryTask(task.id)
    message.success('已开始重试，本任务新增一条执行记录')
    await load(true)
    if (drawerOpen.value && detail.value?.id === updated.id) await showDetail(updated.id)
  }
  catch (reason) { message.error(reason instanceof Error ? reason.message : '重试失败') }
}
async function cancel(task: Task) {
  try { await api.cancelTask(task.id); message.success('当前执行已取消'); await load(true) }
  catch (reason) { message.error(reason instanceof Error ? reason.message : '取消失败') }
}
function selectedTaskIds() {
  return checkedRowKeys.value.map(Number).filter(Number.isFinite)
}
async function retrySelected() {
  const ids = selectedTaskIds()
  if (!ids.length) return
  batchAction.value = 'retry'
  try {
    const result = await api.retryTasks(ids)
    if (result.processed) message.success(`已重试 ${result.processed} 个任务${result.skipped ? `，跳过 ${result.skipped} 个不可重试任务` : ''}`)
    else message.warning('所选任务当前不可重试')
    checkedRowKeys.value = []
    await load(true)
  } catch (reason) { message.error(reason instanceof Error ? reason.message : '批量重试失败') }
  finally { batchAction.value = '' }
}
async function cancelSelected() {
  const ids = selectedTaskIds()
  if (!ids.length) return
  batchAction.value = 'cancel'
  try {
    const result = await api.cancelTasks(ids)
    if (result.processed) message.success(`已取消 ${result.processed} 个当前执行${result.skipped ? `，跳过 ${result.skipped} 个已结束任务` : ''}`)
    else message.warning('所选任务均已结束')
    checkedRowKeys.value = []
    await load(true)
  } catch (reason) { message.error(reason instanceof Error ? reason.message : '批量取消失败') }
  finally { batchAction.value = '' }
}
function changeStatus() {
  checkedRowKeys.value = []
  load()
}

onMounted(() => {
  drawerWidth.value = Math.min(620, window.innerWidth)
  const requestedStatus = typeof route.query.status === 'string' ? route.query.status : ''
  status.value = ['', 'pending', 'running', 'success', 'failed', 'cancelled'].includes(requestedStatus) ? requestedStatus : ''
  load()
  const taskId = Number(route.query.taskId)
  if (Number.isFinite(taskId) && taskId > 0) showDetail(taskId)
  timer = window.setInterval(() => load(true), 5000)
})
onUnmounted(() => window.clearInterval(timer))
</script>

<template>
  <PageHeader title="任务中心" description="统一查看目录扫描与元数据刮削任务，重试、取消和失败都会保留执行记录。">
    <n-button secondary :loading="loading" @click="load()"><template #icon><IconRefresh /></template>刷新</n-button>
  </PageHeader>
  <div class="toolbar">
    <n-select v-model:value="status" class="status-filter" :options="[{label:'全部状态',value:''},{label:'等待中',value:'pending'},{label:'执行中',value:'running'},{label:'成功',value:'success'},{label:'失败',value:'failed'},{label:'已取消',value:'cancelled'}]" @update:value="changeStatus" />
    <n-button secondary :loading="batchAction === 'retry'" :disabled="!checkedRowKeys.length || !!batchAction" @click="retrySelected"><template #icon><IconRotateClockwise /></template>批量重试</n-button>
    <n-button secondary type="warning" :loading="batchAction === 'cancel'" :disabled="!checkedRowKeys.length || !!batchAction" @click="cancelSelected"><template #icon><IconX /></template>批量取消</n-button>
  </div>
  <section class="panel">
    <div v-if="loading" class="skeleton-block"><n-skeleton text :repeat="8" /></div>
    <EmptyState v-else-if="!tasks.items.length" title="没有符合条件的任务" description="扫描媒体目录或刮削媒体后，任务会显示在这里。" />
    <div v-else class="table-wrap"><n-data-table v-model:checked-row-keys="checkedRowKeys" :row-key="rowKey" :columns="columns" :data="tasks.items" :bordered="false" :single-line="false" /></div>
    <PaginationBar v-if="tasks.total > 0" :page="page" :page-size="pageSize" :total="tasks.total" @update:page="changePage" @update:page-size="changePageSize" />
  </section>

  <n-drawer v-model:show="drawerOpen" :width="drawerWidth" placement="right">
    <n-drawer-content title="任务详情" closable>
      <div v-if="detailLoading" class="skeleton-block"><n-skeleton text :repeat="10" /></div>
      <template v-else-if="detail">
        <div class="task-detail-head">
          <div>
            <span class="task-id">任务 #{{ detail.id }}</span>
            <h2>{{ detail.media?.title ?? detail.folder?.name ?? '关联资源已删除' }}</h2>
            <RouterLink v-if="detail.media" class="resource-link" :to="{ path: '/media', query: { mediaId: detail.media.id } }">
              在媒体库中查看
            </RouterLink>
            <RouterLink v-else-if="detail.folder" class="resource-link" to="/folders">查看媒体目录</RouterLink>
          </div>
          <n-tag :type="statusType[detail.status]" :bordered="false">{{ statusLabel[detail.status] }}</n-tag>
        </div>

        <dl class="detail-list task-summary">
          <dt>任务类型</dt><dd>{{ taskTypeLabel[detail.taskType] }}</dd>
          <dt>当前进度</dt><dd><n-progress type="line" :percentage="detail.progress" :processing="detail.status === 'running'" /></dd>
          <dt>关联文件</dt><dd class="mono-value">{{ detail.media?.path ?? detail.folder?.path ?? '无' }}</dd>
          <dt>首次创建</dt><dd>{{ formatDate(detail.createdAt) }}</dd>
          <dt>执行次数</dt><dd>{{ detail.recordCount }} 次</dd>
        </dl>

        <section class="record-section">
          <div class="record-heading">
            <h3>执行记录</h3>
            <span>最近一次在最上方</span>
          </div>
          <ol class="record-list">
            <li v-for="(record, index) in detail.records.items" :key="record.id" class="record-item">
              <div class="record-main">
                <strong>第 {{ detail.records.total - (recordPage - 1) * recordPageSize - index }} 次执行</strong>
                <n-tag :type="statusType[record.status]" :bordered="false" size="small">{{ statusLabel[record.status] }}</n-tag>
              </div>
              <div class="record-meta">
                <span>{{ formatDate(record.createdAt) }}</span>
                <span>进度 {{ record.progress }}%</span>
                <span v-if="record.finishedAt">完成于 {{ formatDate(record.finishedAt) }}</span>
              </div>
              <p v-if="record.errorMessage" class="record-error">{{ record.errorMessage }}</p>
            </li>
          </ol>
          <PaginationBar v-if="detail.records.total > 0" :page="recordPage" :page-size="recordPageSize" :total="detail.records.total" @update:page="changeRecordPage" @update:page-size="changeRecordPageSize" />
        </section>
      </template>
    </n-drawer-content>
  </n-drawer>
</template>

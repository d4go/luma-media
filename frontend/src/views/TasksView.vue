<script setup lang="ts">
import { h, onMounted, onUnmounted, ref } from 'vue'
import { NButton, NDataTable, NDrawer, NDrawerContent, NProgress, NSelect, NSkeleton, NTag, type DataTableColumns, useMessage } from 'naive-ui'
import { IconEye, IconRefresh, IconRotateClockwise, IconX } from '@tabler/icons-vue'
import PageHeader from '../components/PageHeader.vue'
import EmptyState from '../components/EmptyState.vue'
import { api } from '../api'
import { formatDate, statusLabel, statusType, taskTypeLabel } from '../format'
import type { Task, TaskStatus } from '../types'

const message = useMessage()
const tasks = ref<Task[]>([])
const loading = ref(true)
const status = ref('')
const detail = ref<Task | null>(null)
const drawerOpen = ref(false)
const drawerWidth = ref(440)
let timer: number | undefined

const columns: DataTableColumns<Task> = [
  { title: '任务编号', key: 'id', width: 100, render: (row) => `#${row.id}` },
  { title: '类型', key: 'taskType', width: 130, render: (row) => taskTypeLabel[row.taskType] },
  { title: '状态', key: 'status', width: 105, render: (row) => h(NTag, { type: statusType[row.status], bordered: false, size: 'small' }, { default: () => statusLabel[row.status] }) },
  { title: '进度', key: 'progress', minWidth: 150, render: (row) => h('div', { class: 'status-cell' }, [h(NProgress, { type: 'line', percentage: row.progress, height: 5, showIndicator: false, processing: row.status === 'running' }), h('span', `${row.progress}%`)]) },
  { title: '创建时间', key: 'createdAt', width: 150, render: (row) => formatDate(row.createdAt) },
  {
    title: '操作', key: 'actions', width: 160, align: 'right',
    render: (row) => h('div', { class: 'row-actions' }, [
      h(NButton, { size: 'small', quaternary: true, onClick: () => showDetail(row) }, { icon: () => h(IconEye), default: () => '详情' }),
      ...(['failed', 'cancelled'] as TaskStatus[]).includes(row.status)
        ? [h(NButton, { size: 'small', quaternary: true, type: 'primary', onClick: () => retry(row) }, { icon: () => h(IconRotateClockwise), default: () => '重试' })]
        : [],
      ...(['pending', 'running'] as TaskStatus[]).includes(row.status)
        ? [h(NButton, { size: 'small', quaternary: true, type: 'warning', onClick: () => cancel(row) }, { icon: () => h(IconX), default: () => '取消' })]
        : [],
    ]),
  },
]

async function load(silent = false) {
  if (!silent) loading.value = true
  try { tasks.value = await api.tasks(status.value) }
  catch (reason) { if (!silent) message.error(reason instanceof Error ? reason.message : '任务加载失败') }
  finally { loading.value = false }
}
function showDetail(task: Task) { detail.value = task; drawerOpen.value = true }
async function retry(task: Task) {
  try { await api.retryTask(task.id); message.success('已创建重试任务'); await load(true) }
  catch (reason) { message.error(reason instanceof Error ? reason.message : '重试失败') }
}
async function cancel(task: Task) {
  try { await api.cancelTask(task.id); message.success('任务已取消'); await load(true) }
  catch (reason) { message.error(reason instanceof Error ? reason.message : '取消失败') }
}

onMounted(() => { drawerWidth.value = Math.min(440, window.innerWidth); load(); timer = window.setInterval(() => load(true), 5000) })
onUnmounted(() => window.clearInterval(timer))
</script>

<template>
  <PageHeader title="任务中心" description="跟踪扫描与刮削任务，处理失败或长时间运行的作业。">
    <n-button secondary :loading="loading" @click="load()"><template #icon><IconRefresh /></template>刷新</n-button>
  </PageHeader>
  <div class="toolbar">
    <n-select v-model:value="status" style="width: 180px" :options="[{label:'全部状态',value:''},{label:'等待中',value:'pending'},{label:'执行中',value:'running'},{label:'成功',value:'success'},{label:'失败',value:'failed'},{label:'已取消',value:'cancelled'}]" @update:value="load()" />
  </div>
  <section class="panel">
    <div v-if="loading" style="padding: 20px"><n-skeleton text :repeat="8" /></div>
    <EmptyState v-else-if="!tasks.length" title="没有符合条件的任务" description="扫描媒体目录或刮削媒体后，任务会显示在这里。" />
    <div v-else class="table-wrap"><n-data-table :columns="columns" :data="tasks" :bordered="false" :single-line="false" /></div>
  </section>

  <n-drawer v-model:show="drawerOpen" :width="drawerWidth" placement="right">
    <n-drawer-content title="任务详情" closable>
      <dl v-if="detail" class="detail-list">
        <dt>任务编号</dt><dd>#{{ detail.id }}</dd>
        <dt>任务类型</dt><dd>{{ taskTypeLabel[detail.taskType] }}</dd>
        <dt>状态</dt><dd><n-tag :type="statusType[detail.status]" :bordered="false">{{ statusLabel[detail.status] }}</n-tag></dd>
        <dt>进度</dt><dd><n-progress type="line" :percentage="detail.progress" :processing="detail.status==='running'" /></dd>
        <dt>媒体编号</dt><dd>{{ detail.mediaId ?? '无' }}</dd>
        <dt>目录编号</dt><dd>{{ detail.folderId ?? '无' }}</dd>
        <dt>创建时间</dt><dd>{{ formatDate(detail.createdAt) }}</dd>
        <dt>完成时间</dt><dd>{{ formatDate(detail.finishedAt) }}</dd>
        <dt>错误信息</dt><dd>{{ detail.errorMessage ?? '无' }}</dd>
      </dl>
    </n-drawer-content>
  </n-drawer>
</template>

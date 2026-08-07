<script setup lang="ts">
import { h, onMounted, onUnmounted, reactive, ref } from 'vue'
import {
  NButton, NDataTable, NForm, NFormItem, NInput, NInputNumber, NModal, NPopconfirm,
  NSwitch, NTabPane, NTabs, NTag, type DataTableColumns, useMessage,
} from 'naive-ui'
import { IconEdit, IconPlayerPlay, IconPlus, IconRefresh, IconTrash } from '@tabler/icons-vue'
import PageHeader from '../components/PageHeader.vue'
import EmptyState from '../components/EmptyState.vue'
import { api } from '../api'
import { formatDate } from '../format'
import type { CrawlerForm, CrawlerRun, CrawlerScript } from '../types'

const message = useMessage()
const loading = ref(true)
const saving = ref(false)
const modalOpen = ref(false)
const editingId = ref<number | null>(null)
const scripts = ref<CrawlerScript[]>([])
const runs = ref<CrawlerRun[]>([])
const runningId = ref<number | null>(null)
const form = reactive<CrawlerForm>({ name: '', websiteUrl: '', intervalMinutes: 60, enabled: true, autoDownload: false, script: null })
let poller: number | undefined

const statusType = { pending: 'default', running: 'info', success: 'success', failed: 'error', cancelled: 'warning' } as const
const statusLabel = { pending: '等待中', running: '执行中', success: '成功', failed: '失败', cancelled: '已取消' }

function renderRuleState(row: CrawlerScript) {
  return h('div', { class: 'task-link-cell' }, [
    h(NTag, { size: 'small', bordered: false, type: row.enabled ? 'success' : 'default' }, { default: () => row.enabled ? '运行中' : '已停用' }),
    row.autoDownload ? h(NTag, { size: 'small', bordered: false, type: 'info' }, { default: () => '自动下载' }) : null,
  ])
}
const ruleColumns: DataTableColumns<CrawlerScript> = [
  { title: '规则', key: 'name', minWidth: 220, render: row => h('div', { class: 'resource-cell' }, [h('strong', row.name), h('span', { class: 'mono-value' }, row.fileName)]) },
  { title: '来源', key: 'websiteUrl', minWidth: 240, render: row => h('a', { class: 'resource-link', href: row.websiteUrl, target: '_blank', rel: 'noreferrer' }, row.websiteUrl) },
  { title: '执行周期', key: 'intervalMinutes', width: 110, render: row => `${row.intervalMinutes} 分钟` },
  { title: '流程', key: 'state', width: 180, render: renderRuleState },
  { title: '最近结果', key: 'lastResultCount', width: 110, render: row => `${row.lastResultCount} 个` },
  { title: '下次执行', key: 'nextRunAt', width: 150, render: row => row.nextRunAt ? formatDate(row.nextRunAt) : '未安排' },
  { title: '操作', key: 'actions', width: 150, align: 'right', render: row => h('div', { class: 'row-actions' }, [
    h(NButton, { quaternary: true, circle: true, loading: runningId.value === row.id, title: '立即执行', onClick: () => run(row.id) }, { icon: () => h(IconPlayerPlay) }),
    h(NButton, { quaternary: true, circle: true, title: '编辑规则', onClick: () => openEdit(row) }, { icon: () => h(IconEdit) }),
    h(NPopconfirm, { onPositiveClick: () => remove(row.id) }, { trigger: () => h(NButton, { quaternary: true, circle: true, type: 'error', title: '删除规则' }, { icon: () => h(IconTrash) }), default: () => '删除规则及其运行记录和资源结果？' }),
  ]) },
]
const runColumns: DataTableColumns<CrawlerRun> = [
  { title: '运行', key: 'id', width: 90, render: row => `#${row.id}` },
  { title: '规则', key: 'scriptId', minWidth: 180, render: row => scripts.value.find(script => script.id === row.scriptId)?.name ?? `#${row.scriptId}` },
  { title: '状态', key: 'status', width: 100, render: row => h(NTag, { size: 'small', bordered: false, type: statusType[row.status] }, { default: () => statusLabel[row.status] }) },
  { title: '发现资源', key: 'resultCount', width: 110, render: row => `${row.resultCount} 个` },
  { title: '开始时间', key: 'startedAt', width: 150, render: row => formatDate(row.startedAt ?? row.createdAt) },
  { title: '执行信息', key: 'message', minWidth: 260, render: row => h('span', { class: row.errorMessage ? 'record-error inline-error' : 'muted-text' }, row.errorMessage ?? (row.stderr || row.stdout || '执行完成').slice(0, 180)) },
]

async function load(silent = false) {
  if (!silent) loading.value = true
  try { [scripts.value, runs.value] = await Promise.all([api.crawlers(), api.crawlerRuns()]) }
  catch (reason) { if (!silent) message.error(reason instanceof Error ? reason.message : '自动化规则加载失败') }
  finally { loading.value = false }
}
function openCreate() {
  editingId.value = null
  Object.assign(form, { name: '', websiteUrl: '', intervalMinutes: 60, enabled: true, autoDownload: false, script: null })
  modalOpen.value = true
}
function openEdit(script: CrawlerScript) {
  editingId.value = script.id
  Object.assign(form, { name: script.name, websiteUrl: script.websiteUrl, intervalMinutes: script.intervalMinutes, enabled: script.enabled, autoDownload: script.autoDownload, script: null })
  modalOpen.value = true
}
function selectScript(event: Event) { form.script = (event.target as HTMLInputElement).files?.[0] ?? null }
async function submit() {
  if (!form.name.trim() || !form.websiteUrl.trim() || (!editingId.value && !form.script)) return message.warning('请完整填写规则并选择 Python 脚本')
  saving.value = true
  try {
    if (editingId.value) await api.updateCrawler(editingId.value, { ...form })
    else await api.createCrawler({ ...form })
    message.success(editingId.value ? '规则已更新' : '规则已创建')
    modalOpen.value = false
    await load(true)
  } catch (reason) { message.error(reason instanceof Error ? reason.message : '规则保存失败') }
  finally { saving.value = false }
}
async function run(id: number) {
  runningId.value = id
  try { await api.runCrawler(id); message.success('规则已开始执行'); await load(true) }
  catch (reason) { message.error(reason instanceof Error ? reason.message : '规则执行失败') }
  finally { runningId.value = null }
}
async function remove(id: number) {
  try { await api.deleteCrawler(id); message.success('规则已删除'); await load(true) }
  catch (reason) { message.error(reason instanceof Error ? reason.message : '规则删除失败') }
}

onMounted(() => { load(); poller = window.setInterval(() => load(true), 5000) })
onUnmounted(() => window.clearInterval(poller))
</script>

<template>
  <PageHeader title="自动化规则" description="定义资源来源、执行周期和下载策略，规则结果会进入资源发现模块。">
    <n-button secondary :loading="loading" @click="load()"><template #icon><IconRefresh /></template>刷新</n-button>
    <n-button type="primary" @click="openCreate"><template #icon><IconPlus /></template>新建规则</n-button>
  </PageHeader>

  <div class="automation-note">
    <strong>职责边界</strong>
    <span>规则负责发现资源。qBittorrent 负责下载，目录监听与 MetaTube 设置负责后续媒体处理和刮削。</span>
  </div>

  <n-tabs type="line" animated>
    <n-tab-pane name="rules" :tab="`规则 (${scripts.length})`">
      <section class="panel">
        <EmptyState v-if="!loading && !scripts.length" title="还没有自动化规则" description="上传 Python 脚本，设置目标来源和循环周期。" />
        <div v-else class="table-wrap"><n-data-table :loading="loading" :columns="ruleColumns" :data="scripts" :bordered="false" :single-line="false" /></div>
      </section>
    </n-tab-pane>
    <n-tab-pane name="runs" :tab="`运行记录 (${runs.length})`">
      <section class="panel">
        <EmptyState v-if="!loading && !runs.length" title="暂无运行记录" description="手动或定时执行规则后，结果会保存在这里。" />
        <div v-else class="table-wrap"><n-data-table :loading="loading" :columns="runColumns" :data="runs" :bordered="false" :single-line="false" /></div>
      </section>
    </n-tab-pane>
  </n-tabs>

  <n-modal v-model:show="modalOpen" preset="card" :title="editingId ? '编辑自动化规则' : '新建自动化规则'" style="width:min(620px,calc(100vw - 32px))">
    <n-form :model="form" label-placement="top">
      <n-form-item label="规则名称" required><n-input v-model:value="form.name" placeholder="例如：每日电影更新" /></n-form-item>
      <n-form-item label="资源来源" required><n-input v-model:value="form.websiteUrl" placeholder="https://example.com" /></n-form-item>
      <div class="form-grid">
        <n-form-item label="循环间隔（分钟）" required><n-input-number v-model:value="form.intervalMinutes" :min="1" :max="10080" style="width:100%" /></n-form-item>
        <n-form-item label="启用循环执行"><n-switch v-model:value="form.enabled" /></n-form-item>
        <n-form-item label="发现后自动下载"><n-switch v-model:value="form.autoDownload" /></n-form-item>
        <n-form-item :label="editingId ? '替换脚本（可选）' : 'Python 脚本'" :required="!editingId"><input class="file-input" type="file" accept=".py,text/x-python" @change="selectScript" /></n-form-item>
      </div>
      <div class="script-contract">
        脚本接收来源地址并输出 JSON。每条资源至少包含 <code>downloadUrl</code>，可附加 <code>title</code>、<code>size</code>、<code>publishedAt</code> 和 <code>trackers</code>。
      </div>
      <div class="modal-actions"><n-button @click="modalOpen=false">取消</n-button><n-button type="primary" :loading="saving" @click="submit">保存规则</n-button></div>
    </n-form>
  </n-modal>
</template>

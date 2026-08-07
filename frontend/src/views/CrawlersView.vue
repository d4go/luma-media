<script setup lang="ts">
import { h, onMounted, onUnmounted, reactive, ref } from 'vue'
import {
  NButton, NDataTable, NForm, NFormItem, NInput, NInputNumber, NModal, NPopconfirm,
  NSwitch, NTabPane, NTabs, NTag, type DataTableColumns, useMessage,
} from 'naive-ui'
import { IconDownload, IconEdit, IconPlayerPlay, IconPlus, IconRefresh, IconTrash } from '@tabler/icons-vue'
import PageHeader from '../components/PageHeader.vue'
import EmptyState from '../components/EmptyState.vue'
import { api } from '../api'
import { formatDate } from '../format'
import type { CrawlerForm, CrawlerResult, CrawlerRun, CrawlerScript } from '../types'

const message = useMessage()
const loading = ref(true)
const saving = ref(false)
const modalOpen = ref(false)
const editingId = ref<number | null>(null)
const scripts = ref<CrawlerScript[]>([])
const runs = ref<CrawlerRun[]>([])
const results = ref<CrawlerResult[]>([])
const runningId = ref<number | null>(null)
const downloadingId = ref<number | null>(null)
const checkedResultKeys = ref<Array<string | number>>([])
const batchDownloading = ref(false)
const form = reactive<CrawlerForm>({ name: '', websiteUrl: '', intervalMinutes: 60, enabled: true, autoDownload: false, script: null })
let poller: number | undefined

const statusType = { pending: 'default', running: 'info', success: 'success', failed: 'error', cancelled: 'warning' } as const
const statusLabel = { pending: '等待中', running: '执行中', success: '成功', failed: '失败', cancelled: '已取消' }
const downloadLabel = { pending: '待下载', downloading: '提交中', success: '已提交', failed: '失败' }

function renderScriptStatus(row: CrawlerScript) {
  const lastStatus = row.lastRunStatus
  return h('div', { class: 'task-link-cell' }, [
    h(NTag, { size: 'small', bordered: false, type: row.enabled ? 'success' : 'default' }, { default: () => row.enabled ? '已启用' : '已停用' }),
    lastStatus ? h(NTag, { size: 'small', bordered: false, type: statusType[lastStatus] }, { default: () => `${statusLabel[lastStatus]} · ${row.lastResultCount} 条` }) : null,
  ])
}

const scriptColumns: DataTableColumns<CrawlerScript> = [
  { title: '爬虫', key: 'name', render: row => h('div', { class: 'resource-cell' }, [h('strong', row.name), h('a', { class: 'resource-link', href: row.websiteUrl, target: '_blank', rel: 'noreferrer' }, row.websiteUrl)]) },
  { title: '脚本', key: 'fileName', render: row => h('span', { class: 'mono-value' }, row.fileName) },
  { title: '周期', key: 'intervalMinutes', render: row => `${row.intervalMinutes} 分钟` },
  { title: '状态', key: 'lastRunStatus', render: renderScriptStatus },
  { title: '下次执行', key: 'nextRunAt', render: row => row.nextRunAt ? formatDate(row.nextRunAt) : '暂无' },
  { title: '', key: 'actions', align: 'right', render: row => h('div', { class: 'row-actions' }, [
    h(NButton, { quaternary: true, circle: true, loading: runningId.value === row.id, title: '立即执行', onClick: () => run(row.id) }, { icon: () => h(IconPlayerPlay) }),
    h(NButton, { quaternary: true, circle: true, title: '编辑', onClick: () => openEdit(row) }, { icon: () => h(IconEdit) }),
    h(NPopconfirm, { onPositiveClick: () => remove(row.id) }, { trigger: () => h(NButton, { quaternary: true, circle: true, type: 'error', title: '删除' }, { icon: () => h(IconTrash) }), default: () => '删除脚本及全部运行结果？' }),
  ]) },
]

const resultColumns: DataTableColumns<CrawlerResult> = [
  { type: 'selection', disabled: row => !['pending', 'failed'].includes(row.downloadStatus) },
  { title: '标题', key: 'title', render: row => h('div', { class: 'resource-cell' }, [h('strong', row.title), h('span', { class: 'mono-value crawler-url' }, row.downloadUrl)]) },
  { title: 'Tracker', key: 'trackers', render: row => `${row.trackers.length} 个` },
  { title: '获取时间', key: 'createdAt', render: row => formatDate(row.createdAt) },
  { title: '下载状态', key: 'downloadStatus', render: row => h('div', { class: 'resource-cell' }, [
    h(NTag, { size: 'small', bordered: false, type: row.downloadStatus === 'success' ? 'success' : row.downloadStatus === 'failed' ? 'error' : row.downloadStatus === 'downloading' ? 'info' : 'default' }, { default: () => downloadLabel[row.downloadStatus] }),
    row.errorMessage ? h('span', { class: 'muted-text' }, row.errorMessage) : null,
  ]) },
  { title: '', key: 'actions', align: 'right', render: row => h(NButton, {
    secondary: true, size: 'small', type: 'primary', loading: downloadingId.value === row.id,
    disabled: !['pending', 'failed'].includes(row.downloadStatus), onClick: () => download(row.id),
  }, { icon: () => h(IconDownload), default: () => row.downloadStatus === 'failed' ? '重试' : '下载' }) },
]

const runColumns: DataTableColumns<CrawlerRun> = [
  { title: '运行', key: 'id', render: row => `#${row.id}` },
  { title: '脚本', key: 'scriptId', render: row => scripts.value.find(script => script.id === row.scriptId)?.name ?? `#${row.scriptId}` },
  { title: '状态', key: 'status', render: row => h(NTag, { size: 'small', bordered: false, type: statusType[row.status] }, { default: () => statusLabel[row.status] }) },
  { title: '结果', key: 'resultCount', render: row => `${row.resultCount} 条` },
  { title: '开始时间', key: 'startedAt', render: row => formatDate(row.startedAt ?? row.createdAt) },
  { title: '信息', key: 'errorMessage', render: row => h('span', { class: row.errorMessage ? 'record-error inline-error' : 'muted-text' }, row.errorMessage ?? (row.stderr || row.stdout || '暂无').slice(0, 180)) },
]

async function load(silent = false) {
  if (!silent) loading.value = true
  try { [scripts.value, runs.value, results.value] = await Promise.all([api.crawlers(), api.crawlerRuns(), api.crawlerResults()]) }
  catch (reason) { if (!silent) message.error(reason instanceof Error ? reason.message : '加载失败') }
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
  if (!form.name.trim() || !form.websiteUrl.trim() || (!editingId.value && !form.script)) return message.warning('请完整填写并选择 Python 脚本')
  saving.value = true
  try {
    if (editingId.value) await api.updateCrawler(editingId.value, { ...form })
    else await api.createCrawler({ ...form })
    message.success(editingId.value ? '爬虫已更新' : '爬虫已创建')
    modalOpen.value = false
    await load(true)
  } catch (reason) { message.error(reason instanceof Error ? reason.message : '保存失败') }
  finally { saving.value = false }
}
async function run(id: number) {
  runningId.value = id
  try { await api.runCrawler(id); message.success('已开始执行'); await load(true) }
  catch (reason) { message.error(reason instanceof Error ? reason.message : '执行失败') }
  finally { runningId.value = null }
}
async function remove(id: number) {
  try { await api.deleteCrawler(id); message.success('已删除'); await load(true) }
  catch (reason) { message.error(reason instanceof Error ? reason.message : '删除失败') }
}
async function download(id: number) {
  downloadingId.value = id
  try { await api.downloadCrawlerResult(id); message.success('已提交到 qBittorrent'); await load(true) }
  catch (reason) { message.error(reason instanceof Error ? reason.message : '提交失败'); await load(true) }
  finally { downloadingId.value = null }
}
async function downloadSelected() {
  batchDownloading.value = true
  try { await api.downloadCrawlerResults(checkedResultKeys.value.map(Number)); message.success('所选结果已提交'); checkedResultKeys.value = []; await load(true) }
  catch (reason) { message.error(reason instanceof Error ? reason.message : '批量提交失败'); await load(true) }
  finally { batchDownloading.value = false }
}

onMounted(() => { load(); poller = window.setInterval(() => load(true), 5000) })
onUnmounted(() => window.clearInterval(poller))
</script>

<template>
  <PageHeader title="爬虫与下载" description="定时执行 Python 爬虫，保存抓取结果并交给 qBittorrent 下载。">
    <n-button secondary :loading="loading" @click="load()"><template #icon><IconRefresh /></template>刷新</n-button>
    <n-button type="primary" @click="openCreate"><template #icon><IconPlus /></template>上传脚本</n-button>
  </PageHeader>

  <n-tabs type="line" animated>
    <n-tab-pane name="scripts" tab="爬虫脚本">
      <section class="panel">
        <EmptyState v-if="!loading && !scripts.length" title="还没有爬虫脚本" description="上传一个 Python 文件并设置目标网站和执行周期。" />
        <div v-else class="table-wrap"><n-data-table :loading="loading" :columns="scriptColumns" :data="scripts" :bordered="false" :single-line="false" /></div>
      </section>
    </n-tab-pane>
    <n-tab-pane name="results" :tab="`抓取结果 (${results.length})`">
      <div class="toolbar"><n-button type="primary" secondary :disabled="!checkedResultKeys.length" :loading="batchDownloading" @click="downloadSelected"><template #icon><IconDownload /></template>下载所选</n-button></div>
      <section class="panel">
        <EmptyState v-if="!loading && !results.length" title="暂无抓取结果" description="执行脚本后，解析出的下载地址会保存在这里。" />
        <div v-else class="table-wrap"><n-data-table v-model:checked-row-keys="checkedResultKeys" :row-key="(row: CrawlerResult) => row.id" :loading="loading" :columns="resultColumns" :data="results" :bordered="false" :single-line="false" /></div>
      </section>
    </n-tab-pane>
    <n-tab-pane name="runs" :tab="`运行记录 (${runs.length})`">
      <section class="panel">
        <EmptyState v-if="!loading && !runs.length" title="暂无运行记录" description="手动或定时执行脚本后会保留 stdout、stderr 和结果数量。" />
        <div v-else class="table-wrap"><n-data-table :loading="loading" :columns="runColumns" :data="runs" :bordered="false" :single-line="false" /></div>
      </section>
    </n-tab-pane>
  </n-tabs>

  <n-modal v-model:show="modalOpen" preset="card" :title="editingId ? '编辑爬虫' : '上传 Python 爬虫'" style="width:min(620px,calc(100vw - 32px))">
    <n-form :model="form" label-placement="top">
      <n-form-item label="名称" required><n-input v-model:value="form.name" placeholder="例如：站点每日更新" /></n-form-item>
      <n-form-item label="目标网站" required><n-input v-model:value="form.websiteUrl" placeholder="https://example.com" /></n-form-item>
      <div class="form-grid">
        <n-form-item label="循环间隔（分钟）" required><n-input-number v-model:value="form.intervalMinutes" :min="1" :max="10080" style="width:100%" /></n-form-item>
        <n-form-item label="启用定时执行"><n-switch v-model:value="form.enabled" /></n-form-item>
        <n-form-item label="结果自动下载"><n-switch v-model:value="form.autoDownload" /></n-form-item>
        <n-form-item :label="editingId ? '替换脚本（可选）' : 'Python 脚本'" :required="!editingId"><input class="file-input" type="file" accept=".py,text/x-python" @change="selectScript" /></n-form-item>
      </div>
      <div class="script-contract">
        脚本会收到目标网站作为第一个参数，并可读取 <code>LUMA_TARGET_WEBSITE</code>。请向 stdout 输出 JSON 数组，或写入 <code>LUMA_RESULT_PATH</code>；每条结果使用 <code>downloadUrl</code>（也支持 magnet、torrentUrl、url），可附加 title 与 trackers。
      </div>
      <div class="modal-actions"><n-button @click="modalOpen=false">取消</n-button><n-button type="primary" :loading="saving" @click="submit">保存</n-button></div>
    </n-form>
  </n-modal>
</template>

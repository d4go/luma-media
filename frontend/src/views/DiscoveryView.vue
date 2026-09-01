<script setup lang="ts">
import { computed, h, onMounted, onUnmounted, ref } from 'vue'
import { NButton, NDataTable, NInput, NPopconfirm, NSelect, NTag, type DataTableColumns, useMessage } from 'naive-ui'
import { IconBan, IconDownload, IconExternalLink, IconRefresh, IconSearch } from '@tabler/icons-vue'
import PageHeader from '../components/PageHeader.vue'
import EmptyState from '../components/EmptyState.vue'
import { api } from '../api'
import { formatDate } from '../format'
import type { CrawlerResult, Paged } from '../types'
import PaginationBar from '../components/PaginationBar.vue'

const message = useMessage()
const loading = ref(true)
const page = ref(1)
const pageSize = ref(20)
const results = ref<Paged<CrawlerResult>>({ items: [], total: 0, page: 1, pageSize: 20, totalPages: 0 })
const search = ref('')
const status = ref('')
const checkedKeys = ref<Array<string | number>>([])
const actingId = ref<number | null>(null)
const batchDownloading = ref(false)
let poller: number | undefined

const statusLabel = { pending: '待处理', downloading: '提交中', success: '已下载', failed: '失败', ignored: '已忽略' }
const statusType = { pending: 'default', downloading: 'info', success: 'success', failed: 'error', ignored: 'warning' } as const
const statusOptions = [
  { label: '全部状态', value: '' }, { label: '待处理', value: 'pending' },
  { label: '已下载', value: 'success' }, { label: '失败', value: 'failed' }, { label: '已忽略', value: 'ignored' },
]
const filteredResults = computed(() => {
  const keyword = search.value.trim().toLocaleLowerCase()
  return results.value.items.filter((item) => {
    const matchesStatus = !status.value || item.downloadStatus === status.value
    const matchesSearch = !keyword || `${item.title} ${item.source}`.toLocaleLowerCase().includes(keyword)
    return matchesStatus && matchesSearch
  })
})

function canAct(row: CrawlerResult) {
  return ['pending', 'failed'].includes(row.downloadStatus)
}

const columns: DataTableColumns<CrawlerResult> = [
  { type: 'selection', disabled: row => !canAct(row) },
  { title: '资源', key: 'title', minWidth: 280, render: row => h('div', { class: 'resource-cell' }, [
    h('strong', row.title),
    h('span', { class: 'mono-value crawler-url', title: row.downloadUrl }, row.downloadUrl),
  ]) },
  { title: '来源', key: 'source', width: 160, render: row => h('a', { class: 'source-link', href: row.sourceUrl, target: '_blank', rel: 'noreferrer' }, [row.source, h(IconExternalLink, { size: 14 })]) },
  { title: '大小', key: 'size', width: 100, render: row => row.size ?? '未知' },
  { title: '发布时间', key: 'publishedAt', width: 150, render: row => formatDate(row.publishedAt) },
  { title: '状态', key: 'downloadStatus', width: 105, render: row => h(NTag, { size: 'small', bordered: false, type: statusType[row.downloadStatus] }, { default: () => statusLabel[row.downloadStatus] }) },
  { title: '操作', key: 'actions', width: 180, align: 'right', render: row => h('div', { class: 'row-actions' }, [
    h(NButton, { size: 'small', secondary: true, type: 'primary', loading: actingId.value === row.id, disabled: !canAct(row), onClick: () => download(row.id) }, { icon: () => h(IconDownload), default: () => row.downloadStatus === 'failed' ? '重试' : '下载' }),
    h(NPopconfirm, { disabled: !canAct(row), onPositiveClick: () => ignore(row.id) }, {
      trigger: () => h(NButton, { size: 'small', quaternary: true, disabled: !canAct(row) }, { icon: () => h(IconBan), default: () => '忽略' }),
      default: () => '忽略后该资源不会进入下载流程。',
    }),
  ]) },
]

async function load(silent = false) {
  if (!silent) loading.value = true
  try { results.value = await api.crawlerResults(undefined, page.value, pageSize.value) }
  catch (reason) { if (!silent) message.error(reason instanceof Error ? reason.message : '资源加载失败') }
  finally { loading.value = false }
}
function changePage(value: number) { page.value = value; load(true) }
function changePageSize(value: number) { pageSize.value = value; page.value = 1; load(true) }
async function download(id: number) {
  actingId.value = id
  try { await api.downloadCrawlerResult(id); message.success('已加入下载队列'); await load(true) }
  catch (reason) { message.error(reason instanceof Error ? reason.message : '提交下载失败'); await load(true) }
  finally { actingId.value = null }
}
async function ignore(id: number) {
  actingId.value = id
  try { await api.ignoreCrawlerResult(id); message.success('资源已忽略'); await load(true) }
  catch (reason) { message.error(reason instanceof Error ? reason.message : '忽略失败') }
  finally { actingId.value = null }
}
async function downloadSelected() {
  const ids = checkedKeys.value.map(Number).filter(Number.isFinite)
  if (!ids.length) return
  batchDownloading.value = true
  try { await api.downloadCrawlerResults(ids); message.success(`已提交 ${ids.length} 个资源`); checkedKeys.value = []; await load(true) }
  catch (reason) { message.error(reason instanceof Error ? reason.message : '批量下载失败'); await load(true) }
  finally { batchDownloading.value = false }
}

onMounted(() => { load(); poller = window.setInterval(() => load(true), 5000) })
onUnmounted(() => window.clearInterval(poller))
</script>

<template>
  <PageHeader title="资源发现" description="查看爬虫输出的候选资源，确认后交给下载中心，或忽略不需要的结果。">
    <n-button secondary :loading="loading" @click="load()"><template #icon><IconRefresh /></template>刷新</n-button>
    <n-button type="primary" secondary :disabled="!checkedKeys.length" :loading="batchDownloading" @click="downloadSelected">
      <template #icon><IconDownload /></template>下载所选{{ checkedKeys.length ? `（${checkedKeys.length}）` : '' }}
    </n-button>
  </PageHeader>
  <div class="toolbar discovery-toolbar">
    <n-input v-model:value="search" class="search" clearable placeholder="搜索标题或来源">
      <template #prefix><IconSearch :size="18" /></template>
    </n-input>
    <n-select v-model:value="status" class="status-filter" :options="statusOptions" />
    <span class="toolbar-count">{{ filteredResults.length }} 个结果</span>
  </div>
  <section class="panel">
    <EmptyState v-if="!loading && !filteredResults.length" title="没有符合条件的资源" description="运行自动化规则后，资源候选会出现在这里。" />
    <div v-else class="table-wrap">
      <n-data-table v-model:checked-row-keys="checkedKeys" :row-key="(row: CrawlerResult) => row.id" :loading="loading" :columns="columns" :data="filteredResults" :bordered="false" :single-line="false" />
    </div>
    <PaginationBar v-if="results.total > 0" :page="page" :page-size="pageSize" :total="results.total" @update:page="changePage" @update:page-size="changePageSize" />
  </section>
</template>

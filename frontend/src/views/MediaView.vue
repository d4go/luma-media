<script setup lang="ts">
import { h, onMounted, reactive, ref } from 'vue'
import {
  NButton, NDataTable, NForm, NFormItem, NInput, NModal, NSelect, NSkeleton, NSwitch, NTag,
  type DataTableColumns, useMessage,
} from 'naive-ui'
import {
  IconExternalLink, IconFileDescription, IconPhoto, IconRefresh, IconSearch, IconSparkles,
} from '@tabler/icons-vue'
import { RouterLink, useRoute, useRouter } from 'vue-router'
import PageHeader from '../components/PageHeader.vue'
import EmptyState from '../components/EmptyState.vue'
import { api, coverAssetUrl } from '../api'
import { formatDate, statusLabel as taskStatusLabel, statusType as taskStatusType } from '../format'
import type { MediaItem, MediaResourceState, Paged } from '../types'
import PaginationBar from '../components/PaginationBar.vue'

const message = useMessage()
const route = useRoute()
const router = useRouter()
const loading = ref(true)
const page = ref(1)
const pageSize = ref(20)
const media = ref<Paged<MediaItem>>({ items: [], total: 0, page: 1, pageSize: 20, totalPages: 0 })
const search = ref('')
const status = ref('')
const linkedMediaId = ref<number | undefined>()
const checkedRowKeys = ref<Array<string | number>>([])
const modalOpen = ref(false)
const selected = ref<MediaItem | null>(null)
const scrapeTargetIds = ref<number[]>([])
const submitting = ref(false)
const options = reactive({ overwriteNfo: true, overwriteImage: true })

const statusOptions = [
  { label: '全部状态', value: '' },
  { label: '待处理', value: 'pending' },
  { label: '已就绪', value: 'ready' },
  { label: '失败', value: 'failed' },
]
const statusLabel = { pending: '待处理', ready: '已就绪', failed: '失败' }
const statusType = { pending: 'warning', ready: 'info', failed: 'error' } as const
const rowKey = (row: MediaItem) => row.id

const resourceLabel = { ready: '已存在', missing: '缺失', failed: '下载失败' }
const resourceTagType = { ready: 'info', missing: 'default', failed: 'error' } as const

function renderResource(
  label: string,
  icon: typeof IconPhoto,
  resource: MediaResourceState,
  viewUrl?: string,
) {
  const source = resource.source && resource.source !== 'local' ? resource.source : null
  return h('div', { class: 'media-resource-row', title: resource.path ?? undefined }, [
    h('span', { class: 'media-resource-icon', 'aria-hidden': 'true' }, [h(icon, { size: 17, strokeWidth: 1.8 })]),
    h('div', { class: 'media-resource-copy' }, [
      h('span', { class: 'media-resource-name' }, label),
      source ? h('span', { class: 'media-resource-source', title: resource.path ?? undefined }, source) : null,
    ]),
    h(NTag, { type: resourceTagType[resource.status], bordered: false, size: 'small' }, {
      default: () => resourceLabel[resource.status],
    }),
    resource.status === 'ready' && viewUrl
      ? h(NButton, {
          tag: 'a', href: viewUrl, target: '_blank', rel: 'noopener noreferrer',
          text: true, size: 'tiny', class: 'media-resource-view', 'aria-label': `查看${label}`,
        }, { icon: () => h(IconExternalLink, { size: 15 }), default: () => '查看' })
      : null,
  ])
}

const columns: DataTableColumns<MediaItem> = [
  { type: 'selection' },
  { title: '文件名', key: 'filename', minWidth: 240, render: (row) => h('div', [h('strong', row.title), h('div', { class: 'path-cell', title: row.path }, row.filename)]) },
  { title: '类型', key: 'mediaType', width: 90, render: (row) => row.mediaType === 'movie' ? '电影' : row.mediaType === 'tv' ? '剧集' : '混合' },
  { title: '元数据', key: 'status', width: 100, render: (row) => h(NTag, { type: statusType[row.status], bordered: false, size: 'small' }, { default: () => statusLabel[row.status] }) },
  {
    title: '资源状态', key: 'resources', minWidth: 250,
    render: (row) => h('div', { class: 'media-resource-list' }, [
      renderResource('NFO', IconFileDescription, row.resources.nfo),
      renderResource('封面/海报', IconPhoto, row.resources.poster, coverAssetUrl(row.id)),
    ]),
  },
  {
    title: '刮削任务', key: 'scrapeTaskId', minWidth: 150,
    render: (row) => row.scrapeTaskId
      ? h('div', { class: 'task-link-cell' }, [
          h(RouterLink, { class: 'resource-link', to: { path: '/tasks', query: { taskId: row.scrapeTaskId } } }, { default: () => `任务 #${row.scrapeTaskId}` }),
          h(NTag, { type: taskStatusType[row.scrapeTaskStatus!], bordered: false, size: 'small' }, { default: () => taskStatusLabel[row.scrapeTaskStatus!] }),
          h('span', { class: 'muted-text' }, `${row.scrapeRecordCount} 次执行`),
        ])
      : h('span', { class: 'muted-text' }, '尚未创建'),
  },
  { title: 'Provider ID', key: 'providerId', minWidth: 140, render: (row) => row.providerId ?? '未关联' },
  { title: '最近更新', key: 'updatedAt', width: 150, render: (row) => formatDate(row.updatedAt) },
  { title: '操作', key: 'actions', width: 100, align: 'right', render: (row) => h(NButton, { size: 'small', secondary: true, onClick: () => openScrape(row) }, { icon: () => h(IconSparkles), default: () => '刮削' }) },
]

async function load() {
  loading.value = true
  checkedRowKeys.value = []
  try { media.value = await api.media(search.value.trim(), status.value, linkedMediaId.value, page.value, pageSize.value) }
  catch (reason) { message.error(reason instanceof Error ? reason.message : '媒体加载失败') }
  finally { loading.value = false }
}
function changePage(value: number) { page.value = value; load() }
function changePageSize(value: number) { pageSize.value = value; page.value = 1; load() }
async function searchMedia() {
  linkedMediaId.value = undefined
  await router.replace({ path: '/media' })
  await load()
}
function resetScrapeOptions() { options.overwriteNfo = true; options.overwriteImage = true }
function openScrape(item: MediaItem) {
  selected.value = item
  scrapeTargetIds.value = [item.id]
  resetScrapeOptions()
  modalOpen.value = true
}
function openBatchScrape() {
  const ids = checkedRowKeys.value.map(Number).filter(Number.isFinite)
  if (!ids.length) return
  selected.value = null
  scrapeTargetIds.value = ids
  resetScrapeOptions()
  modalOpen.value = true
}
async function scrape() {
  if (!scrapeTargetIds.value.length) return
  submitting.value = true
  try {
    const result = await api.scrapeMediaBatch(scrapeTargetIds.value, { ...options })
    if (result.queued) message.success(`已提交 ${result.queued} 个刮削执行${result.skipped ? `，跳过 ${result.skipped} 个正在处理的项目` : ''}`)
    else message.warning('所选媒体已有正在执行的刮削任务')
    checkedRowKeys.value = []
    modalOpen.value = false
  }
  catch (reason) { message.error(reason instanceof Error ? reason.message : '创建任务失败') }
  finally { submitting.value = false }
}
onMounted(() => {
  const mediaId = Number(route.query.mediaId)
  linkedMediaId.value = Number.isFinite(mediaId) && mediaId > 0 ? mediaId : undefined
  load()
})
</script>

<template>
  <PageHeader title="媒体库" description="浏览已索引文件，分别检查 NFO 与封面/海报状态并按需重新刮削。">
    <n-button secondary tag="div"><RouterLink class="button-link" to="/folders">管理媒体目录</RouterLink></n-button>
    <n-button secondary :loading="loading" @click="load"><template #icon><IconRefresh /></template>刷新</n-button>
  </PageHeader>
  <div class="toolbar">
    <n-input v-model:value="search" class="search" clearable placeholder="搜索文件名或标题" @keyup.enter="searchMedia" @clear="searchMedia">
      <template #prefix><IconSearch :size="18" /></template>
    </n-input>
    <n-select v-model:value="status" :options="statusOptions" class="media-status-filter" @update:value="searchMedia" />
    <n-button type="primary" @click="searchMedia">搜索</n-button>
    <n-button secondary :disabled="!checkedRowKeys.length" @click="openBatchScrape">
      <template #icon><IconSparkles /></template>批量刮削{{ checkedRowKeys.length ? `（${checkedRowKeys.length}）` : '' }}
    </n-button>
  </div>
  <section class="panel">
    <div v-if="loading" style="padding: 20px"><n-skeleton text :repeat="8" /></div>
    <EmptyState v-else-if="!media.items.length" title="媒体库为空" description="请先在媒体目录页面添加路径并执行扫描。">
      <n-button type="primary" tag="a" href="/folders">前往媒体目录</n-button>
    </EmptyState>
    <div v-else class="table-wrap"><n-data-table v-model:checked-row-keys="checkedRowKeys" :row-key="rowKey" :columns="columns" :data="media.items" :bordered="false" :single-line="false" /></div>
    <PaginationBar v-if="media.total > 0" :page="page" :page-size="pageSize" :total="media.total" @update:page="changePage" @update:page-size="changePageSize" />
  </section>

  <n-modal v-model:show="modalOpen" preset="card" :title="scrapeTargetIds.length > 1 ? '批量执行刮削' : '执行刮削'" style="width: min(500px, calc(100vw - 32px))" :bordered="false">
    <p style="margin-top:0">{{ selected?.title ?? `已选择 ${scrapeTargetIds.length} 个媒体文件` }}</p>
    <n-form label-placement="left" label-width="150">
      <n-form-item label="覆盖现有 NFO"><n-switch v-model:value="options.overwriteNfo" /></n-form-item>
      <n-form-item label="覆盖现有图片"><n-switch v-model:value="options.overwriteImage" /></n-form-item>
    </n-form>
    <template #footer><div class="modal-actions"><n-button @click="modalOpen=false">取消</n-button><n-button type="primary" :loading="submitting" @click="scrape">开始刮削</n-button></div></template>
  </n-modal>
</template>

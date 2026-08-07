<script setup lang="ts">
import { h, onMounted, reactive, ref } from 'vue'
import {
  NButton, NDataTable, NForm, NFormItem, NInput, NModal, NSelect, NSkeleton, NSwitch, NTag,
  type DataTableColumns, useMessage,
} from 'naive-ui'
import { IconRefresh, IconSearch, IconSparkles } from '@tabler/icons-vue'
import PageHeader from '../components/PageHeader.vue'
import EmptyState from '../components/EmptyState.vue'
import { api } from '../api'
import { formatDate } from '../format'
import type { MediaItem } from '../types'

const message = useMessage()
const loading = ref(true)
const media = ref<MediaItem[]>([])
const search = ref('')
const status = ref('')
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
const statusType = { pending: 'warning', ready: 'success', failed: 'error' } as const
const rowKey = (row: MediaItem) => row.id

const columns: DataTableColumns<MediaItem> = [
  { type: 'selection' },
  { title: '文件名', key: 'filename', minWidth: 240, render: (row) => h('div', [h('strong', row.title), h('div', { class: 'path-cell', title: row.path }, row.filename)]) },
  { title: '类型', key: 'mediaType', width: 90, render: (row) => row.mediaType === 'movie' ? '电影' : row.mediaType === 'tv' ? '剧集' : '混合' },
  { title: '元数据', key: 'status', width: 100, render: (row) => h(NTag, { type: statusType[row.status], bordered: false, size: 'small' }, { default: () => statusLabel[row.status] }) },
  { title: 'Provider ID', key: 'providerId', minWidth: 140, render: (row) => row.providerId ?? '未关联' },
  { title: '最近更新', key: 'updatedAt', width: 150, render: (row) => formatDate(row.updatedAt) },
  { title: '操作', key: 'actions', width: 100, align: 'right', render: (row) => h(NButton, { size: 'small', secondary: true, onClick: () => openScrape(row) }, { icon: () => h(IconSparkles), default: () => '刮削' }) },
]

async function load() {
  loading.value = true
  checkedRowKeys.value = []
  try { media.value = await api.media(search.value.trim(), status.value) }
  catch (reason) { message.error(reason instanceof Error ? reason.message : '媒体加载失败') }
  finally { loading.value = false }
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
    if (result.created) message.success(`已创建 ${result.created} 个刮削任务${result.skipped ? `，跳过 ${result.skipped} 个正在处理的项目` : ''}`)
    else message.warning('所选媒体已有正在执行的刮削任务')
    checkedRowKeys.value = []
    modalOpen.value = false
  }
  catch (reason) { message.error(reason instanceof Error ? reason.message : '创建任务失败') }
  finally { submitting.value = false }
}
onMounted(load)
</script>

<template>
  <PageHeader title="媒体库" description="浏览已索引文件，检查元数据状态并按需重新刮削。">
    <n-button secondary :loading="loading" @click="load"><template #icon><IconRefresh /></template>刷新</n-button>
  </PageHeader>
  <div class="toolbar">
    <n-input v-model:value="search" class="search" clearable placeholder="搜索文件名或标题" @keyup.enter="load" @clear="load">
      <template #prefix><IconSearch :size="18" /></template>
    </n-input>
    <n-select v-model:value="status" :options="statusOptions" style="width: 150px" @update:value="load" />
    <n-button type="primary" @click="load">搜索</n-button>
    <n-button secondary :disabled="!checkedRowKeys.length" @click="openBatchScrape">
      <template #icon><IconSparkles /></template>批量刮削{{ checkedRowKeys.length ? `（${checkedRowKeys.length}）` : '' }}
    </n-button>
  </div>
  <section class="panel">
    <div v-if="loading" style="padding: 20px"><n-skeleton text :repeat="8" /></div>
    <EmptyState v-else-if="!media.length" title="媒体库为空" description="请先在媒体目录页面添加路径并执行扫描。">
      <n-button type="primary" tag="a" href="/folders">前往媒体目录</n-button>
    </EmptyState>
    <div v-else class="table-wrap"><n-data-table v-model:checked-row-keys="checkedRowKeys" :row-key="rowKey" :columns="columns" :data="media" :bordered="false" :single-line="false" /></div>
  </section>

  <n-modal v-model:show="modalOpen" preset="card" :title="scrapeTargetIds.length > 1 ? '创建批量刮削任务' : '创建刮削任务'" style="width: min(500px, calc(100vw - 32px))" :bordered="false">
    <p style="margin-top:0">{{ selected?.title ?? `已选择 ${scrapeTargetIds.length} 个媒体文件` }}</p>
    <n-form label-placement="left" label-width="150">
      <n-form-item label="覆盖现有 NFO"><n-switch v-model:value="options.overwriteNfo" /></n-form-item>
      <n-form-item label="覆盖现有图片"><n-switch v-model:value="options.overwriteImage" /></n-form-item>
    </n-form>
    <template #footer><div style="display:flex;justify-content:flex-end;gap:10px"><n-button @click="modalOpen=false">取消</n-button><n-button type="primary" :loading="submitting" @click="scrape">创建任务</n-button></div></template>
  </n-modal>
</template>

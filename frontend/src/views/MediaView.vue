<script setup lang="ts">
import { h, onMounted, reactive, ref } from 'vue'
import {
  NButton, NDataTable, NForm, NFormItem, NInput, NModal, NSkeleton, NSwitch, NTag,
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
const modalOpen = ref(false)
const selected = ref<MediaItem | null>(null)
const submitting = ref(false)
const options = reactive({ overwriteNfo: false, overwriteImage: false })

const columns: DataTableColumns<MediaItem> = [
  { title: '文件名', key: 'filename', minWidth: 240, render: (row) => h('div', [h('strong', row.title), h('div', { class: 'path-cell', title: row.path }, row.filename)]) },
  { title: '类型', key: 'mediaType', width: 90, render: (row) => row.mediaType === 'movie' ? '电影' : row.mediaType === 'tv' ? '剧集' : '混合' },
  { title: '元数据', key: 'status', width: 100, render: (row) => h(NTag, { type: row.status === 'ready' ? 'success' : 'warning', bordered: false, size: 'small' }, { default: () => row.status === 'ready' ? '已就绪' : '待处理' }) },
  { title: 'Provider ID', key: 'providerId', minWidth: 140, render: (row) => row.providerId ?? '未关联' },
  { title: '最近更新', key: 'updatedAt', width: 150, render: (row) => formatDate(row.updatedAt) },
  { title: '操作', key: 'actions', width: 100, align: 'right', render: (row) => h(NButton, { size: 'small', secondary: true, onClick: () => openScrape(row) }, { icon: () => h(IconSparkles), default: () => '刮削' }) },
]

async function load() {
  loading.value = true
  try { media.value = await api.media(search.value.trim()) }
  catch (reason) { message.error(reason instanceof Error ? reason.message : '媒体加载失败') }
  finally { loading.value = false }
}
function openScrape(item: MediaItem) { selected.value = item; options.overwriteNfo = false; options.overwriteImage = false; modalOpen.value = true }
async function scrape() {
  if (!selected.value) return
  submitting.value = true
  try { await api.scrapeMedia(selected.value.id, { ...options }); message.success('刮削任务已创建'); modalOpen.value = false }
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
    <n-button type="primary" @click="load">搜索</n-button>
  </div>
  <section class="panel">
    <div v-if="loading" style="padding: 20px"><n-skeleton text :repeat="8" /></div>
    <EmptyState v-else-if="!media.length" title="媒体库为空" description="请先在媒体目录页面添加路径并执行扫描。">
      <n-button type="primary" tag="a" href="/folders">前往媒体目录</n-button>
    </EmptyState>
    <div v-else class="table-wrap"><n-data-table :columns="columns" :data="media" :bordered="false" :single-line="false" /></div>
  </section>

  <n-modal v-model:show="modalOpen" preset="card" title="创建刮削任务" style="width: min(500px, calc(100vw - 32px))" :bordered="false">
    <p style="margin-top:0">{{ selected?.title }}</p>
    <n-form label-placement="left" label-width="150">
      <n-form-item label="覆盖现有 NFO"><n-switch v-model:value="options.overwriteNfo" /></n-form-item>
      <n-form-item label="覆盖现有图片"><n-switch v-model:value="options.overwriteImage" /></n-form-item>
    </n-form>
    <template #footer><div style="display:flex;justify-content:flex-end;gap:10px"><n-button @click="modalOpen=false">取消</n-button><n-button type="primary" :loading="submitting" @click="scrape">创建任务</n-button></div></template>
  </n-modal>
</template>

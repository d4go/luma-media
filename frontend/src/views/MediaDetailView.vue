<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from 'vue'
import { NButton, NSpin, useMessage } from 'naive-ui'
import {
  IconArrowLeft,
  IconCheck,
  IconDatabase,
  IconDownload,
  IconExternalLink,
  IconFile,
  IconMovie,
  IconRefresh,
  IconUser,
} from '@tabler/icons-vue'
import { useRoute, useRouter } from 'vue-router'
import { api, productEventUrl } from '../api'
import type { MediaDetail } from '../types'
import { formatDate } from '../format'

const route = useRoute()
const router = useRouter()
const message = useMessage()
const loading = ref(true)
const acquiring = ref<number | null>(null)
const refreshing = ref(false)
const data = ref<MediaDetail | null>(null)
const id = computed(() => Number(route.params.id))
let eventSource: EventSource | undefined

const fieldLabels: Record<string, string> = {
  title: '标题',
  original_title: '原始标题',
  summary: '简介',
  release_date: '发行日期',
  duration_minutes: '时长',
  poster_url: '海报',
  backdrop_url: '背景图',
}

async function load() {
  loading.value = true
  try {
    data.value = await api.mediaDetail(id.value)
  } catch (reason) {
    message.error(reason instanceof Error ? reason.message : '详情加载失败')
  } finally {
    loading.value = false
  }
}

async function acquire(resourceId: number) {
  acquiring.value = resourceId
  try {
    const item = await api.acquireMedia(id.value, resourceId)
    message.success('获取任务已创建')
    router.push(`/acquisitions/${item.id}`)
  } catch (reason) {
    message.error(reason instanceof Error ? reason.message : '无法创建获取任务')
  } finally {
    acquiring.value = null
  }
}

async function refreshResources() {
  refreshing.value = true
  try {
    const response = await api.refreshMediaResources(id.value)
    message.success(`已创建 ${response.jobIds.length} 个资源刷新任务`)
  } catch (reason) {
    message.error(reason instanceof Error ? reason.message : '无法刷新资源')
    refreshing.value = false
  }
}

function startEvents() {
  eventSource = new EventSource(productEventUrl)
  eventSource.onmessage = async event => {
    try {
      const payload = JSON.parse(event.data) as { event?: string; data?: { mediaId?: number; status?: string } }
      if (payload.event !== 'resource-refresh' || payload.data?.mediaId !== id.value) return
      if (payload.data.status === 'success') {
        refreshing.value = false
        await load()
      } else if (payload.data.status === 'failed') {
        refreshing.value = false
        message.warning('该数据源刷新失败，已保留现有资源')
      }
    } catch {
      // Ignore keep-alive or unrelated events.
    }
  }
}

function size(value: number | null) {
  if (!value) return '大小未知'
  return `${(value / 1024 / 1024 / 1024).toFixed(2)} GB`
}

function qbitStateLabel(value: string) {
  const state = value.toLocaleLowerCase()
  if (state === 'missing') return 'qBittorrent 中已删除'
  if (state.includes('error') || state.includes('missingfiles')) return 'qBittorrent 异常'
  if (state.includes('paused')) return 'qBittorrent 已暂停'
  if (state.includes('upload')) return 'qBittorrent 做种中'
  if (state.includes('stalled')) return 'qBittorrent 等待数据'
  if (state.includes('check')) return 'qBittorrent 校验中'
  if (state.includes('queue')) return 'qBittorrent 排队中'
  return 'qBittorrent 下载中'
}

function acquisitionLabel(value: string | null) {
  if (!value) return ''
  const labels: Record<string, string> = {
    REQUESTED: '准备获取',
    RESOURCE_RESOLVING: '正在解析资源',
    QUEUED: '正在提交 qBittorrent',
    DOWNLOADING: '下载任务同步中',
    DOWNLOADED: '下载已完成',
    PROCESSING: '正在整理文件',
    METADATA: '正在生成元数据',
    LIBRARY_COMMIT: '正在加入媒体库',
    NEEDS_ATTENTION: '需要处理',
  }
  return labels[value] ?? value
}

function sourceFieldNames(providerKey: string) {
  return data.value?.fieldProvenance
    .filter(item => item.providerKey === providerKey)
    .map(item => fieldLabels[item.field] ?? item.field)
    .join('、') || '未选为规范字段'
}

onMounted(() => {
  startEvents()
  load()
})
onUnmounted(() => eventSource?.close())
</script>

<template>
  <n-spin :show="loading">
    <div v-if="data" class="media-detail-page">
      <button class="back-link" @click="router.back()"><IconArrowLeft :size="16" />返回资源</button>

      <section class="media-hero-detail">
        <div class="detail-poster">
          <img v-if="data.media.posterUrl" :src="data.media.posterUrl" :alt="data.media.title">
          <div v-else><IconMovie :size="42" /><span>{{ data.media.code }}</span></div>
        </div>
        <div class="detail-identity">
          <span class="media-type-label">{{ data.media.mediaType.toUpperCase() }}</span>
          <h1>{{ data.media.title }}</h1>
          <div class="identity-code">{{ data.media.code.toUpperCase() }}</div>
          <p>{{ data.media.summary || '当前本地数据还没有作品简介。后台同步会继续合并各来源的规范字段。' }}</p>
          <div class="identity-meta">
            <span>{{ data.media.releaseDate ?? '日期待补全' }}</span>
            <span v-if="data.media.durationMinutes">{{ data.media.durationMinutes }} 分钟</span>
            <span>{{ data.metadataSources.length }} 个元数据来源</span>
          </div>
          <div class="identity-actions">
            <n-button v-if="data.libraryItemId" type="primary">
              <RouterLink class="button-link" :to="`/library/${data.libraryItemId}`"><IconCheck :size="17" />查看本地文件</RouterLink>
            </n-button>
            <n-button v-else-if="data.latestAcquisitionId" type="primary">
              <RouterLink class="button-link" :to="`/acquisitions/${data.latestAcquisitionId}`"><IconDownload :size="17" />查看获取进度</RouterLink>
            </n-button>
          </div>
        </div>
      </section>

      <section class="detail-section detail-information">
        <header class="product-section-head"><div><h2>作品信息</h2><span>规范字段由来源优先级与证据质量共同决定</span></div></header>
        <div class="work-information-grid">
          <div><span>番号</span><strong>{{ data.media.code.toUpperCase() }}</strong></div>
          <div><span>原始标题</span><strong>{{ data.media.originalTitle || '待补全' }}</strong></div>
          <div><span>发行日期</span><strong>{{ data.media.releaseDate || '待补全' }}</strong></div>
          <div><span>时长</span><strong>{{ data.media.durationMinutes ? `${data.media.durationMinutes} 分钟` : '待补全' }}</strong></div>
        </div>
        <div v-if="data.actors.length" class="actor-pills">
          <RouterLink v-for="actor in data.actors" :key="actor.id" :to="`/actors/${actor.id}`"><IconUser :size="15" />{{ actor.name }}</RouterLink>
        </div>
      </section>

      <section class="detail-section">
        <header class="product-section-head"><div><h2>来源信息</h2><span>可以查看每个来源提供了什么，以及最终采用了哪些字段</span></div></header>
        <div v-if="data.metadataSources.length" class="metadata-source-list">
          <article v-for="source in data.metadataSources" :key="source.id" class="metadata-source-row">
            <div class="metadata-source-identity">
              <IconDatabase :size="19" />
              <div><strong>{{ source.providerKey }}</strong><span>{{ source.recordKind }}，优先级 {{ source.priority }}</span></div>
            </div>
            <div class="metadata-source-fields"><span>规范字段</span><strong>{{ sourceFieldNames(source.providerKey) }}</strong></div>
            <div class="metadata-source-time"><span>最近获取</span><strong>{{ formatDate(source.lastSeenAt) }}</strong></div>
            <a v-if="source.sourceUrl" :href="source.sourceUrl" target="_blank" rel="noreferrer" aria-label="打开来源页面"><IconExternalLink :size="17" /></a>
          </article>
        </div>
        <div v-else class="quiet-empty compact"><IconDatabase :size="26" /><strong>暂无来源记录</strong><span>可以在设置中执行增量同步，或在资源搜索页按番号查找。</span></div>
      </section>

      <section class="detail-section">
        <header class="product-section-head resource-section-head">
          <div><h2>下载资源</h2><span>同一 info hash 已跨来源合并，大小和标题会保留更完整的值</span></div>
          <n-button secondary :loading="refreshing" @click="refreshResources"><template #icon><IconRefresh /></template>刷新资源</n-button>
        </header>
        <div v-if="data.resources.length" class="resource-stack">
          <article v-for="(resource, index) in data.resources" :key="resource.id" class="resource-card resource-card-detailed" :class="{ preferred: index === 0 }">
            <div class="resource-rank"><strong>{{ Math.round(resource.score) }}</strong><span>评分</span></div>
            <div class="resource-main">
              <div><span v-if="index === 0" class="preferred-label">推荐</span><strong>{{ resource.title }}</strong></div>
              <p>
                <span>来源 {{ resource.providerKey }}<template v-if="resource.sourceCount > 1">，共 {{ resource.sourceCount }} 个来源</template></span>
                <span>{{ resource.resolution ?? '清晰度未知' }}</span>
                <span v-if="resource.codec">{{ resource.codec }}</span>
                <span>{{ size(resource.sizeBytes) }}</span>
                <span v-if="resource.subtitleLanguages.length">字幕 {{ resource.subtitleLanguages.join(' / ') }}</span>
                <span v-if="resource.lastSeenAt">最近发现 {{ formatDate(resource.lastSeenAt) }}</span>
              </p>
              <ul><li v-for="reason in resource.scoreReasons" :key="reason">{{ reason }}</li></ul>
              <details v-if="resource.infoHash" class="resource-technical">
                <summary>查看 info hash 与 Tracker</summary>
                <code>{{ resource.infoHash }}</code>
                <span>{{ resource.trackers.length }} 个 Tracker</span>
              </details>
            </div>
            <n-button v-if="resource.qbitState" secondary :type="resource.qbitState === 'missing' ? 'error' : 'success'" disabled :title="resource.qbitState">{{ qbitStateLabel(resource.qbitState) }}</n-button>
            <n-button v-else-if="resource.qbitSyncStatus === 'unavailable'" secondary disabled>qBittorrent 暂不可用</n-button>
            <n-button v-else-if="resource.acquisitionId && resource.acquisitionState" secondary>
              <RouterLink class="button-link" :to="`/acquisitions/${resource.acquisitionId}`">{{ acquisitionLabel(resource.acquisitionState) }}</RouterLink>
            </n-button>
            <n-button v-else type="primary" :loading="acquiring === resource.id" @click="acquire(resource.id)">获取</n-button>
          </article>
        </div>
        <div v-else class="quiet-empty">
          <IconExternalLink :size="28" />
          <strong>本地数据库暂无下载资源</strong>
          <span>点击刷新资源会创建后台任务。来源不可用时仍会保留以前已经收录的结果。</span>
        </div>
      </section>

      <section class="detail-section">
        <header class="product-section-head"><div><h2>本地文件</h2><span>NFO 与封面只读取 Luma 规范数据库和本地缓存</span></div></header>
        <div v-if="data.libraryItemId" class="local-file-callout">
          <IconFile :size="22" />
          <div><strong>作品已经进入媒体库</strong><span>可以重新生成 NFO、同步本地封面或重新匹配番号。</span></div>
          <n-button secondary><RouterLink class="button-link" :to="`/library/${data.libraryItemId}`">管理本地文件</RouterLink></n-button>
        </div>
        <div v-else class="quiet-empty compact"><IconFile :size="26" /><strong>还没有本地文件</strong><span>选择资源并完成获取后，Luma 会整理文件并生成 NFO。</span></div>
      </section>
    </div>
  </n-spin>
</template>

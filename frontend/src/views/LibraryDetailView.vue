<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { NButton, NInput, NSpin, useMessage } from 'naive-ui'
import { IconArrowLeft, IconFile, IconFolder, IconPhoto, IconRefresh, IconReplace } from '@tabler/icons-vue'
import { useRoute, useRouter } from 'vue-router'
import { api } from '../api'
import type { LibraryItem } from '../types'
import { formatDate } from '../format'
import MediaCodeLink from '../components/MediaCodeLink.vue'

const route = useRoute()
const router = useRouter()
const message = useMessage()
const loading = ref(true)
const working = ref('')
const item = ref<LibraryItem | null>(null)
const rematchCode = ref('')
const id = computed(() => Number(route.params.id))

function codeFromPath(path: string) {
  const filename = path.split(/[\\/]/).pop() ?? path
  const match = filename.toUpperCase().match(/(?:FC2[-_ ]?PPV[-_ ]?\d{4,8}|[A-Z]{2,12}[-_ ]?\d{2,7})/)
  return match?.[0].replace(/[ _]+/g, '-').replace(/^(FC2)-?(PPV)-?/, '$1-$2-') ?? ''
}

async function load() {
  loading.value = true
  try {
    item.value = await api.libraryItem(id.value)
    rematchCode.value = codeFromPath(item.value.videoPath) || item.value.media.code.toUpperCase()
  } catch (reason) {
    message.error(reason instanceof Error ? reason.message : '详情加载失败')
  } finally {
    loading.value = false
  }
}

async function regenerateNfo() {
  working.value = 'nfo'
  try {
    const result = await api.regenerateLibraryNfo(id.value)
    message.success(result.message)
    await load()
  } catch (reason) {
    message.error(reason instanceof Error ? reason.message : 'NFO 重新生成失败')
  } finally {
    working.value = ''
  }
}

async function syncArtwork() {
  working.value = 'artwork'
  try {
    const result = await api.syncLibraryArtwork(id.value)
    message.success(result.message)
    await load()
  } catch (reason) {
    message.error(reason instanceof Error ? reason.message : '本地封面同步失败')
  } finally {
    working.value = ''
  }
}

async function rematch() {
  working.value = 'rematch'
  try {
    const result = await api.rematchLibrary(id.value, rematchCode.value.trim() || undefined)
    item.value = result.item
    rematchCode.value = result.item.media.code.toUpperCase()
    message.success(result.message)
  } catch (reason) {
    message.error(reason instanceof Error ? reason.message : '重新匹配失败')
  } finally {
    working.value = ''
  }
}

onMounted(load)
</script>

<template>
  <n-spin :show="loading">
    <div v-if="item" class="library-detail-page">
      <button class="back-link" @click="router.back()"><IconArrowLeft :size="16" />返回媒体库</button>

      <section class="library-detail-head">
        <div class="detail-poster compact"><img v-if="item.media.posterUrl" :src="item.media.posterUrl" :alt="item.media.title"><IconFile v-else :size="40" /></div>
        <div>
          <span class="media-type-label">本地媒体</span>
          <h1>{{ item.media.title }}</h1>
          <p><MediaCodeLink :code="item.media.code" :media-id="item.mediaId" />，{{ item.status === 'ready' ? '文件可用' : item.status }}</p>
          <div class="library-export-actions">
            <n-button type="primary" :loading="working === 'nfo'" :disabled="Boolean(working && working !== 'nfo')" @click="regenerateNfo">
              <template #icon><IconRefresh /></template>重新生成 NFO
            </n-button>
            <n-button secondary :loading="working === 'artwork'" :disabled="Boolean(working && working !== 'artwork')" @click="syncArtwork">
              <template #icon><IconPhoto /></template>同步本地封面
            </n-button>
          </div>
        </div>
      </section>

      <section class="library-file-panel panel">
        <header><IconFolder /><div><strong>文件与元数据</strong><span>入库于 {{ formatDate(item.addedAt) }}</span></div></header>
        <dl>
          <div><dt>视频文件</dt><dd>{{ item.videoPath }}</dd></div>
          <div><dt>NFO</dt><dd>{{ item.nfoPath ?? '尚未生成' }}</dd></div>
          <div><dt>本地海报</dt><dd>{{ item.posterPath ?? '暂无本地缓存图片' }}</dd></div>
          <div><dt>文件大小</dt><dd>{{ item.fileSize ? `${(item.fileSize / 1024 / 1024 / 1024).toFixed(2)} GB` : '未知' }}</dd></div>
          <div><dt>来源获取</dt><dd><RouterLink v-if="item.acquisitionId" :to="`/acquisitions/${item.acquisitionId}`">查看获取 #{{ item.acquisitionId }}</RouterLink><span v-else>来自已有媒体目录</span></dd></div>
        </dl>
      </section>

      <section class="library-rematch-panel panel">
        <div class="library-rematch-copy">
          <IconReplace :size="22" />
          <div><strong>重新匹配作品</strong><span>默认从文件名识别番号，也可以在这里修正。匹配后会立即重建 NFO。</span></div>
        </div>
        <div class="library-rematch-form">
          <n-input v-model:value="rematchCode" placeholder="例如 STARS-123" @keyup.enter="rematch" />
          <n-button secondary :loading="working === 'rematch'" :disabled="Boolean(working && working !== 'rematch')" @click="rematch">重新匹配</n-button>
        </div>
      </section>
    </div>
  </n-spin>
</template>

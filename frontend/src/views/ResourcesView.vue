<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref, watch } from 'vue'
import { NButton, NInput, NSpin, useMessage } from 'naive-ui'
import { IconArrowRight, IconCloudDownload, IconSearch, IconUser } from '@tabler/icons-vue'
import { useRoute, useRouter } from 'vue-router'
import { api, productEventUrl } from '../api'
import type { Paged, ProductMedia, SearchResponse } from '../types'
import PageHeader from '../components/PageHeader.vue'
import PosterCard from '../components/PosterCard.vue'
import PaginationBar from '../components/PaginationBar.vue'

const route = useRoute()
const router = useRouter()
const message = useMessage()
const query = ref('')
const loading = ref(false)
const resolving = ref(false)
const resolveStatus = ref('')
const result = ref<SearchResponse | null>(null)
const page = ref(1)
const pageSize = ref(20)
const recentPage = ref(1)
const recentPageSize = ref(20)
const recent = ref<Paged<ProductMedia>>({ items: [], total: 0, page: 1, pageSize: 20, totalPages: 0 })
let eventSource: EventSource | undefined

const requestedCode = computed(() => {
  const match = query.value.trim().toUpperCase().match(/(?:FC2[-_ ]?PPV[-_ ]?\d{4,8}|[A-Z]{2,12}[-_ ]?\d{2,7})/)
  return match?.[0].replace(/[ _]+/g, '-').replace(/^(FC2)-?(PPV)-?/, '$1-$2-') ?? ''
})
const databaseMiss = computed(() => Boolean(result.value && !result.value.media.items.length && !result.value.actors.items.length))

async function runSearch() {
  const q = query.value.trim()
  await router.replace({ query: q ? { q } : {} })
  if (!q) {
    result.value = null
    recent.value = await api.catalogMedia('', recentPage.value, recentPageSize.value)
    return
  }
  loading.value = true
  try {
    result.value = await api.search(q, page.value, pageSize.value)
  } catch (reason) {
    message.error(reason instanceof Error ? reason.message : '搜索失败')
  } finally {
    loading.value = false
  }
}

function changeSearchPage(value: number) { page.value = value; runSearch() }
function changeSearchPageSize(value: number) { pageSize.value = value; page.value = 1; runSearch() }
function changeRecentPage(value: number) { recentPage.value = value; runSearch() }
function changeRecentPageSize(value: number) { recentPageSize.value = value; recentPage.value = 1; runSearch() }

async function resolveFromSources() {
  const term = query.value.trim()
  if (!term) return
  resolving.value = true
  try {
    const response = await api.resolveCatalog(term, Boolean(requestedCode.value))
    resolveStatus.value = `已创建 ${response.jobIds.length} 个按需查找任务，完成后会自动刷新。`
    message.success(`正在从已启用的数据源查找 ${term}`)
  } catch (reason) {
    message.error(reason instanceof Error ? reason.message : '无法创建按需查找任务')
  } finally {
    resolving.value = false
  }
}

async function loadRoute() {
  query.value = String(route.query.q ?? '')
  await runSearch()
}

function startEvents() {
  eventSource = new EventSource(productEventUrl)
  eventSource.onmessage = async event => {
    try {
      const payload = JSON.parse(event.data) as { event?: string; data?: { code?: string; query?: string; status?: string } }
      const eventQuery = String(payload.data?.query ?? payload.data?.code ?? '').trim().toLocaleLowerCase()
      const expectedQuery = (requestedCode.value || query.value.trim()).toLocaleLowerCase()
      if (payload.event !== 'catalog-resolve' || eventQuery !== expectedQuery) return
      if (payload.data?.status === 'success') {
        resolveStatus.value = '数据源查找完成，本地索引已更新。'
        await runSearch()
      }
    } catch {
      // Ignore keep-alive or events from older server versions.
    }
  }
}

watch(() => route.query.q, value => {
  if (String(value ?? '') !== query.value) loadRoute()
})
onMounted(() => {
  startEvents()
  loadRoute()
})
onUnmounted(() => eventSource?.close())
</script>

<template>
  <PageHeader title="资源搜索" description="即时查询 Luma 本地索引。只有你明确点击按需查找时，后台才会访问已启用的数据源。" />

  <div class="search-stage">
    <IconSearch :size="20" />
    <n-input
      v-model:value="query"
      :bordered="false"
      clearable
      size="large"
      placeholder="输入番号、标题或演员姓名"
      @keyup.enter="runSearch"
    />
    <n-button type="primary" size="large" @click="runSearch">搜索</n-button>
  </div>

  <n-spin :show="loading">
    <div v-if="result" class="search-results">
      <section v-if="result.actors.items.length" class="result-section">
        <header class="product-section-head">
          <div><h2>演员</h2><span>别名会一起参与本地匹配</span></div>
          <span>{{ result.actors.total }} 项</span>
        </header>
        <div class="actor-grid">
          <RouterLink v-for="actor in result.actors.items" :key="actor.id" :to="`/actors/${actor.id}`" class="actor-card">
            <span class="actor-avatar"><img v-if="actor.avatarUrl" :src="actor.avatarUrl" :alt="actor.name"><IconUser v-else /></span>
            <span><strong>{{ actor.name }}</strong><small>{{ actor.mediaCount }} 部作品{{ actor.followed ? '，已关注' : '' }}</small></span>
            <IconArrowRight :size="16" />
          </RouterLink>
        </div>
        <PaginationBar :page="page" :page-size="pageSize" :total="result.actors.total" @update:page="changeSearchPage" @update:page-size="changeSearchPageSize" />
      </section>

      <section class="result-section">
        <header class="product-section-head">
          <div><h2>作品</h2><span>标题、原始标题、番号和演员均来自本地数据库</span></div>
          <span>{{ result.media.total }} 项</span>
        </header>
        <div v-if="result.media.items.length" class="poster-grid">
          <PosterCard
            v-for="media in result.media.items"
            :key="media.id"
            :title="media.title"
            :code="media.code"
            :poster-url="media.posterUrl"
            :subtitle="media.releaseDate ?? '日期待补全'"
            :to="`/media/${media.id}`"
          />
        </div>
        <div v-else class="quiet-empty resolve-empty">
          <IconSearch :size="28" />
          <strong>本地数据库暂无结果</strong>
          <span>可以创建高优先级后台任务，从所有已启用的数据源查找这个番号、标题或演员。</span>
          <n-button v-if="databaseMiss" type="primary" :loading="resolving" @click="resolveFromSources">
            <template #icon><IconCloudDownload /></template>
            从数据源查找 {{ query.trim() }}
          </n-button>
          <small v-if="resolveStatus" class="resolve-status">{{ resolveStatus }}</small>
        </div>
        <PaginationBar :page="page" :page-size="pageSize" :total="result.media.total" @update:page="changeSearchPage" @update:page-size="changeSearchPageSize" />
      </section>
    </div>

    <section v-else class="result-section">
      <header class="product-section-head"><div><h2>最近收录</h2><span>后台同步写入本地索引的新内容</span></div></header>
      <div v-if="recent.items.length" class="poster-grid">
        <PosterCard v-for="media in recent.items" :key="media.id" :title="media.title" :code="media.code" :poster-url="media.posterUrl" :to="`/media/${media.id}`" />
      </div>
      <div v-else class="quiet-empty">
        <IconSearch :size="28" />
        <strong>本地索引正在等待内容</strong>
        <span>来源会在后台定时同步，也可以在设置中立即执行一次增量同步。</span>
        <RouterLink to="/settings">管理数据源</RouterLink>
      </div>
      <PaginationBar :page="recentPage" :page-size="recentPageSize" :total="recent.total" @update:page="changeRecentPage" @update:page-size="changeRecentPageSize" />
    </section>
  </n-spin>
</template>

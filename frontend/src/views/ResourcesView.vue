<script setup lang="ts">
import { onMounted, ref, watch } from 'vue'
import { NAlert, NButton, NInput, NSpin, useMessage } from 'naive-ui'
import { IconArrowRight, IconSearch, IconUser } from '@tabler/icons-vue'
import { useRoute, useRouter } from 'vue-router'
import { api } from '../api'
import type { ProductMedia, SearchResponse } from '../types'
import PageHeader from '../components/PageHeader.vue'
import PosterCard from '../components/PosterCard.vue'

const route = useRoute(); const router = useRouter(); const message = useMessage()
const query = ref(''); const loading = ref(false); const result = ref<SearchResponse | null>(null); const recent = ref<ProductMedia[]>([])
async function runSearch() {
  const q = query.value.trim(); router.replace({ query: q ? { q } : {} })
  if (!q) { result.value = null; recent.value = await api.catalogMedia(); return }
  loading.value = true
  try { result.value = await api.search(q) } catch (reason) { message.error(reason instanceof Error ? reason.message : '搜索失败') } finally { loading.value = false }
}
async function loadRoute() { query.value = String(route.query.q ?? ''); await runSearch() }
watch(() => route.query.q, value => { if (String(value ?? '') !== query.value) loadRoute() })
onMounted(loadRoute)
</script>
<template>
  <PageHeader title="资源" description="一个搜索入口聚合作品、演员和可获取资源。来源部分失败时，其余结果仍会保留。" />
  <div class="search-stage"><IconSearch :size="20" /><n-input v-model:value="query" borderless clearable size="large" placeholder="输入番号、标题或演员姓名" @keyup.enter="runSearch" /><n-button type="primary" size="large" @click="runSearch">搜索</n-button></div>
  <n-spin :show="loading">
    <div v-if="result" class="search-results">
      <div v-if="result.providerReports.some(report => !report.ok)" class="provider-report-stack"><n-alert v-for="report in result.providerReports.filter(report => !report.ok)" :key="report.providerKey" type="warning" :title="`${report.providerKey} 暂时不可用`">{{ report.message }}。已显示其他可用结果。</n-alert></div>
      <section v-if="result.actors.length" class="result-section"><header class="product-section-head"><div><span class="eyebrow">PEOPLE</span><h2>演员</h2></div><span>{{ result.actors.length }} 项</span></header><div class="actor-grid"><RouterLink v-for="actor in result.actors" :key="actor.id" :to="`/actors/${actor.id}`" class="actor-card"><span class="actor-avatar"><img v-if="actor.avatarUrl" :src="actor.avatarUrl" :alt="actor.name"><IconUser v-else /></span><span><strong>{{ actor.name }}</strong><small>{{ actor.mediaCount }} 部作品{{ actor.followed ? ' · 已关注' : '' }}</small></span><IconArrowRight :size="16" /></RouterLink></div></section>
      <section class="result-section"><header class="product-section-head"><div><span class="eyebrow">MEDIA</span><h2>作品</h2></div><span>{{ result.media.length }} 项</span></header><div v-if="result.media.length" class="poster-grid"><PosterCard v-for="media in result.media" :key="media.id" :title="media.title" :code="media.code" :poster-url="media.posterUrl" :subtitle="media.releaseDate ?? '等待补全日期'" :to="`/media/${media.id}`" /></div><div v-else class="quiet-empty"><IconSearch :size="28" /><strong>没有匹配的作品</strong><span>尝试完整番号、其他拼写，或稍后重试离线来源。</span></div></section>
    </div>
    <section v-else class="result-section"><header class="product-section-head"><div><span class="eyebrow">RECENTLY SEEN</span><h2>最近发现</h2></div></header><div v-if="recent.length" class="poster-grid"><PosterCard v-for="media in recent" :key="media.id" :title="media.title" :code="media.code" :poster-url="media.posterUrl" :to="`/media/${media.id}`" /></div><div v-else class="quiet-empty"><IconSearch :size="28" /><strong>从第一次搜索开始</strong><span>搜索结果会归一化保存，方便之后选择资源和自动化跟踪。</span></div></section>
  </n-spin>
</template>

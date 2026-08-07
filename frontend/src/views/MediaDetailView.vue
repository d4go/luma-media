<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { NButton, NSpin, useMessage } from 'naive-ui'
import { IconArrowLeft, IconCheck, IconDownload, IconExternalLink, IconMovie, IconUser } from '@tabler/icons-vue'
import { useRoute, useRouter } from 'vue-router'
import { api } from '../api'
import type { MediaDetail } from '../types'
import { formatDate } from '../format'

const route = useRoute(); const router = useRouter(); const message = useMessage(); const loading = ref(true); const acquiring = ref<number | null>(null); const data = ref<MediaDetail | null>(null)
const id = computed(() => Number(route.params.id))
async function load() { loading.value = true; try { data.value = await api.mediaDetail(id.value) } catch (reason) { message.error(reason instanceof Error ? reason.message : '详情加载失败') } finally { loading.value = false } }
async function acquire(resourceId: number) { acquiring.value = resourceId; try { const item = await api.acquireMedia(id.value, resourceId); message.success('获取请求已创建'); router.push(`/acquisitions/${item.id}`) } catch (reason) { message.error(reason instanceof Error ? reason.message : '无法创建获取') } finally { acquiring.value = null } }
function size(value: number | null) { if (!value) return '大小未知'; return `${(value / 1024 / 1024 / 1024).toFixed(2)} GB` }
onMounted(load)
</script>
<template><n-spin :show="loading"><div v-if="data" class="media-detail-page">
  <button class="back-link" @click="router.back()"><IconArrowLeft :size="16" />返回资源</button>
  <section class="media-hero-detail">
    <div class="detail-poster"><img v-if="data.media.posterUrl" :src="data.media.posterUrl" :alt="data.media.title"><div v-else><IconMovie :size="42" /><span>{{ data.media.code }}</span></div></div>
    <div class="detail-identity"><span class="eyebrow">{{ data.media.mediaType.toUpperCase() }}</span><h1>{{ data.media.title }}</h1><div class="identity-code">{{ data.media.code }}</div><p>{{ data.media.summary || '当前来源尚未提供简介。资源和获取功能不受影响，MetaTube 可在入库时继续补全元数据。' }}</p><div class="identity-meta"><span>{{ data.media.releaseDate ?? '日期待补全' }}</span><span v-if="data.media.durationMinutes">{{ data.media.durationMinutes }} 分钟</span><span>{{ data.media.metadataStatus === 'complete' ? '元数据完整' : '元数据待补全' }}</span></div><div class="identity-actions"><n-button v-if="data.libraryItemId" type="primary"><RouterLink :to="`/library/${data.libraryItemId}`"><IconCheck :size="17" />已在媒体库</RouterLink></n-button><n-button v-else-if="data.latestAcquisitionId" type="primary"><RouterLink :to="`/acquisitions/${data.latestAcquisitionId}`"><IconDownload :size="17" />查看获取</RouterLink></n-button></div></div>
  </section>
  <section v-if="data.actors.length" class="detail-section"><header class="product-section-head"><div><span class="eyebrow">CAST</span><h2>演员</h2></div></header><div class="actor-pills"><RouterLink v-for="actor in data.actors" :key="actor.id" :to="`/actors/${actor.id}`"><IconUser :size="15" />{{ actor.name }}</RouterLink></div></section>
  <section class="detail-section"><header class="product-section-head"><div><span class="eyebrow">RANKED RESOURCES</span><h2>可获取资源</h2></div><span>排序由服务端统一计算</span></header>
    <div v-if="data.resources.length" class="resource-stack"><article v-for="(resource, index) in data.resources" :key="resource.id" class="resource-card" :class="{ preferred: index === 0 }"><div class="resource-rank"><strong>{{ Math.round(resource.score) }}</strong><span>评分</span></div><div class="resource-main"><div><span v-if="index === 0" class="preferred-label">推荐</span><strong>{{ resource.title }}</strong></div><p><span>{{ resource.providerKey }}</span><span>{{ resource.resolution ?? '清晰度未知' }}</span><span>{{ size(resource.sizeBytes) }}</span><span v-if="resource.publishedAt">{{ formatDate(resource.publishedAt) }}</span></p><ul><li v-for="reason in resource.scoreReasons" :key="reason">{{ reason }}</li></ul></div><n-button type="primary" :loading="acquiring === resource.id" @click="acquire(resource.id)">获取</n-button></article></div>
    <div v-else class="quiet-empty"><IconExternalLink :size="28" /><strong>来源未返回可获取资源</strong><span>JavBus 来源可能需要 Cookie，或该作品暂时没有公开资源。请在设置中逐个测试来源。</span><RouterLink to="/settings">检查 Provider</RouterLink></div>
  </section>
</div></n-spin></template>

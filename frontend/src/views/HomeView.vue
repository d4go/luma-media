<script setup lang="ts">
import { onMounted, onUnmounted, ref } from 'vue'
import { NButton, NSpin, useMessage } from 'naive-ui'
import { IconAlertTriangle, IconArrowRight, IconBolt, IconBooks, IconDownload, IconInbox, IconLibrary, IconMovie, IconSearch, IconUsers } from '@tabler/icons-vue'
import { api } from '../api'
import type { HomeData } from '../types'

const data = ref<HomeData | null>(null)
const loading = ref(true)
const message = useMessage()
let events: EventSource | null = null
async function load() { try { data.value = await api.home() } catch (reason) { message.error(reason instanceof Error ? reason.message : '首页加载失败') } finally { loading.value = false } }
function stateLabel(value: string, qbitState?: string | null) {
  if (['DOWNLOADING', 'QUEUED', 'NEEDS_ATTENTION', 'CANCELLED'].includes(value) && qbitState) {
    const state = qbitState.toLocaleLowerCase()
    if (state === 'missing') return 'qB 中已删除'
    if (state.includes('paused')) return '已暂停'
    if (state.includes('upload')) return '做种中'
    if (state.includes('stalled')) return '等待数据'
    if (state.includes('check')) return '校验中'
    if (state.includes('queue')) return '排队中'
  }
  return ({ REQUESTED:'已请求',RESOURCE_RESOLVING:'解析资源',QUEUED:'排队',DOWNLOADING:'下载中',DOWNLOADED:'已下载',PROCESSING:'整理文件',METADATA:'补全元数据',LIBRARY_COMMIT:'提交媒体库',COMPLETED:'已完成',NEEDS_ATTENTION:'需要关注',CANCELLED:'已取消' } as Record<string,string>)[value] ?? value
}
onMounted(() => { load(); events = new EventSource('/api/v1/events'); events.onmessage = () => load() })
onUnmounted(() => events?.close())
</script>
<template>
  <n-spin :show="loading">
    <div class="product-home">
      <section class="home-hero">
        <div class="hero-copy"><span class="eyebrow">LUMA MEDIA</span><h1>今天想把什么<br>带回媒体库？</h1><p>搜索作品，选择资源，剩下的下载、整理和入库交给 Luma。</p><div class="hero-actions"><n-button type="primary" size="large"><RouterLink to="/resources"><IconSearch :size="18" />搜索资源</RouterLink></n-button><n-button secondary size="large"><RouterLink to="/library">打开媒体库<IconArrowRight :size="17" /></RouterLink></n-button></div></div>
        <aside class="hero-pulse"><div class="hero-pulse-head"><span><IconBolt :size="18" />当前流程</span><RouterLink to="/downloads">查看全部<IconArrowRight :size="14" /></RouterLink></div><strong>{{ data?.activeAcquisitions ?? 0 }}</strong><span>项获取正在推进</span><div class="hero-pulse-meta"><span><IconAlertTriangle :size="15" />{{ data?.attentionCount ?? 0 }} 项需要关注</span><span><IconBooks :size="15" />{{ data?.libraryCount ?? 0 }} 项已入库</span></div></aside>
      </section>

      <section class="home-metrics" aria-label="产品摘要">
        <RouterLink to="/downloads"><IconDownload /><span><strong>{{ data?.activeAcquisitions ?? 0 }}</strong>进行中的获取</span></RouterLink>
        <RouterLink to="/library"><IconLibrary /><span><strong>{{ data?.libraryCount ?? 0 }}</strong>媒体库项目</span></RouterLink>
        <RouterLink to="/downloads?attention=1"><IconInbox /><span><strong>{{ data?.attentionCount ?? 0 }}</strong>需要关注</span></RouterLink>
        <RouterLink to="/resources"><IconUsers /><span><strong>{{ data?.followedActors ?? 0 }}</strong>已关注演员</span></RouterLink>
      </section>

      <div class="home-workspace">
        <section class="home-section home-recent">
          <header class="product-section-head"><div><h2>最近获取</h2><p>下载状态与 qBittorrent 保持同步</p></div><RouterLink to="/downloads">查看全部<IconArrowRight :size="15" /></RouterLink></header>
          <div v-if="data?.recentAcquisitions.length" class="home-recent-list">
            <RouterLink v-for="item in data.recentAcquisitions" :key="item.id" :to="`/acquisitions/${item.id}`" class="home-recent-row"><span class="home-recent-poster"><img v-if="item.media.posterUrl" :src="item.media.posterUrl" :alt="item.media.title"><IconMovie v-else :size="20" /></span><span class="home-recent-copy"><strong>{{ item.media.title }}</strong><small>{{ item.media.code || '番号待识别' }}<span>{{ item.stateMessage }}</span></small></span><span class="state-chip" :data-state="item.qbitState === 'missing' ? 'NEEDS_ATTENTION' : item.state">{{ stateLabel(item.state, item.qbitState) }}</span><span class="home-recent-progress">{{ Math.round(item.progress * 100) }}%</span><IconArrowRight class="home-row-arrow" :size="16" /></RouterLink>
          </div>
          <div v-else class="quiet-empty"><IconBolt :size="28" /><strong>还没有获取记录</strong><span>从一次搜索开始，Luma 会在这里保留完整进度。</span><RouterLink to="/resources">发现资源</RouterLink></div>
        </section>

        <aside class="home-quick-starts"><header><h2>快捷入口</h2><p>继续完成常用操作</p></header><nav><RouterLink v-for="(item, index) in data?.quickStarts ?? []" :key="item.to" :to="item.to"><span class="quick-start-icon"><IconSearch v-if="index === 0" :size="18" /><IconDownload v-else-if="index === 1" :size="18" /><IconInbox v-else :size="18" /></span><span><strong>{{ item.title }}</strong><small>{{ item.description }}</small></span><IconArrowRight :size="16" /></RouterLink></nav></aside>
      </div>
    </div>
  </n-spin>
</template>

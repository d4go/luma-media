<script setup lang="ts">
import { onMounted, onUnmounted, ref } from 'vue'
import { NButton, NSpin, useMessage } from 'naive-ui'
import { IconArrowRight, IconBolt, IconDownload, IconInbox, IconLibrary, IconSearch, IconUsers } from '@tabler/icons-vue'
import { api } from '../api'
import type { HomeData } from '../types'

const data = ref<HomeData | null>(null)
const loading = ref(true)
const message = useMessage()
let events: EventSource | null = null
async function load() { try { data.value = await api.home() } catch (reason) { message.error(reason instanceof Error ? reason.message : '首页加载失败') } finally { loading.value = false } }
onMounted(() => { load(); events = new EventSource('/api/v1/events'); events.onmessage = () => load() })
onUnmounted(() => events?.close())
</script>
<template>
  <n-spin :show="loading">
    <div class="product-home">
      <section class="home-hero">
        <div class="hero-copy"><span class="eyebrow">YOUR MEDIA, IN ONE FLOW</span><h1>从发现到入库，<br>每一步都清楚。</h1><p>搜索作品和演员，选择系统推荐的资源，然后由 Luma 持续跟踪下载、整理元数据并提交媒体库。</p><div class="hero-actions"><n-button type="primary" size="large"><RouterLink to="/resources"><IconSearch :size="18" />开始搜索</RouterLink></n-button><n-button secondary size="large"><RouterLink to="/downloads">查看获取进度<IconArrowRight :size="17" /></RouterLink></n-button></div></div>
        <div class="hero-pulse"><IconBolt :size="30" /><strong>{{ data?.activeAcquisitions ?? 0 }}</strong><span>项获取正在推进</span><small>状态会实时更新</small></div>
      </section>

      <section class="home-metrics" aria-label="产品摘要">
        <RouterLink to="/downloads"><IconDownload /><span><strong>{{ data?.activeAcquisitions ?? 0 }}</strong>进行中的获取</span></RouterLink>
        <RouterLink to="/library"><IconLibrary /><span><strong>{{ data?.libraryCount ?? 0 }}</strong>媒体库项目</span></RouterLink>
        <RouterLink to="/downloads?attention=1"><IconInbox /><span><strong>{{ data?.attentionCount ?? 0 }}</strong>需要关注</span></RouterLink>
        <RouterLink to="/resources"><IconUsers /><span><strong>{{ data?.followedActors ?? 0 }}</strong>已关注演员</span></RouterLink>
      </section>

      <section class="home-section">
        <header class="product-section-head"><div><span class="eyebrow">RECENT FLOW</span><h2>最近获取</h2></div><RouterLink to="/downloads">查看全部<IconArrowRight :size="15" /></RouterLink></header>
        <div v-if="data?.recentAcquisitions.length" class="acquisition-strip">
          <RouterLink v-for="item in data.recentAcquisitions" :key="item.id" :to="`/acquisitions/${item.id}`" class="flow-mini-card"><span class="state-chip" :data-state="item.state">{{ item.state }}</span><strong>{{ item.media.title }}</strong><small>{{ item.media.code }} · {{ item.stateMessage }}</small><div class="thin-progress"><i :style="{ width: `${item.progress * 100}%` }" /></div></RouterLink>
        </div>
        <div v-else class="quiet-empty"><IconBolt :size="28" /><strong>还没有获取记录</strong><span>从一次搜索开始，Luma 会在这里保留完整进度。</span><RouterLink to="/resources">发现资源</RouterLink></div>
      </section>
    </div>
  </n-spin>
</template>

<script setup lang="ts">
import { computed, inject } from 'vue'
import { NButton, NSkeleton } from 'naive-ui'
import { RouterLink } from 'vue-router'
import {
  IconArrowRight, IconBolt, IconCode, IconFolderPlus, IconPlugConnected,
  IconRoute, IconSettings,
} from '@tabler/icons-vue'
import { serviceStatusKey } from '../service-status'

const serviceContext = inject(serviceStatusKey)
const services = computed(() => [
  { name: 'Luma', description: '任务调度与媒体索引', health: serviceContext?.status.value?.luma },
  { name: 'MetaTube', description: '影片元数据来源', health: serviceContext?.status.value?.metaTube },
  { name: 'qBittorrent', description: '下载与 Tracker 管理', health: serviceContext?.status.value?.qbittorrent },
])
const onlineCount = computed(() => services.value.filter((item) => item.health?.connected).length)
const statusReady = computed(() => Boolean(serviceContext?.status.value))
</script>

<template>
  <div class="guide-page">
    <section class="guide-hero">
      <div class="guide-hero-copy">
        <span class="section-kicker">LUMA CONTROL ROOM</span>
        <h1>从目录到元数据，<br><em>先完成一条可靠链路。</em></h1>
        <p>连接服务、添加媒体目录，再让 Luma 自动监听文件、运行爬虫并把结果交给 qBittorrent。</p>
        <div class="guide-actions">
          <n-button type="primary" size="large" tag="div">
            <RouterLink to="/settings">配置服务 <IconArrowRight :size="18" /></RouterLink>
          </n-button>
          <n-button color="#303338" text-color="#f3f4f5" size="large" tag="div"><RouterLink to="/folders">添加媒体目录</RouterLink></n-button>
        </div>
      </div>
      <div class="guide-signal" aria-label="当前服务连接情况">
        <div class="signal-orbit"><IconRoute :size="48" :stroke-width="1.25" /></div>
        <strong>{{ statusReady ? `${onlineCount} / 3` : '···' }}</strong>
        <span>{{ statusReady ? '服务已连接' : '正在检查服务' }}</span>
        <small>状态每 10 秒自动同步</small>
      </div>
    </section>

    <section class="service-board">
      <div class="section-heading">
        <div><span class="section-kicker">SYSTEM CHECK</span><h2>链路就绪情况</h2></div>
        <n-button text type="primary" :loading="serviceContext?.refreshing.value" @click="serviceContext?.refresh()">立即检查</n-button>
      </div>
      <div class="service-board-grid">
        <article v-for="service in services" :key="service.name" class="service-board-item">
          <template v-if="service.health">
            <span class="status-dot large" :class="{ online: service.health.connected }" />
            <div><strong>{{ service.name }}</strong><span>{{ service.description }}</span></div>
            <div class="service-result">
              <b>{{ service.health.connected ? '在线' : '需要处理' }}</b>
              <small>{{ service.health.message }}</small>
            </div>
          </template>
          <n-skeleton v-else text :repeat="2" />
        </article>
      </div>
    </section>

    <section class="onboarding-flow">
      <div class="section-heading flow-heading">
        <div><span class="section-kicker">FIRST RUN</span><h2>三步开始自动化</h2></div>
        <p>建议按顺序完成。所有配置都保存在 Luma 数据目录中。</p>
      </div>
      <div class="flow-grid">
        <article class="flow-card featured">
          <span class="flow-index">01</span>
          <div class="flow-icon"><IconPlugConnected /></div>
          <h3>接通外部服务</h3>
          <p>确认 MetaTube 与 qBittorrent 地址可从 Luma 容器访问，并分别测试凭据。</p>
          <RouterLink to="/settings">打开系统设置 <IconArrowRight :size="16" /></RouterLink>
        </article>
        <article class="flow-card">
          <span class="flow-index">02</span>
          <div class="flow-icon"><IconFolderPlus /></div>
          <h3>添加媒体目录</h3>
          <p>选择手动、定时或目录监听模式。监听模式会在文件变化后自动创建扫描任务。</p>
          <RouterLink to="/folders">管理目录 <IconArrowRight :size="16" /></RouterLink>
        </article>
        <article class="flow-card compact-flow">
          <span class="flow-index">03</span>
          <div class="flow-icon"><IconCode /></div>
          <h3>上传 Python 爬虫</h3>
          <p>填写目标网站和循环周期，脚本结果可保存并直接创建下载。</p>
          <RouterLink to="/crawlers">配置爬虫 <IconArrowRight :size="16" /></RouterLink>
        </article>
        <aside class="flow-note">
          <IconBolt :size="20" />
          <div><strong>推荐顺序</strong><span>先验证连接，再开启自动下载与 Tracker 更新。</span></div>
          <RouterLink to="/settings" aria-label="前往系统设置"><IconSettings :size="18" /></RouterLink>
        </aside>
      </div>
    </section>
  </div>
</template>

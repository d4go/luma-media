<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from 'vue'
import { NButton, NProgress, NSpin, useDialog, useMessage } from 'naive-ui'
import { IconArrowLeft, IconPlayerPause, IconPlayerPlay, IconRefresh, IconRotateClockwise, IconTrash, IconX } from '@tabler/icons-vue'
import { useRoute, useRouter } from 'vue-router'
import { api } from '../api'
import type { AcquisitionDetail } from '../types'
import { formatDate } from '../format'
import PaginationBar from '../components/PaginationBar.vue'
import MediaCodeLink from '../components/MediaCodeLink.vue'

const route = useRoute(); const router = useRouter(); const message = useMessage(); const dialog = useDialog(); const loading = ref(true); const acting = ref(false); const data = ref<AcquisitionDetail | null>(null); let events: EventSource | null = null
const id = computed(() => Number(route.params.id)); const item = computed(() => data.value?.acquisition)
const page = ref(1); const pageSize = ref(20)
async function load() { try { data.value = await api.acquisition(id.value, page.value, pageSize.value) } catch (reason) { message.error(reason instanceof Error ? reason.message : '获取详情加载失败') } finally { loading.value = false } }
function changePage(value: number) { page.value = value; load() }
function changePageSize(value: number) { pageSize.value = value; page.value = 1; load() }
async function action(value: 'pause'|'resume'|'retry'|'cancel') { acting.value = true; try { await api.acquisitionAction(id.value, value); message.success('操作已提交'); await load() } catch (reason) { message.error(reason instanceof Error ? reason.message : '操作失败') } finally { acting.value = false } }
function stateLabel(state: string, qbitState?: string | null) { if (!['DOWNLOADING', 'QUEUED', 'NEEDS_ATTENTION', 'CANCELLED'].includes(state) || !qbitState) return state; const value = qbitState.toLocaleLowerCase(); if (value === 'missing') return 'QBITTORRENT REMOVED'; if (value.includes('paused')) return 'QBITTORRENT PAUSED'; if (value.includes('upload')) return 'QBITTORRENT SEEDING'; if (value.includes('stalled')) return 'QBITTORRENT STALLED'; if (value.includes('check')) return 'QBITTORRENT CHECKING'; if (value.includes('queue')) return 'QBITTORRENT QUEUED'; return 'QBITTORRENT DOWNLOADING' }
function qbitPaused(state?: string | null) { return !!state?.toLocaleLowerCase().includes('paused') }
function cancel() { dialog.warning({ title: '取消获取', content: '将从 qBittorrent 移除任务，但不会删除已经下载的文件。确认继续吗？', positiveText: '取消获取', negativeText: '返回', onPositiveClick: () => action('cancel') }) }
function isDeletable(value: AcquisitionDetail['acquisition']) { return value.state === 'CANCELLED' || value.qbitState === 'missing' }
function remove() {
  if (!item.value) return
  dialog.warning({
    title: '删除获取记录',
    content: `确认删除“${item.value.media.title}”的获取记录吗？该记录及时间线将被永久移除，已入库文件不受影响。`,
    positiveText: '删除',
    negativeText: '返回',
    onPositiveClick: async () => {
      acting.value = true
      try { await api.deleteAcquisition(item.value!.id); message.success('获取记录已删除'); router.push('/downloads') }
      catch (reason) { message.error(reason instanceof Error ? reason.message : '删除失败') }
      finally { acting.value = false }
    },
  })
}
onMounted(() => { load(); events = new EventSource('/api/v1/events'); events.onmessage = event => { if (event.data.includes(`\"acquisitionId\":${id.value}`)) load() } })
onUnmounted(() => events?.close())
</script>
<template><n-spin :show="loading"><div v-if="item" class="acquisition-detail-page"><button class="back-link" @click="router.back()"><IconArrowLeft :size="16" />返回下载</button><section class="acquisition-detail-head"><div><span class="state-chip" :data-state="item.qbitState === 'missing' ? 'NEEDS_ATTENTION' : item.state">{{ stateLabel(item.state, item.qbitState) }}</span><h1>{{ item.media.title }}</h1><p><MediaCodeLink :code="item.media.code" :media-id="item.mediaId" /> · 获取 #{{ item.id }}</p></div><div class="detail-actions"><n-button v-if="item.state === 'DOWNLOADING' && item.qbitState !== 'missing' && !qbitPaused(item.qbitState)" :loading="acting" @click="action('pause')"><template #icon><IconPlayerPause /></template>暂停</n-button><n-button v-if="item.state === 'DOWNLOADING' && qbitPaused(item.qbitState)" :loading="acting" @click="action('resume')"><template #icon><IconPlayerPlay /></template>继续</n-button><n-button v-if="item.state === 'NEEDS_ATTENTION'" type="primary" :loading="acting" @click="action('retry')"><template #icon><IconRotateClockwise /></template>重试</n-button><n-button v-if="!['COMPLETED','CANCELLED'].includes(item.state)" secondary type="error" :loading="acting" @click="cancel"><template #icon><IconX /></template>取消</n-button><n-button v-if="isDeletable(item)" secondary type="error" :loading="acting" @click="remove"><template #icon><IconTrash /></template>删除记录</n-button></div></section><section class="acquisition-now panel"><div><span class="eyebrow">CURRENT STAGE</span><h2>{{ item.qbitState === 'missing' ? 'qBittorrent 中已删除该任务' : item.stateMessage }}</h2><p v-if="item.lastError" class="error-inline">{{ item.lastError }}</p></div><n-progress type="circle" :percentage="Math.round(item.progress * 100)" :status="item.state === 'NEEDS_ATTENTION' || item.qbitState === 'missing' ? 'error' : item.state === 'COMPLETED' ? 'success' : 'default'" /><dl><div><dt>qBittorrent 状态</dt><dd>{{ item.qbitState ?? '等待同步' }}</dd></div><div><dt>下载速度</dt><dd>{{ (item.downloadSpeed / 1024 / 1024).toFixed(1) }} MB/s</dd></div><div><dt>预计剩余</dt><dd>{{ item.etaSeconds == null || item.etaSeconds > 8640000 ? '计算中' : `${Math.ceil(item.etaSeconds / 60)} 分钟` }}</dd></div><div><dt>请求来源</dt><dd>{{ item.requestedBy }}</dd></div><div><dt>qB Hash</dt><dd>{{ item.qbitHash ?? '尚未生成' }}</dd></div></dl></section><section class="timeline-section"><header class="product-section-head"><div><span class="eyebrow">AUDIT TRAIL</span><h2>获取时间线</h2></div><n-button text @click="load"><template #icon><IconRefresh /></template>刷新</n-button></header><ol class="event-timeline"><li v-for="event in data?.events.items" :key="event.id"><i /><div><strong>{{ event.message }}</strong><span>{{ event.toState }}</span><small>{{ formatDate(event.createdAt) }}</small></div></li></ol><PaginationBar v-if="data && data.events.total > 0" :page="page" :page-size="pageSize" :total="data.events.total" @update:page="changePage" @update:page-size="changePageSize" /></section></div></n-spin></template>

<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from 'vue'
import { NButton, NSelect, NSpin, useDialog, useMessage } from 'naive-ui'
import { IconAlertTriangle, IconArrowRight, IconDownload, IconRefresh, IconTrash } from '@tabler/icons-vue'
import { useRoute } from 'vue-router'
import { api } from '../api'
import type { Acquisition, AttentionItem, Paged } from '../types'
import PageHeader from '../components/PageHeader.vue'
import { formatDate } from '../format'
import PaginationBar from '../components/PaginationBar.vue'
import MediaCodeLink from '../components/MediaCodeLink.vue'

const route = useRoute(); const message = useMessage(); const dialog = useDialog(); const loading = ref(true); const status = ref(''); const page = ref(1); const pageSize = ref(20); const attentionPage = ref(1); const attentionPageSize = ref(20); const acquisitions = ref<Paged<Acquisition>>({ items: [], total: 0, page: 1, pageSize: 20, totalPages: 0 }); const attention = ref<Paged<AttentionItem>>({ items: [], total: 0, page: 1, pageSize: 20, totalPages: 0 }); let events: EventSource | null = null; let refreshTimer: number | undefined
const filteredAttention = computed(() => attention.value.items)
const statusOptions = [{ label: '全部状态', value: '' }, { label: '正在获取', value: 'DOWNLOADING' }, { label: '需要关注', value: 'NEEDS_ATTENTION' }, { label: '已完成', value: 'COMPLETED' }, { label: '已取消', value: 'CANCELLED' }]
async function load(show = false) { if (show) loading.value = true; try { [acquisitions.value, attention.value] = await Promise.all([api.acquisitions(status.value, page.value, pageSize.value), api.attention(attentionPage.value, attentionPageSize.value)]) } catch (reason) { message.error(reason instanceof Error ? reason.message : '获取列表加载失败') } finally { loading.value = false } }
function changePage(value: number) { page.value = value; load(true) }
function changePageSize(value: number) { pageSize.value = value; page.value = 1; load(true) }
function changeAttentionPage(value: number) { attentionPage.value = value; load(true) }
function changeAttentionPageSize(value: number) { attentionPageSize.value = value; attentionPage.value = 1; load(true) }
function scheduleRefresh() { window.clearTimeout(refreshTimer); refreshTimer = window.setTimeout(() => load(), 250) }
async function act(item: AttentionItem, action: string) { try { await api.attentionAction(item.id, action); message.success(action === 'retry' ? '已重新开始处理' : action === 'acquire' ? '获取请求已创建' : '问题已处理'); await load() } catch (reason) { message.error(reason instanceof Error ? reason.message : '操作失败') } }
function isDeletable(item: Acquisition) { return item.state === 'CANCELLED' || item.qbitState === 'missing' }
function confirmDelete(item: Acquisition) {
  dialog.warning({
    title: '删除获取记录',
    content: `确认删除“${item.media.title}”的获取记录吗？该记录及时间线将被永久移除，已入库文件不受影响。`,
    positiveText: '删除',
    negativeText: '返回',
    onPositiveClick: async () => {
      try { await api.deleteAcquisition(item.id); message.success('获取记录已删除'); await load(true) }
      catch (reason) { message.error(reason instanceof Error ? reason.message : '删除失败') }
    },
  })
}
function qbitStateLabel(value: string) { const state = value.toLocaleLowerCase(); if (state === 'missing') return 'qB 中已删除'; if (state.includes('error') || state.includes('missingfiles')) return 'qB 异常'; if (state.includes('paused')) return 'qB 已暂停'; if (state.includes('upload')) return 'qB 做种中'; if (state.includes('stalled')) return 'qB 等待数据'; if (state.includes('check')) return 'qB 校验中'; if (state.includes('queue')) return 'qB 排队中'; return 'qB 下载中' }
function stateLabel(value: string, qbitState?: string | null) { if (['DOWNLOADING', 'QUEUED', 'NEEDS_ATTENTION', 'CANCELLED'].includes(value) && qbitState) return qbitStateLabel(qbitState); return ({ REQUESTED:'已请求',RESOURCE_RESOLVING:'解析资源',QUEUED:'排队',DOWNLOADING:'下载中',DOWNLOADED:'已下载',PROCESSING:'整理文件',METADATA:'补全元数据',LIBRARY_COMMIT:'提交媒体库',COMPLETED:'已完成',NEEDS_ATTENTION:'需要关注',CANCELLED:'已取消' } as Record<string,string>)[value] ?? value }
onMounted(() => { if (route.query.attention) status.value = 'NEEDS_ATTENTION'; load(true); events = new EventSource('/api/v1/events'); events.onmessage = scheduleRefresh })
onUnmounted(() => { events?.close(); window.clearTimeout(refreshTimer) })
</script>
<template>
  <PageHeader title="下载" description="这里展示业务获取流程，而不是裸露的 qBittorrent 任务。每个项目都能追溯到作品、资源和入库结果。"><n-select v-model:value="status" :options="statusOptions" style="width: 150px" @update:value="load(true)" /><n-button secondary @click="load(true)"><template #icon><IconRefresh /></template>刷新</n-button></PageHeader>
  <n-spin :show="loading"><div class="acquisition-page">
    <section v-if="filteredAttention.length" class="attention-panel"><header><div><IconAlertTriangle /><span><strong>需要你处理</strong><small>{{ attention.total }} 个问题阻止了流程继续</small></span></div></header><article v-for="item in filteredAttention" :key="item.id"><span><strong>{{ item.title }}</strong><small><MediaCodeLink :code="item.mediaCode" :media-id="item.mediaId" fallback-to-search /> {{ item.mediaTitle }}</small><p>{{ item.message }}</p></span><div><n-button v-for="action in item.actions" :key="action" :type="action === 'retry' || action === 'acquire' ? 'primary' : 'default'" size="small" @click="act(item, action)">{{ {retry:'重试',cancel:'取消获取',dismiss:'知道了',acquire:'确认获取'}[action] ?? action }}</n-button></div></article><PaginationBar :page="attentionPage" :page-size="attentionPageSize" :total="attention.total" @update:page="changeAttentionPage" @update:page-size="changeAttentionPageSize" /></section>
    <section class="acquisition-list"><RouterLink v-for="item in acquisitions.items" :key="item.id" :to="`/acquisitions/${item.id}`" class="acquisition-card"><div class="acquisition-poster"><img v-if="item.media.posterUrl" :src="item.media.posterUrl" :alt="item.media.title"><IconDownload v-else /></div><div class="acquisition-copy"><div><span class="state-chip" :data-state="item.qbitState === 'missing' ? 'NEEDS_ATTENTION' : item.state">{{ stateLabel(item.state, item.qbitState) }}</span><small>#{{ item.id }} · {{ formatDate(item.updatedAt) }}</small></div><strong>{{ item.media.title }}</strong><p><MediaCodeLink :code="item.media.code" :media-id="item.mediaId" /> · {{ item.stateMessage }}</p><div v-if="!['COMPLETED','CANCELLED','NEEDS_ATTENTION'].includes(item.state)" class="progress-track"><i :style="{ width: `${Math.max(item.progress * 100, 0)}%` }" /><span>{{ Math.round(item.progress * 100) }}%</span></div><span v-else-if="item.lastError" class="error-inline">{{ item.lastError }}</span></div><n-button v-if="isDeletable(item)" quaternary circle type="error" size="small" class="acquisition-delete" aria-label="删除获取记录" @click.prevent.stop="confirmDelete(item)"><template #icon><IconTrash :size="15" /></template></n-button><IconArrowRight class="card-arrow" /></RouterLink><div v-if="!acquisitions.items.length" class="quiet-empty"><IconDownload :size="28" /><strong>当前没有获取记录</strong><span>从资源页选择一个作品并点击“获取”。</span><RouterLink to="/resources">发现资源</RouterLink></div><PaginationBar :page="page" :page-size="pageSize" :total="acquisitions.total" @update:page="changePage" @update:page-size="changePageSize" /></section>
  </div></n-spin>
</template>

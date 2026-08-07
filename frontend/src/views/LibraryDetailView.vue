<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { NButton, NSpin, useMessage } from 'naive-ui'
import { IconArrowLeft, IconFile, IconFolder, IconRefresh } from '@tabler/icons-vue'
import { useRoute, useRouter } from 'vue-router'
import { api } from '../api'
import type { LibraryItem } from '../types'
import { formatDate } from '../format'
const route=useRoute();const router=useRouter();const message=useMessage();const loading=ref(true);const working=ref(false);const item=ref<LibraryItem|null>(null);const id=computed(()=>Number(route.params.id))
async function load(){try{item.value=await api.libraryItem(id.value)}catch(reason){message.error(reason instanceof Error?reason.message:'详情加载失败')}finally{loading.value=false}}
async function reorganize(){working.value=true;try{const result=await api.reorganizeLibrary(id.value);item.value=result.item;message.success(result.message)}catch(reason){message.error(reason instanceof Error?reason.message:'整理失败')}finally{working.value=false}}
onMounted(load)
</script>
<template><n-spin :show="loading"><div v-if="item" class="library-detail-page"><button class="back-link" @click="router.back()"><IconArrowLeft :size="16" />返回媒体库</button><section class="library-detail-head"><div class="detail-poster compact"><img v-if="item.media.posterUrl" :src="item.media.posterUrl" :alt="item.media.title"><IconFile v-else :size="40" /></div><div><span class="eyebrow">LIBRARY ITEM</span><h1>{{ item.media.title }}</h1><p>{{ item.media.code }} · {{ item.status === 'ready' ? '可用' : item.status }}</p><n-button secondary :loading="working" @click="reorganize"><template #icon><IconRefresh /></template>按当前规则重新整理</n-button></div></section><section class="library-file-panel panel"><header><IconFolder /><div><strong>文件与元数据</strong><span>入库于 {{ formatDate(item.addedAt) }}</span></div></header><dl><div><dt>视频文件</dt><dd>{{ item.videoPath }}</dd></div><div><dt>NFO</dt><dd>{{ item.nfoPath ?? '未生成' }}</dd></div><div><dt>海报</dt><dd>{{ item.posterPath ?? '由媒体服务按需读取' }}</dd></div><div><dt>文件大小</dt><dd>{{ item.fileSize ? `${(item.fileSize/1024/1024/1024).toFixed(2)} GB` : '未知' }}</dd></div><div><dt>来源获取</dt><dd><RouterLink v-if="item.acquisitionId" :to="`/acquisitions/${item.acquisitionId}`">查看获取 #{{ item.acquisitionId }}</RouterLink><span v-else>来自既有媒体目录</span></dd></div></dl></section></div></n-spin></template>

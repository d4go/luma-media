<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { NButton, NInput, NSpin, useMessage } from 'naive-ui'
import { IconLibrary, IconRefresh, IconSearch } from '@tabler/icons-vue'
import { api } from '../api'
import type { LibraryItem } from '../types'
import PageHeader from '../components/PageHeader.vue'
import PosterCard from '../components/PosterCard.vue'

const message = useMessage(); const loading = ref(true); const query = ref(''); const items = ref<LibraryItem[]>([])
async function load() { loading.value = true; try { items.value = (await api.library(query.value.trim())).items } catch (reason) { message.error(reason instanceof Error ? reason.message : '媒体库加载失败') } finally { loading.value = false } }
onMounted(load)
</script>
<template><PageHeader title="媒体库" description="只展示已经完成文件整理和入库提交的内容。"><n-button secondary @click="load"><template #icon><IconRefresh /></template>刷新</n-button></PageHeader><div class="library-toolbar"><IconSearch :size="17" /><n-input v-model:value="query" :bordered="false" clearable placeholder="搜索媒体库" @keyup.enter="load" /></div><n-spin :show="loading"><div v-if="items.length" class="poster-grid library-grid"><PosterCard v-for="item in items" :key="item.id" :title="item.media.title" :code="item.media.code" :poster-url="item.media.posterUrl" :subtitle="item.media.metadataStatus === 'complete' ? '元数据完整' : '元数据待补全'" :to="`/library/${item.id}`" /></div><div v-else class="quiet-empty large"><IconLibrary :size="34" /><strong>{{ query ? '没有匹配的入库项目' : '媒体库还没有内容' }}</strong><span>获取完成并经过文件整理后，内容会自动出现在这里。</span><RouterLink to="/resources">发现第一个资源</RouterLink></div></n-spin></template>

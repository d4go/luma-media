<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { NButton, NSpin, useMessage } from 'naive-ui'
import { IconArrowLeft, IconBell, IconBellOff, IconUser } from '@tabler/icons-vue'
import { useRoute, useRouter } from 'vue-router'
import { api } from '../api'
import type { Actor, Paged, ProductMedia } from '../types'
import PosterCard from '../components/PosterCard.vue'
import PaginationBar from '../components/PaginationBar.vue'

const route = useRoute(); const router = useRouter(); const message = useMessage(); const loading = ref(true); const changing = ref(false); const actor = ref<Actor | null>(null); const page = ref(1); const pageSize = ref(20); const media = ref<Paged<ProductMedia>>({ items: [], total: 0, page: 1, pageSize: 20, totalPages: 0 })
const id = computed(() => Number(route.params.id))
async function load() { try { const result = await api.actorDetail(id.value, page.value, pageSize.value); actor.value = result.actor; media.value = result.media } catch (reason) { message.error(reason instanceof Error ? reason.message : '演员详情加载失败') } finally { loading.value = false } }
function changePage(value: number) { page.value = value; load() }
function changePageSize(value: number) { pageSize.value = value; page.value = 1; load() }
async function toggleFollow() { if (!actor.value) return; changing.value = true; try { actor.value = await api.followActor(id.value, !actor.value.followed); message.success(actor.value.followed ? '已关注，自动化可使用此演员' : '已取消关注') } catch (reason) { message.error(reason instanceof Error ? reason.message : '操作失败') } finally { changing.value = false } }
onMounted(load)
</script>
<template><n-spin :show="loading"><div v-if="actor" class="actor-detail-page"><button class="back-link" @click="router.back()"><IconArrowLeft :size="16" />返回</button><section class="actor-detail-head"><span class="actor-detail-avatar"><img v-if="actor.avatarUrl" :src="actor.avatarUrl" :alt="actor.name"><IconUser v-else :size="44" /></span><div><span class="eyebrow">ACTOR</span><h1>{{ actor.name }}</h1><p>{{ actor.mediaCount }} 部已关联作品<span v-if="actor.aliases.length"> · {{ actor.aliases.join('、') }}</span></p><n-button :type="actor.followed ? 'default' : 'primary'" :loading="changing" @click="toggleFollow"><template #icon><IconBellOff v-if="actor.followed" /><IconBell v-else /></template>{{ actor.followed ? '取消关注' : '关注演员' }}</n-button></div></section><section class="detail-section"><header class="product-section-head"><div><span class="eyebrow">FILMOGRAPHY</span><h2>作品</h2></div></header><div v-if="media.items.length" class="poster-grid"><PosterCard v-for="item in media.items" :key="item.id" :title="item.title" :code="item.code" :poster-url="item.posterUrl" :to="`/media/${item.id}`" /></div><div v-else class="quiet-empty"><IconUser :size="28" /><strong>尚未关联作品</strong><span>后续来源刷新会把同名演员的作品聚合到这里。</span></div><PaginationBar :page="page" :page-size="pageSize" :total="media.total" @update:page="changePage" @update:page-size="changePageSize" /></section></div></n-spin></template>

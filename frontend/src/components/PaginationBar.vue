<script setup lang="ts">
import { NPagination } from 'naive-ui'

defineProps<{
  page: number
  pageSize: number
  total: number
  pageSizes?: number[]
}>()

const emit = defineEmits<{
  (e: 'update:page', value: number): void
  (e: 'update:pageSize', value: number): void
}>()
</script>

<template>
  <n-pagination
    v-if="total > 0"
    class="pagination-bar"
    :page="page"
    :page-size="pageSize"
    :item-count="total"
    :page-sizes="pageSizes ?? [10, 20, 50]"
    show-size-picker
    @update:page="emit('update:page', $event)"
    @update:page-size="emit('update:pageSize', $event)"
  />
</template>

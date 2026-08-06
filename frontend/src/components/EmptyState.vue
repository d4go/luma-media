<script setup lang="ts">
import { IconDatabaseOff } from '@tabler/icons-vue'
import { NIcon } from 'naive-ui'

defineProps<{ title: string; description: string }>()
</script>

<template>
  <div class="empty-state">
    <n-icon :size="34" :depth="3"><IconDatabaseOff /></n-icon>
    <strong>{{ title }}</strong>
    <span>{{ description }}</span>
    <slot />
  </div>
</template>

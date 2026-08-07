<script setup lang="ts">
import { ref } from 'vue'
import { IconMovie } from '@tabler/icons-vue'
defineProps<{ title: string; code: string; posterUrl?: string | null; subtitle?: string; to: string }>()
const failed = ref(false)
</script>
<template>
  <RouterLink :to="to" class="poster-card">
    <div class="poster-frame">
      <img v-if="posterUrl && !failed" :src="posterUrl" :alt="`${title} 海报`" loading="lazy" @error="failed = true" />
      <div v-else class="poster-fallback"><IconMovie :size="34" /><span>{{ code.slice(0, 2).toUpperCase() || 'LU' }}</span></div>
    </div>
    <div class="poster-copy"><strong>{{ title }}</strong><span>{{ code }}</span><small v-if="subtitle">{{ subtitle }}</small></div>
  </RouterLink>
</template>

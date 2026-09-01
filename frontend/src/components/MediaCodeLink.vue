<script setup lang="ts">
import { computed } from 'vue'
import { useRouter } from 'vue-router'

const props = defineProps<{
  code?: string | null
  mediaId?: number | null
  fallbackToSearch?: boolean
}>()

const router = useRouter()
const text = computed(() => (props.code ?? '').trim().toUpperCase() || '番号待识别')
const clickable = computed(() => Boolean(props.mediaId) || (props.fallbackToSearch && Boolean((props.code ?? '').trim())))

function go(event: MouseEvent | KeyboardEvent) {
  if (!clickable.value) return
  event.preventDefault()
  event.stopPropagation()
  if (props.mediaId) {
    router.push(`/media/${props.mediaId}`)
  } else if (props.fallbackToSearch) {
    router.push({ path: '/resources', query: { q: text.value } })
  }
}
</script>

<template>
  <span
    class="media-code-link"
    :class="{ 'is-link': clickable }"
    :role="clickable ? 'link' : undefined"
    :tabindex="clickable ? 0 : undefined"
    @click="go"
    @keydown.enter="go"
    @keydown.space.prevent="go"
  >{{ text }}</span>
</template>
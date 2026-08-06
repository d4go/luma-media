<script setup lang="ts">
import { h, onMounted, reactive, ref } from 'vue'
import {
  NButton, NDataTable, NForm, NFormItem, NInput, NModal, NSelect, NSkeleton, NSwitch, NTag,
  type DataTableColumns, type FormInst, type FormRules, useDialog, useMessage,
} from 'naive-ui'
import { IconPencil, IconPlus, IconRefresh, IconTrash } from '@tabler/icons-vue'
import PageHeader from '../components/PageHeader.vue'
import EmptyState from '../components/EmptyState.vue'
import { api } from '../api'
import type { Folder, FolderInput } from '../types'

const message = useMessage()
const dialog = useDialog()
const loading = ref(true)
const saving = ref(false)
const folders = ref<Folder[]>([])
const modalOpen = ref(false)
const editingId = ref<number | null>(null)
const formRef = ref<FormInst | null>(null)
const form = reactive<FolderInput>({ name: '', path: '', type: 'movie', outputFormat: 'nfo', scanMode: 'manual', enabled: true })
const rules: FormRules = {
  name: { required: true, message: '请输入目录名称', trigger: ['blur', 'input'] },
  path: { required: true, message: '请输入服务器上的绝对路径', trigger: ['blur', 'input'] },
}

const mediaTypeLabel = { movie: '电影', tv: '剧集', mixed: '混合' }
const scanModeLabel = { manual: '手动', watch: '目录监听', interval: '定时扫描' }

const columns: DataTableColumns<Folder> = [
  { title: '名称', key: 'name', minWidth: 130, render: (row) => h('strong', row.name) },
  { title: '路径', key: 'path', minWidth: 260, render: (row) => h('div', { class: 'path-cell', title: row.path }, row.path) },
  { title: '类型', key: 'mediaType', width: 90, render: (row) => mediaTypeLabel[row.mediaType] },
  { title: '扫描模式', key: 'scanMode', width: 110, render: (row) => scanModeLabel[row.scanMode] },
  { title: '状态', key: 'enabled', width: 90, render: (row) => h(NTag, { type: row.enabled ? 'success' : 'default', bordered: false, size: 'small' }, { default: () => row.enabled ? '已启用' : '已停用' }) },
  {
    title: '操作', key: 'actions', width: 170, align: 'right',
    render: (row) => h('div', { class: 'row-actions' }, [
      h(NButton, { size: 'small', secondary: true, disabled: !row.enabled, onClick: () => scan(row) }, { icon: () => h(IconRefresh), default: () => '扫描' }),
      h(NButton, { size: 'small', quaternary: true, 'aria-label': `编辑 ${row.name}`, onClick: () => openEdit(row) }, { icon: () => h(IconPencil) }),
      h(NButton, { size: 'small', quaternary: true, type: 'error', 'aria-label': `删除 ${row.name}`, onClick: () => confirmDelete(row) }, { icon: () => h(IconTrash) }),
    ]),
  },
]

async function load() {
  loading.value = true
  try { folders.value = await api.folders() }
  catch (reason) { message.error(reason instanceof Error ? reason.message : '目录加载失败') }
  finally { loading.value = false }
}

function resetForm() {
  Object.assign(form, { name: '', path: '', type: 'movie', outputFormat: 'nfo', scanMode: 'manual', enabled: true })
  editingId.value = null
}

function openCreate() { resetForm(); modalOpen.value = true }
function openEdit(folder: Folder) {
  editingId.value = folder.id
  Object.assign(form, { name: folder.name, path: folder.path, type: folder.mediaType, outputFormat: folder.outputFormat, scanMode: folder.scanMode, enabled: folder.enabled })
  modalOpen.value = true
}

async function save() {
  await formRef.value?.validate()
  saving.value = true
  try {
    if (editingId.value) await api.updateFolder(editingId.value, { ...form })
    else await api.createFolder({ ...form })
    message.success(editingId.value ? '目录已更新' : '目录已添加')
    modalOpen.value = false
    await load()
  } catch (reason) { message.error(reason instanceof Error ? reason.message : '保存失败') }
  finally { saving.value = false }
}

async function scan(folder: Folder) {
  try { await api.scanFolder(folder.id); message.success(`已开始扫描 ${folder.name}`) }
  catch (reason) { message.error(reason instanceof Error ? reason.message : '无法开始扫描') }
}

function confirmDelete(folder: Folder) {
  dialog.warning({
    title: '删除媒体目录',
    content: `确定删除“${folder.name}”吗？已索引的媒体不会从磁盘删除。`,
    positiveText: '删除', negativeText: '取消',
    onPositiveClick: async () => {
      try { await api.deleteFolder(folder.id); message.success('目录已删除'); await load() }
      catch (reason) { message.error(reason instanceof Error ? reason.message : '删除失败') }
    },
  })
}

onMounted(load)
</script>

<template>
  <PageHeader title="媒体目录" description="配置服务器媒体路径、内容类型与扫描方式。">
    <n-button type="primary" @click="openCreate"><template #icon><IconPlus /></template>添加目录</n-button>
  </PageHeader>

  <section class="panel">
    <div v-if="loading" style="padding: 20px"><n-skeleton text :repeat="7" /></div>
    <EmptyState v-else-if="!folders.length" title="还没有媒体目录" description="添加一个服务器路径，Luma Media 就可以开始建立索引。">
      <n-button type="primary" @click="openCreate"><template #icon><IconPlus /></template>添加目录</n-button>
    </EmptyState>
    <div v-else class="table-wrap"><n-data-table :columns="columns" :data="folders" :bordered="false" :single-line="false" /></div>
  </section>

  <n-modal v-model:show="modalOpen" preset="card" :title="editingId ? '编辑媒体目录' : '添加媒体目录'" style="width: min(620px, calc(100vw - 32px))" :bordered="false">
    <n-form ref="formRef" :model="form" :rules="rules" label-placement="top">
      <div class="form-grid">
        <n-form-item label="目录名称" path="name"><n-input v-model:value="form.name" placeholder="例如：电影" /></n-form-item>
        <n-form-item label="媒体类型" path="type"><n-select v-model:value="form.type" :options="[{label:'电影',value:'movie'},{label:'剧集',value:'tv'},{label:'混合',value:'mixed'}]" /></n-form-item>
        <n-form-item class="wide" label="服务器路径" path="path"><n-input v-model:value="form.path" placeholder="/media/movies" /></n-form-item>
        <n-form-item label="扫描模式" path="scanMode"><n-select v-model:value="form.scanMode" :options="[{label:'手动',value:'manual'},{label:'目录监听',value:'watch'},{label:'定时扫描',value:'interval'}]" /></n-form-item>
        <n-form-item label="输出格式" path="outputFormat"><n-select v-model:value="form.outputFormat" :options="[{label:'NFO',value:'nfo'},{label:'JSON',value:'json'},{label:'NFO + JSON',value:'both'}]" /></n-form-item>
        <n-form-item label="启用目录"><NSwitch v-model:value="form.enabled" /></n-form-item>
      </div>
    </n-form>
    <template #footer><div style="display:flex;justify-content:flex-end;gap:10px"><n-button @click="modalOpen=false">取消</n-button><n-button type="primary" :loading="saving" @click="save">保存目录</n-button></div></template>
  </n-modal>
</template>

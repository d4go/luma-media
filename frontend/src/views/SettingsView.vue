<script setup lang="ts">
import { onMounted, reactive, ref } from 'vue'
import {
  NButton, NForm, NFormItem, NInput, NInputNumber, NSelect, NSpin,
  type FormInst, type FormRules, useMessage,
} from 'naive-ui'
import { IconDeviceFloppy, IconPlugConnected } from '@tabler/icons-vue'
import PageHeader from '../components/PageHeader.vue'
import { api } from '../api'
import type { Settings } from '../types'

const message = useMessage()
const loading = ref(true)
const saving = ref(false)
const testing = ref(false)
const formRef = ref<FormInst | null>(null)
const form = reactive<Settings>({ metatubeUrl: '', metatubeToken: '', outputFormat: 'nfo', scanInterval: 60, overwritePolicy: 'missing', logLevel: 'info' })
const rules: FormRules = {
  metatubeUrl: [
    { required: true, message: '请输入 MetaTube 地址', trigger: ['blur', 'input'] },
    { pattern: /^https?:\/\//, message: '地址需要以 http:// 或 https:// 开头', trigger: 'blur' },
  ],
  scanInterval: { type: 'number', min: 1, required: true, message: '扫描间隔必须大于 0', trigger: ['blur', 'change'] },
}

async function load() {
  loading.value = true
  try { Object.assign(form, await api.settings()) }
  catch (reason) { message.error(reason instanceof Error ? reason.message : '设置加载失败') }
  finally { loading.value = false }
}
async function save() {
  await formRef.value?.validate()
  saving.value = true
  try { Object.assign(form, await api.updateSettings({ ...form })); message.success('设置已保存') }
  catch (reason) { message.error(reason instanceof Error ? reason.message : '保存失败') }
  finally { saving.value = false }
}
async function testConnection() {
  await formRef.value?.validate()
  testing.value = true
  try {
    const result = await api.testMetaTube({ ...form })
    message.success(`连接成功，发现 ${result.providerCount} 个影片数据源`)
  } catch (reason) { message.error(reason instanceof Error ? reason.message : 'MetaTube 连接失败') }
  finally { testing.value = false }
}
onMounted(load)
</script>

<template>
  <PageHeader title="系统设置" description="管理 MetaTube 连接、元数据输出与后台扫描策略。" />
  <n-spin :show="loading">
    <div class="settings-layout">
      <section class="panel settings-form">
        <n-form ref="formRef" :model="form" :rules="rules" label-placement="top">
          <n-form-item label="MetaTube 地址" path="metatubeUrl">
            <n-input v-model:value="form.metatubeUrl" placeholder="http://192.168.1.20:8080" />
            <template #feedback>容器部署时请填写 NAS 局域网地址或同一 Docker 网络内的服务名。</template>
          </n-form-item>
          <n-form-item label="访问 Token" path="metatubeToken">
            <n-input v-model:value="form.metatubeToken" type="password" show-password-on="click" placeholder="未启用 Token 时留空" />
          </n-form-item>
          <div class="form-grid">
            <n-form-item label="默认输出格式" path="outputFormat"><n-select v-model:value="form.outputFormat" :options="[{label:'NFO',value:'nfo'},{label:'JSON',value:'json'},{label:'NFO + JSON',value:'both'}]" /></n-form-item>
            <n-form-item label="扫描间隔（分钟）" path="scanInterval"><n-input-number v-model:value="form.scanInterval" :min="1" :max="10080" style="width:100%" /></n-form-item>
            <n-form-item label="覆盖策略" path="overwritePolicy"><n-select v-model:value="form.overwritePolicy" :options="[{label:'仅补充缺失项',value:'missing'},{label:'始终覆盖',value:'always'},{label:'从不覆盖',value:'never'}]" /></n-form-item>
            <n-form-item label="日志级别" path="logLevel"><n-select v-model:value="form.logLevel" :options="[{label:'Debug',value:'debug'},{label:'Info',value:'info'},{label:'Warning',value:'warn'},{label:'Error',value:'error'}]" /></n-form-item>
          </div>
          <div style="display:flex;gap:10px;flex-wrap:wrap">
            <n-button secondary :loading="testing" @click="testConnection"><template #icon><IconPlugConnected /></template>测试连接</n-button>
            <n-button type="primary" :loading="saving" @click="save"><template #icon><IconDeviceFloppy /></template>保存设置</n-button>
          </div>
        </n-form>
      </section>
      <aside class="panel settings-note">
        <h2>配置说明</h2>
        <p>你的 MetaTube 未启用 Token，可以留空。连接成功后，媒体刮削会调用 MetaTube 搜索与详情接口，并将完整元数据保存到数据库。</p>
      </aside>
    </div>
  </n-spin>
</template>

<script setup lang="ts">
import { computed, inject, onMounted, reactive, ref } from 'vue'
import {
  NAlert, NButton, NForm, NFormItem, NInput, NInputNumber, NSelect, NSpin, NSwitch,
  type FormInst, type FormRules, useMessage,
} from 'naive-ui'
import {
  IconArrowBackUp, IconCheck, IconDatabase, IconDeviceFloppy, IconPlugConnected,
  IconRefresh, IconServer,
} from '@tabler/icons-vue'
import PageHeader from '../components/PageHeader.vue'
import { api } from '../api'
import { serviceStatusKey } from '../service-status'
import type { Settings } from '../types'

type TestResult = { type: 'success' | 'error'; message: string } | null

const message = useMessage()
const serviceContext = inject(serviceStatusKey)
const loading = ref(true)
const saving = ref(false)
const testingMetaTube = ref(false)
const testingQbit = ref(false)
const metaTubeResult = ref<TestResult>(null)
const qbitResult = ref<TestResult>(null)
const initialSnapshot = ref('')
const metaTubeFormRef = ref<FormInst | null>(null)
const qbitFormRef = ref<FormInst | null>(null)
const form = reactive<Settings>({
  metatubeUrl: '', metatubeToken: '', outputFormat: 'nfo', scanInterval: 60,
  overwritePolicy: 'missing', logLevel: 'info', qbittorrentUrl: 'http://127.0.0.1:8080',
  qbittorrentUsername: 'admin', qbittorrentPassword: '', qbittorrentAutoUpdateTrackers: false,
  qbittorrentTrackerSourceUrl: 'https://raw.githubusercontent.com/ngosang/trackerslist/master/trackers_best.txt',
  qbittorrentTrackerUpdateInterval: 1440,
})

const urlRule = (label: string) => [
  { required: true, message: `请输入${label}`, trigger: ['blur', 'input'] },
  { pattern: /^https?:\/\//, message: '地址需要以 http:// 或 https:// 开头', trigger: ['blur', 'input'] },
]
const metaTubeRules: FormRules = {
  metatubeUrl: urlRule(' MetaTube 地址'),
  scanInterval: { type: 'number', min: 1, required: true, message: '扫描间隔必须大于 0', trigger: ['blur', 'change'] },
}
const qbitRules: FormRules = {
  qbittorrentUrl: urlRule(' qBittorrent 地址'),
  qbittorrentTrackerUpdateInterval: { type: 'number', min: 1, required: true, message: '更新间隔必须大于 0', trigger: ['blur', 'change'] },
  qbittorrentTrackerSourceUrl: {
    validator: (_rule, value: string) => !form.qbittorrentAutoUpdateTrackers || /^https?:\/\//.test(value),
    message: '请输入有效的 Tracker 列表地址', trigger: ['blur', 'input'],
  },
}
const dirty = computed(() => !loading.value && JSON.stringify(form) !== initialSnapshot.value)

async function load() {
  loading.value = true
  try {
    Object.assign(form, await api.settings())
    initialSnapshot.value = JSON.stringify(form)
    metaTubeResult.value = null
    qbitResult.value = null
  } catch (reason) {
    message.error(reason instanceof Error ? reason.message : '设置加载失败')
  } finally {
    loading.value = false
  }
}

async function validateAll() {
  try {
    await Promise.all([
      metaTubeFormRef.value?.validate(), qbitFormRef.value?.validate(),
    ])
    return true
  } catch {
    message.warning('请先修正标记的配置项')
    return false
  }
}

async function save() {
  if (!await validateAll()) return
  saving.value = true
  try {
    Object.assign(form, await api.updateSettings({ ...form }))
    initialSnapshot.value = JSON.stringify(form)
    message.success('设置已保存')
    await serviceContext?.refresh()
  } catch (reason) {
    message.error(reason instanceof Error ? reason.message : '保存失败')
  } finally {
    saving.value = false
  }
}

function restore() {
  if (!initialSnapshot.value) return
  Object.assign(form, JSON.parse(initialSnapshot.value))
  metaTubeResult.value = null
  qbitResult.value = null
}

async function testMetaTube() {
  try { await metaTubeFormRef.value?.validate() } catch { return }
  testingMetaTube.value = true
  metaTubeResult.value = null
  try {
    const result = await api.testMetaTube({ ...form })
    metaTubeResult.value = { type: 'success', message: `连接成功，发现 ${result.providerCount} 个影片数据源。` }
    await serviceContext?.refresh()
  } catch (reason) {
    metaTubeResult.value = { type: 'error', message: reason instanceof Error ? reason.message : 'MetaTube 连接失败' }
  } finally {
    testingMetaTube.value = false
  }
}

async function testQbit() {
  try { await qbitFormRef.value?.validate() } catch { return }
  testingQbit.value = true
  qbitResult.value = null
  try {
    const result = await api.testQBittorrent({ ...form })
    qbitResult.value = { type: 'success', message: `连接成功，版本 ${result.version}。` }
    await serviceContext?.refresh()
  } catch (reason) {
    qbitResult.value = { type: 'error', message: reason instanceof Error ? reason.message : 'qBittorrent 连接失败' }
  } finally {
    testingQbit.value = false
  }
}

onMounted(load)
</script>

<template>
  <PageHeader title="系统设置" description="每个外部服务独立验证；只有保存后，后台任务才会使用新的配置。">
    <span v-if="dirty" class="unsaved-mark">有未保存更改</span>
    <n-button secondary :disabled="!dirty" @click="restore"><template #icon><IconArrowBackUp /></template>撤销</n-button>
    <n-button type="primary" :loading="saving" :disabled="!dirty" @click="save"><template #icon><IconDeviceFloppy /></template>保存设置</n-button>
  </PageHeader>

  <n-spin :show="loading">
    <div class="settings-workspace">
      <div class="settings-stack">
        <section class="panel setting-section">
          <header class="setting-section-head">
            <span class="setting-icon"><IconDatabase /></span>
            <div><h2>MetaTube</h2><p>用于检索影片信息、海报和 NFO 元数据。</p></div>
            <span class="section-state" :class="{ online: serviceContext?.status.value?.metaTube.connected }">
              {{ serviceContext?.status.value?.metaTube.connected ? '已连接' : '未连接' }}
            </span>
          </header>
          <n-form ref="metaTubeFormRef" :model="form" :rules="metaTubeRules" label-placement="top">
            <div class="form-grid">
              <n-form-item label="服务地址" path="metatubeUrl">
                <n-input v-model:value="form.metatubeUrl" placeholder="http://192.168.1.20:8080" />
              </n-form-item>
              <n-form-item label="访问 Token" path="metatubeToken">
                <n-input v-model:value="form.metatubeToken" type="password" show-password-on="click" placeholder="未启用 Token 时留空" />
              </n-form-item>
            </div>
            <div class="setting-subsection">
              <div class="setting-subsection-head">
                <strong>元数据处理规则</strong>
                <span>控制 MetaTube 元数据的输出、刷新周期和已有文件处理方式。</span>
              </div>
              <div class="form-grid">
                <n-form-item label="默认输出格式" path="outputFormat"><n-select v-model:value="form.outputFormat" :options="[{label:'NFO',value:'nfo'},{label:'JSON',value:'json'},{label:'NFO + JSON',value:'both'}]" /></n-form-item>
                <n-form-item label="扫描间隔（分钟）" path="scanInterval"><n-input-number v-model:value="form.scanInterval" :min="1" :max="10080" style="width:100%" /></n-form-item>
                <n-form-item label="覆盖策略" path="overwritePolicy"><n-select v-model:value="form.overwritePolicy" :options="[{label:'仅补充缺失项',value:'missing'},{label:'始终覆盖',value:'always'},{label:'从不覆盖',value:'never'}]" /></n-form-item>
                <n-form-item label="日志级别" path="logLevel"><n-select v-model:value="form.logLevel" :options="[{label:'Debug',value:'debug'},{label:'Info',value:'info'},{label:'Warning',value:'warn'},{label:'Error',value:'error'}]" /></n-form-item>
              </div>
            </div>
          </n-form>
          <div class="setting-section-actions">
            <n-button secondary :loading="testingMetaTube" @click="testMetaTube"><template #icon><IconPlugConnected /></template>测试 MetaTube</n-button>
            <n-alert v-if="metaTubeResult" :type="metaTubeResult.type" :show-icon="true">{{ metaTubeResult.message }}</n-alert>
          </div>
        </section>

        <section class="panel setting-section">
          <header class="setting-section-head">
            <span class="setting-icon"><IconServer /></span>
            <div><h2>qBittorrent</h2><p>接收爬虫结果，并按计划更新现有下载的 Tracker。</p></div>
            <span class="section-state" :class="{ online: serviceContext?.status.value?.qbittorrent.connected }">
              {{ serviceContext?.status.value?.qbittorrent.connected ? '已连接' : '未连接' }}
            </span>
          </header>
          <n-form ref="qbitFormRef" :model="form" :rules="qbitRules" label-placement="top">
            <n-form-item label="Web UI 地址" path="qbittorrentUrl">
              <n-input v-model:value="form.qbittorrentUrl" placeholder="http://192.168.1.20:8080" />
            </n-form-item>
            <div class="form-grid">
              <n-form-item label="用户名" path="qbittorrentUsername"><n-input v-model:value="form.qbittorrentUsername" placeholder="admin" /></n-form-item>
              <n-form-item label="密码" path="qbittorrentPassword"><n-input v-model:value="form.qbittorrentPassword" type="password" show-password-on="click" placeholder="qBittorrent 密码" /></n-form-item>
            </div>
            <div class="tracker-control">
              <div><strong>自动更新 Tracker</strong><span>按固定周期获取列表，并追加到 qBittorrent 中的现有任务。</span></div>
              <n-switch v-model:value="form.qbittorrentAutoUpdateTrackers" class="tracker-switch" />
            </div>
            <div v-if="form.qbittorrentAutoUpdateTrackers" class="tracker-fields">
              <n-form-item label="Tracker 列表地址" path="qbittorrentTrackerSourceUrl">
                <n-input v-model:value="form.qbittorrentTrackerSourceUrl" placeholder="https://.../trackers_best.txt" />
              </n-form-item>
              <n-form-item label="更新间隔（分钟）" path="qbittorrentTrackerUpdateInterval">
                <n-input-number v-model:value="form.qbittorrentTrackerUpdateInterval" :min="1" :max="10080" style="width:100%" />
              </n-form-item>
            </div>
          </n-form>
          <div class="setting-section-actions">
            <n-button secondary :loading="testingQbit" @click="testQbit"><template #icon><IconPlugConnected /></template>测试 qBittorrent</n-button>
            <n-alert v-if="qbitResult" :type="qbitResult.type" :show-icon="true">{{ qbitResult.message }}</n-alert>
          </div>
        </section>

      </div>

      <aside class="settings-rail">
        <section class="panel settings-note">
          <span class="section-kicker">DEPLOYMENT NOTE</span>
          <h2>容器访问地址</h2>
          <p>MetaTube 默认取当前部署机器的 IP 与 8080 端口。手动填写时，请使用 Luma 容器能够访问的地址，不要误用浏览器所在设备的 localhost。</p>
        </section>
        <section class="settings-summary">
          <div><IconCheck :size="17" /><span>连接测试不会保存配置</span></div>
          <div><IconRefresh :size="17" /><span>侧栏状态每 10 秒刷新</span></div>
          <div><IconDeviceFloppy :size="17" /><span>保存后后台任务立即读取新值</span></div>
        </section>
      </aside>
    </div>
  </n-spin>
</template>

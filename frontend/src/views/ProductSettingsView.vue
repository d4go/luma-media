<script setup lang="ts">
import { computed, onMounted, onUnmounted, reactive, ref } from 'vue'
import {
  NAlert,
  NButton,
  NFormItem,
  NInput,
  NInputNumber,
  NSelect,
  NSpin,
  NSwitch,
  useDialog,
  useMessage,
} from 'naive-ui'
import {
  IconBrandPython,
  IconCheck,
  IconDatabase,
  IconDeviceFloppy,
  IconDownload,
  IconFolder,
  IconPlus,
  IconPlugConnected,
  IconRefresh,
  IconServer,
  IconTrash,
} from '@tabler/icons-vue'
import { api } from '../api'
import { formatDate } from '../format'
import type { ProductSettings, ProviderConfig, Settings } from '../types'
import PageHeader from '../components/PageHeader.vue'

const message = useMessage()
const dialog = useDialog()
const loading = ref(true)
const saving = ref(false)
const creatingSource = ref(false)
const showNewSource = ref(false)
const testing = ref('')
const syncing = ref('')
const providers = ref<ProviderConfig[]>([])
const secrets = reactive<Record<string, string>>({})
const testMessages = reactive<Record<string, { ok: boolean; text: string }>>({})
const sourceAdapterOptions = [
  { label: 'Jav321（当前可直连）', value: 'jav321', name: 'Jav321', baseUrl: 'https://www.jav321.com', hint: '搜索番号时可直接返回作品信息和磁力资源。' },
  { label: 'JavDB', value: 'javdb', name: 'JavDB', baseUrl: 'https://javdb.com', hint: '支持官方站和镜像；Cloudflare 环境需填写 cf_clearance Cookie。' },
  { label: 'JavBus', value: 'javbus', name: 'JavBus', baseUrl: 'https://www.javbus.com', hint: '支持主站、反代和多个镜像；年龄验证站点需 Cookie。' },
  { label: 'JavLibrary', value: 'javlibrary', name: 'JavLibrary', baseUrl: 'https://www.javlibrary.com', hint: '适合作为补充元数据来源，官方站可能需要 Cloudflare Cookie。' },
]
const newSource = reactive({ adapter: 'jav321', displayName: 'Jav321', baseUrl: 'https://www.jav321.com', secret: '', config: { proxyUrl: '', userAgent: '', syncEnabled: true, syncIntervalMinutes: 1440, syncDetailLimit: 8 } })
let syncPollTimer: ReturnType<typeof setTimeout> | undefined

const legacy = reactive<Settings>({
  metatubeUrl: '',
  metatubeToken: '',
  outputFormat: 'nfo',
  scanInterval: 60,
  overwritePolicy: 'missing',
  logLevel: 'info',
  qbittorrentUrl: '',
  qbittorrentUsername: 'admin',
  qbittorrentPassword: '',
  qbittorrentAutoUpdateTrackers: false,
  qbittorrentTrackerSourceUrl: '',
  qbittorrentTrackerUpdateInterval: 1440,
})
const product = reactive<ProductSettings>({
  downloadRoot: '/downloads',
  mediaRoot: '/media',
  qbittorrentSavePath: '/downloads',
  qbittorrentCategory: 'luma',
  qbittorrentTags: 'luma',
  organizerMode: 'hardlink',
  organizerMovieTemplate: '{code}/{code}.{ext}',
  organizerConflictPolicy: 'attention',
})

const providerMap = computed(() => Object.fromEntries(providers.value.map(item => [item.key, item])))
const sourceProviders = computed(() => providers.value.filter(item => item.type === 'source'))
const selectedSourceAdapter = computed(() => sourceAdapterOptions.find(item => item.value === newSource.adapter) ?? sourceAdapterOptions[0])

function sourceAdapter(provider: ProviderConfig) {
  return String(provider.config.adapter ?? provider.key.split('-')[0] ?? 'source')
}

function sourceAdapterName(provider: ProviderConfig) {
  const adapter = sourceAdapter(provider)
  return sourceAdapterOptions.find(item => item.value === adapter)?.name ?? adapter
}

function sourceConfigText(provider: ProviderConfig, key: 'proxyUrl' | 'userAgent') {
  return typeof provider.config[key] === 'string' ? String(provider.config[key]) : ''
}

function updateSourceConfig(provider: ProviderConfig, key: 'proxyUrl' | 'userAgent', value: string) {
  provider.config = { ...provider.config, [key]: value }
}

function sourceSyncEnabled(provider: ProviderConfig) {
  return typeof provider.config.syncEnabled === 'boolean' ? provider.config.syncEnabled : true
}

function sourceSyncInterval(provider: ProviderConfig) {
  const value = Number(provider.config.syncIntervalMinutes)
  return Number.isFinite(value) ? value : 1440
}

function updateSourceSyncEnabled(provider: ProviderConfig, value: boolean) {
  provider.config = { ...provider.config, syncEnabled: value }
}

function updateSourceSyncInterval(provider: ProviderConfig, value: number | null) {
  provider.config = { ...provider.config, syncIntervalMinutes: value ?? 1440 }
}

function sourceSyncDetailLimit(provider: ProviderConfig) {
  const value = Number(provider.config.syncDetailLimit)
  return Number.isFinite(value) ? value : 8
}

function updateSourceSyncDetailLimit(provider: ProviderConfig, value: number | null) {
  provider.config = { ...provider.config, syncDetailLimit: value ?? 8 }
}

const syncStatusLabel: Record<ProviderConfig['syncStatus'], string> = {
  idle: '等待同步',
  running: '同步中',
  success: '同步成功',
  failed: '同步失败',
}

function syncDate(value: string | null, fallback: string) {
  return value ? formatDate(value) : fallback
}

function selectSourceAdapter(value: string) {
  const adapter = sourceAdapterOptions.find(item => item.value === value)
  if (!adapter) return
  newSource.adapter = adapter.value
  newSource.displayName = adapter.name
  newSource.baseUrl = adapter.baseUrl
  newSource.secret = ''
  newSource.config = { proxyUrl: '', userAgent: '', syncEnabled: true, syncIntervalMinutes: 1440, syncDetailLimit: 8 }
}

function scheduleSyncPoll() {
  if (syncPollTimer || !sourceProviders.value.some(provider => provider.syncStatus === 'running')) return
  syncPollTimer = setTimeout(async () => {
    syncPollTimer = undefined
    try {
      providers.value = await api.providers()
    } catch {
      // Keep the current status visible and retry while a synchronization is running.
    } finally {
      scheduleSyncPoll()
    }
  }, 2000)
}

async function load() {
  loading.value = true
  try {
    const [providerData, legacyData, productData] = await Promise.all([
      api.providers(),
      api.settings(),
      api.productSettings(),
    ])
    providers.value = providerData
    Object.assign(legacy, legacyData)
    Object.assign(product, productData)
    scheduleSyncPoll()
  } catch (reason) {
    message.error(reason instanceof Error ? reason.message : '设置加载失败')
  } finally {
    loading.value = false
  }
}

async function persistProvider(provider: ProviderConfig) {
  const updated = await api.updateProvider(provider.key, {
    displayName: provider.displayName,
    baseUrl: provider.baseUrl,
    secret: secrets[provider.key] ?? '',
    config: provider.config,
  })
  Object.assign(provider, updated)
  secrets[provider.key] = ''
}

async function save() {
  saving.value = true
  try {
    for (const provider of providers.value) {
      await api.updateProvider(provider.key, {
        displayName: provider.displayName,
        baseUrl: provider.baseUrl,
        secret: secrets[provider.key] ?? '',
        config: provider.config,
      })
    }
    legacy.metatubeUrl = providerMap.value.metatube?.baseUrl ?? legacy.metatubeUrl
    legacy.metatubeToken = secrets.metatube ?? ''
    legacy.qbittorrentUrl = providerMap.value.qbittorrent?.baseUrl ?? legacy.qbittorrentUrl
    legacy.qbittorrentPassword = secrets.qbittorrent ?? ''
    await Promise.all([api.updateSettings({ ...legacy }), api.updateProductSettings({ ...product })])
    message.success('设置已保存，留空的凭据保持不变')
    Object.keys(secrets).forEach(key => { secrets[key] = '' })
    await load()
  } catch (reason) {
    message.error(reason instanceof Error ? reason.message : '保存失败')
  } finally {
    saving.value = false
  }
}

async function addSource() {
  if (!newSource.displayName.trim() || !newSource.baseUrl.trim()) {
    message.warning('请填写来源名称和服务地址')
    return
  }
  creatingSource.value = true
  try {
    const created = await api.createProvider({ ...newSource })
    providers.value.push(created)
    selectSourceAdapter(newSource.adapter)
    showNewSource.value = false
    message.success('来源已添加')
  } catch (reason) {
    message.error(reason instanceof Error ? reason.message : '添加来源失败')
  } finally {
    creatingSource.value = false
  }
}

function removeSource(provider: ProviderConfig) {
  dialog.warning({
    title: '移除来源',
    content: `确定移除“${provider.displayName}”吗？已抓取的媒体和资源记录会保留。`,
    positiveText: '移除',
    negativeText: '取消',
    async onPositiveClick() {
      await api.deleteProvider(provider.key)
      providers.value = providers.value.filter(item => item.key !== provider.key)
      message.success('来源已移除')
    },
  })
}

async function testProvider(key: string) {
  testing.value = key
  delete testMessages[key]
  try {
    const provider = providers.value.find(item => item.key === key)
    if (provider?.type === 'source') {
      await persistProvider(provider)
    }
    const result = await api.testProvider(key)
    testMessages[key] = { ok: result.connected, text: `${result.message} · ${result.latencyMs} ms` }
    await load()
  } catch (reason) {
    testMessages[key] = { ok: false, text: reason instanceof Error ? reason.message : '连接测试失败' }
  } finally {
    testing.value = ''
  }
}

async function syncProvider(provider: ProviderConfig) {
  syncing.value = provider.key
  try {
    await persistProvider(provider)
    Object.assign(provider, await api.syncProvider(provider.key))
    message.success(`${provider.displayName} 已开始后台同步`)
    scheduleSyncPoll()
  } catch (reason) {
    message.error(reason instanceof Error ? reason.message : '来源同步启动失败')
  } finally {
    syncing.value = ''
  }
}

async function toggleProvider(provider: ProviderConfig, value: boolean) {
  try {
    Object.assign(provider, await api.setProviderEnabled(provider.key, value))
  } catch (reason) {
    message.error(reason instanceof Error ? reason.message : '更新失败')
  }
}

onMounted(load)
onUnmounted(() => {
  if (syncPollTimer) clearTimeout(syncPollTimer)
})
</script>

<template>
  <PageHeader title="设置" description="Provider 负责外部能力，来源可以独立扩展、停用与测试，凭据不会返回明文。">
    <n-button type="primary" :loading="saving" @click="save">
      <template #icon><IconDeviceFloppy /></template>
      保存全部
    </n-button>
  </PageHeader>

  <n-spin :show="loading">
    <div class="product-settings">
      <section class="settings-group">
        <header class="settings-group-heading">
          <div>
            <span class="eyebrow">SOURCE PROVIDERS</span>
            <h2>内容来源</h2>
            <p>来源在后台定时采集并去重写入本地索引；前台搜索只查询本地数据，不会因外部站点超时或拒绝访问而中断。</p>
          </div>
          <n-button secondary @click="showNewSource = !showNewSource">
            <template #icon><IconPlus /></template>
            添加来源
          </n-button>
        </header>

        <article v-if="showNewSource" class="new-source-card panel">
          <div class="new-source-intro">
            <span class="provider-icon"><IconPlus /></span>
            <div><strong>添加内容来源</strong><small>同一种适配器可以添加多个镜像，并在后台独立同步</small></div>
          </div>
          <n-form-item class="new-source-adapter" label="适配器">
            <n-select :value="newSource.adapter" :options="sourceAdapterOptions" @update:value="selectSourceAdapter" />
          </n-form-item>
          <n-alert class="new-source-hint" type="info" :show-icon="false">{{ selectedSourceAdapter.hint }}</n-alert>
          <n-form-item class="new-source-name" label="来源名称"><n-input v-model:value="newSource.displayName" /></n-form-item>
          <n-form-item class="new-source-url" label="服务地址"><n-input v-model:value="newSource.baseUrl" :placeholder="selectedSourceAdapter.baseUrl" /></n-form-item>
          <n-form-item class="new-source-cookie" label="Cookie（可选）"><n-input v-model:value="newSource.secret" type="password" show-password-on="click" placeholder="完整复制浏览器 Cookie；普通直连来源可留空" /></n-form-item>
          <div class="new-source-actions">
            <n-button @click="showNewSource = false">取消</n-button>
            <n-button type="primary" :loading="creatingSource" @click="addSource">添加来源</n-button>
          </div>
        </article>

        <div class="provider-card-grid source-provider-grid">
          <article v-for="provider in sourceProviders" :key="provider.key" class="provider-card">
            <header>
              <span class="provider-icon"><IconDatabase /></span>
              <div><strong>{{ provider.displayName }}</strong><small>{{ sourceAdapterName(provider) }} · {{ provider.key }}</small></div>
              <div class="provider-header-actions">
                <n-switch :value="provider.enabled" @update:value="value => toggleProvider(provider, value)" />
                <n-button quaternary circle type="error" title="移除来源" @click="removeSource(provider)">
                  <template #icon><IconTrash /></template>
                </n-button>
              </div>
            </header>
            <n-form-item label="来源名称"><n-input v-model:value="provider.displayName" /></n-form-item>
            <n-form-item label="服务地址"><n-input v-model:value="provider.baseUrl" /></n-form-item>
            <n-form-item label="Cookie">
              <n-input
                v-model:value="secrets[provider.key]"
                type="password"
                show-password-on="click"
                :placeholder="provider.hasSecret ? '已保存，留空保持不变' : 'Cloudflare / 年龄验证站点可填写完整 Cookie'"
              />
            </n-form-item>
            <n-form-item label="代理 URL（可选）">
              <n-input
                :value="sourceConfigText(provider, 'proxyUrl')"
                placeholder="例如 http://192.168.5.1:7890；必须能从 Luma 容器访问"
                @update:value="value => updateSourceConfig(provider, 'proxyUrl', value)"
              />
            </n-form-item>
            <n-form-item label="浏览器 User-Agent（可选）">
              <n-input
                :value="sourceConfigText(provider, 'userAgent')"
                type="textarea"
                :autosize="{ minRows: 2, maxRows: 3 }"
                placeholder="填写获取验证 Cookie 时浏览器的完整 User-Agent，二者必须一致"
                @update:value="value => updateSourceConfig(provider, 'userAgent', value)"
              />
            </n-form-item>
            <div class="source-sync-settings">
              <div class="compact-switch-row">
                <div><strong>每日后台同步</strong><span>按设定间隔更新本地索引，搜索时不直接请求该网站。</span></div>
                <n-switch
                  :value="sourceSyncEnabled(provider)"
                  @update:value="value => updateSourceSyncEnabled(provider, value)"
                />
              </div>
              <n-form-item label="同步间隔（分钟）">
                <n-input-number
                  :value="sourceSyncInterval(provider)"
                  :min="60"
                  :max="10080"
                  :disabled="!sourceSyncEnabled(provider)"
                  style="width: 100%"
                  @update:value="value => updateSourceSyncInterval(provider, value)"
                />
              </n-form-item>
              <n-form-item label="每轮补全详情数">
                <n-input-number
                  :value="sourceSyncDetailLimit(provider)"
                  :min="0"
                  :max="40"
                  :disabled="!sourceSyncEnabled(provider)"
                  style="width: 100%"
                  @update:value="value => updateSourceSyncDetailLimit(provider, value)"
                />
              </n-form-item>
              <div class="source-sync-summary">
                <header>
                  <div><strong>本地索引同步</strong><small>{{ provider.syncLastMessage || '等待第一次同步' }}</small></div>
                  <span class="source-sync-status" :data-status="provider.syncStatus">{{ syncStatusLabel[provider.syncStatus] }}</span>
                </header>
                <dl>
                  <div><dt>最近成功</dt><dd>{{ syncDate(provider.syncLastSuccessAt, '尚未成功') }}</dd></div>
                  <div><dt>下次执行</dt><dd>{{ sourceSyncEnabled(provider) ? syncDate(provider.syncNextRunAt, '等待安排') : '已关闭' }}</dd></div>
                  <div><dt>本轮收录</dt><dd>{{ provider.syncItemCount }} 项</dd></div>
                  <div><dt>数据变化</dt><dd>新增 {{ provider.syncInsertedCount }} · 更新 {{ provider.syncUpdatedCount }}</dd></div>
                  <div><dt>连续失败</dt><dd>{{ provider.syncFailureCount }} 次</dd></div>
                  <div><dt>最近完成</dt><dd>{{ syncDate(provider.syncLastFinishedAt, '尚未执行') }}</dd></div>
                </dl>
                <n-alert v-if="provider.syncStatus === 'failed' && provider.syncLastMessage" type="error" title="最近一次同步失败">
                  {{ provider.syncLastMessage }}
                </n-alert>
              </div>
            </div>
            <n-alert type="info" :show-icon="false">
              403 代表站点已收到请求但拒绝访问，并非断网。Cloudflare Cookie、User-Agent 和出口 IP 必须保持一致；本机 127.0.0.1 代理不能直接给 Docker 容器使用。
            </n-alert>
            <div class="provider-card-actions">
              <n-button secondary :loading="testing === provider.key" @click="testProvider(provider.key)">
                <template #icon><IconPlugConnected /></template>
                测试来源
              </n-button>
              <n-button
                type="primary"
                :loading="syncing === provider.key || provider.syncStatus === 'running'"
                :disabled="!provider.enabled"
                @click="syncProvider(provider)"
              >
                <template #icon><IconRefresh /></template>
                立即同步
              </n-button>
            </div>
            <n-alert v-if="testMessages[provider.key]" :type="testMessages[provider.key].ok ? 'success' : 'error'">
              {{ testMessages[provider.key].text }}
            </n-alert>
          </article>
        </div>
      </section>

      <section class="settings-group">
        <header>
          <span class="eyebrow">SERVICES</span>
          <h2>处理服务</h2>
          <p>元数据和下载服务拥有各自的连接与处理边界。</p>
        </header>
        <div class="provider-card-grid service-provider-grid">
          <article v-if="providerMap.metatube" class="provider-card">
            <header>
              <span class="provider-icon"><IconServer /></span>
              <div><strong>MetaTube</strong><small>Metadata Provider · 元数据与图片</small></div>
              <n-switch :value="providerMap.metatube.enabled" @update:value="value => toggleProvider(providerMap.metatube, value)" />
            </header>
            <n-form-item label="服务地址"><n-input v-model:value="providerMap.metatube.baseUrl" /></n-form-item>
            <n-form-item label="Token"><n-input v-model:value="secrets.metatube" type="password" show-password-on="click" :placeholder="providerMap.metatube.hasSecret ? '已保存，留空保持不变' : '未启用 Token 时留空'" /></n-form-item>
            <div class="provider-subsettings">
              <strong>MetaTube 处理规则</strong>
              <div class="form-grid">
                <n-form-item label="输出格式"><n-select v-model:value="legacy.outputFormat" :options="[{ label: 'NFO', value: 'nfo' }, { label: 'JSON', value: 'json' }, { label: 'NFO + JSON', value: 'both' }]" /></n-form-item>
                <n-form-item label="覆盖策略"><n-select v-model:value="legacy.overwritePolicy" :options="[{ label: '仅补缺失项', value: 'missing' }, { label: '始终覆盖', value: 'always' }, { label: '从不覆盖', value: 'never' }]" /></n-form-item>
              </div>
            </div>
            <n-button secondary :loading="testing === 'metatube'" @click="testProvider('metatube')"><template #icon><IconPlugConnected /></template>测试元数据服务</n-button>
            <n-alert v-if="testMessages.metatube" :type="testMessages.metatube.ok ? 'success' : 'error'">{{ testMessages.metatube.text }}</n-alert>
          </article>

          <article v-if="providerMap.qbittorrent" class="provider-card">
            <header>
              <span class="provider-icon"><IconDownload /></span>
              <div><strong>qBittorrent</strong><small>Download Provider · 下载与 Tracker</small></div>
              <n-switch :value="providerMap.qbittorrent.enabled" @update:value="value => toggleProvider(providerMap.qbittorrent, value)" />
            </header>
            <n-form-item label="Web UI 地址"><n-input v-model:value="providerMap.qbittorrent.baseUrl" /></n-form-item>
            <div class="form-grid">
              <n-form-item label="用户名"><n-input v-model:value="legacy.qbittorrentUsername" /></n-form-item>
              <n-form-item label="密码"><n-input v-model:value="secrets.qbittorrent" type="password" show-password-on="click" :placeholder="providerMap.qbittorrent.hasSecret ? '已保存，留空保持不变' : 'qBittorrent 密码'" /></n-form-item>
            </div>
            <div class="compact-switch-row">
              <div><strong>自动更新 Tracker</strong><span>按周期向现有任务追加 Tracker，不删除原列表。</span></div>
              <n-switch v-model:value="legacy.qbittorrentAutoUpdateTrackers" />
            </div>
            <div v-if="legacy.qbittorrentAutoUpdateTrackers" class="form-grid">
              <n-form-item label="Tracker 列表地址"><n-input v-model:value="legacy.qbittorrentTrackerSourceUrl" /></n-form-item>
              <n-form-item label="间隔（分钟）"><n-input-number v-model:value="legacy.qbittorrentTrackerUpdateInterval" :min="1" style="width: 100%" /></n-form-item>
            </div>
            <n-button secondary :loading="testing === 'qbittorrent'" @click="testProvider('qbittorrent')"><template #icon><IconPlugConnected /></template>测试下载服务</n-button>
            <n-alert v-if="testMessages.qbittorrent" :type="testMessages.qbittorrent.ok ? 'success' : 'error'">{{ testMessages.qbittorrent.text }}</n-alert>
          </article>
        </div>
      </section>

      <section class="settings-group">
        <header><span class="eyebrow">LUMA</span><h2>入库与路径</h2><p>这些路径必须与 Docker 挂载保持一致，文件只能在允许的下载根目录和媒体根目录之间处理。</p></header>
        <div class="luma-settings-card panel">
          <div class="form-grid">
            <n-form-item label="Luma 下载根目录"><n-input v-model:value="product.downloadRoot" /></n-form-item>
            <n-form-item label="qB 保存路径"><n-input v-model:value="product.qbittorrentSavePath" /></n-form-item>
            <n-form-item label="媒体库根目录"><n-input v-model:value="product.mediaRoot" /></n-form-item>
            <n-form-item label="文件模式"><n-select v-model:value="product.organizerMode" :options="[{ label: '优先硬链接，失败时复制', value: 'hardlink' }, { label: '始终复制', value: 'copy' }]" /></n-form-item>
            <n-form-item label="电影命名模板"><n-input v-model:value="product.organizerMovieTemplate" /></n-form-item>
            <n-form-item label="qB 分类与标签"><div class="inline-fields"><n-input v-model:value="product.qbittorrentCategory" placeholder="分类" /><n-input v-model:value="product.qbittorrentTags" placeholder="标签" /></div></n-form-item>
          </div>
          <div class="boundary-note"><IconCheck /><span><strong>冲突不会静默覆盖</strong><small>目标文件已存在、路径无法映射或整理失败时，获取会进入“需要关注”。</small></span></div>
        </div>
      </section>

      <section class="settings-group">
        <header><span class="eyebrow">ADVANCED</span><h2>高级诊断</h2><p>目录监听和可信 Python 脚本保留为高级工具。</p></header>
        <div class="advanced-links">
          <RouterLink to="/settings/legacy/folders"><IconFolder /><span><strong>媒体目录与监听</strong><small>配置 watch、interval 和手动扫描</small></span></RouterLink>
          <RouterLink to="/settings/legacy/crawlers"><IconBrandPython /><span><strong>Python 来源脚本</strong><small>上传、定时执行和查看持久化结果</small></span></RouterLink>
          <RouterLink to="/settings/legacy/tasks"><IconServer /><span><strong>刮削任务记录</strong><small>查看 MetaTube 执行与重试历史</small></span></RouterLink>
        </div>
      </section>
    </div>
  </n-spin>
</template>

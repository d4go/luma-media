import type {
  BatchScrapeResponse, BatchTaskResponse, CrawlerForm, CrawlerResult, CrawlerRun, CrawlerScript,
  DashboardStats, DownloadItem, Folder, FolderInput, MediaItem, MetaTubeConnection, QBittorrentConnection,
  ScrapeOptions, ServiceStatus, Settings, Task, TaskDetail,
  Acquisition, AcquisitionDetail, Actor, AttentionItem, AutomationRule, BrowserSession, CatalogResolveResponse, HomeData, LibraryExportResponse, LibraryItem, LibraryRematchResponse, MediaDetail, Paged, ProductMedia, ProductSettings, ProviderConfig, ProviderDiagnoseResponse, ProviderReparseResponse, ProviderRuntime, ResourceRefreshResponse, SearchResponse, SyncRunResponse,
} from './types'

const API_BASE = import.meta.env.VITE_API_BASE_URL ?? '/api/v1'
const ASSET_BASE = API_BASE.replace(/\/api\/v1\/?$/, '')

export const coverAssetUrl = (mediaId: number) => `${ASSET_BASE}/asset/cover/${mediaId}`
export const productEventUrl = `${API_BASE}/events`

export class ApiError extends Error {
  constructor(message: string, public readonly status: number) {
    super(message)
  }
}

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const isFormData = options?.body instanceof FormData
  const response = await fetch(`${API_BASE}${path}`, {
    ...options,
    headers: isFormData ? options?.headers : {
      'Content-Type': 'application/json',
      ...options?.headers,
    },
  })
  if (!response.ok) {
    const body = await response.json().catch(() => ({ message: '请求失败，请稍后重试' }))
    throw new ApiError(body.message ?? '请求失败，请稍后重试', response.status)
  }
  if (response.status === 204) return undefined as T
  return response.json() as Promise<T>
}

function crawlerForm(input: CrawlerForm) {
  const form = new FormData()
  form.set('name', input.name)
  form.set('websiteUrl', input.websiteUrl)
  form.set('intervalMinutes', String(input.intervalMinutes))
  form.set('enabled', String(input.enabled))
  form.set('autoDownload', String(input.autoDownload))
  if (input.script) form.set('script', input.script)
  return form
}

function pageQuery(page: number, pageSize: number, extra: Record<string, string> = {}) {
  const query = new URLSearchParams(extra)
  query.set('page', String(page))
  query.set('pageSize', String(pageSize))
  return `?${query.toString()}`
}

export const api = {
  home: (page = 1, pageSize = 6) => request<HomeData>(`/home${pageQuery(page, pageSize)}`),
  search: (q: string, page = 1, pageSize = 20) => request<SearchResponse>(`/search${pageQuery(page, pageSize, { q })}`),
  resolveCatalog: (code: string, includeResources = true) => request<CatalogResolveResponse>('/catalog/resolve', { method: 'POST', body: JSON.stringify({ code, includeResources }) }),
  catalogMedia: (q = '', page = 1, pageSize = 20) => request<Paged<ProductMedia>>(`/catalog/media${pageQuery(page, pageSize, q ? { q } : {})}`),
  mediaDetail: (id: number, page = 1, pageSize = 20) => request<MediaDetail>(`/catalog/media/${id}${pageQuery(page, pageSize)}`),
  refreshMediaResources: (id: number) => request<ResourceRefreshResponse>(`/catalog/media/${id}/resources/refresh`, { method: 'POST' }),
  acquireMedia: (mediaId: number, resourceId?: number) => request<Acquisition>(`/catalog/media/${mediaId}/acquire`, { method: 'POST', body: JSON.stringify({ resourceId, requestedBy: 'manual' }) }),
  actorDetail: (id: number, page = 1, pageSize = 20) => request<{ actor: Actor; media: Paged<ProductMedia> }>(`/actors/${id}${pageQuery(page, pageSize)}`),
  followActor: (id: number, followed: boolean) => request<Actor>(`/actors/${id}/follow`, { method: followed ? 'POST' : 'DELETE' }),
  acquisitions: (status = '', page = 1, pageSize = 20) => request<Paged<Acquisition>>(`/acquisitions${pageQuery(page, pageSize, status ? { status } : {})}`),
  acquisition: (id: number, page = 1, pageSize = 20) => request<AcquisitionDetail>(`/acquisitions/${id}${pageQuery(page, pageSize)}`),
  acquisitionAction: (id: number, action: 'pause' | 'resume' | 'retry' | 'cancel') => request<Acquisition>(`/acquisitions/${id}/${action}`, { method: 'POST' }),
  deleteAcquisition: (id: number) => request<void>(`/acquisitions/${id}`, { method: 'DELETE' }),
  attention: (page = 1, pageSize = 20) => request<Paged<AttentionItem>>(`/attention${pageQuery(page, pageSize)}`),
  attentionAction: (id: number, action: string) => request<{ resolved: boolean }>(`/attention/${id}/action`, { method: 'POST', body: JSON.stringify({ action }) }),
  library: (q = '', page = 1, pageSize = 20) => request<Paged<LibraryItem>>(`/library${pageQuery(page, pageSize, q ? { q } : {})}`),
  libraryItem: (id: number) => request<LibraryItem>(`/library/${id}`),
  reorganizeLibrary: (id: number) => request<{ item: LibraryItem; message: string }>(`/library/${id}/reorganize`, { method: 'POST' }),
  regenerateLibraryNfo: (id: number) => request<LibraryExportResponse>(`/library/${id}/nfo`, { method: 'POST' }),
  syncLibraryArtwork: (id: number) => request<LibraryExportResponse>(`/library/${id}/artwork`, { method: 'POST' }),
  rematchLibrary: (id: number, code?: string) => request<LibraryRematchResponse>(`/library/${id}/rematch`, { method: 'POST', body: JSON.stringify(code ? { code } : {}) }),
  automations: (page = 1, pageSize = 20) => request<Paged<AutomationRule>>(`/automations${pageQuery(page, pageSize)}`),
  createAutomation: (input: Omit<AutomationRule, 'id' | 'lastRunAt' | 'nextRunAt' | 'lastStatus' | 'lastExplanation' | 'createdAt'>) => request<AutomationRule>('/automations', { method: 'POST', body: JSON.stringify(input) }),
  updateAutomation: (id: number, input: Partial<AutomationRule>) => request<AutomationRule>(`/automations/${id}`, { method: 'PUT', body: JSON.stringify(input) }),
  deleteAutomation: (id: number) => request<void>(`/automations/${id}`, { method: 'DELETE' }),
  setAutomationEnabled: (id: number, enabled: boolean) => request<AutomationRule>(`/automations/${id}/enabled`, { method: 'POST', body: JSON.stringify({ enabled }) }),
  providers: (page = 1, pageSize = 20) => request<Paged<ProviderConfig>>(`/providers${pageQuery(page, pageSize)}`),
  createProvider: (input: { displayName: string; baseUrl: string; secret?: string; adapter?: string; config?: Record<string, unknown> }) => request<ProviderConfig>('/providers', { method: 'POST', body: JSON.stringify(input) }),
  updateProvider: (key: string, input: { displayName?: string; baseUrl: string; secret?: string; config?: Record<string, unknown> }) => request<ProviderConfig>(`/providers/${key}`, { method: 'PUT', body: JSON.stringify(input) }),
  deleteProvider: (key: string) => request<void>(`/providers/${key}`, { method: 'DELETE' }),
  setProviderEnabled: (key: string, enabled: boolean) => request<ProviderConfig>(`/providers/${key}/enabled`, { method: 'POST', body: JSON.stringify({ enabled }) }),
  testProvider: (key: string) => request<{ connected: boolean; message: string; latencyMs: number }>(`/providers/${key}/test`, { method: 'POST' }),
  syncProvider: (key: string) => request<ProviderConfig>(`/providers/${key}/sync`, { method: 'POST' }),
  syncProviderIncremental: (key: string) => request<SyncRunResponse>(`/providers/${key}/sync/incremental`, { method: 'POST' }),
  bootstrapProvider: (key: string, from: string, to: string, includeResources = true) => request<SyncRunResponse>(`/providers/${key}/bootstrap`, { method: 'POST', body: JSON.stringify({ from, to, includeResources }) }),
  pauseProviderBootstrap: (key: string) => request<SyncRunResponse>(`/providers/${key}/bootstrap/pause`, { method: 'POST' }),
  resumeProviderBootstrap: (key: string) => request<SyncRunResponse>(`/providers/${key}/bootstrap/resume`, { method: 'POST' }),
  reparseProvider: (key: string, parserVersion = '1', limit = 200) => request<ProviderReparseResponse>(`/providers/${key}/reparse`, { method: 'POST', body: JSON.stringify({ parserVersion, limit }) }),
  providerRuntime: (key: string) => request<ProviderRuntime>(`/providers/${key}/runtime`),
  diagnoseProvider: (key: string) => request<ProviderDiagnoseResponse>(`/providers/${key}/diagnose`, { method: 'POST' }),
  startBrowserSession: (key: string) => request<BrowserSession>(`/providers/${key}/browser-session`, { method: 'POST' }),
  browserSession: (key: string, sessionId: string) => request<BrowserSession>(`/providers/${key}/browser-session/${sessionId}`),
  completeBrowserSession: (key: string, sessionId: string) => request<BrowserSession>(`/providers/${key}/browser-session/${sessionId}/complete`, { method: 'POST' }),
  cancelBrowserSession: (key: string, sessionId: string) => request<BrowserSession>(`/providers/${key}/browser-session/${sessionId}`, { method: 'DELETE' }),
  clearBrowserProfile: (key: string) => request<{ cleared: boolean }>(`/providers/${key}/browser-profile`, { method: 'DELETE' }),
  productSettings: () => request<ProductSettings>('/product-settings'),
  updateProductSettings: (input: ProductSettings) => request<ProductSettings>('/product-settings', { method: 'PUT', body: JSON.stringify(input) }),
  serviceStatus: () => request<ServiceStatus>('/status'),
  dashboard: (page = 1, pageSize = 8) => request<DashboardStats>(`/dashboard${pageQuery(page, pageSize)}`),
  folders: (page = 1, pageSize = 20) => request<Paged<Folder>>(`/folders${pageQuery(page, pageSize)}`),
  createFolder: (input: FolderInput) => request<Folder>('/folders', { method: 'POST', body: JSON.stringify(input) }),
  updateFolder: (id: number, input: FolderInput) => request<Folder>(`/folders/${id}`, { method: 'PUT', body: JSON.stringify(input) }),
  deleteFolder: (id: number) => request<void>(`/folders/${id}`, { method: 'DELETE' }),
  scanFolder: (id: number) => request<Task>(`/folders/${id}/scan`, { method: 'POST' }),
  tasks: (status = '', page = 1, pageSize = 20) => request<Paged<Task>>(`/tasks${pageQuery(page, pageSize, status ? { status } : {})}`),
  task: (id: number, page = 1, pageSize = 20) => request<TaskDetail>(`/tasks/${id}${pageQuery(page, pageSize)}`),
  retryTask: (id: number) => request<Task>(`/tasks/${id}/retry`, { method: 'POST' }),
  cancelTask: (id: number) => request<Task>(`/tasks/${id}/cancel`, { method: 'POST' }),
  retryTasks: (taskIds: number[]) => request<BatchTaskResponse>('/tasks/retry', {
    method: 'POST', body: JSON.stringify({ taskIds }),
  }),
  cancelTasks: (taskIds: number[]) => request<BatchTaskResponse>('/tasks/cancel', {
    method: 'POST', body: JSON.stringify({ taskIds }),
  }),
  media: (search = '', status = '', mediaId?: number, page = 1, pageSize = 20) => {
    const query = new URLSearchParams()
    if (mediaId) query.set('mediaId', String(mediaId))
    if (search) query.set('search', search)
    if (status) query.set('status', status)
    query.set('page', String(page))
    query.set('pageSize', String(pageSize))
    return request<Paged<MediaItem>>(`/media?${query.toString()}`)
  },
  scrapeMedia: (id: number, options: ScrapeOptions) =>
    request<Task>(`/media/${id}/scrape`, { method: 'POST', body: JSON.stringify(options) }),
  scrapeMediaBatch: (mediaIds: number[], options: ScrapeOptions) =>
    request<BatchScrapeResponse>('/media/scrape', {
      method: 'POST',
      body: JSON.stringify({ mediaIds, ...options }),
    }),
  settings: () => request<Settings>('/settings'),
  updateSettings: (settings: Settings) => request<Settings>('/settings', { method: 'PUT', body: JSON.stringify(settings) }),
  testMetaTube: (settings: Settings) => request<MetaTubeConnection>('/settings/metatube/test', { method: 'POST', body: JSON.stringify(settings) }),
  testQBittorrent: (settings: Settings) => request<QBittorrentConnection>('/settings/qbittorrent/test', { method: 'POST', body: JSON.stringify(settings) }),
  crawlers: (page = 1, pageSize = 20) => request<Paged<CrawlerScript>>(`/crawlers${pageQuery(page, pageSize)}`),
  createCrawler: (input: CrawlerForm) => request<CrawlerScript>('/crawlers', { method: 'POST', body: crawlerForm(input) }),
  updateCrawler: (id: number, input: CrawlerForm) => request<CrawlerScript>(`/crawlers/${id}`, { method: 'PUT', body: crawlerForm(input) }),
  deleteCrawler: (id: number) => request<void>(`/crawlers/${id}`, { method: 'DELETE' }),
  runCrawler: (id: number) => request<CrawlerRun>(`/crawlers/${id}/run`, { method: 'POST' }),
  crawlerRuns: (scriptId?: number, page = 1, pageSize = 20) => request<Paged<CrawlerRun>>(`/crawler-runs${pageQuery(page, pageSize, scriptId ? { scriptId: String(scriptId) } : {})}`),
  crawlerResults: (scriptId?: number, page = 1, pageSize = 20) => request<Paged<CrawlerResult>>(`/crawler-results${pageQuery(page, pageSize, scriptId ? { scriptId: String(scriptId) } : {})}`),
  downloadCrawlerResult: (id: number) => request<CrawlerResult>(`/crawler-results/${id}/download`, { method: 'POST' }),
  ignoreCrawlerResult: (id: number) => request<CrawlerResult>(`/crawler-results/${id}/ignore`, { method: 'POST' }),
  downloadCrawlerResults: (resultIds: number[]) => request<CrawlerResult[]>('/crawler-results/download', {
    method: 'POST', body: JSON.stringify({ resultIds }),
  }),
  downloads: (page = 1, pageSize = 20) => request<Paged<DownloadItem>>(`/downloads${pageQuery(page, pageSize)}`),
  pauseDownload: (hash: string) => request<void>(`/downloads/${encodeURIComponent(hash)}/pause`, { method: 'POST' }),
  resumeDownload: (hash: string) => request<void>(`/downloads/${encodeURIComponent(hash)}/resume`, { method: 'POST' }),
  removeDownload: (hash: string) => request<void>(`/downloads/${encodeURIComponent(hash)}`, { method: 'DELETE' }),
}

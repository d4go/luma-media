import type {
  BatchScrapeResponse, BatchTaskResponse, CrawlerForm, CrawlerResult, CrawlerRun, CrawlerScript,
  DashboardStats, DownloadItem, Folder, FolderInput, MediaItem, MetaTubeConnection, QBittorrentConnection,
  ScrapeOptions, ServiceStatus, Settings, Task, TaskDetail,
  Acquisition, AcquisitionDetail, Actor, AttentionItem, AutomationRule, HomeData, LibraryItem, MediaDetail, ProductMedia, ProductSettings, ProviderConfig, SearchResponse,
} from './types'

const API_BASE = import.meta.env.VITE_API_BASE_URL ?? '/api/v1'
const ASSET_BASE = API_BASE.replace(/\/api\/v1\/?$/, '')

export const coverAssetUrl = (mediaId: number) => `${ASSET_BASE}/asset/cover/${mediaId}`

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

export const api = {
  home: () => request<HomeData>('/home'),
  search: (q: string) => request<SearchResponse>(`/search?q=${encodeURIComponent(q)}`),
  catalogMedia: (q = '') => request<ProductMedia[]>(`/catalog/media${q ? `?q=${encodeURIComponent(q)}` : ''}`),
  mediaDetail: (id: number) => request<MediaDetail>(`/catalog/media/${id}`),
  acquireMedia: (mediaId: number, resourceId?: number) => request<Acquisition>(`/catalog/media/${mediaId}/acquire`, { method: 'POST', body: JSON.stringify({ resourceId, requestedBy: 'manual' }) }),
  actorDetail: (id: number) => request<{ actor: Actor; media: ProductMedia[] }>(`/actors/${id}`),
  followActor: (id: number, followed: boolean) => request<Actor>(`/actors/${id}/follow`, { method: followed ? 'POST' : 'DELETE' }),
  acquisitions: (status = '') => request<Acquisition[]>(`/acquisitions${status ? `?status=${encodeURIComponent(status)}` : ''}`),
  acquisition: (id: number) => request<AcquisitionDetail>(`/acquisitions/${id}`),
  acquisitionAction: (id: number, action: 'pause' | 'resume' | 'retry' | 'cancel') => request<Acquisition>(`/acquisitions/${id}/${action}`, { method: 'POST' }),
  attention: () => request<AttentionItem[]>('/attention'),
  attentionAction: (id: number, action: string) => request<{ resolved: boolean }>(`/attention/${id}/action`, { method: 'POST', body: JSON.stringify({ action }) }),
  library: (q = '') => request<{ items: LibraryItem[]; total: number }>(`/library${q ? `?q=${encodeURIComponent(q)}` : ''}`),
  libraryItem: (id: number) => request<LibraryItem>(`/library/${id}`),
  reorganizeLibrary: (id: number) => request<{ item: LibraryItem; message: string }>(`/library/${id}/reorganize`, { method: 'POST' }),
  automations: () => request<AutomationRule[]>('/automations'),
  createAutomation: (input: Omit<AutomationRule, 'id' | 'lastRunAt' | 'nextRunAt' | 'lastStatus' | 'lastExplanation' | 'createdAt'>) => request<AutomationRule>('/automations', { method: 'POST', body: JSON.stringify(input) }),
  updateAutomation: (id: number, input: Partial<AutomationRule>) => request<AutomationRule>(`/automations/${id}`, { method: 'PUT', body: JSON.stringify(input) }),
  deleteAutomation: (id: number) => request<void>(`/automations/${id}`, { method: 'DELETE' }),
  setAutomationEnabled: (id: number, enabled: boolean) => request<AutomationRule>(`/automations/${id}/enabled`, { method: 'POST', body: JSON.stringify({ enabled }) }),
  providers: () => request<ProviderConfig[]>('/providers'),
  createProvider: (input: { displayName: string; baseUrl: string; secret?: string; adapter?: string }) => request<ProviderConfig>('/providers', { method: 'POST', body: JSON.stringify(input) }),
  updateProvider: (key: string, input: { displayName?: string; baseUrl: string; secret?: string; config?: Record<string, unknown> }) => request<ProviderConfig>(`/providers/${key}`, { method: 'PUT', body: JSON.stringify(input) }),
  deleteProvider: (key: string) => request<void>(`/providers/${key}`, { method: 'DELETE' }),
  setProviderEnabled: (key: string, enabled: boolean) => request<ProviderConfig>(`/providers/${key}/enabled`, { method: 'POST', body: JSON.stringify({ enabled }) }),
  testProvider: (key: string) => request<{ connected: boolean; message: string; latencyMs: number }>(`/providers/${key}/test`, { method: 'POST' }),
  productSettings: () => request<ProductSettings>('/product-settings'),
  updateProductSettings: (input: ProductSettings) => request<ProductSettings>('/product-settings', { method: 'PUT', body: JSON.stringify(input) }),
  serviceStatus: () => request<ServiceStatus>('/status'),
  dashboard: () => request<DashboardStats>('/dashboard'),
  folders: () => request<Folder[]>('/folders'),
  createFolder: (input: FolderInput) => request<Folder>('/folders', { method: 'POST', body: JSON.stringify(input) }),
  updateFolder: (id: number, input: FolderInput) => request<Folder>(`/folders/${id}`, { method: 'PUT', body: JSON.stringify(input) }),
  deleteFolder: (id: number) => request<void>(`/folders/${id}`, { method: 'DELETE' }),
  scanFolder: (id: number) => request<Task>(`/folders/${id}/scan`, { method: 'POST' }),
  tasks: (status = '') => request<Task[]>(`/tasks${status ? `?status=${encodeURIComponent(status)}` : ''}`),
  task: (id: number) => request<TaskDetail>(`/tasks/${id}`),
  retryTask: (id: number) => request<Task>(`/tasks/${id}/retry`, { method: 'POST' }),
  cancelTask: (id: number) => request<Task>(`/tasks/${id}/cancel`, { method: 'POST' }),
  retryTasks: (taskIds: number[]) => request<BatchTaskResponse>('/tasks/retry', {
    method: 'POST', body: JSON.stringify({ taskIds }),
  }),
  cancelTasks: (taskIds: number[]) => request<BatchTaskResponse>('/tasks/cancel', {
    method: 'POST', body: JSON.stringify({ taskIds }),
  }),
  media: (search = '', status = '', mediaId?: number) => {
    const query = new URLSearchParams()
    if (mediaId) query.set('mediaId', String(mediaId))
    if (search) query.set('search', search)
    if (status) query.set('status', status)
    return request<MediaItem[]>(`/media${query.size ? `?${query}` : ''}`)
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
  crawlers: () => request<CrawlerScript[]>('/crawlers'),
  createCrawler: (input: CrawlerForm) => request<CrawlerScript>('/crawlers', { method: 'POST', body: crawlerForm(input) }),
  updateCrawler: (id: number, input: CrawlerForm) => request<CrawlerScript>(`/crawlers/${id}`, { method: 'PUT', body: crawlerForm(input) }),
  deleteCrawler: (id: number) => request<void>(`/crawlers/${id}`, { method: 'DELETE' }),
  runCrawler: (id: number) => request<CrawlerRun>(`/crawlers/${id}/run`, { method: 'POST' }),
  crawlerRuns: (scriptId?: number) => request<CrawlerRun[]>(`/crawler-runs${scriptId ? `?scriptId=${scriptId}` : ''}`),
  crawlerResults: (scriptId?: number) => request<CrawlerResult[]>(`/crawler-results${scriptId ? `?scriptId=${scriptId}` : ''}`),
  downloadCrawlerResult: (id: number) => request<CrawlerResult>(`/crawler-results/${id}/download`, { method: 'POST' }),
  ignoreCrawlerResult: (id: number) => request<CrawlerResult>(`/crawler-results/${id}/ignore`, { method: 'POST' }),
  downloadCrawlerResults: (resultIds: number[]) => request<CrawlerResult[]>('/crawler-results/download', {
    method: 'POST', body: JSON.stringify({ resultIds }),
  }),
  downloads: () => request<DownloadItem[]>('/downloads'),
  pauseDownload: (hash: string) => request<void>(`/downloads/${encodeURIComponent(hash)}/pause`, { method: 'POST' }),
  resumeDownload: (hash: string) => request<void>(`/downloads/${encodeURIComponent(hash)}/resume`, { method: 'POST' }),
  removeDownload: (hash: string) => request<void>(`/downloads/${encodeURIComponent(hash)}`, { method: 'DELETE' }),
}

export interface Folder {
  id: number
  name: string
  path: string
  mediaType: 'movie' | 'tv' | 'mixed'
  outputFormat: string
  scanMode: 'manual' | 'watch' | 'interval'
  enabled: boolean
  createdAt: string
  updatedAt: string
}

export interface FolderInput {
  name: string
  path: string
  type: Folder['mediaType']
  outputFormat: string
  scanMode: Folder['scanMode']
  enabled: boolean
}

export type TaskStatus = 'pending' | 'running' | 'success' | 'failed' | 'cancelled'

export interface Task {
  id: number
  mediaId: number | null
  folderId: number | null
  taskType: 'scan' | 'scrape'
  status: TaskStatus
  progress: number
  errorMessage: string | null
  createdAt: string
  updatedAt: string
  finishedAt: string | null
  recordCount: number
  media: TaskMedia | null
  folder: TaskFolder | null
}

export interface TaskMedia {
  id: number
  title: string
  filename: string
  path: string
  status: MediaItem['status']
}

export interface TaskFolder {
  id: number
  name: string
  path: string
}

export interface TaskRecord {
  id: number
  taskId: number
  status: TaskStatus
  progress: number
  errorMessage: string | null
  createdAt: string
  finishedAt: string | null
}

export interface TaskDetail extends Task {
  records: TaskRecord[]
}

export interface MediaItem {
  id: number
  folderId: number | null
  path: string
  filename: string
  hash: string
  title: string
  mediaType: string
  providerId: string | null
  status: 'pending' | 'ready' | 'failed'
  scrapeTaskId: number | null
  scrapeTaskStatus: TaskStatus | null
  scrapeRecordCount: number
  resources: {
    nfo: MediaResourceState
    poster: MediaResourceState
  }
  createdAt: string
  updatedAt: string
}

export interface MediaResourceState {
  status: 'ready' | 'missing' | 'failed'
  source: string | null
  path: string | null
  checkedAt: string | null
}

export interface ScrapeOptions {
  overwriteNfo: boolean
  overwriteImage: boolean
}

export interface BatchScrapeResponse {
  queued: number
  skipped: number
  tasks: Task[]
}

export interface BatchTaskResponse {
  processed: number
  skipped: number
  tasks: Task[]
}

export interface DashboardStats {
  mediaCount: number
  taskCount: number
  successCount: number
  failedCount: number
  candidateCount: number
  downloadCount: number
  pendingScrapeCount: number
  recentActivity: Task[]
}

export interface Settings {
  metatubeUrl: string
  metatubeToken: string
  outputFormat: string
  scanInterval: number
  overwritePolicy: string
  logLevel: string
  qbittorrentUrl: string
  qbittorrentUsername: string
  qbittorrentPassword: string
  qbittorrentAutoUpdateTrackers: boolean
  qbittorrentTrackerSourceUrl: string
  qbittorrentTrackerUpdateInterval: number
}

export interface MetaTubeConnection {
  connected: boolean
  providerCount: number
  message: string
}

export interface QBittorrentConnection {
  connected: boolean
  version: string
  message: string
}

export interface ServiceHealth {
  connected: boolean
  message: string
  latencyMs: number | null
}

export interface ServiceStatus {
  luma: ServiceHealth
  metaTube: ServiceHealth
  qbittorrent: ServiceHealth
  checkedAt: string
}

export interface CrawlerScript {
  id: number
  name: string
  websiteUrl: string
  fileName: string
  intervalMinutes: number
  enabled: boolean
  autoDownload: boolean
  lastStartedAt: string | null
  lastFinishedAt: string | null
  nextRunAt: string | null
  lastRunStatus: TaskStatus | null
  lastResultCount: number
  createdAt: string
  updatedAt: string
}

export interface CrawlerRun {
  id: number
  scriptId: number
  status: TaskStatus
  stdout: string
  stderr: string
  resultCount: number
  errorMessage: string | null
  createdAt: string
  startedAt: string | null
  finishedAt: string | null
}

export interface CrawlerResult {
  id: number
  runId: number
  scriptId: number
  title: string
  source: string
  sourceUrl: string
  size: string | null
  publishedAt: string
  downloadUrl: string
  trackers: string[]
  raw: unknown
  downloadStatus: 'pending' | 'downloading' | 'success' | 'failed' | 'ignored'
  qbitHash: string | null
  errorMessage: string | null
  createdAt: string
  downloadedAt: string | null
}

export interface DownloadItem {
  hash: string
  name: string
  size: number
  progress: number
  state: string
  downloadSpeed: number
  uploadSpeed: number
  eta: number
  savePath: string
  addedOn: number
  completionOn: number
}

export interface CrawlerForm {
  name: string
  websiteUrl: string
  intervalMinutes: number
  enabled: boolean
  autoDownload: boolean
  script?: File | null
}

export interface ProductMedia { id: number; code: string; title: string; originalTitle: string | null; summary: string; releaseDate: string | null; durationMinutes: number | null; posterUrl: string | null; backdropUrl: string | null; mediaType: string; metadataStatus: string; createdAt: string; updatedAt: string }
export interface Actor { id: number; name: string; aliases: string[]; avatarUrl: string | null; followed: boolean; mediaCount: number }
export interface Resource { id: number; mediaId: number; providerKey: string; title: string; downloadUrl: string; infoHash: string | null; sizeBytes: number | null; resolution: string | null; subtitleLanguages: string[]; trackers: string[]; publishedAt: string | null; score: number; scoreReasons: string[]; available: boolean; qbitHash: string | null; qbitState: string | null; qbitSyncStatus: 'synced' | 'unavailable' | 'unknown'; acquisitionId: number | null; acquisitionState: string | null }
export interface Acquisition { id: number; mediaId: number; resourceId: number | null; requestedBy: string; state: string; stateMessage: string; qbitHash: string | null; qbitState: string | null; progress: number; downloadSpeed: number; etaSeconds: number | null; downloadPath: string | null; libraryItemId: number | null; lastError: string | null; retryCount: number; createdAt: string; updatedAt: string; completedAt: string | null; media: ProductMedia; resource: Resource | null }
export interface ProviderReport { providerKey: string; ok: boolean; message: string; resultCount: number }
export interface SearchResponse { query: string; media: ProductMedia[]; actors: Actor[]; providerReports: ProviderReport[] }
export interface HomeData { activeAcquisitions: number; libraryCount: number; attentionCount: number; followedActors: number; recentAcquisitions: Acquisition[]; quickStarts: { title: string; description: string; to: string }[] }
export interface MediaDetail { media: ProductMedia; actors: Actor[]; resources: Resource[]; latestAcquisitionId: number | null; libraryItemId: number | null }
export interface AcquisitionEvent { id: number; eventKey: string; fromState: string | null; toState: string; message: string; payload: unknown; createdAt: string }
export interface AcquisitionDetail { acquisition: Acquisition; events: AcquisitionEvent[] }
export interface AttentionItem { id: number; kind: string; severity: string; title: string; message: string; acquisitionId: number | null; mediaId: number | null; mediaTitle: string | null; mediaCode: string | null; actions: string[]; createdAt: string }
export interface LibraryItem { id: number; mediaId: number; acquisitionId: number | null; videoPath: string; nfoPath: string | null; posterPath: string | null; status: string; fileSize: number | null; addedAt: string; media: { code: string; title: string; posterUrl: string | null; releaseDate: string | null; metadataStatus: string } }
export interface ProviderConfig {
  key: string
  type: string
  displayName: string
  enabled: boolean
  baseUrl: string
  hasSecret: boolean
  config: Record<string, unknown>
  lastStatus: string
  lastMessage: string
  lastCheckedAt: string | null
  syncStatus: 'idle' | 'running' | 'success' | 'failed'
  syncLastStartedAt: string | null
  syncLastFinishedAt: string | null
  syncLastSuccessAt: string | null
  syncNextRunAt: string | null
  syncLastMessage: string
  syncFailureCount: number
  syncItemCount: number
  syncInsertedCount: number
  syncUpdatedCount: number
}
export type ProviderFetchMode = 'auto' | 'http' | 'browser'
export type ProviderRuntimeState = 'ready' | 'degraded' | 'cooldown' | 'interaction_required' | 'unavailable'
export type ProviderPageKind = 'valid_content' | 'age_gate' | 'login_required' | 'interaction_required' | 'access_denied' | 'rate_limited' | 'temporary_unavailable' | 'invalid_content'
export interface ProviderRuntime {
  providerKey: string
  state: ProviderRuntimeState
  activeFetchMode: ProviderFetchMode
  lastSuccessAt: string | null
  lastFailureAt: string | null
  lastFailureKind: string | null
  lastFailureMessage: string | null
  failureCount: number
  cooldownUntil: string | null
  browserEnabled: boolean
  browserExecutable: string
  browserProfilePath: string
}
export interface ProviderDiagnoseAttempt {
  attempted: boolean
  success: boolean
  status: number | null
  pageKind: ProviderPageKind | null
  finalUrl: string | null
  elapsedMs: number | null
  error: string | null
}
export interface ProviderDiagnoseResponse {
  provider: string
  http: ProviderDiagnoseAttempt
  browser: ProviderDiagnoseAttempt
  runtime: ProviderRuntime
}
export type BrowserSessionStatus = 'active' | 'completed' | 'cancelled'
export interface BrowserSession {
  sessionId: string
  providerKey: string
  status: BrowserSessionStatus
  port: number
  password: string | null
  startedAt: string
  expiresAt: string
}
export interface ProductSettings { downloadRoot: string; mediaRoot: string; qbittorrentSavePath: string; qbittorrentCategory: string; qbittorrentTags: string; organizerMode: 'hardlink' | 'copy'; organizerMovieTemplate: string; organizerConflictPolicy: string }
export interface AutomationRule { id: number; name: string; enabled: boolean; triggerType: string; triggerConfig: Record<string, unknown>; conditions: Record<string, unknown>; actionType: string; actionConfig: Record<string, unknown>; mode: 'AUTO' | 'CONFIRM' | 'NOTIFY'; lastRunAt: string | null; nextRunAt: string | null; lastStatus: string | null; lastExplanation: string | null; createdAt: string }

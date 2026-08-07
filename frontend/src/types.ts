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

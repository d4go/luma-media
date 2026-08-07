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
  recentActivity: Task[]
}

export interface Settings {
  metatubeUrl: string
  metatubeToken: string
  outputFormat: string
  scanInterval: number
  overwritePolicy: string
  logLevel: string
}

export interface MetaTubeConnection {
  connected: boolean
  providerCount: number
  message: string
}

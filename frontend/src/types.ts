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
  finishedAt: string | null
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
  createdAt: string
  updatedAt: string
}

export interface ScrapeOptions {
  overwriteNfo: boolean
  overwriteImage: boolean
}

export interface BatchScrapeResponse {
  created: number
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

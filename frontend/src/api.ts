import type { DashboardStats, Folder, FolderInput, MediaItem, MetaTubeConnection, Settings, Task } from './types'

const API_BASE = import.meta.env.VITE_API_BASE_URL ?? '/api/v1'

export class ApiError extends Error {
  constructor(message: string, public readonly status: number) {
    super(message)
  }
}

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    ...options,
    headers: {
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

export const api = {
  dashboard: () => request<DashboardStats>('/dashboard'),
  folders: () => request<Folder[]>('/folders'),
  createFolder: (input: FolderInput) => request<Folder>('/folders', { method: 'POST', body: JSON.stringify(input) }),
  updateFolder: (id: number, input: FolderInput) => request<Folder>(`/folders/${id}`, { method: 'PUT', body: JSON.stringify(input) }),
  deleteFolder: (id: number) => request<void>(`/folders/${id}`, { method: 'DELETE' }),
  scanFolder: (id: number) => request<Task>(`/folders/${id}/scan`, { method: 'POST' }),
  tasks: (status = '') => request<Task[]>(`/tasks${status ? `?status=${encodeURIComponent(status)}` : ''}`),
  task: (id: number) => request<Task>(`/tasks/${id}`),
  retryTask: (id: number) => request<Task>(`/tasks/${id}/retry`, { method: 'POST' }),
  cancelTask: (id: number) => request<Task>(`/tasks/${id}/cancel`, { method: 'POST' }),
  media: (search = '') => request<MediaItem[]>(`/media${search ? `?search=${encodeURIComponent(search)}` : ''}`),
  scrapeMedia: (id: number, options: { overwriteNfo: boolean; overwriteImage: boolean }) =>
    request<Task>(`/media/${id}/scrape`, { method: 'POST', body: JSON.stringify(options) }),
  settings: () => request<Settings>('/settings'),
  updateSettings: (settings: Settings) => request<Settings>('/settings', { method: 'PUT', body: JSON.stringify(settings) }),
  testMetaTube: (settings: Settings) => request<MetaTubeConnection>('/settings/metatube/test', { method: 'POST', body: JSON.stringify(settings) }),
}

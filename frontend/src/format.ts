import type { TaskStatus } from './types'

export function formatDate(value: string | null): string {
  if (!value) return '未完成'
  const normalized = value.includes('T') ? value : `${value.replace(' ', 'T')}Z`
  const date = new Date(normalized)
  if (Number.isNaN(date.getTime())) return value
  return new Intl.DateTimeFormat('zh-CN', {
    month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit',
  }).format(date)
}

export function inclusiveDateRangeDays(from: string, to: string): number | null {
  const parseDateOnly = (value: string) => {
    if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return null
    const timestamp = Date.parse(`${value}T00:00:00Z`)
    if (!Number.isFinite(timestamp) || new Date(timestamp).toISOString().slice(0, 10) !== value) return null
    return timestamp
  }
  const fromTime = parseDateOnly(from)
  const toTime = parseDateOnly(to)
  if (fromTime == null || toTime == null) return null
  return Math.floor((toTime - fromTime) / 86_400_000) + 1
}

export const statusLabel: Record<TaskStatus, string> = {
  pending: '等待中', running: '执行中', success: '成功', failed: '失败', cancelled: '已取消',
}

export const statusType: Record<TaskStatus, 'default' | 'info' | 'success' | 'error' | 'warning'> = {
  pending: 'default', running: 'info', success: 'success', failed: 'error', cancelled: 'warning',
}

export const taskTypeLabel = { scan: '目录扫描', scrape: '元数据刮削' } as const


import type { InjectionKey, Ref } from 'vue'
import type { ServiceStatus } from './types'

export interface ServiceStatusContext {
  status: Ref<ServiceStatus | null>
  refreshing: Ref<boolean>
  refresh: () => Promise<void>
}

export const serviceStatusKey: InjectionKey<ServiceStatusContext> = Symbol('service-status')

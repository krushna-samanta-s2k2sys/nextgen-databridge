import axios from 'axios'

const BASE = import.meta.env.VITE_API_URL || ''

export const api = axios.create({ baseURL: BASE })

api.interceptors.request.use(cfg => {
  const tok = localStorage.getItem('df_token')
  if (tok) cfg.headers.Authorization = `Bearer ${tok}`
  return cfg
})

api.interceptors.response.use(
  r => r,
  err => {
    if (err.response?.status === 401) {
      localStorage.removeItem('df_token')
      window.location.href = '/login'
    }
    return Promise.reject(err)
  }
)

// Auth
export const login = (u: string, p: string) =>
  api.post('/api/auth/login', { username: u, password: p }).then(r => r.data)
export const me = () => api.get('/api/auth/me').then(r => r.data)

// Pipelines
export const getPipelines = (p?: Record<string,string>) =>
  api.get('/api/pipelines', { params: p }).then(r => r.data)
export const getPipeline = (id: string) =>
  api.get(`/api/pipelines/${id}`).then(r => r.data)
export const createPipeline = (d: any) =>
  api.post('/api/pipelines', d).then(r => r.data)
export const updatePipeline = (id: string, d: any) =>
  api.put(`/api/pipelines/${id}`, d).then(r => r.data)
export const pausePipeline = (id: string) =>
  api.post(`/api/pipelines/${id}/pause`).then(r => r.data)
export const resumePipeline = (id: string) =>
  api.post(`/api/pipelines/${id}/resume`).then(r => r.data)
export const triggerPipeline = (id: string, d?: any) =>
  api.post(`/api/pipelines/${id}/trigger`, d || {}).then(r => r.data)

// Config
export const validateConfig = (config: any) =>
  api.post('/api/config/validate', { config }).then(r => r.data)
export const getConfigVersions = (id: string) =>
  api.get(`/api/pipelines/${id}/configs`).then(r => r.data)
export const getConfigVersion = (id: string, v: string) =>
  api.get(`/api/pipelines/${id}/configs/${v}`).then(r => r.data)

// Runs
export const getRuns = (p?: Record<string,string>) =>
  api.get('/api/runs', { params: p }).then(r => r.data)
export const getRun = (id: string) =>
  api.get(`/api/runs/${id}`).then(r => r.data)
export const rerunTask = (runId: string, d: any) =>
  api.post(`/api/runs/${runId}/rerun`, d).then(r => r.data)
export const syncRun = (id: string) =>
  api.post(`/api/runs/${id}/sync`).then(r => r.data)

// Tasks
export const getTasks = (p?: Record<string,string>) =>
  api.get('/api/tasks', { params: p }).then(r => r.data)

// Audit
export const getAudit = (p?: Record<string,string>) =>
  api.get('/api/audit', { params: p }).then(r => r.data)

// Alerts
export const getAlerts = (p?: Record<string,string>) =>
  api.get('/api/alerts', { params: p }).then(r => r.data)
export const resolveAlert = (id: string) =>
  api.post(`/api/alerts/${id}/resolve`).then(r => r.data)

// Connections
export const getConnections = () =>
  api.get('/api/connections').then(r => r.data)
export const createConnection = (d: any) =>
  api.post('/api/connections', d).then(r => r.data)
export const testConnection = (id: string) =>
  api.post(`/api/connections/${id}/test`).then(r => r.data)

// Deployments
export const getDeployments = (p?: Record<string,string>) =>
  api.get('/api/deployments', { params: p }).then(r => r.data)
export const createDeployment = (d: any) =>
  api.post('/api/deployments', d).then(r => r.data)
export const approveDeployment = (id: string, token?: string) =>
  api.post(`/api/deployments/${id}/approve`, null, { params: token ? { token } : {} }).then(r => r.data)
export const rejectDeployment = (id: string, reason: string) =>
  api.post(`/api/deployments/${id}/reject`, { reason }).then(r => r.data)

// Query
export const queryDuckDB = (d: any) =>
  api.post('/api/query', d).then(r => r.data)
export const getDuckDBFiles = (p?: Record<string,string>) =>
  api.get('/api/query/duckdb-files', { params: p }).then(r => r.data)

// Metrics
export const getDashboardMetrics = () =>
  api.get('/api/metrics/dashboard').then(r => r.data)
export const getThroughputMetrics = (hours?: number) =>
  api.get('/api/metrics/throughput', { params: { hours } }).then(r => r.data)

// EKS
export const getEKSJobs = (p?: Record<string,string>) =>
  api.get('/api/eks/jobs', { params: p }).then(r => r.data)

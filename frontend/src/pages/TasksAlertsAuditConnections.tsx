import React, { useState, useMemo } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import toast from 'react-hot-toast'
import {
  getTasks, getAlerts, resolveAlert, getAudit,
  getConnections, createConnection, testConnection,
} from '../api/client'
import {
  Card, CardHeader, Table, StatusBadge, Button, Modal,
  Input, Select, Textarea, Spinner, Duration, TimeAgo,
} from '../components/ui'
import {
  CheckCircle, XCircle, Plus, FlaskConical, RefreshCw,
  ChevronDown, ChevronRight, Database, Search, Shield,
  Wifi, WifiOff, Server, Cloud, MessageSquare, Hash,
} from 'lucide-react'
import clsx from 'clsx'

// ─────────────────────────────────────────────────────────────────────────────
// Tasks
// ─────────────────────────────────────────────────────────────────────────────
const TASK_TYPE_COLORS: Record<string, { bg: string; text: string }> = {
  sql_extract:       { bg: 'bg-blue-50',   text: 'text-blue-700' },
  duckdb_transform:  { bg: 'bg-purple-50', text: 'text-purple-700' },
  data_quality:      { bg: 'bg-emerald-50', text: 'text-emerald-700' },
  load_target:       { bg: 'bg-orange-50', text: 'text-orange-700' },
  notification:      { bg: 'bg-gray-50',   text: 'text-gray-500' },
  schema_validation: { bg: 'bg-teal-50',   text: 'text-teal-700' },
  file_ingest:       { bg: 'bg-indigo-50', text: 'text-indigo-700' },
  eks_job:           { bg: 'bg-yellow-50', text: 'text-yellow-700' },
}

export function Tasks() {
  const nav = useNavigate()
  const [expanded, setExpanded] = useState<Set<string>>(new Set())
  const [pipelineFilter, setPipelineFilter] = useState('')
  const [statusFilter, setStatusFilter] = useState('')

  const { data, isLoading, refetch } = useQuery({
    queryKey: ['tasks', pipelineFilter, statusFilter],
    queryFn: () => getTasks({
      page_size: '200',
      ...(pipelineFilter ? { pipeline_id: pipelineFilter } : {}),
      ...(statusFilter ? { status: statusFilter } : {}),
    }),
    refetchInterval: 10_000,
  })
  const tasks = data?.tasks ?? []

  const runs = useMemo(() => {
    const map = new Map<string, {
      run_id: string; pipeline_id: string; tasks: any[]
      minStart: string | null; maxEnd: string | null
    }>()
    tasks.forEach((t: any) => {
      if (!map.has(t.run_id)) {
        map.set(t.run_id, { run_id: t.run_id, pipeline_id: t.pipeline_id, tasks: [], minStart: null, maxEnd: null })
      }
      const g = map.get(t.run_id)!
      g.tasks.push(t)
      if (t.start_time && (!g.minStart || t.start_time < g.minStart)) g.minStart = t.start_time
      if (t.end_time   && (!g.maxEnd   || t.end_time   > g.maxEnd))   g.maxEnd   = t.end_time
    })
    return Array.from(map.values()).sort((a, b) => (b.minStart ?? '').localeCompare(a.minStart ?? ''))
  }, [tasks])

  function toggle(runId: string) {
    setExpanded(s => { const n = new Set(s); n.has(runId) ? n.delete(runId) : n.add(runId); return n })
  }

  function runStatus(ts: any[]) {
    if (ts.some(t => t.status === 'running')) return 'running'
    if (ts.some(t => t.status === 'failed'))  return 'failed'
    if (ts.every(t => t.status === 'success')) return 'success'
    return 'pending'
  }

  return (
    <div className="flex flex-col h-full overflow-hidden bg-gray-50">
      {/* Header */}
      <div className="flex items-center justify-between px-6 py-4 border-b border-gray-200 bg-white flex-shrink-0">
        <div>
          <h1 className="text-xl font-bold text-gray-900">Task Monitor</h1>
          <p className="text-sm text-gray-500 mt-0.5">{runs.length} runs · {tasks.length} tasks</p>
        </div>
        <div className="flex items-center gap-2">
          <div className="relative">
            <Search size={13} className="absolute left-2.5 top-1/2 -translate-y-1/2 text-gray-400"/>
            <input
              value={pipelineFilter} onChange={e => setPipelineFilter(e.target.value)}
              placeholder="Pipeline ID…"
              className="bg-white border border-gray-300 rounded-lg pl-8 pr-3 py-1.5 text-sm text-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-500 w-44 placeholder:text-gray-400 shadow-sm"
            />
          </div>
          <select
            value={statusFilter} onChange={e => setStatusFilter(e.target.value)}
            className="bg-white border border-gray-300 rounded-lg px-3 py-1.5 text-sm text-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-500 shadow-sm"
          >
            <option value="">All statuses</option>
            {['running','success','failed','pending','skipped'].map(s => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>
          <Button variant="secondary" size="sm" onClick={() => refetch()}>
            <RefreshCw size={13}/> Refresh
          </Button>
        </div>
      </div>

      {/* Run groups */}
      <div className="flex-1 overflow-y-auto">
        {isLoading && (
          <div className="flex justify-center py-16"><Spinner size="lg"/></div>
        )}
        {!isLoading && runs.length === 0 && (
          <div className="flex flex-col items-center justify-center py-20 gap-2 text-gray-400">
            <Database size={28} className="text-gray-300"/>
            <p className="text-sm">No task runs found</p>
          </div>
        )}
        {runs.map(run => {
          const open     = expanded.has(run.run_id)
          const status   = runStatus(run.tasks)
          const success  = run.tasks.filter((t: any) => t.status === 'success').length
          const failed   = run.tasks.filter((t: any) => t.status === 'failed').length
          const totalRows = run.tasks.reduce((s: number, t: any) => s + (t.output_row_count || 0), 0)

          return (
            <div key={run.run_id} className="border-b border-gray-200 last:border-0">
              <button
                onClick={() => toggle(run.run_id)}
                className="w-full flex items-center gap-3 px-5 py-3.5 hover:bg-white transition-colors text-left group"
              >
                <span className="text-gray-400 flex-shrink-0 group-hover:text-gray-600">
                  {open ? <ChevronDown size={14}/> : <ChevronRight size={14}/>}
                </span>
                <span className="font-mono text-sm font-semibold text-blue-600 w-44 flex-shrink-0 truncate">
                  {run.pipeline_id}
                </span>
                <span className="font-mono text-xs text-gray-400 flex-1 truncate">
                  {run.run_id.slice(-30)}
                </span>
                <StatusBadge status={status}/>
                <span className="text-xs text-gray-400 w-28 flex-shrink-0">
                  <TimeAgo ts={run.minStart}/>
                </span>
                <span className="text-xs font-mono w-20 flex-shrink-0 text-right">
                  <span className="text-emerald-600 font-semibold">{success}</span>
                  <span className="text-gray-300">/{run.tasks.length}</span>
                  {failed > 0 && <span className="text-red-500 ml-1">{failed}✗</span>}
                </span>
                {totalRows > 0 && (
                  <span className="text-xs font-mono text-gray-500 w-28 flex-shrink-0 text-right">
                    {totalRows.toLocaleString()} rows
                  </span>
                )}
              </button>

              {open && (
                <div className="bg-gray-50 border-t border-gray-100">
                  {run.tasks.map((t: any) => {
                    const typeStyle = TASK_TYPE_COLORS[t.task_type] || { bg: 'bg-gray-50', text: 'text-gray-500' }
                    return (
                      <div
                        key={t.task_run_id}
                        className="flex items-center gap-3 px-8 py-2.5 border-b border-gray-100 last:border-0 hover:bg-white transition-colors"
                      >
                        <div className="w-5 flex-shrink-0">
                          {t.status === 'success' && <CheckCircle size={14} className="text-emerald-500"/>}
                          {t.status === 'failed'  && <XCircle size={14} className="text-red-500"/>}
                          {t.status === 'running' && (
                            <div className="w-3.5 h-3.5 border-2 border-blue-500 border-t-transparent rounded-full animate-spin"/>
                          )}
                          {!['success','failed','running'].includes(t.status) && (
                            <div className="w-3.5 h-3.5 rounded-full bg-gray-300"/>
                          )}
                        </div>

                        <div className="w-52 flex-shrink-0">
                          <div className="font-mono text-sm font-semibold text-gray-800">{t.task_id}</div>
                          <div className={clsx('text-xs mt-0.5 inline-block px-1.5 py-0.5 rounded', typeStyle.bg, typeStyle.text)}>
                            {t.task_type?.replace(/_/g, ' ')}
                            {t.attempt_number > 1 && <span className="text-amber-600 ml-1">retry#{t.attempt_number}</span>}
                          </div>
                        </div>

                        <div className="w-20 flex-shrink-0 text-xs text-gray-500">
                          <Duration seconds={t.duration_seconds}/>
                        </div>
                        <div className="w-28 flex-shrink-0 text-xs font-mono text-gray-600">
                          {t.output_row_count != null ? `${t.output_row_count.toLocaleString()} rows` : ''}
                        </div>

                        <div className="w-20 flex-shrink-0 text-xs">
                          {t.qc_passed === true  && <span className="text-emerald-600 font-medium">QC ✓</span>}
                          {t.qc_passed === false && <span className="text-red-600">QC ✗ {t.qc_failures}</span>}
                        </div>

                        {t.error_message ? (
                          <div
                            className="flex-1 text-xs text-red-600 truncate font-mono bg-red-50 px-2 py-0.5 rounded"
                            title={t.error_message}
                          >
                            {t.error_message.slice(0, 80)}{t.error_message.length > 80 ? '…' : ''}
                          </div>
                        ) : <div className="flex-1"/>}

                        <div className="flex items-center gap-1.5 flex-shrink-0">
                          {t.output_duckdb_path && (
                            <Button
                              size="xs" variant="primary"
                              onClick={() => nav(`/query?path=${encodeURIComponent(t.output_duckdb_path)}&pipeline=${t.pipeline_id}&run=${t.run_id}&task=${t.task_id}`)}
                            >
                              <Database size={11}/> View Data
                            </Button>
                          )}
                          {t.input_sources?.filter((s: any) => s.type === 'duckdb').map((s: any, i: number) => (
                            <Button
                              key={i} size="xs" variant="ghost"
                              onClick={() => nav(`/query?path=${encodeURIComponent(s.path)}&pipeline=${t.pipeline_id}&run=${t.run_id}&task=${t.task_id}`)}
                            >
                              <Database size={11}/> Input {i + 1}
                            </Button>
                          ))}
                        </div>
                      </div>
                    )
                  })}
                </div>
              )}
            </div>
          )
        })}
      </div>
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// Alerts
// ─────────────────────────────────────────────────────────────────────────────
export function Alerts() {
  const qc = useQueryClient()
  const [resolved, setResolved] = useState(false)

  const { data, isLoading } = useQuery({
    queryKey: ['alerts', resolved],
    queryFn: () => getAlerts({ resolved: String(resolved), page_size: '100' }),
    refetchInterval: 15_000,
  })
  const alerts = data?.alerts ?? []

  const resolveMut = useMutation({
    mutationFn: (id: string) => resolveAlert(id),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['alerts'] }); toast.success('Alert resolved') },
  })

  return (
    <div className="p-6 space-y-5 overflow-y-auto h-full bg-gray-50">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-gray-900">Alerts</h1>
          <p className="text-sm text-gray-500 mt-0.5">
            {alerts.length} {resolved ? 'resolved' : 'active'} alerts
          </p>
        </div>
        <button
          onClick={() => setResolved(!resolved)}
          className={clsx(
            'px-4 py-2 rounded-lg text-sm font-medium transition-colors border',
            resolved
              ? 'bg-blue-600 text-white border-blue-600'
              : 'bg-white text-gray-700 border-gray-300 hover:bg-gray-50',
          )}
        >
          {resolved ? 'Show Active' : 'Show Resolved'}
        </button>
      </div>

      <Card>
        {isLoading ? (
          <div className="flex justify-center py-12"><Spinner/></div>
        ) : (
          <Table
            columns={[
              { key: 'severity', header: 'Severity', render: v => <StatusBadge status={v}/> },
              {
                key: 'title', header: 'Title',
                render: v => <span className="text-gray-900 text-sm font-medium">{v}</span>,
              },
              {
                key: 'pipeline_id', header: 'Pipeline',
                render: v => v ? <span className="font-mono text-xs text-blue-600">{v}</span> : <span className="text-gray-300">—</span>,
              },
              {
                key: 'task_id', header: 'Task',
                render: v => v ? <span className="font-mono text-xs text-gray-600">{v}</span> : <span className="text-gray-300">—</span>,
              },
              {
                key: 'message', header: 'Message',
                render: v => <span className="text-gray-500 text-xs truncate max-w-64 block">{v}</span>,
              },
              { key: 'fired_at', header: 'Fired', render: v => <TimeAgo ts={v}/> },
              {
                key: 'is_resolved', header: 'Action',
                render: (v, row) => !v ? (
                  <Button
                    size="xs" variant="ghost"
                    onClick={e => { e.stopPropagation(); resolveMut.mutate(row.id) }}
                  >
                    <CheckCircle size={12}/> Resolve
                  </Button>
                ) : <span className="text-gray-400 text-xs">Resolved</span>,
              },
            ]}
            data={alerts}
            emptyMsg={`No ${resolved ? 'resolved' : 'active'} alerts`}
          />
        )}
      </Card>
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// Audit Trail
// ─────────────────────────────────────────────────────────────────────────────
export function AuditTrail() {
  const [pipelineFilter, setPipelineFilter] = useState('')
  const [userFilter, setUserFilter] = useState('')

  const { data, isLoading } = useQuery({
    queryKey: ['audit', pipelineFilter, userFilter],
    queryFn: () => getAudit({
      ...(pipelineFilter ? { pipeline_id: pipelineFilter } : {}),
      ...(userFilter ? { user: userFilter } : {}),
      page_size: '100',
    }),
    refetchInterval: 30_000,
  })
  const logs = data?.logs ?? []

  const SEVERITY_DOT: Record<string, string> = {
    info: 'bg-blue-500', warning: 'bg-amber-500',
    error: 'bg-red-500', critical: 'bg-red-700',
  }
  const SEVERITY_TEXT: Record<string, string> = {
    info: 'text-blue-700', warning: 'text-amber-700',
    error: 'text-red-700', critical: 'text-red-800',
  }

  return (
    <div className="p-6 space-y-5 overflow-y-auto h-full bg-gray-50">
      <div>
        <h1 className="text-xl font-bold text-gray-900">Audit Trail</h1>
        <p className="text-sm text-gray-500 mt-0.5">Immutable log of all platform events</p>
      </div>

      <div className="flex gap-3">
        <input
          value={pipelineFilter} onChange={e => setPipelineFilter(e.target.value)}
          placeholder="Filter by pipeline…"
          className="bg-white border border-gray-300 rounded-lg px-3 py-2 text-sm text-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-500 w-56 placeholder:text-gray-400 shadow-sm"
        />
        <input
          value={userFilter} onChange={e => setUserFilter(e.target.value)}
          placeholder="Filter by user…"
          className="bg-white border border-gray-300 rounded-lg px-3 py-2 text-sm text-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-500 w-48 placeholder:text-gray-400 shadow-sm"
        />
      </div>

      <Card>
        {isLoading ? (
          <div className="flex justify-center py-12"><Spinner/></div>
        ) : (
          <div className="divide-y divide-gray-100 max-h-[calc(100vh-280px)] overflow-y-auto">
            {logs.length === 0 && (
              <p className="text-center text-gray-400 text-sm py-12">No audit events found</p>
            )}
            {logs.map((l: any) => (
              <div key={l.id} className="px-5 py-3.5 flex items-start gap-4 hover:bg-gray-50 transition-colors">
                <div
                  className={clsx(
                    'w-1.5 h-1.5 rounded-full mt-1.5 flex-shrink-0',
                    SEVERITY_DOT[l.severity] || 'bg-gray-400',
                  )}
                />
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2 flex-wrap">
                    <span className={clsx(
                      'text-xs font-mono font-semibold',
                      SEVERITY_TEXT[l.severity] || 'text-gray-700',
                    )}>
                      {l.event_type}
                    </span>
                    {l.pipeline_id && (
                      <span className="text-xs text-blue-600 font-mono bg-blue-50 px-1.5 py-0.5 rounded">
                        {l.pipeline_id}
                      </span>
                    )}
                    {l.task_id && <span className="text-xs text-gray-400">/ {l.task_id}</span>}
                    {l.user && <span className="text-xs text-gray-400">· {l.user}</span>}
                  </div>
                  {l.details && Object.keys(l.details).length > 0 && (
                    <p className="text-xs text-gray-400 mt-0.5 truncate font-mono">
                      {JSON.stringify(l.details).slice(0, 120)}
                    </p>
                  )}
                </div>
                <div className="text-xs text-gray-400 flex-shrink-0">
                  <TimeAgo ts={l.timestamp}/>
                </div>
              </div>
            ))}
          </div>
        )}
      </Card>
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// Connections — dynamic fields per connection type
// ─────────────────────────────────────────────────────────────────────────────
const CONN_TYPES = [
  { value: 'sqlserver',   label: 'SQL Server',  icon: Server },
  { value: 'oracle',      label: 'Oracle DB',   icon: Database },
  { value: 'postgresql',  label: 'PostgreSQL',  icon: Database },
  { value: 'mysql',       label: 'MySQL',       icon: Database },
  { value: 's3',          label: 'Amazon S3',   icon: Cloud },
  { value: 'kafka',       label: 'Apache Kafka',icon: MessageSquare },
  { value: 'redis',       label: 'Redis',       icon: Hash },
  { value: 'http',        label: 'HTTP / REST', icon: Wifi },
]

type ConnForm = Record<string, string>

const DEFAULT_FORM: ConnForm = {
  connection_id: '', name: '', connection_type: 'sqlserver',
  // RDBMS
  host: '', port: '', database: '', username: '', password: '',
  // SQL Server extras
  driver: 'ODBC Driver 18 for SQL Server', encrypt: 'yes', trust_server_certificate: 'no',
  // PostgreSQL extras
  sslmode: 'require',
  // S3
  region: 'us-east-1', bucket: '', access_key_id: '', secret_access_key: '', role_arn: '',
  // Kafka
  bootstrap_servers: '', security_protocol: 'PLAINTEXT', sasl_mechanism: 'PLAIN',
  sasl_username: '', sasl_password: '',
  // HTTP
  base_url: '', auth_type: 'none', api_key: '',
}

function ConnectionForm({ form, set }: { form: ConnForm; set: (k: string, v: string) => void }) {
  const t = form.connection_type

  const isRDBMS = ['sqlserver', 'oracle', 'postgresql', 'mysql'].includes(t)
  const isS3    = t === 's3'
  const isKafka = t === 'kafka'
  const isHttp  = t === 'http'
  const isRedis = t === 'redis'

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-2 gap-3">
        <Input
          label="Connection ID *"
          value={form.connection_id}
          onChange={e => set('connection_id', e.target.value.toLowerCase().replace(/\s+/g,'_').replace(/[^a-z0-9_]/g,''))}
          placeholder="sqlserver_prod"
          hint="Lowercase letters, numbers, underscores"
        />
        <Input
          label="Display Name"
          value={form.name}
          onChange={e => set('name', e.target.value)}
          placeholder="Production SQL Server"
        />
      </div>

      <Select label="Connection Type" value={form.connection_type} onChange={e => set('connection_type', e.target.value)}>
        {CONN_TYPES.map(ct => (
          <option key={ct.value} value={ct.value}>{ct.label}</option>
        ))}
      </Select>

      {/* RDBMS fields */}
      {isRDBMS && (
        <>
          <div className="grid grid-cols-3 gap-3">
            <div className="col-span-2">
              <Input label="Host" value={form.host} onChange={e => set('host', e.target.value)} placeholder="db.example.com"/>
            </div>
            <Input label="Port" value={form.port} onChange={e => set('port', e.target.value)} placeholder={t === 'sqlserver' ? '1433' : t === 'postgresql' ? '5432' : t === 'oracle' ? '1521' : '3306'} type="number"/>
          </div>
          <Input label="Database / Schema" value={form.database} onChange={e => set('database', e.target.value)} placeholder={t === 'oracle' ? 'SID or Service Name' : 'database_name'}/>
          <div className="grid grid-cols-2 gap-3">
            <Input label="Username" value={form.username} onChange={e => set('username', e.target.value)}/>
            <Input label="Password" value={form.password} onChange={e => set('password', e.target.value)} type="password"/>
          </div>
          {t === 'sqlserver' && (
            <>
              <Input label="ODBC Driver" value={form.driver} onChange={e => set('driver', e.target.value)} placeholder="ODBC Driver 18 for SQL Server"/>
              <div className="grid grid-cols-2 gap-3">
                <Select label="Encrypt" value={form.encrypt} onChange={e => set('encrypt', e.target.value)}>
                  <option value="yes">Yes</option>
                  <option value="no">No</option>
                </Select>
                <Select label="Trust Server Certificate" value={form.trust_server_certificate} onChange={e => set('trust_server_certificate', e.target.value)}>
                  <option value="no">No (production)</option>
                  <option value="yes">Yes (dev/self-signed)</option>
                </Select>
              </div>
            </>
          )}
          {t === 'postgresql' && (
            <Select label="SSL Mode" value={form.sslmode} onChange={e => set('sslmode', e.target.value)}>
              <option value="disable">Disable</option>
              <option value="allow">Allow</option>
              <option value="prefer">Prefer</option>
              <option value="require">Require</option>
              <option value="verify-ca">Verify CA</option>
              <option value="verify-full">Verify Full</option>
            </Select>
          )}
        </>
      )}

      {/* Redis */}
      {isRedis && (
        <>
          <div className="grid grid-cols-3 gap-3">
            <div className="col-span-2">
              <Input label="Host" value={form.host} onChange={e => set('host', e.target.value)} placeholder="redis.example.com"/>
            </div>
            <Input label="Port" value={form.port} onChange={e => set('port', e.target.value)} placeholder="6379" type="number"/>
          </div>
          <Input label="Password" value={form.password} onChange={e => set('password', e.target.value)} type="password"/>
          <Input label="Database Number" value={form.database} onChange={e => set('database', e.target.value)} placeholder="0"/>
        </>
      )}

      {/* S3 */}
      {isS3 && (
        <>
          <div className="grid grid-cols-2 gap-3">
            <Select label="AWS Region" value={form.region} onChange={e => set('region', e.target.value)}>
              {['us-east-1','us-east-2','us-west-1','us-west-2','eu-west-1','eu-central-1','ap-southeast-1','ap-northeast-1'].map(r => (
                <option key={r} value={r}>{r}</option>
              ))}
            </Select>
            <Input label="Default Bucket (optional)" value={form.bucket} onChange={e => set('bucket', e.target.value)} placeholder="my-data-lake"/>
          </div>
          <p className="text-xs text-gray-500 font-medium">Credentials (leave blank for IAM role):</p>
          <div className="grid grid-cols-2 gap-3">
            <Input label="Access Key ID" value={form.access_key_id} onChange={e => set('access_key_id', e.target.value)} placeholder="AKIA…"/>
            <Input label="Secret Access Key" value={form.secret_access_key} onChange={e => set('secret_access_key', e.target.value)} type="password"/>
          </div>
          <Input label="IAM Role ARN (assume-role)" value={form.role_arn} onChange={e => set('role_arn', e.target.value)} placeholder="arn:aws:iam::123456789012:role/NextGenDatabridgeRole"/>
        </>
      )}

      {/* Kafka */}
      {isKafka && (
        <>
          <Input label="Bootstrap Servers" value={form.bootstrap_servers} onChange={e => set('bootstrap_servers', e.target.value)} placeholder="broker1:9092,broker2:9092" hint="Comma-separated host:port pairs"/>
          <div className="grid grid-cols-2 gap-3">
            <Select label="Security Protocol" value={form.security_protocol} onChange={e => set('security_protocol', e.target.value)}>
              <option value="PLAINTEXT">PLAINTEXT</option>
              <option value="SASL_PLAINTEXT">SASL_PLAINTEXT</option>
              <option value="SSL">SSL</option>
              <option value="SASL_SSL">SASL_SSL</option>
            </Select>
            <Select label="SASL Mechanism" value={form.sasl_mechanism} onChange={e => set('sasl_mechanism', e.target.value)}>
              <option value="PLAIN">PLAIN</option>
              <option value="SCRAM-SHA-256">SCRAM-SHA-256</option>
              <option value="SCRAM-SHA-512">SCRAM-SHA-512</option>
              <option value="GSSAPI">GSSAPI</option>
            </Select>
          </div>
          {form.security_protocol.startsWith('SASL') && (
            <div className="grid grid-cols-2 gap-3">
              <Input label="SASL Username" value={form.sasl_username} onChange={e => set('sasl_username', e.target.value)}/>
              <Input label="SASL Password" value={form.sasl_password} onChange={e => set('sasl_password', e.target.value)} type="password"/>
            </div>
          )}
        </>
      )}

      {/* HTTP */}
      {isHttp && (
        <>
          <Input label="Base URL" value={form.base_url} onChange={e => set('base_url', e.target.value)} placeholder="https://api.example.com/v1"/>
          <Select label="Authentication" value={form.auth_type} onChange={e => set('auth_type', e.target.value)}>
            <option value="none">None</option>
            <option value="api_key">API Key (header)</option>
            <option value="basic">Basic Auth</option>
            <option value="bearer">Bearer Token</option>
          </Select>
          {form.auth_type === 'api_key' && (
            <Input label="API Key" value={form.api_key} onChange={e => set('api_key', e.target.value)} type="password"/>
          )}
          {form.auth_type === 'basic' && (
            <div className="grid grid-cols-2 gap-3">
              <Input label="Username" value={form.username} onChange={e => set('username', e.target.value)}/>
              <Input label="Password" value={form.password} onChange={e => set('password', e.target.value)} type="password"/>
            </div>
          )}
          {form.auth_type === 'bearer' && (
            <Input label="Bearer Token" value={form.api_key} onChange={e => set('api_key', e.target.value)} type="password"/>
          )}
        </>
      )}
    </div>
  )
}

export function Connections() {
  const qc = useQueryClient()
  const [showCreate, setShowCreate] = useState(false)
  const [form, setForm] = useState<ConnForm>({ ...DEFAULT_FORM })

  const { data, isLoading } = useQuery({ queryKey: ['connections'], queryFn: getConnections })
  const conns = data?.connections ?? []

  const createMut = useMutation({
    mutationFn: createConnection,
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['connections'] })
      setShowCreate(false)
      setForm({ ...DEFAULT_FORM })
      toast.success('Connection created')
    },
    onError: (e: any) => toast.error(e.response?.data?.detail || 'Failed to create'),
  })
  const testMut = useMutation({
    mutationFn: (id: string) => testConnection(id),
    onSuccess: (res, id) => {
      qc.invalidateQueries({ queryKey: ['connections'] })
      res.success ? toast.success(`${id}: Connected ✓`) : toast.error(`${id}: ${res.error}`)
    },
  })

  function set(k: string, v: string) { setForm(f => ({ ...f, [k]: v })) }

  function buildPayload() {
    const t = form.connection_type
    const base = { connection_id: form.connection_id, name: form.name || form.connection_id, connection_type: t }
    if (['sqlserver','oracle','postgresql','mysql','redis'].includes(t)) {
      const extra: any = {}
      if (t === 'sqlserver') { extra.driver = form.driver; extra.encrypt = form.encrypt; extra.trust_server_certificate = form.trust_server_certificate }
      if (t === 'postgresql') extra.sslmode = form.sslmode
      return { ...base, host: form.host, port: form.port ? parseInt(form.port) : undefined, database: form.database, username: form.username, password: form.password, ...extra }
    }
    if (t === 's3') {
      const ec: any = { region: form.region }
      if (form.bucket)            ec.bucket             = form.bucket
      if (form.access_key_id)     ec.access_key_id      = form.access_key_id
      if (form.secret_access_key) ec.secret_access_key  = form.secret_access_key
      if (form.role_arn)          ec.role_arn           = form.role_arn
      return { ...base, extra_config: ec }
    }
    if (t === 'kafka') {
      const ec: any = { bootstrap_servers: form.bootstrap_servers, security_protocol: form.security_protocol, sasl_mechanism: form.sasl_mechanism }
      if (form.sasl_username) ec.sasl_username = form.sasl_username
      if (form.sasl_password) ec.sasl_password = form.sasl_password
      return { ...base, extra_config: ec }
    }
    if (t === 'http') {
      const ec: any = { base_url: form.base_url, auth_type: form.auth_type }
      if (form.api_key)  ec.api_key  = form.api_key
      if (form.username) ec.username = form.username
      if (form.password) ec.password = form.password
      return { ...base, extra_config: ec }
    }
    return base
  }

  const TypeIcon = CONN_TYPES.find(c => c.value === form.connection_type)?.icon || Database

  return (
    <div className="p-6 space-y-5 overflow-y-auto h-full bg-gray-50">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-gray-900">Connections</h1>
          <p className="text-sm text-gray-500 mt-0.5">Source and target data connections</p>
        </div>
        <Button variant="primary" onClick={() => setShowCreate(true)}>
          <Plus size={14}/> New Connection
        </Button>
      </div>

      <Card>
        {isLoading ? (
          <div className="flex justify-center py-12"><Spinner/></div>
        ) : (
          <Table
            columns={[
              {
                key: 'connection_id', header: 'Connection ID',
                render: v => <span className="font-mono text-sm text-blue-600 font-medium">{v}</span>,
              },
              { key: 'name', header: 'Name', render: v => <span className="text-gray-700">{v}</span> },
              {
                key: 'connection_type', header: 'Type',
                render: v => <StatusBadge status={v}/>,
              },
              {
                key: 'host', header: 'Host / Endpoint',
                render: (v, row) => {
                  const ec = row.extra_config || {}
                  if (row.connection_type === 's3') return <span className="text-xs text-gray-500">{ec.region || '—'} · {ec.bucket || 'any bucket'}</span>
                  if (row.connection_type === 'kafka') return <span className="font-mono text-xs text-gray-500 truncate max-w-40">{ec.bootstrap_servers || '—'}</span>
                  if (row.connection_type === 'http') return <span className="font-mono text-xs text-gray-500 truncate max-w-40">{ec.base_url || '—'}</span>
                  return <span className="font-mono text-xs text-gray-500">{v || '—'}{row.port ? `:${row.port}` : ''}</span>
                },
              },
              {
                key: 'database', header: 'Database',
                render: (v, row) => {
                  if (['s3','kafka','http'].includes(row.connection_type)) return <span className="text-gray-300">—</span>
                  return <span className="text-gray-500 text-xs">{v || '—'}</span>
                },
              },
              {
                key: 'last_test_success', header: 'Last Test',
                render: (v) => (
                  v === true  ? <span className="inline-flex items-center gap-1 text-xs text-emerald-700 bg-emerald-50 border border-emerald-200 px-2 py-0.5 rounded-full"><CheckCircle size={11}/> OK</span> :
                  v === false ? <span className="inline-flex items-center gap-1 text-xs text-red-700 bg-red-50 border border-red-200 px-2 py-0.5 rounded-full"><XCircle size={11}/> Failed</span> :
                  <span className="text-gray-400 text-xs">Not tested</span>
                ),
              },
              {
                key: '_test', header: '',
                render: (_, row) => (
                  <Button
                    size="xs" variant="ghost"
                    onClick={e => { e.stopPropagation(); testMut.mutate(row.connection_id) }}
                  >
                    <FlaskConical size={12}/> Test
                  </Button>
                ),
              },
            ]}
            data={conns}
            emptyMsg="No connections defined — add your first connection to get started"
          />
        )}
      </Card>

      <Modal title="New Data Connection" open={showCreate} onClose={() => { setShowCreate(false); setForm({ ...DEFAULT_FORM }) }} size="lg">
        <ConnectionForm form={form} set={set}/>
        <div className="flex justify-end gap-2 pt-4 mt-4 border-t border-gray-100">
          <Button variant="ghost" onClick={() => { setShowCreate(false); setForm({ ...DEFAULT_FORM }) }}>Cancel</Button>
          <Button
            variant="primary"
            onClick={() => createMut.mutate(buildPayload())}
            disabled={!form.connection_id || createMut.isPending}
          >
            {createMut.isPending ? <Spinner size="sm"/> : <Plus size={14}/>} Create Connection
          </Button>
        </div>
      </Modal>
    </div>
  )
}

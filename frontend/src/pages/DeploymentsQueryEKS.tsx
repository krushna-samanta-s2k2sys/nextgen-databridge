import React, { useState, useEffect } from 'react'
import Editor from '@monaco-editor/react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useLocation } from 'react-router-dom'
import toast from 'react-hot-toast'
import {
  getDeployments, createDeployment, approveDeployment, rejectDeployment,
  queryDuckDB, getDuckDBFiles, getEKSJobs,
} from '../api/client'
import {
  Card, CardHeader, Table, StatusBadge, Button, Modal,
  Input, Select, Textarea, Spinner, TimeAgo, Duration,
} from '../components/ui'
import {
  Rocket, Plus, CheckCircle, XCircle, Play, Server, Database,
  ChevronRight, Download, Copy, AlertCircle, Search, RefreshCw,
  ArrowRight, Clock, Package, Shield,
} from 'lucide-react'
import clsx from 'clsx'

// ─────────────────────────────────────────────────────────────────────────────
// Deployments — multi-environment pipeline with approval gates
// ─────────────────────────────────────────────────────────────────────────────

const ENVS = [
  { key: 'dev',        label: 'Development', color: 'text-blue-700 bg-blue-50 border-blue-200' },
  { key: 'staging',    label: 'Staging',     color: 'text-purple-700 bg-purple-50 border-purple-200' },
  { key: 'production', label: 'Production',  color: 'text-red-700 bg-red-50 border-red-200' },
]

export function Deployments() {
  const qc = useQueryClient()
  const [showCreate, setShowCreate] = useState(false)
  const [rejectModal, setRejectModal] = useState<any>(null)
  const [rejectReason, setRejectReason] = useState('')
  const [envFilter, setEnvFilter] = useState('')
  const [form, setForm] = useState({
    pipeline_id: '', version: '', deployment_type: 'config',
    environment: 'staging', change_description: '', approver_email: '',
  })

  const { data, isLoading } = useQuery({
    queryKey: ['deployments', envFilter],
    queryFn: () => getDeployments(envFilter ? { environment: envFilter } : undefined),
    refetchInterval: 15_000,
  })
  const deps = data?.deployments ?? []

  const createMut = useMutation({
    mutationFn: createDeployment,
    onSuccess: (res) => {
      qc.invalidateQueries({ queryKey: ['deployments'] })
      setShowCreate(false)
      toast.success(`Deployment #${res.deployment_number} submitted — approval email sent`)
    },
    onError: (e: any) => toast.error(e.response?.data?.detail || 'Failed'),
  })
  const approveMut = useMutation({
    mutationFn: (id: string) => approveDeployment(id),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['deployments'] }); toast.success('Approved — deploying now') },
  })
  const rejectMut = useMutation({
    mutationFn: ({ id, reason }: { id: string; reason: string }) => rejectDeployment(id, reason),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['deployments'] }); setRejectModal(null); toast.success('Deployment rejected') },
  })

  function set(k: string, v: string) { setForm(f => ({ ...f, [k]: v })) }

  // Group by environment for pipeline view
  const byEnv: Record<string, any[]> = { dev: [], staging: [], production: [] }
  deps.forEach((d: any) => { if (byEnv[d.environment]) byEnv[d.environment].push(d) })

  const pending = deps.filter((d: any) => d.status === 'pending_approval')

  return (
    <div className="p-6 space-y-6 overflow-y-auto h-full bg-gray-50">
      <div className="flex items-start justify-between">
        <div>
          <h1 className="text-xl font-bold text-gray-900">Deployment Pipeline</h1>
          <p className="text-sm text-gray-500 mt-0.5">
            Multi-environment promotion with approval gates
          </p>
        </div>
        <Button variant="primary" onClick={() => setShowCreate(true)}>
          <Rocket size={14}/> New Deployment
        </Button>
      </div>

      {/* Pending approvals banner */}
      {pending.length > 0 && (
        <div className="bg-amber-50 border border-amber-200 rounded-xl px-5 py-4 flex items-center gap-3">
          <AlertCircle size={18} className="text-amber-600 flex-shrink-0"/>
          <div className="flex-1">
            <p className="text-sm font-semibold text-amber-800">
              {pending.length} deployment{pending.length > 1 ? 's' : ''} awaiting approval
            </p>
            <p className="text-xs text-amber-600 mt-0.5">
              Review and approve or reject below
            </p>
          </div>
        </div>
      )}

      {/* Environment pipeline visualization */}
      <div className="grid grid-cols-3 gap-4">
        {ENVS.map((env, idx) => (
          <div key={env.key} className="relative">
            {idx < ENVS.length - 1 && (
              <div className="hidden md:block absolute top-8 -right-2 z-10">
                <ArrowRight size={16} className="text-gray-300"/>
              </div>
            )}
            <Card>
              <div className="px-4 py-3 border-b border-gray-100 flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <span className={clsx('text-xs font-semibold px-2 py-0.5 rounded-full border', env.color)}>
                    {env.label}
                  </span>
                </div>
                <span className="text-xs text-gray-400">{byEnv[env.key].length} deployments</span>
              </div>
              <div className="divide-y divide-gray-100 max-h-52 overflow-y-auto">
                {byEnv[env.key].length === 0 && (
                  <p className="text-xs text-gray-400 text-center py-6">No deployments</p>
                )}
                {byEnv[env.key].slice(0, 5).map((d: any) => (
                  <div key={d.id} className="px-4 py-2.5">
                    <div className="flex items-center justify-between gap-2 mb-1">
                      <span className="font-mono text-xs text-blue-600 font-medium truncate">{d.pipeline_id}</span>
                      <StatusBadge status={d.status}/>
                    </div>
                    <div className="flex items-center justify-between">
                      <span className="text-xs text-gray-400">v{d.version} · {d.deployment_type}</span>
                      <TimeAgo ts={d.submitted_at}/>
                    </div>
                    {d.status === 'pending_approval' && (
                      <div className="flex gap-1 mt-2">
                        <Button size="xs" variant="primary" onClick={() => approveMut.mutate(d.id)}>
                          <CheckCircle size={10}/> Approve
                        </Button>
                        <Button size="xs" variant="danger" onClick={() => setRejectModal(d)}>
                          <XCircle size={10}/> Reject
                        </Button>
                      </div>
                    )}
                  </div>
                ))}
              </div>
            </Card>
          </div>
        ))}
      </div>

      {/* Full deployment history table */}
      <Card>
        <CardHeader
          title="Deployment History"
          subtitle="All deployments across environments"
          actions={
            <select
              value={envFilter} onChange={e => setEnvFilter(e.target.value)}
              className="bg-white border border-gray-300 rounded-lg px-3 py-1.5 text-xs text-gray-700 focus:outline-none"
            >
              <option value="">All environments</option>
              {ENVS.map(e => <option key={e.key} value={e.key}>{e.label}</option>)}
            </select>
          }
        />
        {isLoading ? (
          <div className="flex justify-center py-12"><Spinner/></div>
        ) : (
          <Table
            columns={[
              {
                key: 'deployment_number', header: '#',
                render: v => <span className="font-mono text-sm font-bold text-gray-700">#{v}</span>,
              },
              {
                key: 'pipeline_id', header: 'Pipeline',
                render: v => <span className="font-mono text-xs text-blue-600 font-medium">{v}</span>,
              },
              {
                key: 'version', header: 'Version',
                render: v => <code className="text-xs bg-gray-100 px-1.5 py-0.5 rounded font-mono text-gray-700">{v}</code>,
              },
              { key: 'environment', header: 'Env', render: v => <StatusBadge status={v}/> },
              {
                key: 'deployment_type', header: 'Type',
                render: v => <span className="text-xs text-gray-600 capitalize">{v}</span>,
              },
              { key: 'status', header: 'Status', render: v => <StatusBadge status={v}/> },
              {
                key: 'submitted_by', header: 'Submitted by',
                render: v => <span className="text-gray-500 text-xs">{v}</span>,
              },
              { key: 'submitted_at', header: 'When', render: v => <TimeAgo ts={v}/> },
              {
                key: '_actions', header: '',
                render: (_, row) => row.status === 'pending_approval' ? (
                  <div className="flex gap-1" onClick={e => e.stopPropagation()}>
                    <Button size="xs" variant="primary" onClick={() => approveMut.mutate(row.id)}>
                      <CheckCircle size={11}/> Approve
                    </Button>
                    <Button size="xs" variant="danger" onClick={() => setRejectModal(row)}>
                      <XCircle size={11}/>
                    </Button>
                  </div>
                ) : null,
              },
            ]}
            data={deps}
            emptyMsg="No deployments yet — submit your first deployment above"
          />
        )}
      </Card>

      {/* Create deployment modal */}
      <Modal title="New Deployment" open={showCreate} onClose={() => setShowCreate(false)} size="lg">
        <div className="space-y-4">
          <div className="bg-blue-50 border border-blue-200 rounded-lg px-4 py-3 flex items-start gap-2">
            <Shield size={14} className="text-blue-600 flex-shrink-0 mt-0.5"/>
            <p className="text-xs text-blue-700">
              An approval email will be sent to the designated approver. The deployment will not execute until approved.
            </p>
          </div>

          <div className="grid grid-cols-2 gap-3">
            <Input label="Pipeline ID *" value={form.pipeline_id} onChange={e => set('pipeline_id', e.target.value)} placeholder="my_pipeline"/>
            <Input label="Version *" value={form.version} onChange={e => set('version', e.target.value)} placeholder="1.2.0"/>
          </div>
          <div className="grid grid-cols-2 gap-3">
            <Select label="Deployment Type" value={form.deployment_type} onChange={e => set('deployment_type', e.target.value)}>
              {[
                ['config',    'Config Change'],
                ['code',      'Code Update'],
                ['container', 'Container Image'],
                ['full',      'Full Stack'],
              ].map(([v, l]) => <option key={v} value={v}>{l}</option>)}
            </Select>
            <Select label="Target Environment *" value={form.environment} onChange={e => set('environment', e.target.value)}>
              {ENVS.map(e => <option key={e.key} value={e.key}>{e.label}</option>)}
            </Select>
          </div>
          <Textarea
            label="Change Description *"
            value={form.change_description}
            onChange={e => set('change_description', e.target.value)}
            placeholder="What changed? Why is this deployment needed?"
            rows={3}
          />
          <Input
            label="Approver Email *"
            value={form.approver_email}
            onChange={e => set('approver_email', e.target.value)}
            placeholder="approver@company.com"
            type="email"
            hint="This person will receive approve/reject links via email"
          />
          <div className="flex justify-end gap-2 pt-2">
            <Button variant="ghost" onClick={() => setShowCreate(false)}>Cancel</Button>
            <Button
              variant="primary"
              onClick={() => createMut.mutate(form)}
              disabled={createMut.isPending || !form.pipeline_id || !form.version || !form.approver_email}
            >
              {createMut.isPending ? <Spinner size="sm"/> : <Rocket size={14}/>} Submit for Approval
            </Button>
          </div>
        </div>
      </Modal>

      {/* Reject modal */}
      <Modal
        title={`Reject Deployment #${rejectModal?.deployment_number}`}
        open={!!rejectModal}
        onClose={() => setRejectModal(null)}
      >
        <div className="space-y-4">
          <Textarea
            label="Rejection Reason *"
            value={rejectReason}
            onChange={e => setRejectReason(e.target.value)}
            rows={4}
            placeholder="Why is this deployment being rejected?"
          />
          <div className="flex justify-end gap-2">
            <Button variant="ghost" onClick={() => setRejectModal(null)}>Cancel</Button>
            <Button
              variant="danger"
              onClick={() => rejectMut.mutate({ id: rejectModal.id, reason: rejectReason })}
              disabled={!rejectReason}
            >
              <XCircle size={14}/> Reject Deployment
            </Button>
          </div>
        </div>
      </Modal>
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// Query Explorer — functional SQL explorer for DuckDB pipeline outputs
// ─────────────────────────────────────────────────────────────────────────────
export function QueryExplorer() {
  const location = useLocation()
  const [selectedFile, setSelectedFile] = useState<any>(null)
  const [sql, setSql] = useState('SELECT *\nFROM result\nLIMIT 100')
  const [result, setResult] = useState<any>(null)
  const [fileFilter, setFileFilter] = useState('')
  const [copied, setCopied] = useState(false)

  // Auto-select file from URL params (when navigating from Tasks page)
  useEffect(() => {
    const params = new URLSearchParams(location.search)
    const path = params.get('path')
    const pipeline = params.get('pipeline')
    const task = params.get('task')
    if (path) {
      setSelectedFile({ s3_path: path, pipeline_id: pipeline || '', task_id: task || '', run_id: params.get('run') || '' })
      setSql(`-- ${task || 'output'}\nSELECT *\nFROM result\nLIMIT 500`)
    }
  }, [location.search])

  const { data: filesData, isLoading: filesLoading, refetch: refetchFiles } = useQuery({
    queryKey: ['duckdb-files', fileFilter],
    queryFn: () => getDuckDBFiles(fileFilter ? { pipeline_id: fileFilter } : undefined),
    refetchInterval: 30_000,
  })
  const files = filesData?.files ?? []

  const queryMut = useMutation({
    mutationFn: () => queryDuckDB({
      pipeline_id: selectedFile?.pipeline_id || '',
      run_id:      selectedFile?.run_id || '',
      task_id:     selectedFile?.task_id || '',
      duckdb_path: selectedFile?.s3_path || '',
      sql,
      limit: 1000,
    }),
    onSuccess: (res) => {
      setResult(res)
      toast.success(`${res.row_count.toLocaleString()} rows · ${res.duration_ms}ms`)
    },
    onError: (e: any) => toast.error(e.response?.data?.detail || 'Query failed'),
  })

  function copyResults() {
    if (!result) return
    const headers = result.columns.join('\t')
    const rows = result.rows.map((r: any[]) => r.map(c => c ?? '').join('\t')).join('\n')
    navigator.clipboard.writeText(`${headers}\n${rows}`)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  function downloadCSV() {
    if (!result) return
    const headers = result.columns.join(',')
    const rows = result.rows.map((r: any[]) =>
      r.map(c => c === null ? '' : String(c).includes(',') ? `"${c}"` : c).join(',')
    ).join('\n')
    const blob = new Blob([`${headers}\n${rows}`], { type: 'text/csv' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `query_result_${Date.now()}.csv`
    a.click()
    URL.revokeObjectURL(url)
  }

  const fileName = selectedFile?.s3_path?.split('/').slice(-1)[0] || ''

  return (
    <div className="flex flex-col h-full overflow-hidden bg-gray-50">
      {/* Header */}
      <div className="flex items-center justify-between px-6 py-4 border-b border-gray-200 bg-white flex-shrink-0">
        <div>
          <h1 className="text-xl font-bold text-gray-900">Query Explorer</h1>
          <p className="text-sm text-gray-500 mt-0.5">Ad-hoc SQL over any DuckDB pipeline output</p>
        </div>
        {selectedFile && (
          <div className="flex items-center gap-2 text-xs text-gray-500 bg-gray-50 border border-gray-200 px-3 py-1.5 rounded-lg">
            <Database size={12} className="text-blue-500"/>
            <span className="font-mono font-medium text-gray-700 truncate max-w-56">{fileName}</span>
          </div>
        )}
      </div>

      <div className="flex flex-1 overflow-hidden">
        {/* File browser panel */}
        <div className="w-64 border-r border-gray-200 bg-white flex flex-col flex-shrink-0">
          <div className="px-3 py-2.5 border-b border-gray-100 flex items-center gap-2">
            <div className="relative flex-1">
              <Search size={12} className="absolute left-2 top-1/2 -translate-y-1/2 text-gray-400"/>
              <input
                value={fileFilter}
                onChange={e => setFileFilter(e.target.value)}
                placeholder="Filter pipeline…"
                className="w-full bg-gray-50 border border-gray-200 rounded-md pl-7 pr-2 py-1.5 text-xs text-gray-700 focus:outline-none focus:ring-1 focus:ring-blue-500 placeholder:text-gray-400"
              />
            </div>
            <button
              onClick={() => refetchFiles()}
              className="text-gray-400 hover:text-gray-600 p-1 rounded hover:bg-gray-100 transition-colors flex-shrink-0"
            >
              <RefreshCw size={12}/>
            </button>
          </div>

          <div className="flex-1 overflow-y-auto">
            {filesLoading && <div className="flex justify-center py-8"><Spinner size="sm"/></div>}
            {!filesLoading && files.length === 0 && (
              <div className="flex flex-col items-center py-10 gap-2 text-gray-300">
                <Database size={20}/>
                <p className="text-xs text-gray-400">No DuckDB files yet</p>
              </div>
            )}
            {files.map((f: any) => {
              const fname = f.s3_path.split('/').slice(-1)[0]
              const isSelected = selectedFile?.s3_path === f.s3_path
              return (
                <div
                  key={f.s3_path}
                  onClick={() => {
                    setSelectedFile(f)
                    setSql(`-- ${f.task_id}\nSELECT *\nFROM result\nLIMIT 100`)
                  }}
                  className={clsx(
                    'px-3 py-2.5 cursor-pointer border-b border-gray-50 transition-colors',
                    isSelected
                      ? 'bg-blue-50 border-l-2 border-l-blue-500'
                      : 'hover:bg-gray-50',
                  )}
                >
                  <div className="font-mono text-xs text-gray-800 truncate font-medium">{fname}</div>
                  <div className="text-xs text-blue-600 mt-0.5 truncate">{f.pipeline_id}</div>
                  <div className="flex items-center gap-2 mt-0.5">
                    <span className="text-xs text-gray-400">{f.task_id}</span>
                    {f.row_count != null && (
                      <span className="text-xs text-gray-400">{f.row_count.toLocaleString()} rows</span>
                    )}
                  </div>
                </div>
              )
            })}
          </div>

          <div className="border-t border-gray-100 px-3 py-2">
            <p className="text-xs text-gray-400">{files.length} file{files.length !== 1 ? 's' : ''} available</p>
          </div>
        </div>

        {/* Editor + results */}
        <div className="flex-1 flex flex-col overflow-hidden">
          {/* SQL editor */}
          <div className="border-b border-gray-200 bg-white flex-shrink-0">
            <div className="flex items-center justify-between px-4 py-2.5 border-b border-gray-100">
              <div className="flex items-center gap-2">
                <span className="text-xs font-semibold text-gray-700">SQL Query</span>
                {selectedFile && (
                  <span className="text-xs text-gray-400">
                    · <span className="font-mono">{fileName}</span>
                  </span>
                )}
              </div>
              <div className="flex items-center gap-2">
                <span className="text-xs text-gray-400 hidden md:block">Ctrl+Enter to run</span>
                <Button
                  variant="primary"
                  size="sm"
                  onClick={() => queryMut.mutate()}
                  disabled={!selectedFile || queryMut.isPending}
                >
                  {queryMut.isPending ? <Spinner size="sm"/> : <Play size={12}/>} Run Query
                </Button>
              </div>
            </div>
            {!selectedFile && (
              <div className="px-4 py-2 bg-amber-50 border-b border-amber-100">
                <p className="text-xs text-amber-700 flex items-center gap-1.5">
                  <AlertCircle size={12}/> Select a file from the panel on the left to run queries
                </p>
              </div>
            )}
            <div className="border-b border-gray-800">
              <Editor
                height="180px"
                language="sql"
                theme="vs-dark"
                value={sql}
                onChange={(v: string | undefined) => setSql(v ?? '')}
                options={{
                  minimap: { enabled: false },
                  fontSize: 13,
                  lineNumbers: 'on',
                  scrollBeyondLastLine: false,
                  wordWrap: 'on',
                  tabSize: 2,
                  automaticLayout: true,
                  padding: { top: 10, bottom: 10 },
                }}
                onMount={(editor: any, monaco: any) => {
                  editor.addCommand(
                    monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter,
                    () => { if (selectedFile) queryMut.mutate() }
                  )
                }}
              />
            </div>
          </div>

          {/* Results */}
          <div className="flex-1 overflow-hidden flex flex-col">
            {queryMut.isPending && (
              <div className="flex items-center justify-center flex-1 gap-3 text-gray-500">
                <Spinner size="md"/>
                <span className="text-sm">Executing query…</span>
              </div>
            )}

            {!queryMut.isPending && !result && (
              <div className="flex flex-col items-center justify-center flex-1 gap-2 text-gray-300">
                <Play size={28}/>
                <p className="text-sm text-gray-400">Run a query to see results</p>
              </div>
            )}

            {!queryMut.isPending && result && (
              <div className="flex flex-col flex-1 overflow-hidden">
                {/* Result bar */}
                <div className="flex items-center justify-between px-4 py-2.5 border-b border-gray-200 bg-white flex-shrink-0">
                  <div className="flex items-center gap-3">
                    <span className="text-xs font-semibold text-gray-700">
                      {result.row_count.toLocaleString()} rows
                    </span>
                    <span className="text-xs text-gray-400">{result.duration_ms}ms</span>
                    {result.columns && (
                      <span className="text-xs text-gray-400">{result.columns.length} columns</span>
                    )}
                  </div>
                  <div className="flex items-center gap-1.5">
                    <Button size="xs" variant="ghost" onClick={copyResults}>
                      {copied ? <CheckCircle size={11} className="text-emerald-500"/> : <Copy size={11}/>}
                      {copied ? 'Copied!' : 'Copy TSV'}
                    </Button>
                    <Button size="xs" variant="secondary" onClick={downloadCSV}>
                      <Download size={11}/> Export CSV
                    </Button>
                  </div>
                </div>

                {/* Result table */}
                <div className="flex-1 overflow-auto">
                  <table className="w-full text-xs border-collapse">
                    <thead className="sticky top-0 bg-gray-50 shadow-sm">
                      <tr>
                        <th className="px-3 py-2 text-left text-gray-400 font-medium border-b border-r border-gray-200 w-10 text-center">#</th>
                        {result.columns.map((c: string) => (
                          <th
                            key={c}
                            className="px-3 py-2 text-left text-gray-600 font-semibold border-b border-r border-gray-200 whitespace-nowrap"
                          >
                            {c}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {result.rows.map((row: any[], i: number) => (
                        <tr key={i} className={clsx('border-b border-gray-100', i % 2 === 0 ? 'bg-white' : 'bg-gray-50/50')}>
                          <td className="px-3 py-2 text-gray-300 font-mono text-right border-r border-gray-100 w-10">{i + 1}</td>
                          {row.map((cell, j) => (
                            <td
                              key={j}
                              className="px-3 py-2 text-gray-700 font-mono whitespace-nowrap max-w-64 truncate border-r border-gray-100"
                              title={cell === null ? 'NULL' : String(cell)}
                            >
                              {cell === null
                                ? <span className="text-gray-300 italic">NULL</span>
                                : typeof cell === 'number'
                                  ? <span className="text-blue-700">{cell}</span>
                                  : String(cell)
                              }
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  {result.row_count === 0 && (
                    <p className="text-center text-gray-400 text-sm py-10">Query returned 0 rows</p>
                  )}
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// EKS Jobs
// ─────────────────────────────────────────────────────────────────────────────
export function EKSJobs() {
  const [statusFilter, setStatusFilter] = useState('')

  const { data, isLoading, refetch } = useQuery({
    queryKey: ['eks-jobs', statusFilter],
    queryFn: () => getEKSJobs(statusFilter ? { status: statusFilter } : undefined),
    refetchInterval: 15_000,
  })
  const jobs = data?.jobs ?? []

  const running = jobs.filter((j: any) => j.status === 'running').length
  const failed  = jobs.filter((j: any) => j.status === 'failed').length

  return (
    <div className="p-6 space-y-5 overflow-y-auto h-full bg-gray-50">
      <div className="flex items-start justify-between">
        <div>
          <h1 className="text-xl font-bold text-gray-900">EKS Jobs</h1>
          <p className="text-sm text-gray-500 mt-0.5">
            Kubernetes job executions · {running > 0 && <span className="text-blue-600 font-medium">{running} running</span>}
            {running > 0 && failed > 0 && ' · '}
            {failed > 0 && <span className="text-red-600 font-medium">{failed} failed</span>}
          </p>
        </div>
        <div className="flex items-center gap-2">
          <select
            value={statusFilter} onChange={e => setStatusFilter(e.target.value)}
            className="bg-white border border-gray-300 rounded-lg px-3 py-2 text-sm text-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-500 shadow-sm"
          >
            <option value="">All statuses</option>
            {['pending','running','success','failed'].map(s => (
              <option key={s} value={s} className="capitalize">{s}</option>
            ))}
          </select>
          <Button variant="secondary" size="sm" onClick={() => refetch()}>
            <RefreshCw size={13}/>
          </Button>
        </div>
      </div>

      {/* Summary cards */}
      <div className="grid grid-cols-4 gap-4">
        {[
          { label: 'Total Jobs',   value: jobs.length,   color: 'border-l-gray-400' },
          { label: 'Running',      value: running,        color: 'border-l-blue-500' },
          { label: 'Succeeded',    value: jobs.filter((j: any) => j.status === 'success').length, color: 'border-l-emerald-500' },
          { label: 'Failed',       value: failed,         color: 'border-l-red-500' },
        ].map(c => (
          <div key={c.label} className={clsx('bg-white border border-gray-200 rounded-xl p-4 shadow-sm border-l-4', c.color)}>
            <p className="text-xs text-gray-500 font-semibold uppercase tracking-wide">{c.label}</p>
            <p className="text-2xl font-bold text-gray-900 mt-1 tabular-nums">{c.value}</p>
          </div>
        ))}
      </div>

      <Card>
        {isLoading ? (
          <div className="flex justify-center py-12"><Spinner size="md"/></div>
        ) : (
          <Table
            columns={[
              {
                key: 'job_name', header: 'Job Name',
                render: v => <span className="font-mono text-xs text-purple-600 truncate block max-w-48">{v}</span>,
              },
              {
                key: 'pipeline_id', header: 'Pipeline',
                render: v => <span className="font-mono text-xs text-blue-600 font-medium">{v}</span>,
              },
              {
                key: 'task_id', header: 'Task',
                render: v => <span className="font-mono text-xs text-gray-600">{v}</span>,
              },
              {
                key: 'image', header: 'Image',
                render: v => (
                  <div className="flex items-center gap-1.5">
                    <Package size={11} className="text-gray-400 flex-shrink-0"/>
                    <span className="font-mono text-xs text-gray-500 truncate max-w-36">
                      {v?.split('/').slice(-1)[0]}
                    </span>
                  </div>
                ),
              },
              {
                key: 'cpu_request', header: 'Resources',
                render: (v, row) => (
                  <span className="text-xs font-mono text-gray-600">
                    {v} CPU · {row.memory_request}
                  </span>
                ),
              },
              { key: 'status', header: 'Status', render: v => <StatusBadge status={v}/> },
              {
                key: 'pod_name', header: 'Pod',
                render: v => v ? (
                  <span className="font-mono text-xs text-gray-400 truncate block max-w-28">{v?.slice(-20)}</span>
                ) : <span className="text-gray-300">—</span>,
              },
              { key: 'submitted_at', header: 'Submitted', render: v => <TimeAgo ts={v}/> },
              {
                key: 'started_at', header: 'Duration',
                render: (v, row) => {
                  if (!v) return <span className="text-gray-300">—</span>
                  const endTs = row.completed_at || new Date().toISOString()
                  const secs = (new Date(endTs).getTime() - new Date(v).getTime()) / 1000
                  return <Duration seconds={secs}/>
                },
              },
            ]}
            data={jobs}
            emptyMsg="No EKS jobs found"
          />
        )}
      </Card>
    </div>
  )
}

import React, { useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import Editor from '@monaco-editor/react'
import { getPipelines, getPipeline } from '../api/client'
import { StatusBadge, Card, Spinner } from '../components/ui'
import {
  Search, Settings2, ChevronRight, ChevronDown, Clock, Tag,
  Users, AlertTriangle, Database, Server, ArrowRight,
  FileJson, X, RefreshCw, XCircle,
} from 'lucide-react'
import clsx from 'clsx'

const TASK_TYPE_COLOR: Record<string, string> = {
  sql_extract:      'bg-blue-50 text-blue-700 border-blue-200',
  file_ingest:      'bg-indigo-50 text-indigo-700 border-indigo-200',
  duckdb_transform: 'bg-violet-50 text-violet-700 border-violet-200',
  data_quality:     'bg-amber-50 text-amber-700 border-amber-200',
  load_target:      'bg-emerald-50 text-emerald-700 border-emerald-200',
  notification:     'bg-gray-100 text-gray-600 border-gray-200',
}

function TaskTypeBadge({ type }: { type: string }) {
  const cls = TASK_TYPE_COLOR[type] ?? 'bg-gray-100 text-gray-600 border-gray-200'
  return (
    <span className={clsx('inline-flex items-center px-2 py-0.5 rounded text-xs font-medium border whitespace-nowrap', cls)}>
      {type.replace(/_/g, ' ')}
    </span>
  )
}

function ErrorBanner({ message }: { message: string }) {
  return (
    <div className="flex items-center gap-2 text-xs text-red-700 bg-red-50 border border-red-200 rounded-lg px-4 py-2.5 m-4">
      <XCircle size={13} className="flex-shrink-0 text-red-500" />
      <span className="font-medium">Error:</span> {message}
    </div>
  )
}

// ── Left panel: pipeline list ─────────────────────────────────────────────────
function PipelineList({
  selectedId, onSelect,
}: {
  selectedId?: string
  onSelect: (id: string) => void
}) {
  const [search, setSearch] = useState('')
  const { data, isLoading, isError, error, refetch } = useQuery({
    queryKey: ['pipelines-list'],
    queryFn: () => getPipelines({ page_size: '100' }),
    staleTime: 30_000,
  })

  const pipelines: any[] = (data?.pipelines ?? []).filter((p: any) => {
    if (!search) return true
    const id = (p.pipeline_id || p.id || '').toLowerCase()
    return id.includes(search.toLowerCase())
  })

  return (
    <div className="flex flex-col h-full border-r border-gray-200 bg-white w-60 flex-shrink-0">
      {/* Search bar */}
      <div className="px-3 py-3 border-b border-gray-100 flex items-center gap-2">
        <div className="relative flex-1">
          <Search size={12} className="absolute left-2.5 top-1/2 -translate-y-1/2 text-gray-400" />
          <input
            value={search}
            onChange={e => setSearch(e.target.value)}
            placeholder="Search pipelines…"
            className="w-full bg-gray-50 border border-gray-200 rounded-lg pl-7 pr-2 py-1.5 text-xs text-gray-700 focus:outline-none focus:ring-1 focus:ring-blue-500 placeholder:text-gray-400"
          />
        </div>
        <button
          onClick={() => refetch()}
          className="text-gray-400 hover:text-gray-600 p-1 rounded hover:bg-gray-100 transition-colors flex-shrink-0"
          title="Refresh"
        >
          <RefreshCw size={12} />
        </button>
      </div>

      {/* List */}
      <div className="flex-1 overflow-y-auto">
        {isLoading && <div className="flex justify-center py-10"><Spinner size="sm" /></div>}
        {isError && (
          <p className="text-xs text-red-500 px-4 py-4">
            {(error as any)?.response?.data?.detail ?? 'Failed to load pipelines'}
          </p>
        )}
        {!isLoading && !isError && pipelines.length === 0 && (
          <div className="flex flex-col items-center py-10 gap-2">
            <Settings2 size={20} className="text-gray-300" />
            <p className="text-xs text-gray-400">{search ? 'No match' : 'No pipelines'}</p>
          </div>
        )}
        {pipelines.map((p: any) => {
          const pid = p.pipeline_id || p.id
          const isSelected = pid === selectedId
          return (
            <div
              key={pid}
              onClick={() => onSelect(pid)}
              className={clsx(
                'px-4 py-3 cursor-pointer border-b border-gray-50 transition-colors',
                isSelected
                  ? 'bg-blue-50 border-l-2 border-l-blue-500'
                  : 'hover:bg-gray-50',
              )}
            >
              <div className="flex items-center justify-between gap-2 mb-0.5">
                <span className="text-xs font-mono font-semibold text-gray-800 truncate">{pid}</span>
                <StatusBadge status={p.status ?? 'active'} />
              </div>
              <p className="text-xs text-gray-400 truncate">{p.schedule || 'no schedule'}</p>
              <p className="text-xs text-gray-400 truncate mt-0.5">{p.version ? `v${p.version}` : ''}</p>
            </div>
          )
        })}
      </div>

      <div className="border-t border-gray-100 px-4 py-2">
        <p className="text-xs text-gray-400">{pipelines.length} pipeline{pipelines.length !== 1 ? 's' : ''}</p>
      </div>
    </div>
  )
}

function getTaskSql(t: any): string | null {
  return t.sql || t.transform_sql || t.source?.query || t.source?.sql || null
}

// ── Right panel: pipeline detail ──────────────────────────────────────────────
function PipelineDetail({ pipelineId }: { pipelineId: string }) {
  const nav = useNavigate()
  const [showJson, setShowJson] = useState(false)
  const [expandedSql, setExpandedSql] = useState<Set<string | number>>(new Set())
  function toggleSql(key: string | number) {
    setExpandedSql(prev => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key); else next.add(key)
      return next
    })
  }

  const { data: pipeline, isLoading, isError, error } = useQuery({
    queryKey: ['pipeline', pipelineId],
    queryFn: () => getPipeline(pipelineId),
    enabled: !!pipelineId,
    staleTime: 30_000,
  })

  if (isLoading) return <div className="flex justify-center py-20"><Spinner size="md" /></div>
  if (isError) return <ErrorBanner message={(error as any)?.response?.data?.detail ?? 'Failed to load pipeline'} />
  if (!pipeline) return null

  const p     = pipeline
  const tasks = p.tasks ?? p.config?.tasks ?? []
  const cfg   = p.config ?? p

  return (
    <div className="flex-1 overflow-y-auto p-6 space-y-5">
      {/* Header */}
      <div className="flex items-start justify-between">
        <div>
          <div className="flex items-center gap-3 mb-1">
            <h2 className="text-lg font-bold text-gray-900 font-mono">{p.pipeline_id || p.id}</h2>
            <StatusBadge status={p.status ?? 'active'} />
          </div>
          {p.description && <p className="text-sm text-gray-500">{p.description}</p>}
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={() => nav(`/runs?pipeline_id=${p.pipeline_id || p.id}`)}
            className="flex items-center gap-1.5 text-xs text-blue-600 hover:text-blue-700 bg-blue-50 hover:bg-blue-100 border border-blue-200 px-3 py-1.5 rounded-lg transition-colors"
          >
            View runs <ArrowRight size={11} />
          </button>
          <button
            onClick={() => setShowJson(true)}
            className="flex items-center gap-1.5 text-xs text-gray-600 hover:text-gray-800 bg-white hover:bg-gray-50 border border-gray-300 px-3 py-1.5 rounded-lg transition-colors shadow-sm"
          >
            <FileJson size={12} /> View JSON
          </button>
        </div>
      </div>

      {/* Metadata grid */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        {[
          { icon: Clock,        label: 'Schedule',  value: p.schedule || cfg.schedule || '—' },
          { icon: AlertTriangle,label: 'SLA',       value: (p.sla_minutes || cfg.sla_minutes) ? `${p.sla_minutes || cfg.sla_minutes} min` : '—' },
          { icon: Database,     label: 'Version',   value: p.version || cfg.version || '—' },
          { icon: Users,        label: 'Owner',     value: p.owner || cfg.owner || '—' },
        ].map(({ icon: Icon, label, value }) => (
          <div key={label} className="bg-white border border-gray-200 rounded-xl p-4 shadow-sm">
            <div className="flex items-center gap-1.5 mb-1">
              <Icon size={12} className="text-gray-400" />
              <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide">{label}</p>
            </div>
            <p className="text-sm font-mono text-gray-800 truncate">{value}</p>
          </div>
        ))}
      </div>

      {/* Tags */}
      {(p.tags ?? cfg.tags ?? []).length > 0 && (
        <div className="flex items-center gap-2 flex-wrap">
          <Tag size={12} className="text-gray-400" />
          {(p.tags ?? cfg.tags ?? []).map((t: string) => (
            <span key={t} className="text-xs bg-gray-100 text-gray-600 border border-gray-200 px-2 py-0.5 rounded-full">{t}</span>
          ))}
        </div>
      )}

      {/* Tasks */}
      <Card>
        <div className="px-5 py-4 border-b border-gray-100 flex items-center justify-between">
          <div>
            <h3 className="text-sm font-semibold text-gray-900">Tasks</h3>
            <p className="text-xs text-gray-500 mt-0.5">{tasks.length} task{tasks.length !== 1 ? 's' : ''} in pipeline</p>
          </div>
        </div>
        <div className="divide-y divide-gray-100">
          {tasks.length === 0 && (
            <p className="text-center text-xs text-gray-400 py-8">No tasks defined</p>
          )}
          {tasks.map((t: any, i: number) => (
            <div key={t.task_id ?? i} className="px-5 py-3.5 flex items-start gap-4">
              <div className="w-6 h-6 rounded-full bg-gray-100 border border-gray-200 flex items-center justify-center flex-shrink-0 mt-0.5">
                <span className="text-xs font-bold text-gray-500">{i + 1}</span>
              </div>
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2 flex-wrap mb-1">
                  <span className="text-sm font-mono font-semibold text-gray-800">{t.task_id}</span>
                  <TaskTypeBadge type={t.type} />
                  {t.engine === 'eks' && (
                    <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium bg-purple-50 text-purple-700 border border-purple-200">
                      <Server size={10} /> EKS
                    </span>
                  )}
                </div>
                {t.depends_on?.length > 0 && (
                  <div className="flex items-center gap-1 flex-wrap">
                    <span className="text-xs text-gray-400">depends on:</span>
                    {t.depends_on.map((dep: string) => (
                      <span key={dep} className="text-xs font-mono text-blue-600 bg-blue-50 border border-blue-100 px-1.5 py-0.5 rounded">
                        {dep}
                      </span>
                    ))}
                  </div>
                )}
                {t.source?.connection && (
                  <p className="text-xs text-gray-400 mt-0.5">
                    source: <span className="font-mono">{t.source.connection}</span>
                  </p>
                )}
                {t.target && (
                  <p className="text-xs text-gray-400 mt-0.5">
                    target: <span className="font-mono">{t.target.connection}</span>
                    {t.target.table && <> → <span className="font-mono">{t.target.schema}.{t.target.table}</span></>}
                  </p>
                )}
                {getTaskSql(t) && (() => {
                  const key = t.task_id ?? i
                  const sql = getTaskSql(t)!
                  const isOpen = expandedSql.has(key)
                  return (
                    <div className="mt-2">
                      <button
                        onClick={() => toggleSql(key)}
                        className="flex items-center gap-1 text-xs text-blue-600 hover:text-blue-800 font-medium transition-colors"
                      >
                        {isOpen ? <ChevronDown size={11} /> : <ChevronRight size={11} />}
                        {isOpen ? 'Hide SQL' : 'Show SQL'}
                      </button>
                      {isOpen && (
                        <pre className="mt-1.5 text-xs font-mono bg-gray-900 text-gray-100 rounded-lg px-3 py-2.5 overflow-x-auto max-h-72 leading-relaxed whitespace-pre-wrap break-words">
                          {sql}
                        </pre>
                      )}
                    </div>
                  )
                })()}
              </div>
              {t.execution && (
                <div className="text-right flex-shrink-0">
                  <p className="text-xs text-gray-400 font-mono">{t.execution.cpu} CPU</p>
                  <p className="text-xs text-gray-400 font-mono">{t.execution.memory}</p>
                </div>
              )}
            </div>
          ))}
        </div>
      </Card>

      {/* JSON Config Modal */}
      {showJson && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-6 bg-black/50">
          <div className="bg-white rounded-2xl shadow-2xl w-full max-w-4xl">
            <div className="flex items-center justify-between px-5 py-4 border-b border-gray-200">
              <div className="flex items-center gap-2">
                <FileJson size={15} className="text-blue-600" />
                <h3 className="text-sm font-semibold text-gray-900">Pipeline Config JSON</h3>
                <span className="text-xs text-gray-400 font-mono">{p.pipeline_id || p.id}</span>
              </div>
              <button
                onClick={() => setShowJson(false)}
                className="text-gray-400 hover:text-gray-600 p-1 rounded hover:bg-gray-100"
              >
                <X size={16} />
              </button>
            </div>
            <div className="overflow-hidden rounded-b-2xl">
              <Editor
                height="calc(85vh - 72px)"
                language="json"
                theme="vs-dark"
                value={JSON.stringify(cfg, null, 2)}
                options={{
                  readOnly: true,
                  minimap: { enabled: false },
                  fontSize: 12,
                  lineNumbers: 'on',
                  scrollBeyondLastLine: false,
                  wordWrap: 'on',
                  folding: true,
                  automaticLayout: true,
                  padding: { top: 12, bottom: 12 },
                }}
              />
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

// ── Route: /pipelines ─────────────────────────────────────────────────────────
export function ConfiguredPipelines() {
  const nav = useNavigate()
  return (
    <div className="flex h-full overflow-hidden bg-gray-50">
      <PipelineList onSelect={id => nav(`/pipelines/${id}`)} />
      <div className="flex-1 flex items-center justify-center">
        <div className="text-center">
          <Settings2 size={32} className="text-gray-200 mx-auto mb-3" />
          <p className="text-sm font-medium text-gray-400">Select a pipeline</p>
          <p className="text-xs text-gray-300 mt-1">Click any pipeline on the left to view its configuration</p>
        </div>
      </div>
    </div>
  )
}

// ── Route: /pipelines/:id ─────────────────────────────────────────────────────
export function PipelineConfigDetail() {
  const { id } = useParams<{ id: string }>()
  const nav    = useNavigate()
  return (
    <div className="flex h-full overflow-hidden bg-gray-50">
      <PipelineList selectedId={id} onSelect={pid => nav(`/pipelines/${pid}`)} />
      {id
        ? <PipelineDetail pipelineId={id} />
        : <div className="flex-1 flex items-center justify-center text-gray-400 text-sm">Select a pipeline</div>
      }
    </div>
  )
}

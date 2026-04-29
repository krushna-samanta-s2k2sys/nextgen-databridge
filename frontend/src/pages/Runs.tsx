import React, { useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { getRuns, getRun } from '../api/client'
import { Card, CardHeader, StatusBadge, Spinner, Duration, TimeAgo } from '../components/ui'
import {
  CheckCircle, XCircle, Database, ChevronRight, ChevronDown,
  Filter, Activity, Search, ExternalLink,
} from 'lucide-react'
import clsx from 'clsx'

function ErrorBanner({ message }: { message: string }) {
  return (
    <div className="flex items-center gap-2 text-xs text-red-700 bg-red-50 border border-red-200 rounded-lg px-4 py-2.5">
      <XCircle size={13} className="flex-shrink-0 text-red-500" />
      <span className="font-medium">Error:</span> {message}
    </div>
  )
}

// ── Runs list page ────────────────────────────────────────────────────────────
export function Runs() {
  const nav = useNavigate()
  const [statusFilter, setStatusFilter]     = useState('')
  const [pipelineFilter, setPipelineFilter] = useState('')

  const { data, isLoading, isError, error } = useQuery({
    queryKey: ['runs', statusFilter, pipelineFilter],
    queryFn: () => getRuns({
      ...(statusFilter   ? { status:      statusFilter   } : {}),
      ...(pipelineFilter ? { pipeline_id: pipelineFilter } : {}),
      page_size: '100',
    }),
    refetchInterval: 10_000,
  })
  const runs = data?.runs ?? []

  const byStatus = {
    running: runs.filter((r: any) => r.status === 'running').length,
    failed:  runs.filter((r: any) => r.status === 'failed').length,
    success: runs.filter((r: any) => r.status === 'success').length,
  }

  return (
    <div className="p-6 space-y-5 overflow-y-auto h-full bg-gray-50">
      <div className="flex items-start justify-between">
        <div>
          <h1 className="text-xl font-bold text-gray-900">Pipeline Runs</h1>
          <p className="text-sm text-gray-500 mt-0.5">
            {runs.length} runs
            {byStatus.running > 0 && <span className="ml-2 text-blue-600 font-medium">· {byStatus.running} running</span>}
            {byStatus.failed  > 0 && <span className="ml-2 text-red-600 font-medium">· {byStatus.failed} failed</span>}
          </p>
        </div>
        <div className="flex items-center gap-2 text-xs text-gray-500 bg-white border border-gray-200 px-3 py-1.5 rounded-lg shadow-sm">
          <Activity size={12} className="text-blue-500 animate-pulse" />
          Auto-refresh 10s
        </div>
      </div>

      {isError && <ErrorBanner message={(error as any)?.response?.data?.detail ?? 'Failed to load runs'} />}

      {/* Filters */}
      <div className="flex gap-3 items-center flex-wrap">
        <div className="flex items-center gap-1.5 text-xs text-gray-500">
          <Filter size={13} /> Filter:
        </div>
        <select
          value={statusFilter}
          onChange={e => setStatusFilter(e.target.value)}
          className="bg-white border border-gray-300 rounded-lg px-3 py-2 text-sm text-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-500 shadow-sm"
        >
          <option value="">All statuses</option>
          {['running', 'queued', 'success', 'failed', 'cancelled'].map(s => (
            <option key={s} value={s}>{s}</option>
          ))}
        </select>
        <div className="relative">
          <Search size={12} className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" />
          <input
            value={pipelineFilter}
            onChange={e => setPipelineFilter(e.target.value)}
            placeholder="Filter by pipeline ID…"
            className="bg-white border border-gray-300 rounded-lg pl-8 pr-3 py-2 text-sm text-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-500 placeholder:text-gray-400 w-56 shadow-sm"
          />
        </div>
        {(statusFilter || pipelineFilter) && (
          <button
            onClick={() => { setStatusFilter(''); setPipelineFilter('') }}
            className="text-xs text-gray-500 hover:text-gray-700 underline"
          >
            Clear filters
          </button>
        )}
      </div>

      <Card>
        {isLoading ? (
          <div className="flex justify-center py-16"><Spinner size="md" /></div>
        ) : runs.length === 0 ? (
          <div className="flex flex-col items-center py-16 gap-2 text-gray-300">
            <Activity size={28} />
            <p className="text-sm text-gray-400">No runs found</p>
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-gray-100 bg-gray-50/50">
                  <th className="px-5 py-3 text-left text-gray-500 font-semibold">Pipeline</th>
                  <th className="px-4 py-3 text-left text-gray-500 font-semibold">Run ID</th>
                  <th className="px-4 py-3 text-left text-gray-500 font-semibold">Status</th>
                  <th className="px-4 py-3 text-left text-gray-500 font-semibold">Started</th>
                  <th className="px-4 py-3 text-left text-gray-500 font-semibold">Duration</th>
                  <th className="px-4 py-3 text-left text-gray-500 font-semibold">Tasks</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {runs.map((r: any) => (
                  <tr
                    key={r.run_id}
                    className="hover:bg-blue-50/40 cursor-pointer transition-colors"
                    onClick={() => nav(`/runs/${r.run_id}`)}
                  >
                    <td className="px-5 py-3 font-mono font-semibold text-blue-600">{r.pipeline_id}</td>
                    <td className="px-4 py-3 font-mono text-gray-500 text-xs break-all" title={r.run_id}>{r.run_id}</td>
                    <td className="px-4 py-3"><StatusBadge status={r.status} /></td>
                    <td className="px-4 py-3 text-gray-500"><TimeAgo ts={r.start_time} /></td>
                    <td className="px-4 py-3 text-gray-500"><Duration seconds={r.duration_seconds} /></td>
                    <td className="px-4 py-3 font-mono">
                      <span className="text-emerald-600">{r.completed_tasks ?? 0}</span>
                      <span className="text-gray-400">/{r.total_tasks ?? 0}</span>
                      {r.failed_tasks > 0 && <span className="text-red-500 ml-1">({r.failed_tasks}✗)</span>}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Card>
    </div>
  )
}

// ── Run detail page ───────────────────────────────────────────────────────────
export function RunDetail() {
  const { runId } = useParams<{ runId: string }>()
  const nav = useNavigate()

  // Hooks must be at top level — before any conditional returns
  const [expandedItems, setExpandedItems] = useState<Set<string>>(new Set())
  function toggleItem(key: string) {
    setExpandedItems(prev => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key); else next.add(key)
      return next
    })
  }

  const { data: run, isLoading, isError, error } = useQuery({
    queryKey: ['run', runId],
    queryFn: () => getRun(runId!),
    enabled: !!runId,
    refetchInterval: (data: any) => data?.status === 'running' ? 5_000 : false,
  })

  if (isLoading) return <div className="flex justify-center py-20 bg-gray-50 h-full"><Spinner size="lg" /></div>
  if (isError) return (
    <div className="p-6 bg-gray-50 h-full">
      <ErrorBanner message={(error as any)?.response?.data?.detail ?? 'Failed to load run'} />
    </div>
  )
  if (!run) return <div className="p-6 text-gray-500 bg-gray-50 h-full">Run not found</div>

  const tasks     = run.tasks ?? []
  const failed    = tasks.filter((t: any) => t.status === 'failed')
  const running   = tasks.filter((t: any) => t.status === 'running')
  const succeeded = tasks.filter((t: any) => t.status === 'success')

  function queryLink(t: any): string {
    if (!t.output_duckdb_path) return ''
    const params = new URLSearchParams({
      path:     t.output_duckdb_path,
      task:     t.task_id,
      pipeline: run.pipeline_id,
      run:      runId ?? '',
      table:    t.output_table ?? 'result',
    })
    return `/query?${params.toString()}`
  }

  return (
    <div className="p-6 space-y-5 overflow-y-auto h-full bg-gray-50">
      {/* Breadcrumb */}
      <div className="flex items-center gap-2 text-xs text-gray-400">
        <span className="cursor-pointer hover:text-blue-600 transition-colors" onClick={() => nav('/runs')}>
          Pipeline Runs
        </span>
        <ChevronRight size={12} />
        <span className="cursor-pointer hover:text-blue-600 transition-colors" onClick={() => nav(`/pipelines/${run.pipeline_id}`)}>
          {run.pipeline_id}
        </span>
        <ChevronRight size={12} />
        <span className="text-gray-700 font-mono">{runId?.slice(-20)}</span>
      </div>

      {/* Run summary */}
      <div className="bg-white border border-gray-200 rounded-xl p-5 shadow-sm">
        <div className="flex items-center gap-3 mb-4">
          <h1 className="text-lg font-bold text-gray-900 font-mono">{run.pipeline_id}</h1>
          <StatusBadge status={run.status} />
        </div>
        <p className="text-xs text-gray-400 font-mono mb-4 break-all">{runId}</p>
        <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
          <div>
            <p className="text-xs text-gray-400 mb-0.5">Started</p>
            <p className="text-sm font-medium text-gray-700"><TimeAgo ts={run.start_time} /></p>
          </div>
          <div>
            <p className="text-xs text-gray-400 mb-0.5">Duration</p>
            <p className="text-sm font-medium text-gray-700"><Duration seconds={run.duration_seconds} /></p>
          </div>
          <div>
            <p className="text-xs text-gray-400 mb-0.5">Tasks</p>
            <p className="text-sm font-mono">
              <span className="text-emerald-600 font-semibold">{succeeded.length}</span>
              <span className="text-gray-400">/{tasks.length}</span>
              {failed.length > 0  && <span className="text-red-500 ml-2">{failed.length} failed</span>}
              {running.length > 0 && <span className="text-blue-500 ml-2">{running.length} running</span>}
            </p>
          </div>
        </div>
      </div>

      {/* Task list */}
      <Card>
        <CardHeader
          title="Task Execution"
          subtitle={`${tasks.length} tasks · ${failed.length} failed · ${running.length} running`}
        />
        <div className="divide-y divide-gray-100">
          {tasks.length === 0 && (
            <p className="text-center text-gray-400 text-sm py-10">No task details available</p>
          )}
          {tasks.map((t: any) => {
            const link = queryLink(t)
            return (
              <div key={t.task_run_id} className="px-5 py-3.5 flex items-start gap-4">
                {/* Status icon */}
                <div className="w-5 flex-shrink-0 pt-0.5">
                  {t.status === 'success' && <CheckCircle size={15} className="text-emerald-500" />}
                  {t.status === 'failed'  && <XCircle size={15} className="text-red-500" />}
                  {t.status === 'running' && (
                    <div className="w-3.5 h-3.5 border-2 border-blue-500 border-t-transparent rounded-full animate-spin" />
                  )}
                  {['pending', 'skipped', 'upstream_failed'].includes(t.status) && (
                    <div className="w-3.5 h-3.5 rounded-full bg-gray-200 border border-gray-300" />
                  )}
                </div>

                {/* Task info */}
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2 flex-wrap">
                    <span className="text-sm font-mono font-semibold text-gray-800">{t.task_id}</span>
                    <span className="text-xs text-gray-400 bg-gray-100 border border-gray-200 px-1.5 py-0.5 rounded">
                      {t.task_type?.replace(/_/g, ' ')}
                    </span>
                    <StatusBadge status={t.status} />
                    {t.attempt_number > 1 && (
                      <span className="text-xs text-amber-600 bg-amber-50 border border-amber-200 px-1.5 py-0.5 rounded">
                        attempt {t.attempt_number}
                      </span>
                    )}
                  </div>

                  {t.error_message && (() => {
                    const key = `${t.task_run_id}_err`
                    const expanded = expandedItems.has(key)
                    return (
                      <div className="mt-1.5">
                        <p className={clsx(
                          'text-xs text-red-600 font-mono bg-red-50 border border-red-100 px-2.5 py-1.5 rounded-lg',
                          !expanded && 'truncate',
                          expanded && 'whitespace-pre-wrap break-words',
                        )}>
                          {t.error_message}
                        </p>
                        <button onClick={() => toggleItem(key)}
                          className="flex items-center gap-0.5 text-xs text-red-400 hover:text-red-600 mt-0.5 transition-colors">
                          {expanded ? <><ChevronDown size={10} /> Show less</> : <><ChevronRight size={10} /> Show full error</>}
                        </button>
                      </div>
                    )
                  })()}

                  {(t.sql || t.transform_sql || t.query) && (() => {
                    const key = `${t.task_run_id}_sql`
                    const expanded = expandedItems.has(key)
                    const sqlText = t.sql || t.transform_sql || t.query
                    return (
                      <div className="mt-1.5">
                        <button onClick={() => toggleItem(key)}
                          className="flex items-center gap-0.5 text-xs text-gray-400 hover:text-blue-600 transition-colors">
                          {expanded ? <><ChevronDown size={10} /> Hide SQL</> : <><ChevronRight size={10} /> Show SQL</>}
                        </button>
                        {expanded && (
                          <pre className="mt-1 text-xs font-mono bg-gray-900 text-gray-100 rounded-lg px-3 py-2.5 overflow-x-auto max-h-56 leading-relaxed whitespace-pre-wrap break-words">
                            {sqlText}
                          </pre>
                        )}
                      </div>
                    )
                  })()}

                  {t.output_row_count != null && (
                    <p className="text-xs text-gray-500 mt-1 flex items-center gap-1.5">
                      <Database size={10} />
                      <span className="font-mono font-medium">{t.output_row_count.toLocaleString()} rows</span>
                      {t.output_duckdb_path && (
                        <span className="font-mono text-gray-400">
                          · {t.output_duckdb_path.split('/').slice(-1)[0]}
                        </span>
                      )}
                    </p>
                  )}

                  {t.qc_results && (
                    <p className="text-xs mt-1">
                      QC: {t.qc_passed
                        ? <span className="text-emerald-600 font-medium">all checks passed</span>
                        : <span className="text-red-600 font-medium">{t.qc_failures} failed, {t.qc_warnings} warnings</span>
                      }
                    </p>
                  )}
                </div>

                {/* Right: duration + query link */}
                <div className="flex-shrink-0 text-right flex flex-col items-end gap-1.5">
                  <Duration seconds={t.duration_seconds} />
                  {link && (
                    <button
                      onClick={() => nav(link)}
                      className="flex items-center gap-1 text-xs text-blue-600 hover:text-blue-700 bg-blue-50 hover:bg-blue-100 border border-blue-200 px-2 py-1 rounded transition-colors"
                    >
                      <Database size={10} /> Query <ExternalLink size={9} />
                    </button>
                  )}
                </div>
              </div>
            )
          })}
        </div>
      </Card>
    </div>
  )
}

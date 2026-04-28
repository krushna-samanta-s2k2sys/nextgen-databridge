import React, { useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import toast from 'react-hot-toast'
import { getRuns, getRun, rerunTask } from '../api/client'
import { Card, CardHeader, Table, StatusBadge, Button, Modal, Select, Textarea, Spinner, Duration, TimeAgo } from '../components/ui'
import { RotateCw, ChevronRight, Database, CheckCircle, XCircle, Filter } from 'lucide-react'

export function Runs() {
  const nav = useNavigate()
  const [statusFilter, setStatusFilter] = useState('')
  const [pipelineFilter, setPipelineFilter] = useState('')

  const { data, isLoading } = useQuery({
    queryKey: ['runs', statusFilter, pipelineFilter],
    queryFn: () => getRuns({
      ...(statusFilter ? { status: statusFilter } : {}),
      ...(pipelineFilter ? { pipeline_id: pipelineFilter } : {}),
      page_size: '50',
    }),
    refetchInterval: 10_000,
  })
  const runs = data?.runs ?? []

  return (
    <div className="p-6 space-y-5 overflow-y-auto h-full bg-gray-50">
      <div className="flex items-start justify-between">
        <div>
          <h1 className="text-xl font-bold text-gray-900">Pipeline Runs</h1>
          <p className="text-sm text-gray-500 mt-0.5">{runs.length} runs</p>
        </div>
      </div>

      {/* Filters */}
      <div className="flex gap-3 items-center">
        <div className="flex items-center gap-1.5 text-xs text-gray-500">
          <Filter size={13} />
          Filter:
        </div>
        <select
          value={statusFilter} onChange={e => setStatusFilter(e.target.value)}
          className="bg-white border border-gray-300 rounded-lg px-3 py-2 text-sm text-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-500 shadow-sm"
        >
          <option value="">All statuses</option>
          {['running','queued','success','failed','cancelled'].map(s => (
            <option key={s} value={s} className="capitalize">{s}</option>
          ))}
        </select>
        <input
          value={pipelineFilter} onChange={e => setPipelineFilter(e.target.value)}
          placeholder="Filter by pipeline ID…"
          className="bg-white border border-gray-300 rounded-lg px-3 py-2 text-sm text-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-500 placeholder:text-gray-400 w-64 shadow-sm"
        />
        {(statusFilter || pipelineFilter) && (
          <button
            onClick={() => { setStatusFilter(''); setPipelineFilter('') }}
            className="text-xs text-gray-500 hover:text-gray-700 underline"
          >
            Clear
          </button>
        )}
      </div>

      <Card>
        {isLoading ? (
          <div className="flex justify-center py-12"><Spinner size="md"/></div>
        ) : (
          <Table
            onRowClick={r => nav(`/runs/${r.run_id}`)}
            columns={[
              {
                key: 'pipeline_id', header: 'Pipeline',
                render: v => <span className="font-mono text-xs text-blue-600 font-medium">{v}</span>,
              },
              {
                key: 'run_id', header: 'Run ID',
                render: v => <span className="font-mono text-xs text-gray-500 truncate block max-w-40">{v?.slice(-20)}</span>,
              },
              { key: 'status', header: 'Status', render: v => <StatusBadge status={v}/> },
              {
                key: 'trigger_type', header: 'Trigger',
                render: v => <span className="text-gray-500 text-xs capitalize">{v}</span>,
              },
              {
                key: 'triggered_by', header: 'By',
                render: v => <span className="text-gray-500 text-xs">{v || '—'}</span>,
              },
              { key: 'start_time', header: 'Started', render: v => <TimeAgo ts={v}/> },
              { key: 'duration_seconds', header: 'Duration', render: v => <Duration seconds={v}/> },
              {
                key: 'total_tasks', header: 'Tasks',
                render: (v, row) => (
                  <span className="text-xs font-mono">
                    <span className="text-emerald-600">{row.completed_tasks}</span>
                    <span className="text-gray-400">/{v || 0}</span>
                    {row.failed_tasks > 0 && <span className="text-red-500"> ({row.failed_tasks}✗)</span>}
                  </span>
                ),
              },
              {
                key: 'total_rows_processed', header: 'Rows',
                render: v => <span className="font-mono text-xs text-gray-700">{(v||0).toLocaleString()}</span>,
              },
            ]}
            data={runs}
            emptyMsg="No runs found"
          />
        )}
      </Card>
    </div>
  )
}

export function RunDetail() {
  const { runId } = useParams<{ runId: string }>()
  const nav = useNavigate()
  const qc = useQueryClient()
  const [rerunModal, setRerunModal] = useState<any>(null)
  const [rerunMode, setRerunMode] = useState('downstream')
  const [rerunReason, setRerunReason] = useState('')

  const { data: run, isLoading } = useQuery({
    queryKey: ['run', runId],
    queryFn: () => getRun(runId!),
    enabled: !!runId,
    refetchInterval: (data: any) => data?.status === 'running' ? 5_000 : false,
  })

  const rerunMut = useMutation({
    mutationFn: () => rerunTask(runId!, { task_id: rerunModal?.task_id, mode: rerunMode, reason: rerunReason }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['run', runId] })
      setRerunModal(null)
      toast.success('Rerun submitted')
    },
    onError: (e: any) => toast.error(e.response?.data?.detail || 'Rerun failed'),
  })

  if (isLoading) return (
    <div className="flex justify-center py-20 bg-gray-50 h-full"><Spinner size="lg"/></div>
  )
  if (!run) return (
    <div className="p-6 text-gray-500 bg-gray-50 h-full">Run not found</div>
  )

  const tasks = run.tasks || []
  const failed = tasks.filter((t: any) => t.status === 'failed')
  const running = tasks.filter((t: any) => t.status === 'running')
  const succeeded = tasks.filter((t: any) => t.status === 'success')

  return (
    <div className="p-6 space-y-5 overflow-y-auto h-full bg-gray-50">
      {/* Breadcrumb */}
      <div className="flex items-center gap-2 text-xs text-gray-400">
        <span className="cursor-pointer hover:text-blue-600 transition-colors" onClick={() => nav('/runs')}>
          Pipeline Runs
        </span>
        <ChevronRight size={12}/>
        <span className="cursor-pointer hover:text-blue-600 transition-colors" onClick={() => nav(`/pipelines/${run.pipeline_id}`)}>
          {run.pipeline_id}
        </span>
        <ChevronRight size={12}/>
        <span className="text-gray-700 font-mono">{runId?.slice(-16)}</span>
      </div>

      {/* Run summary */}
      <div className="bg-white border border-gray-200 rounded-xl p-5 shadow-sm">
        <div className="flex items-start justify-between mb-4">
          <div>
            <div className="flex items-center gap-3 mb-1">
              <h1 className="text-lg font-bold text-gray-900">{run.pipeline_id}</h1>
              <StatusBadge status={run.status}/>
            </div>
            <p className="text-xs text-gray-500 font-mono">{runId}</p>
          </div>
        </div>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <div>
            <p className="text-xs text-gray-400 mb-0.5">Started</p>
            <p className="text-sm font-medium text-gray-700"><TimeAgo ts={run.start_time}/></p>
          </div>
          <div>
            <p className="text-xs text-gray-400 mb-0.5">Duration</p>
            <p className="text-sm font-medium text-gray-700"><Duration seconds={run.duration_seconds}/></p>
          </div>
          <div>
            <p className="text-xs text-gray-400 mb-0.5">Rows Processed</p>
            <p className="text-sm font-mono font-semibold text-gray-900">{(run.total_rows_processed||0).toLocaleString()}</p>
          </div>
          <div>
            <p className="text-xs text-gray-400 mb-0.5">Tasks</p>
            <p className="text-sm font-mono">
              <span className="text-emerald-600 font-semibold">{succeeded.length}</span>
              <span className="text-gray-400">/{tasks.length}</span>
              {failed.length > 0 && <span className="text-red-500 ml-1">{failed.length} failed</span>}
              {running.length > 0 && <span className="text-blue-500 ml-1">{running.length} running</span>}
            </p>
          </div>
        </div>
      </div>

      {/* Task execution list */}
      <Card>
        <CardHeader
          title="Task Execution"
          subtitle={`${tasks.length} tasks · ${failed.length} failed · ${running.length} running`}
        />
        <div className="divide-y divide-gray-100">
          {tasks.map((t: any) => (
            <div key={t.task_run_id} className="px-5 py-3.5 flex items-center gap-4 hover:bg-gray-50 transition-colors">
              <div className="w-5 flex-shrink-0">
                {t.status === 'success' && <CheckCircle size={15} className="text-emerald-500"/>}
                {t.status === 'failed' && <XCircle size={15} className="text-red-500"/>}
                {t.status === 'running' && (
                  <div className="w-3.5 h-3.5 border-2 border-blue-500 border-t-transparent rounded-full animate-spin"/>
                )}
                {['pending','skipped','upstream_failed'].includes(t.status) && (
                  <div className="w-3.5 h-3.5 rounded-full bg-gray-200 border border-gray-300"/>
                )}
              </div>
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2 flex-wrap">
                  <span className="text-sm font-mono font-semibold text-gray-800">{t.task_id}</span>
                  <span className="text-xs text-gray-400 bg-gray-100 px-1.5 py-0.5 rounded">
                    {t.task_type?.replace(/_/g,' ')}
                  </span>
                  <StatusBadge status={t.status}/>
                  {t.attempt_number > 1 && (
                    <span className="text-xs text-amber-600 bg-amber-50 border border-amber-200 px-1.5 py-0.5 rounded">
                      attempt {t.attempt_number}
                    </span>
                  )}
                </div>
                {t.error_message && (
                  <p className="text-xs text-red-600 mt-1 truncate font-mono bg-red-50 px-2 py-1 rounded">
                    {t.error_message}
                  </p>
                )}
                {t.output_row_count != null && (
                  <p className="text-xs text-gray-500 mt-0.5 flex items-center gap-1">
                    <Database size={10}/>
                    {t.output_row_count.toLocaleString()} rows
                    {t.output_duckdb_path && (
                      <span className="ml-1 font-mono text-gray-400">
                        {t.output_duckdb_path.split('/').slice(-1)[0]}
                      </span>
                    )}
                  </p>
                )}
                {t.qc_results && (
                  <p className="text-xs mt-0.5">
                    QC: {t.qc_passed
                      ? <span className="text-emerald-600">All passed</span>
                      : <span className="text-red-600">{t.qc_failures} failed, {t.qc_warnings} warnings</span>
                    }
                  </p>
                )}
              </div>
              <div className="text-right flex-shrink-0">
                <Duration seconds={t.duration_seconds}/>
                {(t.status === 'failed' || t.status === 'success') && (
                  <div className="mt-1">
                    <Button size="xs" variant="ghost" onClick={() => setRerunModal(t)}>
                      <RotateCw size={11}/> Rerun
                    </Button>
                  </div>
                )}
              </div>
            </div>
          ))}
          {tasks.length === 0 && (
            <p className="text-center text-gray-400 text-sm py-10">No task details available</p>
          )}
        </div>
      </Card>

      {/* Rerun modal */}
      <Modal title={`Rerun Task: ${rerunModal?.task_id}`} open={!!rerunModal} onClose={() => setRerunModal(null)}>
        <div className="space-y-4">
          <Select label="Rerun Mode" value={rerunMode} onChange={e => setRerunMode(e.target.value)}>
            <option value="downstream">Downstream — rerun this task and all dependents</option>
            <option value="single">Single — rerun only this task</option>
            <option value="full">Full — restart entire pipeline run</option>
          </Select>
          <Textarea
            label="Reason (required for audit)"
            value={rerunReason}
            onChange={e => setRerunReason(e.target.value)}
            placeholder="Why is this task being rerun?"
            rows={3}
          />
          <div className="flex justify-end gap-2 pt-1">
            <Button variant="ghost" onClick={() => setRerunModal(null)}>Cancel</Button>
            <Button
              variant="primary"
              onClick={() => rerunMut.mutate()}
              disabled={!rerunReason || rerunMut.isPending}
            >
              {rerunMut.isPending ? <Spinner size="sm"/> : <RotateCw size={14}/>} Submit Rerun
            </Button>
          </div>
        </div>
      </Modal>
    </div>
  )
}

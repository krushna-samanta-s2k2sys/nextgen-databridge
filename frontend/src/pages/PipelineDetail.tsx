import React, { useState, useMemo } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import toast from 'react-hot-toast'
import {
  ReactFlow, Background, Controls,
  type Node, type Edge,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'

import {
  getPipeline, getConfigVersions, getRuns, getRun,
  triggerPipeline, updatePipeline,
} from '../api/client'
import {
  Card, CardHeader, Table, StatusBadge, Button,
  Tabs, Spinner, Duration, TimeAgo, Input, Select, Textarea,
} from '../components/ui'
import {
  Play, Settings, ChevronRight, CheckCircle, XCircle,
  Clock, GitBranch, Plus, Trash2, ChevronDown, ChevronUp,
  Database, Upload, Radio, Cpu, Shield, Server, Bell, X,
  ArrowLeft, ArrowRight,
} from 'lucide-react'
import clsx from 'clsx'

// ─────────────────────────────────────────────────────────────────────────────
// Task type registry
// ─────────────────────────────────────────────────────────────────────────────
const TYPE_COLORS: Record<string, string> = {
  sql_extract:       '#3b82f6',
  cdc_extract:       '#3b82f6',
  file_ingest:       '#6366f1',
  kafka_consume:     '#ec4899',
  kafka_produce:     '#ec4899',
  duckdb_transform:  '#a855f7',
  duckdb_query:      '#8b5cf6',
  sql_transform:     '#a855f7',
  data_quality:      '#10b981',
  schema_validate:   '#14b8a6',
  load_target:       '#f97316',
  eks_job:           '#eab308',
  notification:      '#71717a',
}

const TYPE_LABEL: Record<string, string> = {
  sql_extract:       'SQL Extract',
  cdc_extract:       'CDC Extract',
  file_ingest:       'File Ingest',
  kafka_consume:     'Kafka Consume',
  kafka_produce:     'Kafka Produce',
  duckdb_transform:  'DuckDB Transform',
  duckdb_query:      'DuckDB Query',
  sql_transform:     'SQL Transform',
  data_quality:      'Data Quality',
  schema_validate:   'Schema Validate',
  load_target:       'Load Target',
  eks_job:           'EKS Job',
  notification:      'Notification',
}

const STATUS_COLORS: Record<string, string> = {
  success: '#10b981', running: '#3b82f6', failed: '#ef4444',
  pending: '#94a3b8', skipped: '#94a3b8', retrying: '#f59e0b',
}

const TASK_CATEGORIES = [
  { label: 'Extract',   items: ['sql_extract','cdc_extract','file_ingest','kafka_consume'] },
  { label: 'Transform', items: ['duckdb_transform','duckdb_query','sql_transform'] },
  { label: 'Quality',   items: ['data_quality','schema_validate'] },
  { label: 'Load',      items: ['load_target','eks_job'] },
  { label: 'Notify',    items: ['notification'] },
]

// ─────────────────────────────────────────────────────────────────────────────
// Auto-layout + flow graph
// ─────────────────────────────────────────────────────────────────────────────
function autoLayout(tasks: any[]) {
  if (!tasks.length) return {}
  const layerOf: Record<string, number> = {}
  const roots = tasks.filter(t => !(t.depends_on?.length)).map(t => t.task_id)
  roots.forEach(r => { layerOf[r] = 0 })
  const q = [...roots]
  while (q.length) {
    const id = q.shift()!
    const l = layerOf[id] ?? 0
    tasks.filter(t => (t.depends_on || []).includes(id)).forEach(child => {
      layerOf[child.task_id] = Math.max(layerOf[child.task_id] ?? 0, l + 1)
      q.push(child.task_id)
    })
  }
  tasks.forEach(t => { if (layerOf[t.task_id] === undefined) layerOf[t.task_id] = 0 })
  const byLayer: Record<number, string[]> = {}
  tasks.forEach(t => {
    const l = layerOf[t.task_id]
    if (!byLayer[l]) byLayer[l] = []
    byLayer[l].push(t.task_id)
  })
  const pos: Record<string, { x: number; y: number }> = {}
  Object.entries(byLayer).forEach(([ls, ids]) => {
    const l = Number(ls)
    ids.forEach((id, i) => {
      pos[id] = { x: l * 260, y: i * 110 - ((ids.length - 1) * 55) }
    })
  })
  return pos
}

function buildFlowGraph(tasks: any[], taskRuns: any[]): { nodes: Node[]; edges: Edge[] } {
  const pos = autoLayout(tasks)
  const runMap: Record<string, any> = {}
  taskRuns.forEach(tr => { runMap[tr.task_id] = tr })

  const nodes: Node[] = tasks.map(t => {
    const run = runMap[t.task_id]
    const typeColor   = TYPE_COLORS[t.type] || '#52525b'
    const statusColor = run ? (STATUS_COLORS[run.status?.toLowerCase()] || '#94a3b8') : null
    return {
      id: t.task_id,
      type: 'default',
      position: pos[t.task_id] || { x: 0, y: 0 },
      style: {
        background: '#1e293b', border: `1.5px solid ${statusColor || '#334155'}`,
        borderRadius: 10, padding: 0, width: 200,
        boxShadow: statusColor && run?.status !== 'pending' ? `0 0 0 3px ${statusColor}20` : '0 1px 4px rgba(0,0,0,0.3)',
      },
      data: {
        label: (
          <div style={{ padding: '10px 14px' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 4 }}>
              <div style={{ width: 3, height: 32, background: typeColor, borderRadius: 2, flexShrink: 0 }} />
              <div style={{ flex: 1, minWidth: 0 }}>
                <div style={{ fontSize: 11, fontFamily: 'monospace', color: '#f1f5f9', fontWeight: 600, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                  {t.task_id}
                </div>
                <div style={{ fontSize: 10, color: typeColor, marginTop: 2 }}>{t.type?.replace(/_/g, ' ')}</div>
              </div>
              {run && <div style={{ width: 8, height: 8, borderRadius: '50%', background: statusColor || '#94a3b8', flexShrink: 0, boxShadow: run.status === 'running' ? `0 0 6px ${statusColor}` : undefined }} />}
            </div>
            {run?.duration_seconds != null && (
              <div style={{ fontSize: 10, color: '#94a3b8', paddingLeft: 9 }}>
                {run.duration_seconds < 60 ? `${run.duration_seconds.toFixed(1)}s` : `${Math.floor(run.duration_seconds / 60)}m ${Math.round(run.duration_seconds % 60)}s`}
                {run.output_row_count != null && ` · ${run.output_row_count.toLocaleString()} rows`}
              </div>
            )}
          </div>
        ),
      },
    }
  })

  const edges: Edge[] = []
  tasks.forEach(t => {
    (t.depends_on || []).forEach((dep: string) => {
      edges.push({ id: `${dep}→${t.task_id}`, source: dep, target: t.task_id, type: 'smoothstep', animated: false, style: { stroke: '#475569', strokeWidth: 1.5 } })
    })
  })
  return { nodes, edges }
}

// ─────────────────────────────────────────────────────────────────────────────
// Task field editor — no JSON, per-type form fields
// ─────────────────────────────────────────────────────────────────────────────
function TaskFields({ task, allTaskIds, onChange }: { task: any; allTaskIds: string[]; onChange: (t: any) => void }) {
  const s = (k: string, v: any) => onChange({ ...task, [k]: v })
  const src = task.source || {}
  const out = task.output || {}
  const tgt = task.target || {}
  const exec = task.execution || {}
  const setSrc  = (k: string, v: any) => s('source',    { ...src,  [k]: v })
  const setOut  = (k: string, v: any) => s('output',    { ...out,  [k]: v })
  const setTgt  = (k: string, v: any) => s('target',    { ...tgt,  [k]: v })
  const setExec = (k: string, v: any) => s('execution', { ...exec, [k]: v })

  const otherTasks = allTaskIds.filter(id => id !== task.task_id)

  return (
    <div className="space-y-3 pt-2">
      {/* Depends on */}
      <div>
        <label className="text-xs font-medium text-gray-700">Depends on</label>
        <div className="mt-1 flex flex-wrap gap-2">
          {otherTasks.length === 0 && <span className="text-xs text-gray-400">No other tasks yet</span>}
          {otherTasks.map(id => {
            const checked = (task.depends_on || []).includes(id)
            return (
              <button
                key={id}
                type="button"
                onClick={() => {
                  const deps = task.depends_on || []
                  s('depends_on', checked ? deps.filter((d: string) => d !== id) : [...deps, id])
                }}
                className={clsx(
                  'px-2.5 py-1 rounded-full text-xs font-mono border transition-colors',
                  checked
                    ? 'bg-blue-600 text-white border-blue-600'
                    : 'bg-white text-gray-600 border-gray-300 hover:border-blue-400',
                )}
              >
                {id}
              </button>
            )
          })}
        </div>
      </div>

      {/* SQL Extract / CDC Extract */}
      {(task.type === 'sql_extract' || task.type === 'cdc_extract') && (
        <>
          <Input label="Connection ID" value={src.connection || ''} onChange={e => setSrc('connection', e.target.value)} placeholder="mssql_wwi" hint="ID of a configured connection"/>
          <Select label="Extract Mode" value={src.mode || 'full'} onChange={e => setSrc('mode', e.target.value)}>
            <option value="full">Full Load</option>
            <option value="incremental">Incremental (watermark)</option>
            <option value="cdc">Change Data Capture</option>
          </Select>
          <Textarea label="SQL Query" value={src.query || ''} onChange={e => setSrc('query', e.target.value)} rows={4} placeholder={`SELECT * FROM orders\nWHERE updated_at >= '{{ ds }}'`}/>
          <div className="grid grid-cols-2 gap-3">
            <Input label="Watermark Column" value={src.watermark_column || ''} onChange={e => setSrc('watermark_column', e.target.value)} placeholder="updated_at"/>
            <Input label="Output Table Name" value={out.table || ''} onChange={e => setOut('table', e.target.value)} placeholder="orders"/>
          </div>
        </>
      )}

      {/* DuckDB Transform / Query */}
      {(task.type === 'duckdb_transform' || task.type === 'duckdb_query' || task.type === 'sql_transform') && (
        <>
          <Textarea label="SQL" value={task.sql || ''} onChange={e => s('sql', e.target.value)} rows={6} placeholder="SELECT order_id, total\nFROM orders\nWHERE status = 'shipped'"/>
          <Input label="Output Table Name" value={out.table || ''} onChange={e => setOut('table', e.target.value)} placeholder="transformed_orders"/>
        </>
      )}

      {/* Data Quality */}
      {task.type === 'data_quality' && (
        <div className="space-y-2">
          <label className="text-xs font-medium text-gray-700">Quality Checks</label>
          {(task.checks || []).map((check: any, i: number) => (
            <div key={i} className="flex items-start gap-2 p-3 bg-gray-50 border border-gray-200 rounded-lg">
              <div className="flex-1 grid grid-cols-3 gap-2">
                <Select label="Type" value={check.type || ''} onChange={e => {
                  const c = [...(task.checks || [])]; c[i] = { ...c[i], type: e.target.value }; s('checks', c)
                }}>
                  <option value="not_null">Not null</option>
                  <option value="unique">Unique</option>
                  <option value="row_count_min">Row count min</option>
                  <option value="row_count_max">Row count max</option>
                  <option value="regex">Regex match</option>
                  <option value="custom_sql">Custom SQL</option>
                </Select>
                <Input label="Column / Value" value={check.column || check.value || ''} onChange={e => {
                  const c = [...(task.checks || [])]; c[i] = { ...c[i], column: e.target.value }; s('checks', c)
                }}/>
                <Select label="Action" value={check.action || 'fail'} onChange={e => {
                  const c = [...(task.checks || [])]; c[i] = { ...c[i], action: e.target.value }; s('checks', c)
                }}>
                  <option value="fail">Fail pipeline</option>
                  <option value="warn">Warn only</option>
                  <option value="drop_rows">Drop bad rows</option>
                </Select>
              </div>
              <button
                type="button"
                onClick={() => s('checks', (task.checks || []).filter((_: any, j: number) => j !== i))}
                className="text-gray-400 hover:text-red-500 mt-5 transition-colors"
              >
                <X size={14}/>
              </button>
            </div>
          ))}
          <Button size="xs" variant="secondary" onClick={() => s('checks', [...(task.checks || []), { type: 'not_null', column: '', action: 'fail' }])}>
            <Plus size={11}/> Add Check
          </Button>
        </div>
      )}

      {/* Schema Validate */}
      {task.type === 'schema_validate' && (
        <div className="space-y-2">
          <label className="text-xs font-medium text-gray-700">Expected Columns</label>
          {(task.schema?.columns || []).map((col: any, i: number) => (
            <div key={i} className="flex items-center gap-2">
              <Input
                value={col.name || ''}
                onChange={e => {
                  const cols = [...(task.schema?.columns || [])]; cols[i] = { ...cols[i], name: e.target.value }
                  s('schema', { ...task.schema, columns: cols })
                }}
                placeholder="column_name"
              />
              <Select
                value={col.type || 'varchar'}
                onChange={e => {
                  const cols = [...(task.schema?.columns || [])]; cols[i] = { ...cols[i], type: e.target.value }
                  s('schema', { ...task.schema, columns: cols })
                }}
              >
                {['varchar','integer','bigint','float','boolean','date','timestamp','json'].map(t => (
                  <option key={t} value={t}>{t}</option>
                ))}
              </Select>
              <button
                type="button"
                onClick={() => s('schema', { ...task.schema, columns: (task.schema?.columns || []).filter((_: any, j: number) => j !== i) })}
                className="text-gray-400 hover:text-red-500 transition-colors flex-shrink-0"
              >
                <X size={14}/>
              </button>
            </div>
          ))}
          <Button size="xs" variant="secondary" onClick={() => s('schema', { ...task.schema, columns: [...(task.schema?.columns || []), { name: '', type: 'varchar' }] })}>
            <Plus size={11}/> Add Column
          </Button>
        </div>
      )}

      {/* Load Target */}
      {task.type === 'load_target' && (
        <>
          <Select label="Target Type" value={tgt.type || 's3'} onChange={e => setTgt('type', e.target.value)}>
            <option value="s3">Amazon S3</option>
            <option value="mssql">SQL Server</option>
            <option value="postgresql">PostgreSQL</option>
          </Select>
          {(tgt.type === 's3' || !tgt.type) && (
            <>
              <div className="grid grid-cols-2 gap-3">
                <Input label="S3 Bucket" value={tgt.bucket || ''} onChange={e => setTgt('bucket', e.target.value)} placeholder="my-data-lake"/>
                <Select label="File Format" value={tgt.format || 'parquet'} onChange={e => setTgt('format', e.target.value)}>
                  <option value="parquet">Parquet</option>
                  <option value="csv">CSV</option>
                  <option value="json">JSON (Lines)</option>
                </Select>
              </div>
              <Input label="S3 Path Template" value={tgt.path || ''} onChange={e => setTgt('path', e.target.value)} placeholder="orders/{{ ds }}/data.parquet"/>
            </>
          )}
          {(tgt.type === 'mssql' || tgt.type === 'postgresql') && (
            <>
              <Input label="Connection ID" value={tgt.connection || ''} onChange={e => setTgt('connection', e.target.value)}/>
              <Input label="Target Table" value={tgt.table || ''} onChange={e => setTgt('table', e.target.value)}/>
              <Select label="Write Mode" value={tgt.mode || 'append'} onChange={e => setTgt('mode', e.target.value)}>
                <option value="append">Append</option>
                <option value="overwrite">Overwrite</option>
                <option value="upsert">Upsert (merge)</option>
              </Select>
            </>
          )}
        </>
      )}

      {/* File Ingest */}
      {task.type === 'file_ingest' && (
        <>
          <Input label="S3 Path" value={task.s3_path || ''} onChange={e => s('s3_path', e.target.value)} placeholder="s3://bucket/path/*.csv"/>
          <div className="grid grid-cols-2 gap-3">
            <Select label="File Format" value={task.format || 'csv'} onChange={e => s('format', e.target.value)}>
              <option value="csv">CSV</option>
              <option value="parquet">Parquet</option>
              <option value="json">JSON (Lines)</option>
            </Select>
            <Input label="Output Table Name" value={out.table || ''} onChange={e => setOut('table', e.target.value)}/>
          </div>
        </>
      )}

      {/* Kafka */}
      {(task.type === 'kafka_consume' || task.type === 'kafka_produce') && (
        <>
          <Input label="Topic" value={task.topic || ''} onChange={e => s('topic', e.target.value)} placeholder="orders.events"/>
          <div className="grid grid-cols-2 gap-3">
            <Input label="Bootstrap Servers" value={task.bootstrap_servers || ''} onChange={e => s('bootstrap_servers', e.target.value)} placeholder="broker:9092"/>
            <Input label="Group ID" value={task.group_id || ''} onChange={e => s('group_id', e.target.value)}/>
          </div>
        </>
      )}

      {/* EKS Job */}
      {task.type === 'eks_job' && (
        <>
          <Input label="Docker Image" value={exec.image || ''} onChange={e => setExec('image', e.target.value)} placeholder="877707676590.dkr.ecr.us-east-1.amazonaws.com/myrepo:latest"/>
          <div className="grid grid-cols-3 gap-3">
            <Input label="CPU Request" value={exec.cpu || '1'} onChange={e => setExec('cpu', e.target.value)} placeholder="1"/>
            <Input label="Memory" value={exec.memory || '2Gi'} onChange={e => setExec('memory', e.target.value)} placeholder="2Gi"/>
            <Input label="Namespace" value={exec.namespace || 'nextgen-databridge-jobs'} onChange={e => setExec('namespace', e.target.value)}/>
          </div>
          <Textarea label="Environment Variables (KEY=value, one per line)" value={exec.env_vars || ''} onChange={e => setExec('env_vars', e.target.value)} rows={3} placeholder="DB_HOST=localhost&#10;DB_PORT=5432"/>
        </>
      )}

      {/* Notification */}
      {task.type === 'notification' && (
        <>
          <Select label="Channel" value={task.channel || 'email'} onChange={e => s('channel', e.target.value)}>
            <option value="email">Email</option>
            <option value="slack">Slack</option>
          </Select>
          <Input label="Recipient / Webhook" value={task.recipient || ''} onChange={e => s('recipient', e.target.value)} placeholder="team@company.com or https://hooks.slack.com/…"/>
          <Select label="When to notify" value={task.on_event || 'failure'} onChange={e => s('on_event', e.target.value)}>
            <option value="failure">On failure</option>
            <option value="success">On success</option>
            <option value="always">Always</option>
            <option value="sla_breach">SLA breach</option>
          </Select>
          <Textarea label="Message (optional override)" value={task.message || ''} onChange={e => s('message', e.target.value)} rows={2} placeholder="Pipeline {{ pipeline_id }} {{ status }}"/>
        </>
      )}

      {/* Universal: trigger rule */}
      <Select label="Trigger Rule" value={task.trigger_rule || 'all_success'} onChange={e => s('trigger_rule', e.target.value)}>
        <option value="all_success">all_success — run when all upstream succeed</option>
        <option value="all_done">all_done — run regardless of upstream status</option>
        <option value="one_success">one_success — run when any upstream succeeds</option>
        <option value="none_failed">none_failed — run unless an upstream failed</option>
      </Select>
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// Pipeline Editor — intuitive form-based, no JSON exposed
// ─────────────────────────────────────────────────────────────────────────────
const SCHEDULE_PRESETS = [
  { label: 'Manual (no schedule)',       value: '' },
  { label: 'Every hour',                 value: '0 * * * *' },
  { label: 'Every 6 hours',              value: '0 */6 * * *' },
  { label: 'Daily at midnight',          value: '0 0 * * *' },
  { label: 'Daily at 6 AM',              value: '0 6 * * *' },
  { label: 'Weekdays at 8 AM',           value: '0 8 * * 1-5' },
  { label: 'Weekly on Monday at 6 AM',   value: '0 6 * * 1' },
  { label: 'Monthly on the 1st at 6 AM', value: '0 6 1 * *' },
  { label: 'Custom cron…',               value: '__custom__' },
]

function PipelineEditor({
  pipeline,
  onSave,
  onClose,
}: {
  pipeline: any
  onSave: (config: any) => void
  onClose: () => void
}) {
  const cfg = pipeline.config || {}
  const [step, setStep] = useState<'settings' | 'tasks'>('settings')

  // Settings state
  const [meta, setMeta] = useState({
    name:                  pipeline.name || cfg.name || '',
    description:           pipeline.description || cfg.description || '',
    schedule:              pipeline.schedule || cfg.schedule || '',
    schedulePreset:        SCHEDULE_PRESETS.find(p => p.value === (pipeline.schedule || cfg.schedule || ''))?.value ?? '__custom__',
    owner:                 pipeline.owner || cfg.owner || '',
    sla_minutes:           String(pipeline.sla_minutes || cfg.sla_minutes || ''),
    retries:               String(cfg.retries ?? 3),
    retry_delay_minutes:   String(cfg.retry_delay_minutes ?? 5),
    tags:                  (pipeline.tags || cfg.tags || []).join(', '),
    alerting_failure:      (cfg.alerting?.on_failure || []).join(', '),
    alerting_sla:          (cfg.alerting?.on_sla_breach || []).join(', '),
  })

  function setField(k: string, v: string) { setMeta(m => ({ ...m, [k]: v })) }

  // Tasks state — each task is a plain object
  const [tasks, setTasks] = useState<any[]>(cfg.tasks || [])
  const [expandedTask, setExpandedTask] = useState<string | null>(null)
  const [showAddTask, setShowAddTask] = useState(false)

  function addTask(type: string) {
    const n = tasks.length + 1
    const newTask = { task_id: `${type}_${n}`, type, depends_on: [] }
    setTasks(ts => [...ts, newTask])
    setExpandedTask(newTask.task_id)
    setShowAddTask(false)
  }

  function updateTask(updated: any) {
    setTasks(ts => ts.map(t => t.task_id === updated.task_id ? updated : t))
  }

  function deleteTask(taskId: string) {
    setTasks(ts => {
      const remaining = ts.filter(t => t.task_id !== taskId)
      // Remove deleted task from other tasks' depends_on
      return remaining.map(t => ({ ...t, depends_on: (t.depends_on || []).filter((d: string) => d !== taskId) }))
    })
    if (expandedTask === taskId) setExpandedTask(null)
  }

  function buildAndSave() {
    const schedule = meta.schedulePreset === '__custom__' ? meta.schedule : meta.schedulePreset
    const config = {
      pipeline_id:          pipeline.pipeline_id,
      name:                 meta.name || pipeline.pipeline_id,
      description:          meta.description,
      schedule:             schedule || undefined,
      owner:                meta.owner,
      sla_minutes:          meta.sla_minutes ? Number(meta.sla_minutes) : undefined,
      retries:              Number(meta.retries || 3),
      retry_delay_minutes:  Number(meta.retry_delay_minutes || 5),
      tags:                 meta.tags.split(',').map((t: string) => t.trim()).filter(Boolean),
      alerting: {
        on_failure:    meta.alerting_failure.split(',').map((s: string) => s.trim()).filter(Boolean),
        on_sla_breach: meta.alerting_sla.split(',').map((s: string) => s.trim()).filter(Boolean),
      },
      tasks,
    }
    onSave(config)
  }

  const allTaskIds = tasks.map(t => t.task_id)

  return (
    <div className="fixed inset-0 z-50 flex flex-col bg-white">
      {/* Top bar */}
      <div className="flex items-center justify-between px-6 py-3.5 bg-slate-900 border-b border-slate-800 flex-shrink-0">
        <div className="flex items-center gap-4">
          <button onClick={onClose} className="text-slate-400 hover:text-slate-200 transition-colors">
            <ArrowLeft size={16}/>
          </button>
          <div>
            <h1 className="text-sm font-bold text-white">Edit Pipeline: {pipeline.pipeline_id}</h1>
            <p className="text-xs text-slate-500">{pipeline.name}</p>
          </div>
        </div>
        {/* Step tabs */}
        <div className="flex items-center gap-1 bg-slate-800 rounded-lg p-1">
          <button
            onClick={() => setStep('settings')}
            className={clsx(
              'px-4 py-1.5 rounded-md text-xs font-medium transition-all',
              step === 'settings' ? 'bg-white text-gray-900 shadow-sm' : 'text-slate-400 hover:text-slate-200',
            )}
          >
            1 · Pipeline Settings
          </button>
          <button
            onClick={() => setStep('tasks')}
            className={clsx(
              'px-4 py-1.5 rounded-md text-xs font-medium transition-all',
              step === 'tasks' ? 'bg-white text-gray-900 shadow-sm' : 'text-slate-400 hover:text-slate-200',
            )}
          >
            2 · Tasks ({tasks.length})
          </button>
        </div>
        <div className="flex items-center gap-2">
          <button onClick={onClose} className="text-xs text-slate-400 hover:text-slate-200 px-3 py-1.5">Cancel</button>
          <button
            onClick={buildAndSave}
            className="bg-blue-600 hover:bg-blue-500 text-white text-xs font-medium px-4 py-1.5 rounded-lg transition-colors"
          >
            Save Pipeline
          </button>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto bg-gray-50">
        {/* ── Step 1: Settings ── */}
        {step === 'settings' && (
          <div className="max-w-2xl mx-auto py-8 px-4 space-y-6">
            {/* Identity */}
            <div className="bg-white border border-gray-200 rounded-xl p-6 shadow-sm space-y-4">
              <h2 className="text-sm font-bold text-gray-900 flex items-center gap-2">
                <span className="w-5 h-5 bg-blue-600 text-white rounded-full flex items-center justify-center text-xs">1</span>
                Identity
              </h2>
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="text-xs font-medium text-gray-500 block mb-1">Pipeline ID</label>
                  <div className="bg-gray-50 border border-gray-200 rounded-lg px-3 py-2 text-sm font-mono text-gray-500">
                    {pipeline.pipeline_id}
                  </div>
                  <p className="text-xs text-gray-400 mt-1">Cannot be changed after creation</p>
                </div>
                <Input
                  label="Display Name"
                  value={meta.name}
                  onChange={e => setField('name', e.target.value)}
                  placeholder="My Pipeline"
                />
              </div>
              <Textarea
                label="Description"
                value={meta.description}
                onChange={e => setField('description', e.target.value)}
                rows={2}
                placeholder="Describe what this pipeline does…"
              />
              <div className="grid grid-cols-2 gap-4">
                <Input label="Owner / Team" value={meta.owner} onChange={e => setField('owner', e.target.value)} placeholder="data-engineering"/>
                <Input label="Tags (comma-separated)" value={meta.tags} onChange={e => setField('tags', e.target.value)} placeholder="finance, daily, critical"/>
              </div>
            </div>

            {/* Scheduling */}
            <div className="bg-white border border-gray-200 rounded-xl p-6 shadow-sm space-y-4">
              <h2 className="text-sm font-bold text-gray-900 flex items-center gap-2">
                <span className="w-5 h-5 bg-blue-600 text-white rounded-full flex items-center justify-center text-xs">2</span>
                Scheduling
              </h2>
              <Select
                label="Run schedule"
                value={meta.schedulePreset}
                onChange={e => {
                  setField('schedulePreset', e.target.value)
                  if (e.target.value !== '__custom__') setField('schedule', e.target.value)
                }}
              >
                {SCHEDULE_PRESETS.map(p => <option key={p.value} value={p.value}>{p.label}</option>)}
              </Select>
              {meta.schedulePreset === '__custom__' && (
                <Input
                  label="Cron Expression"
                  value={meta.schedule}
                  onChange={e => setField('schedule', e.target.value)}
                  placeholder="0 6 * * *"
                  hint="Standard cron: minute hour day month weekday"
                />
              )}
              {meta.schedulePreset && meta.schedulePreset !== '__custom__' && (
                <div className="bg-blue-50 border border-blue-200 rounded-lg px-3 py-2">
                  <p className="text-xs text-blue-700">
                    Cron expression: <code className="font-mono font-bold">{meta.schedulePreset}</code>
                  </p>
                </div>
              )}
            </div>

            {/* Reliability */}
            <div className="bg-white border border-gray-200 rounded-xl p-6 shadow-sm space-y-4">
              <h2 className="text-sm font-bold text-gray-900 flex items-center gap-2">
                <span className="w-5 h-5 bg-blue-600 text-white rounded-full flex items-center justify-center text-xs">3</span>
                Reliability & SLA
              </h2>
              <div className="grid grid-cols-3 gap-4">
                <Input label="SLA (minutes)" type="number" value={meta.sla_minutes} onChange={e => setField('sla_minutes', e.target.value)} placeholder="120" hint="Alert if exceeded"/>
                <Input label="Max Retries" type="number" value={meta.retries} onChange={e => setField('retries', e.target.value)}/>
                <Input label="Retry Delay (min)" type="number" value={meta.retry_delay_minutes} onChange={e => setField('retry_delay_minutes', e.target.value)}/>
              </div>
            </div>

            {/* Alerting */}
            <div className="bg-white border border-gray-200 rounded-xl p-6 shadow-sm space-y-4">
              <h2 className="text-sm font-bold text-gray-900 flex items-center gap-2">
                <span className="w-5 h-5 bg-blue-600 text-white rounded-full flex items-center justify-center text-xs">4</span>
                Alerting
              </h2>
              <Input
                label="On failure — notify"
                value={meta.alerting_failure}
                onChange={e => setField('alerting_failure', e.target.value)}
                placeholder="email:team@company.com, slack:#data-alerts"
                hint="Prefix with email: or slack: · separate multiple with commas"
              />
              <Input
                label="On SLA breach — notify"
                value={meta.alerting_sla}
                onChange={e => setField('alerting_sla', e.target.value)}
                placeholder="slack:#data-alerts"
              />
            </div>

            <div className="flex justify-end">
              <button
                onClick={() => setStep('tasks')}
                className="flex items-center gap-2 bg-blue-600 hover:bg-blue-500 text-white text-sm font-medium px-5 py-2.5 rounded-lg transition-colors"
              >
                Next: Configure Tasks <ArrowRight size={14}/>
              </button>
            </div>
          </div>
        )}

        {/* ── Step 2: Tasks ── */}
        {step === 'tasks' && (
          <div className="max-w-3xl mx-auto py-8 px-4 space-y-4">
            <div className="flex items-center justify-between mb-2">
              <div>
                <h2 className="text-base font-bold text-gray-900">Pipeline Tasks</h2>
                <p className="text-sm text-gray-500">
                  {tasks.length} task{tasks.length !== 1 ? 's' : ''} · drag-and-drop ordering coming soon
                </p>
              </div>
              <button
                onClick={() => setShowAddTask(!showAddTask)}
                className="flex items-center gap-2 bg-blue-600 hover:bg-blue-500 text-white text-sm font-medium px-4 py-2 rounded-lg transition-colors"
              >
                <Plus size={14}/> Add Task
              </button>
            </div>

            {/* Add task type picker */}
            {showAddTask && (
              <div className="bg-white border border-gray-200 rounded-xl shadow-lg p-5">
                <p className="text-sm font-semibold text-gray-700 mb-4">Choose task type:</p>
                <div className="space-y-4">
                  {TASK_CATEGORIES.map(cat => (
                    <div key={cat.label}>
                      <p className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-2">{cat.label}</p>
                      <div className="flex flex-wrap gap-2">
                        {cat.items.map(type => (
                          <button
                            key={type}
                            onClick={() => addTask(type)}
                            className="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 bg-gray-50 hover:bg-white hover:border-blue-400 hover:shadow-sm transition-all text-sm text-gray-700"
                          >
                            <div
                              className="w-2 h-2 rounded-full flex-shrink-0"
                              style={{ background: TYPE_COLORS[type] || '#94a3b8' }}
                            />
                            {TYPE_LABEL[type] || type}
                          </button>
                        ))}
                      </div>
                    </div>
                  ))}
                </div>
                <button onClick={() => setShowAddTask(false)} className="mt-4 text-xs text-gray-400 hover:text-gray-600">
                  Cancel
                </button>
              </div>
            )}

            {/* Empty state */}
            {tasks.length === 0 && !showAddTask && (
              <div className="bg-white border-2 border-dashed border-gray-200 rounded-xl flex flex-col items-center justify-center py-16 gap-3 text-gray-300">
                <GitBranch size={32}/>
                <p className="text-sm text-gray-400">No tasks yet</p>
                <button
                  onClick={() => setShowAddTask(true)}
                  className="text-sm text-blue-600 hover:text-blue-700 font-medium"
                >
                  Add your first task →
                </button>
              </div>
            )}

            {/* Task cards */}
            {tasks.map((task, idx) => {
              const isOpen = expandedTask === task.task_id
              const color  = TYPE_COLORS[task.type] || '#94a3b8'
              const deps   = (task.depends_on || []) as string[]
              return (
                <div
                  key={task.task_id}
                  className={clsx(
                    'bg-white border rounded-xl shadow-sm overflow-hidden transition-all',
                    isOpen ? 'border-blue-300' : 'border-gray-200',
                  )}
                >
                  {/* Task header */}
                  <div
                    className="flex items-center gap-3 px-5 py-4 cursor-pointer hover:bg-gray-50 transition-colors"
                    onClick={() => setExpandedTask(isOpen ? null : task.task_id)}
                  >
                    <div className="w-3 h-3 rounded-full flex-shrink-0" style={{ background: color }}/>
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2">
                        <span className="font-mono text-sm font-semibold text-gray-800">{task.task_id}</span>
                        <span
                          className="text-xs px-2 py-0.5 rounded-full font-medium"
                          style={{ background: color + '20', color }}
                        >
                          {TYPE_LABEL[task.type] || task.type}
                        </span>
                        {deps.length > 0 && (
                          <span className="text-xs text-gray-400">
                            after {deps.join(', ')}
                          </span>
                        )}
                      </div>
                    </div>
                    <div className="flex items-center gap-1 flex-shrink-0">
                      <span className="text-xs text-gray-400 w-6 text-center">{idx + 1}</span>
                      <button
                        type="button"
                        onClick={e => { e.stopPropagation(); deleteTask(task.task_id) }}
                        className="p-1 text-gray-300 hover:text-red-500 transition-colors rounded"
                        title="Delete task"
                      >
                        <Trash2 size={14}/>
                      </button>
                      <div className="text-gray-400 p-1">
                        {isOpen ? <ChevronUp size={14}/> : <ChevronDown size={14}/>}
                      </div>
                    </div>
                  </div>

                  {/* Expanded task config */}
                  {isOpen && (
                    <div className="border-t border-gray-100 px-5 pb-5">
                      <div className="mb-3 mt-4">
                        <Input
                          label="Task ID"
                          value={task.task_id}
                          onChange={e => {
                            const newId = e.target.value.toLowerCase().replace(/\s+/g,'_').replace(/[^a-z0-9_]/g,'')
                            // Update depends_on references in other tasks
                            setTasks(ts => ts.map(t => {
                              if (t.task_id === task.task_id) return { ...t, task_id: newId }
                              return { ...t, depends_on: (t.depends_on || []).map((d: string) => d === task.task_id ? newId : d) }
                            }))
                            if (expandedTask === task.task_id) setExpandedTask(newId)
                          }}
                          hint="Unique identifier for this task — lowercase, underscores"
                        />
                      </div>
                      <TaskFields
                        task={task}
                        allTaskIds={allTaskIds}
                        onChange={updateTask}
                      />
                    </div>
                  )}
                </div>
              )
            })}

            <div className="flex items-center justify-between pt-4">
              <button
                onClick={() => setStep('settings')}
                className="flex items-center gap-2 text-sm text-gray-500 hover:text-gray-700 font-medium"
              >
                <ArrowLeft size={14}/> Back to Settings
              </button>
              <button
                onClick={buildAndSave}
                className="flex items-center gap-2 bg-blue-600 hover:bg-blue-500 text-white text-sm font-medium px-6 py-2.5 rounded-lg transition-colors"
              >
                <CheckCircle size={14}/> Save Pipeline
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// PipelineDetail page
// ─────────────────────────────────────────────────────────────────────────────
export default function PipelineDetail() {
  const { id } = useParams<{ id: string }>()
  const nav = useNavigate()
  const qc = useQueryClient()
  const [tab, setTab] = useState('dag')
  const [showEditor, setShowEditor] = useState(false)
  const [selectedRun, setSelectedRun] = useState<string | null>(null)

  const { data: pipeline, isLoading } = useQuery({
    queryKey: ['pipeline', id],
    queryFn: () => getPipeline(id!),
    enabled: !!id,
  })
  const { data: versionsData } = useQuery({
    queryKey: ['config-versions', id],
    queryFn: () => getConfigVersions(id!),
    enabled: !!id,
  })
  const { data: runsData } = useQuery({
    queryKey: ['runs', id],
    queryFn: () => getRuns({ pipeline_id: id!, page_size: '20' }),
    enabled: !!id,
    refetchInterval: 15_000,
  })

  const runs     = runsData?.runs || []
  const displayRun = selectedRun
    ? runs.find((r: any) => r.run_id === selectedRun)
    : runs[0]

  const { data: runDetail } = useQuery({
    queryKey: ['run', displayRun?.run_id],
    queryFn: () => getRun(displayRun!.run_id),
    enabled: !!displayRun?.run_id,
    refetchInterval: displayRun?.status === 'running' ? 5_000 : false,
  })

  const tasks    = pipeline?.config?.tasks || []
  const versions = versionsData?.versions || []
  const taskRuns: any[] = runDetail?.tasks || []

  const { nodes, edges } = useMemo(
    () => buildFlowGraph(tasks, taskRuns),
    [tasks, taskRuns],
  )

  const triggerMut = useMutation({
    mutationFn: () => triggerPipeline(id!, { trigger_type: 'manual' }),
    onSuccess: () => { toast.success('Run triggered'); nav('/runs') },
    onError: (e: any) => toast.error(e.message),
  })
  const updateMut = useMutation({
    mutationFn: (config: any) => updatePipeline(id!, { config }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['pipeline', id] })
      setShowEditor(false)
      toast.success('Pipeline updated')
    },
    onError: (e: any) => toast.error(e.response?.data?.detail || 'Update failed'),
  })

  if (isLoading) {
    return (
      <div className="flex justify-center items-center h-full bg-gray-50">
        <Spinner size="lg"/>
      </div>
    )
  }
  if (!pipeline) {
    return (
      <div className="p-8 text-center bg-gray-50 h-full">
        <p className="text-gray-500 text-sm">Pipeline not found.</p>
        <button onClick={() => nav('/pipelines')} className="mt-2 text-blue-600 text-sm hover:underline">
          Back to Pipelines
        </button>
      </div>
    )
  }

  return (
    <>
      {/* Full-screen form editor overlay */}
      {showEditor && (
        <PipelineEditor
          pipeline={pipeline}
          onSave={config => updateMut.mutate(config)}
          onClose={() => setShowEditor(false)}
        />
      )}

      <div className="flex flex-col h-full overflow-hidden bg-gray-50">
        {/* Header */}
        <div className="flex items-start justify-between px-6 py-4 border-b border-gray-200 flex-shrink-0 bg-white">
          <div>
            <div className="flex items-center gap-2 text-xs text-gray-400 mb-1">
              <button onClick={() => nav('/pipelines')} className="hover:text-blue-600 transition-colors">Pipelines</button>
              <ChevronRight size={12}/>
              <span className="text-gray-700 font-medium">{id}</span>
            </div>
            <h1 className="text-lg font-bold text-gray-900">{pipeline.name}</h1>
            {pipeline.description && <p className="text-xs text-gray-500 mt-0.5">{pipeline.description}</p>}
          </div>
          <div className="flex items-center gap-2">
            <Button variant="secondary" onClick={() => setShowEditor(true)}>
              <Settings size={13}/> Edit Pipeline
            </Button>
            <Button variant="primary" onClick={() => triggerMut.mutate()} disabled={triggerMut.isPending}>
              <Play size={13}/> Trigger Run
            </Button>
          </div>
        </div>

        {/* Metadata strip */}
        <div className="flex flex-wrap items-center gap-5 px-6 py-3 border-b border-gray-200 bg-white flex-shrink-0">
          <div className="flex items-center gap-1.5"><span className="text-xs text-gray-400">Status</span><StatusBadge status={pipeline.status}/></div>
          <div className="text-xs text-gray-400">
            Schedule <span className="text-gray-700 ml-1 font-medium">{pipeline.schedule || 'manual trigger'}</span>
          </div>
          <div className="text-xs text-gray-400">Owner <span className="text-gray-700 ml-1">{pipeline.owner || '—'}</span></div>
          {pipeline.sla_minutes && (
            <div className="flex items-center gap-1 text-xs text-gray-400">
              <Clock size={11}/> SLA <span className="text-gray-700 ml-1">{pipeline.sla_minutes}m</span>
            </div>
          )}
          <div className="text-xs text-gray-400">Tasks <span className="font-semibold text-gray-700 ml-1">{tasks.length}</span></div>
          {(pipeline.tags || []).map((tag: string) => (
            <span key={tag} className="text-xs bg-blue-50 text-blue-700 border border-blue-200 px-2 py-0.5 rounded-full">{tag}</span>
          ))}
        </div>

        {/* Tabs */}
        <div className="px-6 py-3 border-b border-gray-200 bg-white flex-shrink-0">
          <Tabs
            tabs={[
              { id: 'dag',      label: 'DAG View',    count: tasks.length },
              { id: 'runs',     label: 'Run History', count: runs.length },
              { id: 'config',   label: 'Configuration' },
              { id: 'versions', label: 'Versions',    count: versions.length },
            ]}
            active={tab}
            onChange={setTab}
          />
        </div>

        {/* Tab content */}
        <div className="flex-1 overflow-hidden">
          {/* ── DAG View ── */}
          {tab === 'dag' && (
            <div className="h-full flex flex-col">
              {runs.length > 0 && (
                <div className="flex items-center gap-3 px-6 py-2.5 border-b border-gray-200 bg-white flex-shrink-0">
                  <span className="text-xs text-gray-500">Showing run status:</span>
                  <select
                    value={selectedRun || runs[0]?.run_id || ''}
                    onChange={e => setSelectedRun(e.target.value)}
                    className="bg-white border border-gray-300 rounded-lg px-2.5 py-1 text-xs text-gray-700 focus:outline-none focus:ring-1 focus:ring-blue-500"
                  >
                    {runs.slice(0, 10).map((r: any) => (
                      <option key={r.run_id} value={r.run_id}>
                        {r.run_id?.slice(-24)} · {r.status} · {r.start_time ? new Date(r.start_time).toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }) : 'unknown'}
                      </option>
                    ))}
                  </select>
                  {displayRun && <StatusBadge status={displayRun.status}/>}
                </div>
              )}
              {tasks.length === 0 ? (
                <div className="flex flex-col items-center justify-center flex-1 gap-3 text-gray-400">
                  <GitBranch size={32} className="text-gray-300"/>
                  <p className="text-sm">No tasks defined yet</p>
                  <Button variant="primary" size="sm" onClick={() => setShowEditor(true)}>
                    <Plus size={13}/> Add Tasks
                  </Button>
                </div>
              ) : (
                <div className="flex-1 h-full min-h-0">
                  <ReactFlow nodes={nodes} edges={edges} nodesDraggable={false} nodesConnectable={false} fitView fitViewOptions={{ padding: 0.25 }} minZoom={0.3} maxZoom={2} style={{ background: '#0f172a' }}>
                    <Background color="#1e293b" gap={24} size={1}/>
                    <Controls className="!bg-slate-800 !border-slate-600 !shadow-lg"/>
                  </ReactFlow>
                </div>
              )}
            </div>
          )}

          {/* ── Run History ── */}
          {tab === 'runs' && (
            <div className="overflow-y-auto h-full p-6">
              <Card>
                <CardHeader title="Run History" subtitle="Last 20 runs"/>
                <Table
                  onRowClick={r => nav(`/runs/${r.run_id}`)}
                  columns={[
                    { key: 'run_id', header: 'Run ID', render: v => <span className="font-mono text-xs text-blue-600 truncate block max-w-56">{v?.slice(-28)}</span> },
                    { key: 'status', header: 'Status', render: v => <StatusBadge status={v}/> },
                    { key: 'trigger_type', header: 'Trigger', render: v => <span className="text-gray-500 text-xs capitalize">{v}</span> },
                    { key: 'start_time', header: 'Started', render: v => <TimeAgo ts={v}/> },
                    { key: 'duration_seconds', header: 'Duration', render: v => <Duration seconds={v}/> },
                    { key: 'total_rows_processed', header: 'Rows', render: v => <span className="font-mono text-xs text-gray-700">{(v || 0).toLocaleString()}</span> },
                    {
                      key: 'total_tasks', header: 'Tasks',
                      render: (v, row) => (
                        <span className="text-xs font-mono">
                          <span className="text-emerald-600">{row.completed_tasks}</span>
                          <span className="text-gray-400">/{v}</span>
                          {row.failed_tasks > 0 && <span className="text-red-500"> ({row.failed_tasks}✗)</span>}
                        </span>
                      ),
                    },
                  ]}
                  data={runs}
                  emptyMsg="No runs yet — trigger this pipeline to see history"
                />
              </Card>
            </div>
          )}

          {/* ── Configuration (human-readable, no JSON) ── */}
          {tab === 'config' && (
            <div className="overflow-y-auto h-full p-6 space-y-5">
              <div className="flex items-center justify-between">
                <h2 className="text-sm font-bold text-gray-900">Pipeline Configuration</h2>
                <Button variant="primary" size="sm" onClick={() => setShowEditor(true)}>
                  <Settings size={12}/> Edit Pipeline
                </Button>
              </div>

              {/* Pipeline metadata */}
              <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
                {[
                  { label: 'Schedule',  value: pipeline.schedule || 'Manual trigger' },
                  { label: 'Owner',     value: pipeline.owner || '—' },
                  { label: 'Retries',   value: `${pipeline.config?.retries ?? 3} × (${pipeline.config?.retry_delay_minutes ?? 5}m delay)` },
                  { label: 'SLA',       value: pipeline.sla_minutes ? `${pipeline.sla_minutes} minutes` : 'Not set' },
                ].map(item => (
                  <div key={item.label} className="bg-white border border-gray-200 rounded-xl p-4 shadow-sm">
                    <p className="text-xs font-semibold text-gray-400 uppercase tracking-wide mb-1">{item.label}</p>
                    <p className="text-sm font-medium text-gray-800">{item.value}</p>
                  </div>
                ))}
              </div>

              {/* Alerting */}
              {(pipeline.config?.alerting?.on_failure?.length > 0 || pipeline.config?.alerting?.on_sla_breach?.length > 0) && (
                <Card>
                  <CardHeader title="Alerting" subtitle="Notification channels"/>
                  <div className="px-5 py-4 space-y-3">
                    {pipeline.config?.alerting?.on_failure?.length > 0 && (
                      <div>
                        <p className="text-xs font-semibold text-gray-500 mb-1.5">On failure:</p>
                        <div className="flex flex-wrap gap-2">
                          {pipeline.config.alerting.on_failure.map((ch: string) => (
                            <span key={ch} className="text-xs bg-red-50 text-red-700 border border-red-200 px-2.5 py-1 rounded-full">{ch}</span>
                          ))}
                        </div>
                      </div>
                    )}
                    {pipeline.config?.alerting?.on_sla_breach?.length > 0 && (
                      <div>
                        <p className="text-xs font-semibold text-gray-500 mb-1.5">On SLA breach:</p>
                        <div className="flex flex-wrap gap-2">
                          {pipeline.config.alerting.on_sla_breach.map((ch: string) => (
                            <span key={ch} className="text-xs bg-amber-50 text-amber-700 border border-amber-200 px-2.5 py-1 rounded-full">{ch}</span>
                          ))}
                        </div>
                      </div>
                    )}
                  </div>
                </Card>
              )}

              {/* Tasks */}
              <Card>
                <CardHeader title="Tasks" subtitle={`${tasks.length} task${tasks.length !== 1 ? 's' : ''} in this pipeline`}/>
                <div className="divide-y divide-gray-100">
                  {tasks.length === 0 && (
                    <div className="px-5 py-8 text-center">
                      <p className="text-sm text-gray-400">No tasks configured</p>
                      <button onClick={() => setShowEditor(true)} className="mt-2 text-sm text-blue-600 hover:underline">
                        Add tasks →
                      </button>
                    </div>
                  )}
                  {tasks.map((t: any, idx: number) => {
                    const color = TYPE_COLORS[t.type] || '#94a3b8'
                    const deps  = (t.depends_on || []) as string[]
                    return (
                      <div key={t.task_id} className="px-5 py-4 flex items-start gap-4">
                        <div className="w-8 h-8 rounded-lg flex items-center justify-center flex-shrink-0 text-xs font-bold text-white"
                          style={{ background: color }}>
                          {idx + 1}
                        </div>
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-2 mb-1">
                            <span className="font-mono text-sm font-semibold text-gray-900">{t.task_id}</span>
                            <span
                              className="text-xs px-2 py-0.5 rounded-full font-medium"
                              style={{ background: color + '15', color }}
                            >
                              {TYPE_LABEL[t.type] || t.type}
                            </span>
                          </div>
                          {deps.length > 0 && (
                            <p className="text-xs text-gray-400 mb-1">
                              Runs after: {deps.join(', ')}
                            </p>
                          )}
                          {/* Show key config highlights per type */}
                          <div className="flex flex-wrap gap-x-4 gap-y-1 text-xs text-gray-500">
                            {t.source?.connection && <span>Connection: <span className="font-mono text-gray-700">{t.source.connection}</span></span>}
                            {t.source?.mode       && <span>Mode: <span className="text-gray-700 capitalize">{t.source.mode}</span></span>}
                            {t.output?.table      && <span>Output: <span className="font-mono text-gray-700">{t.output.table}</span></span>}
                            {t.target?.type       && <span>Target: <span className="text-gray-700">{t.target.type}</span></span>}
                            {t.target?.bucket     && <span>Bucket: <span className="font-mono text-gray-700">{t.target.bucket}</span></span>}
                            {t.execution?.image   && <span>Image: <span className="font-mono text-gray-700 truncate max-w-40 block">{t.execution.image.split('/').slice(-1)[0]}</span></span>}
                            {t.channel            && <span>Channel: <span className="text-gray-700">{t.channel}</span></span>}
                            {t.checks?.length > 0 && <span>{t.checks.length} quality check{t.checks.length > 1 ? 's' : ''}</span>}
                            {t.trigger_rule && t.trigger_rule !== 'all_success' && (
                              <span>Trigger: <span className="font-mono text-amber-600">{t.trigger_rule}</span></span>
                            )}
                          </div>
                        </div>
                      </div>
                    )
                  })}
                </div>
              </Card>
            </div>
          )}

          {/* ── Versions ── */}
          {tab === 'versions' && (
            <div className="overflow-y-auto h-full p-6">
              <Card>
                <CardHeader title="Config Versions" subtitle="History of all pipeline configuration changes"/>
                <Table
                  columns={[
                    { key: 'version', header: 'Version', render: v => <code className="text-xs bg-gray-100 px-2 py-0.5 rounded text-gray-700 font-mono">{v}</code> },
                    { key: 'is_active', header: 'Active', render: v => v ? <span className="text-emerald-600 text-xs font-semibold">● Active</span> : <span className="text-gray-400 text-xs">—</span> },
                    { key: 'is_valid', header: 'Valid', render: v => v ? <CheckCircle size={13} className="text-emerald-500"/> : <XCircle size={13} className="text-red-500"/> },
                    { key: 'created_by', header: 'Changed By', render: v => <span className="text-gray-500 text-xs">{v}</span> },
                    { key: 'created_at', header: 'Date', render: v => <TimeAgo ts={v}/> },
                  ]}
                  data={versions}
                  emptyMsg="No versions found"
                />
              </Card>
            </div>
          )}
        </div>
      </div>
    </>
  )
}

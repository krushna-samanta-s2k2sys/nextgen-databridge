import React, { useState, useCallback, useRef } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import toast from 'react-hot-toast'
import {
  ReactFlow, Background, Controls,
  useNodesState, useEdgesState, addEdge,
  Handle, Position,
  type Node, type Edge, type Connection, type NodeProps,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'

import {
  getPipelines, createPipeline, pausePipeline, resumePipeline,
  triggerPipeline, validateConfig,
} from '../api/client'
import {
  Card, CardHeader, Table, StatusBadge, Button, Modal,
  Input, Select, Textarea, Spinner, Tabs, TimeAgo,
} from '../components/ui'
import {
  Plus, Play, Pause, Search, GitBranch, ChevronRight,
  CheckCircle, XCircle, AlertCircle, Trash2, LayoutGrid,
  Database, Upload, Radio, Cpu, Shield, Server, Bell,
  Settings, X, ArrowLeft, ArrowRight,
} from 'lucide-react'
import clsx from 'clsx'

// ─────────────────────────────────────────────────────────────────────────────
// Task type definitions
// ─────────────────────────────────────────────────────────────────────────────
interface TaskNodeData extends Record<string, unknown> {
  task_id: string
  type: string
  config: Record<string, any>
  onSelect: (id: string) => void
  onDelete: (id: string) => void
}

const TASK_CATEGORIES = [
  {
    label: 'Extract',
    items: [
      { type: 'sql_extract',   label: 'SQL Extract',    color: '#3b82f6', icon: Database },
      { type: 'file_ingest',   label: 'File Ingest',    color: '#6366f1', icon: Upload   },
      { type: 'kafka_consume', label: 'Kafka Consume',  color: '#ec4899', icon: Radio    },
    ],
  },
  {
    label: 'Transform',
    items: [
      { type: 'duckdb_transform', label: 'DuckDB Transform', color: '#a855f7', icon: Cpu },
      { type: 'duckdb_query',     label: 'DuckDB Query',     color: '#8b5cf6', icon: Cpu },
    ],
  },
  {
    label: 'Quality',
    items: [
      { type: 'data_quality',   label: 'Data Quality',    color: '#10b981', icon: Shield    },
      { type: 'schema_validate',label: 'Schema Validate', color: '#14b8a6', icon: CheckCircle },
    ],
  },
  {
    label: 'Load',
    items: [
      { type: 'load_target', label: 'Load Target', color: '#f97316', icon: Upload },
      { type: 'eks_job',     label: 'EKS Job',     color: '#eab308', icon: Server },
    ],
  },
  {
    label: 'Notify',
    items: [
      { type: 'notification', label: 'Notification', color: '#71717a', icon: Bell },
    ],
  },
]

const TYPE_COLOR: Record<string, string> = {}
TASK_CATEGORIES.forEach(cat => cat.items.forEach(it => { TYPE_COLOR[it.type] = it.color }))

// ─────────────────────────────────────────────────────────────────────────────
// React Flow – custom TaskNode (must be outside component)
// ─────────────────────────────────────────────────────────────────────────────
function TaskNode({ id, data, selected }: NodeProps) {
  const d = data as TaskNodeData
  const color = TYPE_COLOR[d.type] || '#52525b'

  return (
    <div
      className={clsx(
        'relative bg-zinc-900 border rounded-xl w-52 shadow-xl transition-all cursor-pointer select-none',
        selected ? 'border-blue-500 shadow-blue-500/20' : 'border-zinc-700 hover:border-zinc-500',
      )}
      onClick={() => d.onSelect(id)}
    >
      <Handle
        type="target" position={Position.Left}
        className="!w-3 !h-3 !bg-zinc-700 !border-zinc-500 !-left-1.5"
      />
      {/* Left color strip */}
      <div
        className="absolute left-0 top-0 bottom-0 w-1 rounded-l-xl"
        style={{ background: color }}
      />
      <div className="pl-3.5 pr-2.5 py-3">
        <div className="flex items-start justify-between gap-1 mb-1">
          <span className="text-xs font-mono font-medium text-zinc-100 truncate leading-tight">
            {d.task_id || 'untitled'}
          </span>
          <button
            className="text-zinc-700 hover:text-red-400 flex-shrink-0 transition-colors"
            onClick={(e) => { e.stopPropagation(); d.onDelete(id) }}
          >
            <X size={12} />
          </button>
        </div>
        <span
          className="text-xs font-medium px-1.5 py-0.5 rounded text-white/80"
          style={{ background: color + '40' }}
        >
          {d.type?.replace(/_/g, ' ')}
        </span>
      </div>
      <Handle
        type="source" position={Position.Right}
        className="!w-3 !h-3 !bg-zinc-700 !border-zinc-500 !-right-1.5"
      />
    </div>
  )
}

const nodeTypes = { taskNode: TaskNode }

// ─────────────────────────────────────────────────────────────────────────────
// Layout helpers
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
      pos[id] = { x: l * 280, y: i * 110 - ((ids.length - 1) * 55) }
    })
  })
  return pos
}

function makeNodes(
  tasks: any[],
  onSelect: (id: string) => void,
  onDelete: (id: string) => void,
): Node<TaskNodeData>[] {
  const pos = autoLayout(tasks)
  return tasks.map(t => ({
    id: t.task_id,
    type: 'taskNode',
    position: pos[t.task_id] || { x: 0, y: 0 },
    data: {
      task_id:  t.task_id,
      type:     t.type,
      config:   t,
      onSelect,
      onDelete,
    },
  }))
}

function makeEdges(tasks: any[]): Edge[] {
  const edges: Edge[] = []
  tasks.forEach(t => {
    (t.depends_on || []).forEach((dep: string) => {
      edges.push({
        id: `${dep}→${t.task_id}`,
        source: dep, target: t.task_id,
        type: 'smoothstep', animated: true,
        style: { stroke: '#3f3f46', strokeWidth: 1.5 },
      })
    })
  })
  return edges
}

function nodesToTasks(nodes: Node<TaskNodeData>[], edges: Edge[]): any[] {
  return nodes.map(n => {
    const deps = edges
      .filter(e => e.target === n.id)
      .map(e => nodes.find(x => x.id === e.source)?.data.task_id)
      .filter(Boolean)
    const { task_id, type, depends_on: _d, ...rest } = n.data.config as any
    return { task_id: n.data.task_id, type: n.data.type, depends_on: deps, ...rest }
  })
}

// ─────────────────────────────────────────────────────────────────────────────
// Task Config Panel (right sidebar in canvas step)
// ─────────────────────────────────────────────────────────────────────────────
function TaskConfigPanel({
  node, onChange,
}: {
  node: Node<TaskNodeData>
  onChange: (id: string, updates: Partial<TaskNodeData>) => void
}) {
  const d = node.data
  const cfg = (d.config || {}) as Record<string, any>
  const src = cfg.source || {}
  const out = cfg.output || {}
  const tgt = cfg.target || {}
  const exec = cfg.execution || {}

  function setTaskId(v: string) { onChange(node.id, { task_id: v }) }
  function setSrc(k: string, v: any) { onChange(node.id, { config: { ...cfg, source: { ...src, [k]: v } } }) }
  function setOut(k: string, v: any) { onChange(node.id, { config: { ...cfg, output: { ...out, [k]: v } } }) }
  function setTgt(k: string, v: any) { onChange(node.id, { config: { ...cfg, target: { ...tgt, [k]: v } } }) }
  function setExec(k: string, v: any) { onChange(node.id, { config: { ...cfg, execution: { ...exec, [k]: v } } }) }
  function setField(k: string, v: any) { onChange(node.id, { config: { ...cfg, [k]: v } }) }

  const color = TYPE_COLOR[d.type] || '#52525b'

  return (
    <div className="h-full overflow-y-auto">
      <div className="flex items-center gap-2 mb-4 pb-3 border-b border-zinc-800">
        <div className="w-2 h-5 rounded flex-shrink-0" style={{ background: color }} />
        <div>
          <p className="text-xs font-semibold text-zinc-200">{d.type?.replace(/_/g, ' ')}</p>
          <p className="text-xs text-zinc-600">Task configuration</p>
        </div>
      </div>

      <div className="space-y-3">
        <Input
          label="Task ID *"
          value={d.task_id}
          onChange={e => setTaskId(e.target.value)}
          placeholder="extract_orders"
        />

        {(d.type === 'sql_extract' || d.type === 'cdc_extract') && (
          <>
            <Input label="Connection ID" value={src.connection || ''} onChange={e => setSrc('connection', e.target.value)} placeholder="mssql_wwi" />
            <Select label="Extract Mode" value={src.mode || 'full'} onChange={e => setSrc('mode', e.target.value)}>
              <option value="full">Full Load</option>
              <option value="incremental">Incremental</option>
              <option value="cdc">CDC</option>
            </Select>
            <Textarea label="SQL Query" value={src.query || ''} onChange={e => setSrc('query', e.target.value)} rows={4} placeholder="SELECT * FROM orders WHERE updated_at >= '{{ ds }}'" />
            <Input label="Watermark Column" value={src.watermark_column || ''} onChange={e => setSrc('watermark_column', e.target.value)} placeholder="updated_at" />
            <Input label="Output Table" value={out.table || ''} onChange={e => setOut('table', e.target.value)} placeholder="orders" />
          </>
        )}

        {(d.type === 'duckdb_transform' || d.type === 'duckdb_query') && (
          <>
            <Textarea label="SQL" value={cfg.sql || ''} onChange={e => setField('sql', e.target.value)} rows={6} placeholder="SELECT ... FROM source_table" />
            <Input label="Output Table" value={out.table || ''} onChange={e => setOut('table', e.target.value)} placeholder="transformed_orders" />
          </>
        )}

        {d.type === 'data_quality' && (
          <Textarea
            label="Checks (JSON)"
            value={JSON.stringify(cfg.checks || [], null, 2)}
            onChange={e => { try { setField('checks', JSON.parse(e.target.value)) } catch {} }}
            rows={8}
            placeholder={'[\n  {"type": "not_null", "column": "id", "action": "fail"},\n  {"type": "row_count_min", "value": 1, "action": "fail"}\n]'}
          />
        )}

        {d.type === 'schema_validate' && (
          <Textarea
            label="Schema (JSON)"
            value={JSON.stringify(cfg.schema || {}, null, 2)}
            onChange={e => { try { setField('schema', JSON.parse(e.target.value)) } catch {} }}
            rows={6}
          />
        )}

        {d.type === 'load_target' && (
          <>
            <Select label="Target Type" value={tgt.type || 's3'} onChange={e => setTgt('type', e.target.value)}>
              <option value="s3">Amazon S3</option>
              <option value="mssql">SQL Server</option>
              <option value="postgresql">PostgreSQL</option>
            </Select>
            {(tgt.type === 's3' || !tgt.type) && (
              <>
                <Input label="S3 Bucket" value={tgt.bucket || ''} onChange={e => setTgt('bucket', e.target.value)} placeholder="my-data-lake" />
                <Input label="S3 Path" value={tgt.path || ''} onChange={e => setTgt('path', e.target.value)} placeholder="orders/{{ ds }}/data.parquet" />
                <Select label="Format" value={tgt.format || 'parquet'} onChange={e => setTgt('format', e.target.value)}>
                  <option value="parquet">Parquet</option>
                  <option value="csv">CSV</option>
                  <option value="json">JSON</option>
                </Select>
              </>
            )}
            {(tgt.type === 'mssql' || tgt.type === 'postgresql') && (
              <>
                <Input label="Connection ID" value={tgt.connection || ''} onChange={e => setTgt('connection', e.target.value)} />
                <Input label="Target Table" value={tgt.table || ''} onChange={e => setTgt('table', e.target.value)} />
                <Select label="Load Mode" value={tgt.mode || 'append'} onChange={e => setTgt('mode', e.target.value)}>
                  <option value="append">Append</option>
                  <option value="overwrite">Overwrite</option>
                  <option value="upsert">Upsert</option>
                </Select>
              </>
            )}
          </>
        )}

        {d.type === 'file_ingest' && (
          <>
            <Input label="S3 Path" value={cfg.s3_path || ''} onChange={e => setField('s3_path', e.target.value)} placeholder="s3://bucket/path/*.csv" />
            <Select label="Format" value={cfg.format || 'csv'} onChange={e => setField('format', e.target.value)}>
              <option value="csv">CSV</option>
              <option value="parquet">Parquet</option>
              <option value="json">JSON (Lines)</option>
            </Select>
            <Input label="Output Table" value={out.table || ''} onChange={e => setOut('table', e.target.value)} />
          </>
        )}

        {(d.type === 'kafka_consume' || d.type === 'kafka_produce') && (
          <>
            <Input label="Topic" value={cfg.topic || ''} onChange={e => setField('topic', e.target.value)} />
            <Input label="Group ID" value={cfg.group_id || ''} onChange={e => setField('group_id', e.target.value)} />
            <Input label="Bootstrap Servers" value={cfg.bootstrap_servers || ''} onChange={e => setField('bootstrap_servers', e.target.value)} />
          </>
        )}

        {d.type === 'eks_job' && (
          <>
            <Input label="Docker Image" value={exec.image || ''} onChange={e => setExec('image', e.target.value)} placeholder="myrepo/transform:latest" />
            <div className="grid grid-cols-2 gap-2">
              <Input label="CPU Request" value={exec.cpu || '1'} onChange={e => setExec('cpu', e.target.value)} />
              <Input label="Memory" value={exec.memory || '2Gi'} onChange={e => setExec('memory', e.target.value)} />
            </div>
            <Input label="Namespace" value={exec.namespace || 'nextgen-databridge-jobs'} onChange={e => setExec('namespace', e.target.value)} />
          </>
        )}

        {d.type === 'notification' && (
          <>
            <Select label="Channel" value={cfg.channel || 'email'} onChange={e => setField('channel', e.target.value)}>
              <option value="email">Email</option>
              <option value="slack">Slack</option>
            </Select>
            <Input label="Recipient / Webhook" value={cfg.recipient || ''} onChange={e => setField('recipient', e.target.value)} />
            <Textarea label="Message Template" value={cfg.message || ''} onChange={e => setField('message', e.target.value)} rows={3} />
          </>
        )}

        <div className="pt-2 border-t border-zinc-800">
          <Input
            label="Group (optional)"
            value={cfg.group || ''}
            onChange={e => setField('group', e.target.value)}
            placeholder="parallel_group"
          />
          <div className="mt-2">
            <Select label="Trigger Rule" value={cfg.trigger_rule || 'all_success'} onChange={e => setField('trigger_rule', e.target.value)}>
              <option value="all_success">all_success</option>
              <option value="all_done">all_done</option>
              <option value="one_success">one_success</option>
              <option value="one_failed">one_failed</option>
              <option value="none_failed">none_failed</option>
            </Select>
          </div>
        </div>
      </div>
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// Pipeline Builder (full-screen wizard)
// ─────────────────────────────────────────────────────────────────────────────
const DEFAULT_META = {
  pipeline_id: '', name: '', description: '', schedule: '',
  owner: '', sla_minutes: '', retries: '3', retry_delay_minutes: '5',
  tags: '', alerting_failure: '', alerting_sla: '',
}

function PipelineBuilder({
  initial,
  onSave,
  onCancel,
}: {
  initial?: any
  onSave: (config: any) => void
  onCancel: () => void
}) {
  const [step, setStep] = useState(1)
  const [meta, setMeta] = useState(() => {
    if (!initial) return DEFAULT_META
    return {
      pipeline_id: initial.pipeline_id || '',
      name:        initial.name || '',
      description: initial.description || '',
      schedule:    initial.schedule || '',
      owner:       initial.owner || '',
      sla_minutes: String(initial.sla_minutes || ''),
      retries:     String(initial.retries || '3'),
      retry_delay_minutes: String(initial.retry_delay_minutes || '5'),
      tags: (initial.tags || []).join(', '),
      alerting_failure: (initial.alerting?.on_failure || []).join(', '),
      alerting_sla:     (initial.alerting?.on_sla_breach || []).join(', '),
    }
  })

  const [selectedId, setSelectedId] = useState<string | null>(null)
  const [nodeCount, setNodeCount] = useState(0)

  const initialTasks = initial?.tasks || []
  const [nodes, setNodes, onNodesChange] = useNodesState<Node<TaskNodeData>>(
    makeNodes(initialTasks, id => setSelectedId(id), id => deleteNode(id))
  )
  const [edges, setEdges, onEdgesChange] = useEdgesState(makeEdges(initialTasks))

  const [valResult, setValResult] = useState<any>(null)
  const [validating, setValidating] = useState(false)
  const valMut = useMutation({ mutationFn: validateConfig, onSuccess: r => setValResult(r) })

  function deleteNode(id: string) {
    setNodes(ns => ns.filter(n => n.id !== id))
    setEdges(es => es.filter(e => e.source !== id && e.target !== id))
    setSelectedId(prev => prev === id ? null : prev)
  }

  const handleSelect = useCallback((id: string) => {
    setSelectedId(id)
  }, [])

  const handleDelete = useCallback((id: string) => {
    deleteNode(id)
  }, [])

  const onConnect = useCallback((conn: Connection) => {
    setEdges(es => addEdge({
      ...conn,
      type: 'smoothstep', animated: true,
      style: { stroke: '#3f3f46', strokeWidth: 1.5 },
    }, es))
  }, [setEdges])

  function addTaskNode(type: string) {
    const count = nodeCount + 1
    setNodeCount(count)
    const id  = `task_${count}`
    const newNode: Node<TaskNodeData> = {
      id,
      type: 'taskNode',
      position: { x: 80 + Math.random() * 200, y: 80 + Math.random() * 150 },
      data: {
        task_id:  `${type}_${count}`,
        type,
        config:   {},
        onSelect: handleSelect,
        onDelete: handleDelete,
      },
    }
    setNodes(ns => [...ns, newNode])
    setSelectedId(id)
  }

  function updateNodeData(id: string, updates: Partial<TaskNodeData>) {
    setNodes(ns => ns.map(n => n.id !== id ? n : {
      ...n,
      data: {
        ...n.data,
        ...updates,
        onSelect: handleSelect,
        onDelete: handleDelete,
      },
    }))
  }

  function autoLayoutAll() {
    const tasks = nodesToTasks(nodes, edges)
    const pos = autoLayout(tasks)
    setNodes(ns => ns.map(n => ({
      ...n,
      position: pos[n.data.task_id] || n.position,
    })))
  }

  function buildConfig() {
    const tags = meta.tags.split(',').map(t => t.trim()).filter(Boolean)
    const failureChannels = meta.alerting_failure.split(',').map(s => s.trim()).filter(Boolean)
    const slaChannels     = meta.alerting_sla.split(',').map(s => s.trim()).filter(Boolean)
    return {
      pipeline_id:          meta.pipeline_id,
      name:                 meta.name || meta.pipeline_id,
      description:          meta.description,
      schedule:             meta.schedule || undefined,
      owner:                meta.owner,
      sla_minutes:          meta.sla_minutes ? Number(meta.sla_minutes) : undefined,
      retries:              Number(meta.retries || 3),
      retry_delay_minutes:  Number(meta.retry_delay_minutes || 5),
      tags,
      alerting: {
        on_failure:   failureChannels,
        on_sla_breach: slaChannels,
      },
      tasks: nodesToTasks(nodes, edges),
    }
  }

  const selectedNode = nodes.find(n => n.id === selectedId) || null

  const STEPS = [
    { num: 1, label: 'Pipeline Settings' },
    { num: 2, label: 'Task Canvas' },
    { num: 3, label: 'Review & Deploy' },
  ]

  return (
    <div className="fixed inset-0 z-50 bg-zinc-950 flex flex-col">
      {/* Top bar */}
      <div className="flex items-center justify-between px-6 py-3.5 bg-zinc-900 border-b border-zinc-800 flex-shrink-0">
        <div className="flex items-center gap-4">
          <button onClick={onCancel} className="text-zinc-500 hover:text-zinc-200 transition-colors">
            <ArrowLeft size={16} />
          </button>
          <div>
            <h1 className="text-sm font-bold text-zinc-100">
              {initial ? `Edit Pipeline: ${initial.pipeline_id}` : 'New Pipeline'}
            </h1>
            <p className="text-xs text-zinc-600">
              {meta.pipeline_id || 'Define your pipeline configuration'}
            </p>
          </div>
        </div>

        {/* Step indicators */}
        <div className="flex items-center gap-1">
          {STEPS.map((s, i) => (
            <React.Fragment key={s.num}>
              <button
                onClick={() => setStep(s.num)}
                className={clsx(
                  'flex items-center gap-2 px-3 py-1.5 rounded-lg text-xs transition-all',
                  step === s.num
                    ? 'bg-blue-600 text-white font-medium'
                    : step > s.num
                      ? 'bg-zinc-800 text-emerald-400'
                      : 'text-zinc-600',
                )}
              >
                <span className={clsx(
                  'w-4 h-4 rounded-full flex items-center justify-center text-xs font-bold flex-shrink-0',
                  step === s.num ? 'bg-white/20' : step > s.num ? 'bg-emerald-500/20' : 'bg-zinc-700',
                )}>
                  {step > s.num ? '✓' : s.num}
                </span>
                {s.label}
              </button>
              {i < STEPS.length - 1 && (
                <ChevronRight size={14} className="text-zinc-700 flex-shrink-0" />
              )}
            </React.Fragment>
          ))}
        </div>

        <div className="flex items-center gap-2">
          <Button variant="ghost" onClick={onCancel} className="!text-zinc-300 hover:!text-white hover:!bg-zinc-700">Cancel</Button>
          <Button
            variant="primary"
            onClick={() => onSave(buildConfig())}
            disabled={!meta.pipeline_id}
          >
            {initial ? 'Save Changes' : 'Create Pipeline'}
          </Button>
        </div>
      </div>

      {/* ── Step 1: Pipeline Settings ── */}
      {step === 1 && (
        <div className="flex-1 overflow-y-auto p-6 bg-gray-50">
          <div className="max-w-2xl mx-auto space-y-6">
            <div>
              <h2 className="text-sm font-semibold text-gray-900 mb-1">Pipeline Settings</h2>
              <p className="text-xs text-gray-500">Configure the pipeline identity, schedule, and alerting</p>
            </div>

            {/* Identity */}
            <div className="bg-white border border-gray-200 rounded-xl p-5 space-y-4 shadow-sm">
              <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wide">Identity</h3>
              <div className="grid grid-cols-2 gap-4">
                <Input
                  label="Pipeline ID *"
                  value={meta.pipeline_id}
                  onChange={e => setMeta({ ...meta, pipeline_id: e.target.value.toLowerCase().replace(/\s+/g, '_').replace(/[^a-z0-9_]/g, '') })}
                  placeholder="my_pipeline"
                  hint="Lowercase letters, numbers, underscores"
                />
                <Input
                  label="Display Name"
                  value={meta.name}
                  onChange={e => setMeta({ ...meta, name: e.target.value })}
                  placeholder="My Pipeline"
                />
              </div>
              <Textarea
                label="Description"
                value={meta.description}
                onChange={e => setMeta({ ...meta, description: e.target.value })}
                rows={2}
                placeholder="Describe what this pipeline does…"
              />
              <div className="grid grid-cols-2 gap-4">
                <Input
                  label="Owner"
                  value={meta.owner}
                  onChange={e => setMeta({ ...meta, owner: e.target.value })}
                  placeholder="data-engineering"
                />
                <Input
                  label="Tags (comma-separated)"
                  value={meta.tags}
                  onChange={e => setMeta({ ...meta, tags: e.target.value })}
                  placeholder="finance, daily, critical"
                />
              </div>
            </div>

            {/* Scheduling */}
            <div className="bg-white border border-gray-200 rounded-xl p-5 space-y-4 shadow-sm">
              <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wide">Scheduling</h3>
              <div className="grid grid-cols-2 gap-4">
                <Input
                  label="Cron Schedule"
                  value={meta.schedule}
                  onChange={e => setMeta({ ...meta, schedule: e.target.value })}
                  placeholder="0 6 * * *   (leave blank for manual)"
                  hint="Standard cron expression or @daily, @hourly"
                />
                <Input
                  label="SLA (minutes)"
                  type="number"
                  value={meta.sla_minutes}
                  onChange={e => setMeta({ ...meta, sla_minutes: e.target.value })}
                  placeholder="120"
                />
              </div>
            </div>

            {/* Reliability */}
            <div className="bg-white border border-gray-200 rounded-xl p-5 space-y-4 shadow-sm">
              <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wide">Reliability</h3>
              <div className="grid grid-cols-2 gap-4">
                <Input
                  label="Max Retries"
                  type="number"
                  value={meta.retries}
                  onChange={e => setMeta({ ...meta, retries: e.target.value })}
                />
                <Input
                  label="Retry Delay (minutes)"
                  type="number"
                  value={meta.retry_delay_minutes}
                  onChange={e => setMeta({ ...meta, retry_delay_minutes: e.target.value })}
                />
              </div>
            </div>

            {/* Alerting */}
            <div className="bg-white border border-gray-200 rounded-xl p-5 space-y-4 shadow-sm">
              <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wide">Alerting</h3>
              <Input
                label="On Failure Channels"
                value={meta.alerting_failure}
                onChange={e => setMeta({ ...meta, alerting_failure: e.target.value })}
                placeholder="email:team@company.com, slack:#data-alerts"
                hint="Prefix with email: or slack: — separate multiple with commas"
              />
              <Input
                label="On SLA Breach Channels"
                value={meta.alerting_sla}
                onChange={e => setMeta({ ...meta, alerting_sla: e.target.value })}
                placeholder="slack:#data-alerts"
              />
            </div>

            <div className="flex justify-end">
              <Button
                variant="primary"
                onClick={() => setStep(2)}
                disabled={!meta.pipeline_id}
              >
                Next: Task Canvas <ArrowRight size={14} />
              </Button>
            </div>
          </div>
        </div>
      )}

      {/* ── Step 2: Task Canvas ── */}
      {step === 2 && (
        <div className="flex-1 flex overflow-hidden min-h-0">
          {/* Task Palette */}
          <div className="w-52 bg-zinc-900 border-r border-zinc-800 flex-shrink-0 overflow-y-auto p-3">
            <p className="text-xs font-semibold text-zinc-500 uppercase tracking-wide mb-3 px-1">
              Task Types
            </p>
            {TASK_CATEGORIES.map(cat => (
              <div key={cat.label} className="mb-4">
                <p className="text-xs text-zinc-600 px-1 mb-1.5">{cat.label}</p>
                {cat.items.map(it => {
                  const Icon = it.icon
                  return (
                    <button
                      key={it.type}
                      onClick={() => addTaskNode(it.type)}
                      className="w-full flex items-center gap-2.5 px-2.5 py-2 rounded-lg text-xs text-zinc-300 hover:bg-zinc-800 hover:text-zinc-100 transition-all mb-0.5 text-left group"
                    >
                      <div
                        className="w-6 h-6 rounded-md flex items-center justify-center flex-shrink-0 opacity-80 group-hover:opacity-100"
                        style={{ background: it.color + '25' }}
                      >
                        <Icon size={12} style={{ color: it.color }} />
                      </div>
                      <span className="truncate">{it.label}</span>
                      <Plus size={10} className="ml-auto text-zinc-700 group-hover:text-zinc-400" />
                    </button>
                  )
                })}
              </div>
            ))}
            <div className="border-t border-zinc-800 pt-3 mt-2">
              <button
                onClick={autoLayoutAll}
                className="w-full flex items-center gap-2 px-2.5 py-2 rounded-lg text-xs text-zinc-500 hover:text-zinc-300 hover:bg-zinc-800 transition-all"
              >
                <LayoutGrid size={12} />
                Auto Layout
              </button>
            </div>
          </div>

          {/* React Flow Canvas */}
          <div className="flex-1 relative min-w-0">
            {nodes.length === 0 && (
              <div className="absolute inset-0 flex flex-col items-center justify-center gap-3 pointer-events-none z-10">
                <div className="w-12 h-12 rounded-xl bg-zinc-800 border border-zinc-700 flex items-center justify-center">
                  <GitBranch size={22} className="text-zinc-600" />
                </div>
                <p className="text-sm text-zinc-500">Click a task type on the left to add it to the canvas</p>
                <p className="text-xs text-zinc-700">Drag nodes to reposition · Connect handles to set dependencies</p>
              </div>
            )}
            <ReactFlow
              nodes={nodes}
              edges={edges}
              onNodesChange={onNodesChange}
              onEdgesChange={onEdgesChange}
              onConnect={onConnect}
              nodeTypes={nodeTypes}
              onNodeClick={(_, node) => setSelectedId(node.id)}
              onPaneClick={() => setSelectedId(null)}
              fitView
              fitViewOptions={{ padding: 0.3 }}
              defaultEdgeOptions={{
                type: 'smoothstep',
                animated: true,
                style: { stroke: '#3f3f46', strokeWidth: 1.5 },
              }}
            >
              <Background color="#27272a" gap={24} size={1} />
              <Controls className="!bg-zinc-900 !border-zinc-700 !shadow-xl" />
            </ReactFlow>
            {/* Bottom status bar */}
            <div className="absolute bottom-0 left-0 right-0 flex items-center justify-between px-4 py-2 bg-zinc-900/80 border-t border-zinc-800 text-xs text-zinc-600 backdrop-blur-sm">
              <span>{nodes.length} tasks · {edges.length} dependencies</span>
              <span>Drag between handles to connect tasks</span>
            </div>
          </div>

          {/* Task Config Panel */}
          {selectedNode && (
            <div className="w-72 bg-zinc-900 border-l border-zinc-800 flex-shrink-0 flex flex-col">
              <div className="flex items-center justify-between px-4 py-3 border-b border-zinc-800 flex-shrink-0">
                <span className="text-xs font-semibold text-zinc-300">Configure Task</span>
                <button
                  onClick={() => setSelectedId(null)}
                  className="text-zinc-600 hover:text-zinc-300"
                >
                  <X size={14} />
                </button>
              </div>
              <div className="flex-1 overflow-y-auto p-4">
                <TaskConfigPanel node={selectedNode} onChange={updateNodeData} />
              </div>
            </div>
          )}
        </div>
      )}

      {/* ── Step 3: Review ── */}
      {step === 3 && (
        <div className="flex-1 overflow-y-auto p-6 bg-gray-50">
          <div className="max-w-3xl mx-auto space-y-5">
            <div>
              <h2 className="text-sm font-semibold text-gray-900 mb-1">Review & Deploy</h2>
              <p className="text-xs text-gray-500">Review the pipeline configuration before saving</p>
            </div>

            <div className="flex items-center gap-3">
              <Button
                variant="secondary"
                onClick={() => { setValidating(true); valMut.mutate(buildConfig()) }}
                disabled={valMut.isPending}
              >
                {valMut.isPending ? <Spinner size="sm"/> : <CheckCircle size={14}/>}
                Validate Config
              </Button>
              {valResult && (
                <div className={clsx(
                  'flex items-center gap-1.5 text-xs px-3 py-1.5 rounded-lg border',
                  valResult.valid ? 'text-emerald-700 bg-emerald-50 border-emerald-200' : 'text-red-700 bg-red-50 border-red-200',
                )}>
                  {valResult.valid
                    ? <><CheckCircle size={12}/> Config is valid</>
                    : <><XCircle size={12}/> {valResult.errors?.length} error(s) found</>}
                </div>
              )}
            </div>

            {valResult?.errors?.map((e: string, i: number) => (
              <div key={i} className="flex items-start gap-2 text-xs text-red-700 bg-red-50 border border-red-200 rounded-lg px-3 py-2">
                <XCircle size={12} className="flex-shrink-0 mt-0.5"/>{e}
              </div>
            ))}

            {/* Pipeline summary — no raw JSON */}
            {(() => {
              const cfg = buildConfig()
              const tasks = cfg.tasks || []
              return (
                <div className="space-y-4">
                  {/* Identity */}
                  <div className="bg-white border border-gray-200 rounded-xl p-5 shadow-sm">
                    <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-3">Pipeline Identity</h3>
                    <div className="grid grid-cols-2 gap-x-8 gap-y-2.5 text-xs">
                      <div><span className="text-gray-500">ID</span><span className="ml-3 font-mono text-gray-900">{cfg.pipeline_id}</span></div>
                      <div><span className="text-gray-500">Name</span><span className="ml-3 text-gray-900">{cfg.name || '—'}</span></div>
                      <div><span className="text-gray-500">Owner</span><span className="ml-3 text-gray-900">{cfg.owner || '—'}</span></div>
                      <div><span className="text-gray-500">Schedule</span><span className="ml-3 font-mono text-gray-900">{cfg.schedule || 'manual'}</span></div>
                      {cfg.description && (
                        <div className="col-span-2"><span className="text-gray-500">Description</span><span className="ml-3 text-gray-700">{cfg.description}</span></div>
                      )}
                      {cfg.tags?.length > 0 && (
                        <div className="col-span-2 flex items-center gap-1.5 flex-wrap">
                          <span className="text-gray-500 mr-1">Tags</span>
                          {cfg.tags.map((t: string) => (
                            <span key={t} className="bg-gray-100 border border-gray-200 text-gray-700 text-xs px-2 py-0.5 rounded">{t}</span>
                          ))}
                        </div>
                      )}
                    </div>
                  </div>

                  {/* Reliability + Alerting */}
                  <div className="grid grid-cols-2 gap-4">
                    <div className="bg-white border border-gray-200 rounded-xl p-4 shadow-sm">
                      <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-3">Reliability</h3>
                      <div className="space-y-1.5 text-xs">
                        <div className="flex justify-between"><span className="text-gray-500">Max retries</span><span className="text-gray-900">{cfg.retries}</span></div>
                        <div className="flex justify-between"><span className="text-gray-500">Retry delay</span><span className="text-gray-900">{cfg.retry_delay_minutes} min</span></div>
                        {cfg.sla_minutes && <div className="flex justify-between"><span className="text-gray-500">SLA</span><span className="text-gray-900">{cfg.sla_minutes} min</span></div>}
                      </div>
                    </div>
                    <div className="bg-white border border-gray-200 rounded-xl p-4 shadow-sm">
                      <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-3">Alerting</h3>
                      <div className="space-y-1.5 text-xs">
                        {cfg.alerting?.on_failure?.length > 0
                          ? <div><span className="text-gray-500 block mb-1">On failure</span>{cfg.alerting.on_failure.map((c: string) => <div key={c} className="text-gray-700 font-mono truncate">{c}</div>)}</div>
                          : <span className="text-gray-400">No failure alerts configured</span>
                        }
                        {cfg.alerting?.on_sla_breach?.length > 0 && (
                          <div className="mt-2"><span className="text-gray-500 block mb-1">On SLA breach</span>{cfg.alerting.on_sla_breach.map((c: string) => <div key={c} className="text-gray-700 font-mono truncate">{c}</div>)}</div>
                        )}
                      </div>
                    </div>
                  </div>

                  {/* Tasks */}
                  <div className="bg-white border border-gray-200 rounded-xl p-5 shadow-sm">
                    <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-3">{tasks.length} Task{tasks.length !== 1 ? 's' : ''}</h3>
                    {tasks.length === 0
                      ? <p className="text-xs text-gray-400">No tasks added yet — go back to Step 2 to add tasks.</p>
                      : (
                        <div className="space-y-2">
                          {tasks.map((t: any, i: number) => {
                            const color = TYPE_COLOR[t.type] || '#9ca3af'
                            return (
                              <div key={t.task_id} className="flex items-center gap-3 bg-gray-50 border border-gray-100 rounded-lg px-3 py-2.5">
                                <span className="w-5 h-5 rounded flex items-center justify-center text-xs text-gray-400 flex-shrink-0">{i + 1}</span>
                                <div className="w-2 h-4 rounded flex-shrink-0" style={{ background: color }}/>
                                <div className="flex-1 min-w-0">
                                  <span className="text-xs font-mono font-semibold text-gray-800">{t.task_id}</span>
                                  <span className="ml-2 text-xs text-gray-500">{t.type?.replace(/_/g, ' ')}</span>
                                  {t.depends_on?.length > 0 && (
                                    <span className="ml-2 text-xs text-gray-400">← {t.depends_on.join(', ')}</span>
                                  )}
                                </div>
                              </div>
                            )
                          })}
                        </div>
                      )
                    }
                  </div>
                </div>
              )
            })()}
          </div>
        </div>
      )}

      {/* Bottom nav */}
      {step !== 1 && (
        <div className="flex items-center justify-between px-6 py-3 bg-zinc-900 border-t border-zinc-800 flex-shrink-0">
          <Button variant="ghost" onClick={() => setStep(s => Math.max(1, s - 1))}>
            <ArrowLeft size={14}/> Back
          </Button>
          {step < 3
            ? <Button variant="primary" onClick={() => setStep(s => Math.min(3, s + 1))}>
                Next <ArrowRight size={14}/>
              </Button>
            : <Button
                variant="primary"
                onClick={() => onSave(buildConfig())}
                disabled={!meta.pipeline_id}
              >
                {initial ? 'Save Changes' : 'Create Pipeline'}
              </Button>
          }
        </div>
      )}
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// Main Pipelines page
// ─────────────────────────────────────────────────────────────────────────────
export default function Pipelines() {
  const nav = useNavigate()
  const qc = useQueryClient()
  const [search, setSearch]         = useState('')
  const [builderOpen, setBuilderOpen] = useState(false)
  const [editTarget, setEditTarget] = useState<any>(null)

  const { data, isLoading } = useQuery({
    queryKey: ['pipelines', search],
    queryFn: () => getPipelines(search ? { search } : undefined),
    refetchInterval: 15_000,
  })
  const pipelines = data?.pipelines ?? []

  const createMut = useMutation({
    mutationFn: createPipeline,
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['pipelines'] })
      setBuilderOpen(false)
      setEditTarget(null)
      toast.success('Pipeline created')
    },
    onError: (e: any) => toast.error(e.response?.data?.detail || 'Create failed'),
  })

  const pauseMut = useMutation({
    mutationFn: (id: string) => pausePipeline(id),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['pipelines'] }); toast.success('Paused') },
  })
  const resumeMut = useMutation({
    mutationFn: (id: string) => resumePipeline(id),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['pipelines'] }); toast.success('Resumed') },
  })
  const triggerMut = useMutation({
    mutationFn: (id: string) => triggerPipeline(id, { trigger_type: 'manual' }),
    onSuccess: (_, id) => { toast.success(`Triggered ${id}`); nav('/runs') },
  })

  function handleSave(config: any) {
    createMut.mutate({
      pipeline_id: config.pipeline_id,
      name:        config.name || config.pipeline_id,
      config,
      schedule:    config.schedule,
      tags:        config.tags || [],
      owner:       config.owner,
      sla_minutes: config.sla_minutes,
    })
  }

  return (
    <>
      {builderOpen && (
        <PipelineBuilder
          initial={editTarget}
          onSave={handleSave}
          onCancel={() => { setBuilderOpen(false); setEditTarget(null) }}
        />
      )}

      <div className="p-6 space-y-5 overflow-y-auto h-full bg-gray-50">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-xl font-bold text-gray-900">Pipelines</h1>
            <p className="text-sm text-gray-500 mt-0.5">
              {pipelines.length} pipeline{pipelines.length !== 1 ? 's' : ''} registered
            </p>
          </div>
          <Button
            variant="primary"
            onClick={() => { setEditTarget(null); setBuilderOpen(true) }}
          >
            <Plus size={14}/> New Pipeline
          </Button>
        </div>

        {/* Search */}
        <div className="relative max-w-xs">
          <Search size={13} className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" />
          <input
            value={search}
            onChange={e => setSearch(e.target.value)}
            placeholder="Search pipelines…"
            className="w-full bg-white border border-gray-300 rounded-lg pl-8 pr-3 py-2 text-sm text-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-500 placeholder:text-gray-400 shadow-sm"
          />
        </div>

        <Card>
          {isLoading
            ? <div className="flex justify-center py-14"><Spinner size="md"/></div>
            : (
              <Table
                onRowClick={r => nav(`/pipelines/${r.pipeline_id}`)}
                columns={[
                  {
                    key: 'pipeline_id', header: 'Pipeline ID',
                    render: v => <span className="font-mono text-sm text-blue-600 font-medium">{v}</span>,
                  },
                  { key: 'name', header: 'Name', render: v => <span className="text-gray-800">{v}</span> },
                  { key: 'status', header: 'Status', render: v => <StatusBadge status={v} /> },
                  {
                    key: 'schedule', header: 'Schedule',
                    render: v => v
                      ? <code className="text-xs bg-gray-100 px-2 py-0.5 rounded text-gray-700 font-mono">{v}</code>
                      : <span className="text-gray-400 text-xs">manual</span>,
                  },
                  {
                    key: 'last_run', header: 'Last Run',
                    render: v => v
                      ? <StatusBadge status={v.status} />
                      : <span className="text-gray-400 text-xs">never</span>,
                  },
                  {
                    key: 'owner', header: 'Owner',
                    render: v => <span className="text-gray-500 text-xs">{v || '—'}</span>,
                  },
                  {
                    key: '_actions', header: '',
                    render: (_, row) => (
                      <div className="flex gap-1" onClick={e => e.stopPropagation()}>
                        <Button size="xs" variant="ghost" title="Trigger run"
                          onClick={() => triggerMut.mutate(row.pipeline_id)}>
                          <Play size={11}/>
                        </Button>
                        <Button size="xs" variant="ghost" title="Configure"
                          onClick={() => { setEditTarget(row.config); setBuilderOpen(true) }}>
                          <Settings size={11}/>
                        </Button>
                        {row.status === 'active'
                          ? <Button size="xs" variant="ghost" title="Pause"
                              onClick={() => pauseMut.mutate(row.pipeline_id)}>
                              <Pause size={11}/>
                            </Button>
                          : <Button size="xs" variant="ghost" title="Resume"
                              onClick={() => resumeMut.mutate(row.pipeline_id)}>
                              <Play size={11}/>
                            </Button>
                        }
                      </div>
                    ),
                  },
                ]}
                data={pipelines}
                emptyMsg="No pipelines — click 'New Pipeline' to create your first one"
              />
            )
          }
        </Card>
      </div>
    </>
  )
}

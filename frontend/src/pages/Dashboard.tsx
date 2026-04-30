import React, { useState, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import {
  ScatterChart, Scatter, XAxis, YAxis, Tooltip as RechartsTooltip,
  ResponsiveContainer, CartesianGrid, Legend,
} from 'recharts'
import { getDashboardMetrics, getRuns, getPipelines } from '../api/client'
import { Card, CardHeader, StatusBadge, Spinner, Duration, TimeAgo } from '../components/ui'
import {
  Activity, CheckCircle, ArrowRight, AlertTriangle,
  XCircle, Clock, BarChart2,
} from 'lucide-react'
import clsx from 'clsx'

// ── Time window options ───────────────────────────────────────────────────────
const TIME_WINDOWS = [
  { label: '15m', ms: 15   * 60 * 1000 },
  { label: '30m', ms: 30   * 60 * 1000 },
  { label: '1h',  ms:  1   * 60 * 60 * 1000 },
  { label: '6h',  ms:  6   * 60 * 60 * 1000 },
  { label: '12h', ms: 12   * 60 * 60 * 1000 },
  { label: '1d',  ms: 24   * 60 * 60 * 1000 },
  { label: '7d',  ms:  7 * 24 * 60 * 60 * 1000 },
  { label: '30d', ms: 30 * 24 * 60 * 60 * 1000 },
]
const DEFAULT_WINDOW = TIME_WINDOWS[5] // 1d

// ── Status series for scatter chart ──────────────────────────────────────────
const STATUS_SERIES = [
  { key: 'success', label: 'Success', color: '#10b981' },
  { key: 'failed',  label: 'Failed',  color: '#ef4444' },
  { key: 'running', label: 'Running', color: '#3b82f6' },
]

function xTickFmt(ts: number, windowMs: number): string {
  const d = new Date(ts)
  if (windowMs <= 60 * 60 * 1000) return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
  if (windowMs <= 24 * 60 * 60 * 1000) return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
  return d.toLocaleDateString([], { month: 'short', day: 'numeric' })
}

// ── Scatter tooltip ───────────────────────────────────────────────────────────
function RunTooltip({ active, payload }: any) {
  if (!active || !payload?.length) return null
  const d = payload[0]?.payload
  if (!d) return null
  const durLabel = d.y >= 1 ? `${d.y} min` : `${Math.round(d.y * 60)}s`
  return (
    <div className="bg-white border border-gray-200 rounded-lg p-3 text-xs shadow-xl min-w-[165px]">
      <p className="font-semibold text-gray-800 mb-1">{d.pipeline_id}</p>
      <p className="text-gray-500">{new Date(d.x).toLocaleString()}</p>
      <p className="text-gray-700 mt-1">Duration: <b>{durLabel}</b></p>
      <p className={clsx('mt-0.5 font-medium capitalize',
        d.status === 'success' ? 'text-emerald-600' :
        d.status === 'failed'  ? 'text-red-500'     : 'text-blue-500')}>
        {d.status}
      </p>
    </div>
  )
}

// ── Stat card ─────────────────────────────────────────────────────────────────
function StatCard({ label, value, sub, color, icon }: {
  label: string; value: string | number; sub?: string; color: string; icon: React.ReactNode
}) {
  const palette: Record<string, string> = {
    blue:   'text-blue-600 bg-blue-50 border-blue-100',
    green:  'text-emerald-600 bg-emerald-50 border-emerald-100',
    red:    'text-red-600 bg-red-50 border-red-100',
    amber:  'text-amber-600 bg-amber-50 border-amber-100',
  }
  return (
    <div className="bg-white border border-gray-200 rounded-xl p-5 shadow-sm">
      <div className="flex items-center justify-between mb-3">
        <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide">{label}</p>
        <div className={`w-8 h-8 rounded-lg flex items-center justify-center border ${palette[color] ?? palette.blue}`}>
          {icon}
        </div>
      </div>
      <p className="text-2xl font-bold text-gray-900 tabular-nums">{value}</p>
      {sub && <p className="text-xs text-gray-400 mt-1">{sub}</p>}
    </div>
  )
}

function ErrorBanner({ message }: { message: string }) {
  return (
    <div className="flex items-center gap-2 text-xs text-red-700 bg-red-50 border border-red-200 rounded-lg px-4 py-2.5">
      <XCircle size={13} className="flex-shrink-0 text-red-500" />
      <span className="font-medium">API error:</span> {message}
    </div>
  )
}

// ── Dashboard ─────────────────────────────────────────────────────────────────
export default function Dashboard() {
  const nav = useNavigate()
  const [timeWindow, setTimeWindow] = useState(DEFAULT_WINDOW)

  const { data: metrics, isLoading, isError, error: metricsErr } = useQuery({
    queryKey: ['dashboard-metrics'],
    queryFn: getDashboardMetrics,
    refetchInterval: 15_000,
  })
  const { data: runsData, isError: runsErr } = useQuery({
    queryKey: ['dashboard-runs'],
    queryFn: () => getRuns({ page_size: '500' }),
    refetchInterval: 10_000,
  })
  const { data: pipelinesData } = useQuery({
    queryKey: ['pipelines-health'],
    queryFn: () => getPipelines({ page_size: '50' }),
    refetchInterval: 30_000,
  })

  const allRuns   = runsData?.runs      ?? []
  const pipelines = pipelinesData?.pipelines ?? []
  const m         = metrics ?? {}

  // Filter runs to the selected time window
  const filteredRuns = useMemo(() => {
    const start = Date.now() - timeWindow.ms
    return (allRuns as any[]).filter(r =>
      r.start_time && new Date(r.start_time).getTime() >= start
    )
  }, [allRuns, timeWindow.ms])

  // KPIs derived from filtered runs
  const activeRuns   = filteredRuns.filter(r => r.status === 'running').length
  const failedCount  = filteredRuns.filter(r => r.status === 'failed').length
  const successCount = filteredRuns.filter(r => r.status === 'success').length
  const totalDone    = successCount + failedCount
  const successRate  = totalDone > 0 ? Math.round(successCount / totalDone * 100) : (m.success_rate ?? 0)

  // Scatter chart data split by status
  const scatterByStatus = useMemo(() => {
    const buckets: Record<string, any[]> = { success: [], failed: [], running: [] }
    filteredRuns.forEach(r => {
      const key = r.status in buckets ? r.status : 'running'
      buckets[key].push({
        x:           new Date(r.start_time).getTime(),
        y:           Math.round((r.duration_seconds || 0) / 60 * 10) / 10,
        pipeline_id: r.pipeline_id,
        status:      r.status,
        run_id:      r.run_id,
      })
    })
    return buckets
  }, [filteredRuns])

  const xMin = Date.now() - timeWindow.ms
  const xMax = Date.now()

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-full bg-gray-50">
        <div className="flex flex-col items-center gap-3">
          <Spinner size="lg" />
          <p className="text-gray-400 text-sm">Loading…</p>
        </div>
      </div>
    )
  }

  return (
    <div className="p-6 space-y-5 overflow-y-auto h-full bg-gray-50">

      {/* Header */}
      <div className="flex items-center justify-between flex-wrap gap-3">
        <div>
          <h1 className="text-xl font-bold text-gray-900">Dashboard</h1>
          <p className="text-sm text-gray-500 mt-0.5">Pipeline health at a glance</p>
        </div>
        <div className="flex items-center gap-3 flex-wrap">
          {/* Time window selector */}
          <div className="flex items-center bg-white border border-gray-200 rounded-lg p-0.5 shadow-sm">
            {TIME_WINDOWS.map(w => (
              <button
                key={w.label}
                onClick={() => setTimeWindow(w)}
                className={clsx(
                  'px-2.5 py-1 text-xs rounded-md font-medium transition-colors',
                  timeWindow.label === w.label
                    ? 'bg-blue-600 text-white shadow-sm'
                    : 'text-gray-500 hover:text-gray-800 hover:bg-gray-100',
                )}
              >
                {w.label}
              </button>
            ))}
          </div>
          <div className="flex items-center gap-2 text-xs text-gray-500 bg-white border border-gray-200 px-3 py-1.5 rounded-lg shadow-sm">
            <Activity size={12} className="text-emerald-500 animate-pulse" />
            Live · auto-refresh
          </div>
        </div>
      </div>

      {isError && (
        <ErrorBanner message={(metricsErr as any)?.response?.data?.detail ?? (metricsErr as any)?.message ?? 'metrics unavailable'} />
      )}

      {/* KPI cards — 3 columns, no Rows Processed */}
      <div className="grid grid-cols-2 lg:grid-cols-3 gap-4">
        <StatCard
          label="Active Runs"
          value={m.active_runs ?? activeRuns}
          sub={`in last ${timeWindow.label}`}
          color="blue"
          icon={<Activity size={16} />}
        />
        <StatCard
          label="Success Rate"
          value={`${successRate}%`}
          sub={`${successCount} of ${totalDone} completed`}
          color={successRate >= 80 ? 'green' : 'amber'}
          icon={<CheckCircle size={16} />}
        />
        <StatCard
          label="Failed"
          value={failedCount}
          sub={`in last ${timeWindow.label}`}
          color={failedCount > 0 ? 'red' : 'green'}
          icon={<AlertTriangle size={16} />}
        />
      </div>

      {/* DAG Run Timeline + Pipeline health */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">

        {/* Scatter chart */}
        <div className="lg:col-span-2">
          <Card>
            <CardHeader
              title="DAG Run Timeline"
              subtitle={`${filteredRuns.length} run${filteredRuns.length !== 1 ? 's' : ''} · last ${timeWindow.label} · Y = duration (min)`}
            />
            <div className="px-2 pb-4 pt-2 h-60">
              {filteredRuns.length > 0 ? (
                <ResponsiveContainer width="100%" height="100%">
                  <ScatterChart margin={{ top: 8, right: 12, bottom: 8, left: -10 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
                    <XAxis
                      dataKey="x"
                      type="number"
                      domain={[xMin, xMax]}
                      tickCount={6}
                      tickFormatter={v => xTickFmt(v, timeWindow.ms)}
                      tick={{ fill: '#9ca3af', fontSize: 10 }}
                      axisLine={false}
                      tickLine={false}
                    />
                    <YAxis
                      dataKey="y"
                      type="number"
                      tick={{ fill: '#9ca3af', fontSize: 10 }}
                      axisLine={false}
                      tickLine={false}
                      width={36}
                      tickFormatter={v => `${v}m`}
                    />
                    <RechartsTooltip content={<RunTooltip />} cursor={{ strokeDasharray: '3 3' }} />
                    <Legend
                      iconType="circle"
                      iconSize={8}
                      wrapperStyle={{ fontSize: '11px', paddingTop: '4px' }}
                    />
                    {STATUS_SERIES.map(s => (
                      <Scatter
                        key={s.key}
                        name={s.label}
                        data={scatterByStatus[s.key]}
                        fill={s.color}
                        opacity={0.85}
                        r={5}
                      />
                    ))}
                  </ScatterChart>
                </ResponsiveContainer>
              ) : (
                <div className="flex flex-col items-center justify-center h-full gap-2 text-gray-300">
                  <BarChart2 size={28} />
                  <p className="text-xs text-gray-400">No runs in the last {timeWindow.label}</p>
                </div>
              )}
            </div>
          </Card>
        </div>

        {/* Pipeline health */}
        <Card>
          <CardHeader
            title="Pipeline Health"
            subtitle={`${pipelines.length} configured`}
            actions={
              <button onClick={() => nav('/pipelines')} className="text-xs text-blue-600 hover:text-blue-700 flex items-center gap-1">
                View <ArrowRight size={12} />
              </button>
            }
          />
          <div className="divide-y divide-gray-100 max-h-56 overflow-y-auto">
            {pipelines.length === 0 ? (
              <div className="flex flex-col items-center py-8 gap-2 text-gray-300">
                <Activity size={20} />
                <p className="text-xs text-gray-400">No pipelines configured</p>
              </div>
            ) : pipelines.map((p: any) => {
              const pid = p.pipeline_id || p.id
              return (
                <div
                  key={pid}
                  className="px-4 py-2.5 hover:bg-gray-50 cursor-pointer transition-colors flex items-center gap-3"
                  onClick={() => nav(`/pipelines/${pid}`)}
                >
                  <div className="flex-1 min-w-0">
                    <p className="text-xs font-mono font-medium text-gray-800 truncate">{pid}</p>
                    <p className="text-xs text-gray-400 mt-0.5 truncate">{p.schedule || 'no schedule'}</p>
                  </div>
                  <StatusBadge status={p.status} />
                </div>
              )
            })}
          </div>
        </Card>
      </div>

      {/* Recent runs table */}
      <Card>
        <CardHeader
          title="Pipeline Runs"
          subtitle={`${filteredRuns.length} runs in last ${timeWindow.label}`}
          actions={
            <button onClick={() => nav('/runs')} className="text-xs text-blue-600 hover:text-blue-700 flex items-center gap-1">
              View all <ArrowRight size={12} />
            </button>
          }
        />
        {runsErr ? (
          <div className="px-5 py-4">
            <ErrorBanner message="Failed to load runs — check API connectivity" />
          </div>
        ) : filteredRuns.length === 0 ? (
          <div className="flex flex-col items-center py-10 gap-2 text-gray-300">
            <Clock size={24} />
            <p className="text-xs text-gray-400">No runs in the last {timeWindow.label}</p>
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-gray-100 bg-gray-50/50">
                  <th className="px-5 py-2.5 text-left text-gray-500 font-semibold">Pipeline</th>
                  <th className="px-4 py-2.5 text-left text-gray-500 font-semibold">Status</th>
                  <th className="px-4 py-2.5 text-left text-gray-500 font-semibold">Started</th>
                  <th className="px-4 py-2.5 text-left text-gray-500 font-semibold">Duration</th>
                  <th className="px-4 py-2.5 text-left text-gray-500 font-semibold">Tasks</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {filteredRuns.slice(0, 20).map((r: any) => (
                  <tr
                    key={r.run_id}
                    className="hover:bg-blue-50/40 cursor-pointer transition-colors"
                    onClick={() => nav(`/runs/${r.run_id}`)}
                  >
                    <td className="px-5 py-3 font-mono font-medium text-blue-600">{r.pipeline_id}</td>
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

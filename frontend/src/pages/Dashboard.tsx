import React from 'react'
import { useQuery } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import {
  AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer,
} from 'recharts'
import { getDashboardMetrics, getThroughputMetrics, getRuns, getPipelines } from '../api/client'
import { Card, CardHeader, StatusBadge, Spinner, Duration, TimeAgo } from '../components/ui'
import {
  Activity, TrendingUp, CheckCircle, Database,
  ArrowRight, AlertTriangle, XCircle, Clock,
} from 'lucide-react'

function ChartTooltip({ active, payload, label }: any) {
  if (!active || !payload?.length) return null
  return (
    <div className="bg-white border border-gray-200 rounded-lg p-3 text-xs shadow-xl">
      <p className="text-gray-500 mb-1 font-medium">{label}</p>
      {payload.map((p: any, i: number) => (
        <div key={i} className="flex items-center gap-2">
          <span className="w-2 h-2 rounded-full" style={{ background: p.color }} />
          <span className="text-gray-700">{p.name}: <b>{p.value?.toLocaleString()}</b></span>
        </div>
      ))}
    </div>
  )
}

function StatCard({ label, value, sub, color, icon }: {
  label: string; value: string | number; sub?: string; color: string; icon: React.ReactNode
}) {
  const colors: Record<string, string> = {
    blue:   'text-blue-600 bg-blue-50 border-blue-100',
    green:  'text-emerald-600 bg-emerald-50 border-emerald-100',
    red:    'text-red-600 bg-red-50 border-red-100',
    purple: 'text-purple-600 bg-purple-50 border-purple-100',
    amber:  'text-amber-600 bg-amber-50 border-amber-100',
  }
  return (
    <div className="bg-white border border-gray-200 rounded-xl p-5 shadow-sm">
      <div className="flex items-center justify-between mb-3">
        <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide">{label}</p>
        <div className={`w-8 h-8 rounded-lg flex items-center justify-center border ${colors[color] ?? colors.blue}`}>
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

export default function Dashboard() {
  const nav = useNavigate()

  const { data: metrics, isLoading, isError, error: metricsErr } = useQuery({
    queryKey: ['dashboard-metrics'],
    queryFn: getDashboardMetrics,
    refetchInterval: 15_000,
  })
  const { data: throughput } = useQuery({
    queryKey: ['throughput'],
    queryFn: () => getThroughputMetrics(24),
    refetchInterval: 60_000,
  })
  const { data: runsData, isError: runsErr } = useQuery({
    queryKey: ['recent-runs'],
    queryFn: () => getRuns({ page: '1', page_size: '12' }),
    refetchInterval: 10_000,
  })
  const { data: pipelinesData } = useQuery({
    queryKey: ['pipelines-health'],
    queryFn: () => getPipelines({ page_size: '50' }),
    refetchInterval: 30_000,
  })

  const runs      = runsData?.runs ?? []
  const pipelines = pipelinesData?.pipelines ?? []
  const m         = metrics ?? {}

  const hourly = (throughput?.hourly ?? []).map((h: any) => ({
    ...h,
    hour: h.hour ? new Date(h.hour).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) : '',
  }))

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

  const failedToday = runs.filter((r: any) => r.status === 'failed').length
  const activeRuns  = runs.filter((r: any) => r.status === 'running').length

  return (
    <div className="p-6 space-y-5 overflow-y-auto h-full bg-gray-50">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-gray-900">Dashboard</h1>
          <p className="text-sm text-gray-500 mt-0.5">Pipeline health at a glance</p>
        </div>
        <div className="flex items-center gap-2 text-xs text-gray-500 bg-white border border-gray-200 px-3 py-1.5 rounded-lg shadow-sm">
          <Activity size={12} className="text-emerald-500 animate-pulse" />
          Live · auto-refresh
        </div>
      </div>

      {/* Error banners */}
      {isError && (
        <ErrorBanner message={(metricsErr as any)?.response?.data?.detail ?? (metricsErr as any)?.message ?? 'metrics unavailable'} />
      )}

      {/* KPI cards */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <StatCard
          label="Active Runs"
          value={m.active_runs ?? activeRuns}
          sub="currently executing"
          color="blue"
          icon={<Activity size={16} />}
        />
        <StatCard
          label="Success Rate"
          value={`${m.success_rate ?? 0}%`}
          sub={`${m.today_success ?? 0} / ${m.today_runs ?? runs.length} today`}
          color="green"
          icon={<CheckCircle size={16} />}
        />
        <StatCard
          label="Failed Today"
          value={m.failed_today ?? failedToday}
          sub={`${m.failed_tasks_today ?? 0} failed tasks`}
          color={failedToday > 0 ? 'red' : 'green'}
          icon={<AlertTriangle size={16} />}
        />
        <StatCard
          label="Rows Processed"
          value={(m.rows_today ?? 0).toLocaleString()}
          sub="today across all pipelines"
          color="purple"
          icon={<Database size={16} />}
        />
      </div>

      {/* Charts + Pipeline health */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {/* Throughput chart */}
        <div className="lg:col-span-2">
          <Card>
            <CardHeader title="24h Throughput" subtitle="Rows processed per hour" />
            <div className="p-4 h-52">
              {hourly.length > 0 ? (
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart data={hourly} margin={{ top: 4, right: 4, bottom: 0, left: -10 }}>
                    <defs>
                      <linearGradient id="rowsGrad" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%"  stopColor="#3b82f6" stopOpacity={0.2} />
                        <stop offset="95%" stopColor="#3b82f6" stopOpacity={0} />
                      </linearGradient>
                    </defs>
                    <XAxis dataKey="hour" tick={{ fill: '#9ca3af', fontSize: 10 }} axisLine={false} tickLine={false} />
                    <YAxis tick={{ fill: '#9ca3af', fontSize: 10 }} axisLine={false} tickLine={false} />
                    <Tooltip content={<ChartTooltip />} />
                    <Area type="monotone" dataKey="rows_processed" name="Rows" stroke="#3b82f6" fill="url(#rowsGrad)" strokeWidth={2} />
                  </AreaChart>
                </ResponsiveContainer>
              ) : (
                <div className="flex flex-col items-center justify-center h-full gap-2 text-gray-300">
                  <TrendingUp size={28} />
                  <p className="text-xs text-gray-400">No throughput data yet</p>
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
          <div className="divide-y divide-gray-100 max-h-52 overflow-y-auto">
            {pipelines.length === 0 && (
              <div className="flex flex-col items-center py-8 gap-2 text-gray-300">
                <Database size={20} />
                <p className="text-xs text-gray-400">No pipelines configured</p>
              </div>
            )}
            {pipelines.map((p: any) => (
              <div
                key={p.id}
                className="px-4 py-2.5 hover:bg-gray-50 cursor-pointer transition-colors flex items-center gap-3"
                onClick={() => nav(`/pipelines/${p.id}`)}
              >
                <div className="flex-1 min-w-0">
                  <p className="text-xs font-mono font-medium text-gray-800 truncate">{p.id || p.pipeline_id}</p>
                  <p className="text-xs text-gray-400 mt-0.5 truncate">{p.schedule || 'no schedule'}</p>
                </div>
                <StatusBadge status={p.status} />
              </div>
            ))}
          </div>
        </Card>
      </div>

      {/* Recent runs */}
      <Card>
        <CardHeader
          title="Recent Pipeline Runs"
          subtitle="Latest across all pipelines"
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
        ) : runs.length === 0 ? (
          <div className="flex flex-col items-center py-10 gap-2 text-gray-300">
            <Clock size={24} />
            <p className="text-xs text-gray-400">No runs yet</p>
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
                  <th className="px-4 py-2.5 text-left text-gray-500 font-semibold">Rows</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {runs.map((r: any) => (
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
                    <td className="px-4 py-3 font-mono text-gray-700">{(r.total_rows_processed ?? 0).toLocaleString()}</td>
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

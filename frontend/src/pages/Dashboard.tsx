import React from 'react'
import { useQuery } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import {
  AreaChart, Area, XAxis, YAxis, Tooltip,
  ResponsiveContainer, BarChart, Bar, Cell,
} from 'recharts'
import { getDashboardMetrics, getThroughputMetrics, getRuns, getAlerts } from '../api/client'
import { StatCard, Card, CardHeader, Table, StatusBadge, Duration, TimeAgo, Spinner } from '../components/ui'
import {
  Activity, TrendingUp, AlertTriangle, CheckCircle,
  Database, ArrowRight,
} from 'lucide-react'

const TIP = ({ active, payload, label }: any) => {
  if (!active || !payload?.length) return null
  return (
    <div className="bg-white border border-gray-200 rounded-lg p-3 text-xs shadow-xl">
      <p className="text-gray-500 mb-1.5 font-medium">{label}</p>
      {payload.map((p: any, i: number) => (
        <div key={i} className="flex items-center gap-2">
          <span className="w-2 h-2 rounded-full" style={{ background: p.color }} />
          <span className="text-gray-700">{p.name}: <b className="text-gray-900">{p.value?.toLocaleString()}</b></span>
        </div>
      ))}
    </div>
  )
}

export default function Dashboard() {
  const nav = useNavigate()
  const { data: metrics, isLoading } = useQuery({
    queryKey: ['dashboard-metrics'],
    queryFn: getDashboardMetrics,
    refetchInterval: 15_000,
  })
  const { data: throughput } = useQuery({
    queryKey: ['throughput'],
    queryFn: () => getThroughputMetrics(24),
    refetchInterval: 60_000,
  })
  const { data: runsData } = useQuery({
    queryKey: ['recent-runs'],
    queryFn: () => getRuns({ page: '1', page_size: '10' }),
    refetchInterval: 10_000,
  })
  const { data: alertsData } = useQuery({
    queryKey: ['active-alerts'],
    queryFn: () => getAlerts({ resolved: 'false', page_size: '6' }),
    refetchInterval: 15_000,
  })

  const runs   = runsData?.runs ?? []
  const alerts = alertsData?.alerts ?? []
  const hourly = (throughput?.hourly ?? []).map((h: any) => ({
    ...h,
    hour: h.hour ? new Date(h.hour).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) : '',
  }))

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-full bg-gray-50">
        <div className="flex flex-col items-center gap-3">
          <Spinner size="lg" />
          <p className="text-gray-400 text-sm">Loading dashboard…</p>
        </div>
      </div>
    )
  }

  const m = metrics || {}

  return (
    <div className="p-6 space-y-6 overflow-y-auto h-full bg-gray-50">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-gray-900">Dashboard</h1>
          <p className="text-sm text-gray-500 mt-0.5">Real-time pipeline health and activity</p>
        </div>
        <div className="flex items-center gap-2 text-xs text-gray-500 bg-white border border-gray-200 px-3 py-1.5 rounded-lg shadow-sm">
          <Activity size={12} className="text-emerald-500 animate-pulse" />
          Live monitoring
        </div>
      </div>

      {/* KPI cards */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <StatCard
          label="Active Runs"
          value={m.active_runs ?? 0}
          sub="currently executing"
          color="blue"
          icon={<Activity size={18} />}
        />
        <StatCard
          label="Success Rate"
          value={`${m.success_rate ?? 0}%`}
          sub={`${m.today_success ?? 0} / ${m.today_runs ?? 0} today`}
          color="green"
          icon={<CheckCircle size={18} />}
        />
        <StatCard
          label="Active Alerts"
          value={m.active_alerts ?? 0}
          sub={`${m.failed_tasks_today ?? 0} failed tasks today`}
          color={m.active_alerts > 0 ? 'red' : 'green'}
          icon={<AlertTriangle size={18} />}
        />
        <StatCard
          label="Rows Today"
          value={(m.rows_today ?? 0).toLocaleString()}
          sub="processed across all pipelines"
          color="purple"
          icon={<Database size={18} />}
        />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {/* Throughput chart */}
        <div className="lg:col-span-2">
          <Card>
            <CardHeader
              title="24h Throughput"
              subtitle="Rows processed per hour"
            />
            <div className="p-4 h-56">
              {hourly.length > 0 ? (
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart data={hourly} margin={{ top: 4, right: 4, bottom: 0, left: -10 }}>
                    <defs>
                      <linearGradient id="rowsGrad" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%"  stopColor="#3b82f6" stopOpacity={0.2} />
                        <stop offset="95%" stopColor="#3b82f6" stopOpacity={0} />
                      </linearGradient>
                    </defs>
                    <XAxis
                      dataKey="hour"
                      tick={{ fill: '#9ca3af', fontSize: 10 }}
                      axisLine={false} tickLine={false}
                    />
                    <YAxis
                      tick={{ fill: '#9ca3af', fontSize: 10 }}
                      axisLine={false} tickLine={false}
                    />
                    <Tooltip content={<TIP />} />
                    <Area
                      type="monotone" dataKey="rows_processed" name="Rows"
                      stroke="#3b82f6" fill="url(#rowsGrad)" strokeWidth={2}
                    />
                  </AreaChart>
                </ResponsiveContainer>
              ) : (
                <div className="flex flex-col items-center justify-center h-full gap-2 text-gray-300">
                  <TrendingUp size={28} />
                  <p className="text-xs text-gray-400">No throughput data yet — run a pipeline to see metrics</p>
                </div>
              )}
            </div>
          </Card>
        </div>

        {/* Active alerts panel */}
        <Card>
          <CardHeader
            title="Active Alerts"
            subtitle={`${alerts.length} unresolved`}
            actions={
              alerts.length > 0 && (
                <button
                  onClick={() => nav('/alerts')}
                  className="text-xs text-blue-600 hover:text-blue-700 flex items-center gap-1"
                >
                  View all <ArrowRight size={12} />
                </button>
              )
            }
          />
          <div className="divide-y divide-gray-100 max-h-56 overflow-y-auto">
            {alerts.length === 0 && (
              <div className="flex flex-col items-center py-10 gap-2 text-gray-300">
                <CheckCircle size={22} className="text-emerald-400" />
                <p className="text-xs text-gray-400">All clear — no active alerts</p>
              </div>
            )}
            {alerts.map((a: any) => (
              <div key={a.id} className="px-4 py-3 hover:bg-gray-50 cursor-pointer transition-colors" onClick={() => nav('/alerts')}>
                <div className="flex items-center justify-between gap-2 mb-1">
                  <span className="text-xs font-medium text-gray-800 truncate">{a.title}</span>
                  <StatusBadge status={a.severity} />
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-xs text-gray-400 truncate">{a.pipeline_id}</span>
                  <TimeAgo ts={a.fired_at} />
                </div>
              </div>
            ))}
          </div>
        </Card>
      </div>

      {/* Recent runs */}
      <Card>
        <CardHeader
          title="Recent Pipeline Runs"
          subtitle="Latest runs across all pipelines"
          actions={
            <button
              onClick={() => nav('/runs')}
              className="text-xs text-blue-600 hover:text-blue-700 flex items-center gap-1"
            >
              View all <ArrowRight size={12} />
            </button>
          }
        />
        <Table
          onRowClick={r => nav(`/runs/${r.run_id}`)}
          columns={[
            {
              key: 'pipeline_id', header: 'Pipeline',
              render: v => <span className="font-mono text-xs text-blue-600 font-medium">{v}</span>,
            },
            {
              key: 'status', header: 'Status',
              render: v => <StatusBadge status={v} />,
            },
            {
              key: 'trigger_type', header: 'Trigger',
              render: v => <span className="text-gray-500 text-xs capitalize">{v}</span>,
            },
            {
              key: 'start_time', header: 'Started',
              render: v => <TimeAgo ts={v} />,
            },
            {
              key: 'duration_seconds', header: 'Duration',
              render: v => <Duration seconds={v} />,
            },
            {
              key: 'total_rows_processed', header: 'Rows',
              render: v => (
                <span className="font-mono text-xs text-gray-700">{(v || 0).toLocaleString()}</span>
              ),
            },
            {
              key: 'failed_tasks', header: 'Failed',
              render: v => v > 0
                ? <span className="text-red-600 font-mono text-xs font-semibold">{v}</span>
                : <span className="text-gray-300 text-xs">0</span>,
            },
          ]}
          data={runs}
          emptyMsg="No runs yet — trigger a pipeline to see activity"
        />
      </Card>
    </div>
  )
}

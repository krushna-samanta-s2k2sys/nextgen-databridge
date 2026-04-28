import React from 'react'
import clsx from 'clsx'

// ── Status Badge ──────────────────────────────────────────────────────────────
const STATUS_MAP: Record<string, string> = {
  success:          'bg-emerald-50 text-emerald-700 border-emerald-200',
  running:          'bg-blue-50 text-blue-700 border-blue-200 animate-pulse',
  queued:           'bg-amber-50 text-amber-700 border-amber-200',
  failed:           'bg-red-50 text-red-700 border-red-200',
  pending:          'bg-gray-100 text-gray-600 border-gray-200',
  skipped:          'bg-gray-100 text-gray-500 border-gray-200',
  active:           'bg-emerald-50 text-emerald-700 border-emerald-200',
  paused:           'bg-amber-50 text-amber-700 border-amber-200',
  draft:            'bg-gray-100 text-gray-600 border-gray-200',
  deployed:         'bg-emerald-50 text-emerald-700 border-emerald-200',
  pending_approval: 'bg-purple-50 text-purple-700 border-purple-200',
  approved:         'bg-blue-50 text-blue-700 border-blue-200',
  rejected:         'bg-red-50 text-red-700 border-red-200',
  info:             'bg-blue-50 text-blue-700 border-blue-200',
  warning:          'bg-amber-50 text-amber-700 border-amber-200',
  error:            'bg-red-50 text-red-700 border-red-200',
  critical:         'bg-red-100 text-red-800 border-red-300',
  upstream_failed:  'bg-orange-50 text-orange-700 border-orange-200',
  retrying:         'bg-amber-50 text-amber-700 border-amber-200',
  dev:              'bg-blue-50 text-blue-700 border-blue-200',
  staging:          'bg-purple-50 text-purple-700 border-purple-200',
  production:       'bg-red-50 text-red-700 border-red-200',
  sqlserver:        'bg-sky-50 text-sky-700 border-sky-200',
  postgresql:       'bg-indigo-50 text-indigo-700 border-indigo-200',
  kafka:            'bg-orange-50 text-orange-700 border-orange-200',
  s3:               'bg-amber-50 text-amber-700 border-amber-200',
}

export function StatusBadge({ status }: { status: string }) {
  const cls = STATUS_MAP[status?.toLowerCase()] ?? 'bg-gray-100 text-gray-600 border-gray-200'
  return (
    <span className={clsx('inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium border whitespace-nowrap', cls)}>
      {status?.replace(/_/g, ' ')}
    </span>
  )
}

// ── Card ──────────────────────────────────────────────────────────────────────
export function Card({ children, className }: { children: React.ReactNode; className?: string }) {
  return (
    <div className={clsx('bg-white border border-gray-200 rounded-xl overflow-hidden shadow-sm', className)}>
      {children}
    </div>
  )
}

export function CardHeader({
  title, subtitle, actions,
}: { title: string; subtitle?: string; actions?: React.ReactNode }) {
  return (
    <div className="px-5 py-4 border-b border-gray-100 flex items-center justify-between">
      <div>
        <h3 className="text-sm font-semibold text-gray-900">{title}</h3>
        {subtitle && <p className="text-xs text-gray-500 mt-0.5">{subtitle}</p>}
      </div>
      {actions && <div className="flex items-center gap-2">{actions}</div>}
    </div>
  )
}

// ── Button ────────────────────────────────────────────────────────────────────
type BtnVariant = 'primary' | 'secondary' | 'ghost' | 'danger'
type BtnSize = 'xs' | 'sm' | 'md'

const BTN_VARIANT: Record<BtnVariant, string> = {
  primary:   'bg-blue-600 hover:bg-blue-500 text-white shadow-sm',
  secondary: 'bg-white hover:bg-gray-50 text-gray-700 border border-gray-300 shadow-sm',
  ghost:     'hover:bg-gray-100 text-gray-600 hover:text-gray-900',
  danger:    'bg-red-600 hover:bg-red-500 text-white shadow-sm',
}
const BTN_SIZE: Record<BtnSize, string> = {
  xs: 'px-2 py-1 text-xs gap-1',
  sm: 'px-3 py-1.5 text-xs gap-1.5',
  md: 'px-4 py-2 text-sm gap-2',
}

export function Button({
  children, variant = 'secondary', size = 'sm', onClick, disabled, className, type = 'button', title,
}: {
  children: React.ReactNode
  variant?: BtnVariant
  size?: BtnSize
  onClick?: () => void
  disabled?: boolean
  className?: string
  type?: 'button' | 'submit'
  title?: string
}) {
  return (
    <button
      type={type}
      onClick={onClick}
      disabled={disabled}
      title={title}
      className={clsx(
        'inline-flex items-center rounded-lg font-medium transition-all duration-150',
        'disabled:opacity-40 disabled:cursor-not-allowed',
        BTN_VARIANT[variant], BTN_SIZE[size], className,
      )}
    >
      {children}
    </button>
  )
}

// ── Input / Select / Textarea ─────────────────────────────────────────────────
type InputProps = React.InputHTMLAttributes<HTMLInputElement> & { label?: string; hint?: string }

export function Input({ label, hint, ...props }: InputProps) {
  return (
    <div className="flex flex-col gap-1">
      {label && <label className="text-xs font-medium text-gray-700">{label}</label>}
      <input
        {...props}
        className={clsx(
          'bg-white border border-gray-300 rounded-lg px-3 py-2 text-sm text-gray-900',
          'focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500',
          'placeholder:text-gray-400', props.className,
        )}
      />
      {hint && <p className="text-xs text-gray-500">{hint}</p>}
    </div>
  )
}

type SelectProps = React.SelectHTMLAttributes<HTMLSelectElement> & { label?: string }

export function Select({ label, children, ...props }: SelectProps) {
  return (
    <div className="flex flex-col gap-1">
      {label && <label className="text-xs font-medium text-gray-700">{label}</label>}
      <select
        {...props}
        className={clsx(
          'bg-white border border-gray-300 rounded-lg px-3 py-2 text-sm text-gray-900',
          'focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500', props.className,
        )}
      >
        {children}
      </select>
    </div>
  )
}

type TextareaProps = React.TextareaHTMLAttributes<HTMLTextAreaElement> & { label?: string }

export function Textarea({ label, ...props }: TextareaProps) {
  return (
    <div className="flex flex-col gap-1">
      {label && <label className="text-xs font-medium text-gray-700">{label}</label>}
      <textarea
        {...props}
        className={clsx(
          'bg-white border border-gray-300 rounded-lg px-3 py-2 text-sm text-gray-900 font-mono',
          'focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 resize-none', props.className,
        )}
      />
    </div>
  )
}

// ── Modal ─────────────────────────────────────────────────────────────────────
export function Modal({
  title, open, onClose, children, size = 'md',
}: {
  title: string; open: boolean; onClose: () => void
  children: React.ReactNode; size?: 'sm' | 'md' | 'lg' | 'xl' | 'full'
}) {
  if (!open) return null
  const widths: Record<string, string> = {
    sm: 'max-w-sm', md: 'max-w-lg', lg: 'max-w-2xl',
    xl: 'max-w-4xl', full: 'max-w-7xl',
  }
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/50 backdrop-blur-sm">
      <div className={clsx('bg-white border border-gray-200 rounded-2xl shadow-2xl w-full max-h-[90vh] flex flex-col', widths[size])}>
        <div className="flex items-center justify-between px-5 py-4 border-b border-gray-100 flex-shrink-0">
          <h2 className="text-sm font-semibold text-gray-900">{title}</h2>
          <button
            onClick={onClose}
            className="w-7 h-7 rounded-lg flex items-center justify-center text-gray-400 hover:text-gray-700 hover:bg-gray-100 transition-colors text-lg leading-none"
          >
            ×
          </button>
        </div>
        <div className="p-5 overflow-y-auto flex-1">{children}</div>
      </div>
    </div>
  )
}

// ── Table ─────────────────────────────────────────────────────────────────────
export function Table({
  columns, data, onRowClick, emptyMsg = 'No data',
}: {
  columns: {
    key: string; header: string
    render?: (v: any, row: any) => React.ReactNode
    className?: string
  }[]
  data: any[]
  onRowClick?: (row: any) => void
  emptyMsg?: string
}) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full">
        <thead>
          <tr className="border-b border-gray-100 bg-gray-50">
            {columns.map(c => (
              <th
                key={c.key}
                className={clsx(
                  'px-4 py-3 text-left text-xs font-semibold text-gray-500 uppercase tracking-wide whitespace-nowrap',
                  c.className,
                )}
              >
                {c.header}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.length === 0 && (
            <tr>
              <td colSpan={columns.length} className="px-4 py-12 text-center text-gray-400 text-sm">
                {emptyMsg}
              </td>
            </tr>
          )}
          {data.map((row, i) => (
            <tr
              key={i}
              onClick={() => onRowClick?.(row)}
              className={clsx(
                'border-b border-gray-100 transition-colors',
                onRowClick ? 'cursor-pointer hover:bg-blue-50/40' : '',
              )}
            >
              {columns.map(c => (
                <td key={c.key} className={clsx('px-4 py-3 text-gray-700 text-sm', c.className)}>
                  {c.render ? c.render(row[c.key], row) : (row[c.key] ?? '—')}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

// ── Stat Card ─────────────────────────────────────────────────────────────────
const STAT_COLORS: Record<string, { icon: string; border: string; ring: string }> = {
  blue:   { icon: 'text-blue-600 bg-blue-50',   border: 'border-l-blue-500',   ring: 'ring-blue-100' },
  green:  { icon: 'text-emerald-600 bg-emerald-50', border: 'border-l-emerald-500', ring: 'ring-emerald-100' },
  red:    { icon: 'text-red-600 bg-red-50',      border: 'border-l-red-500',    ring: 'ring-red-100' },
  yellow: { icon: 'text-amber-600 bg-amber-50',  border: 'border-l-amber-500',  ring: 'ring-amber-100' },
  purple: { icon: 'text-purple-600 bg-purple-50', border: 'border-l-purple-500', ring: 'ring-purple-100' },
}

export function StatCard({
  label, value, sub, delta, color = 'blue', icon,
}: {
  label: string; value: string | number; sub?: string; delta?: string
  color?: 'blue' | 'green' | 'red' | 'yellow' | 'purple'; icon?: React.ReactNode
}) {
  const c = STAT_COLORS[color]
  return (
    <div className={clsx('bg-white border border-gray-200 rounded-xl p-5 shadow-sm border-l-4', c.border)}>
      <div className="flex items-start justify-between">
        <div className="flex-1 min-w-0">
          <p className="text-xs text-gray-500 mb-1 font-semibold uppercase tracking-wide">{label}</p>
          <p className="text-2xl font-bold text-gray-900 tabular-nums">{value}</p>
          {sub && <p className="text-xs text-gray-500 mt-1">{sub}</p>}
        </div>
        {icon && (
          <div className={clsx('p-2.5 rounded-xl flex-shrink-0', c.icon)}>
            {icon}
          </div>
        )}
      </div>
      {delta && <p className="text-xs text-gray-500 mt-3 pt-3 border-t border-gray-100">{delta}</p>}
    </div>
  )
}

// ── Spinner ───────────────────────────────────────────────────────────────────
export function Spinner({ size = 'sm' }: { size?: 'sm' | 'md' | 'lg' }) {
  const s = { sm: 'w-4 h-4 border-2', md: 'w-6 h-6 border-2', lg: 'w-10 h-10 border-[3px]' }
  return (
    <div className={clsx('border-gray-200 border-t-blue-600 rounded-full animate-spin', s[size])} />
  )
}

// ── Empty state ───────────────────────────────────────────────────────────────
export function Empty({ message, icon }: { message: string; icon?: React.ReactNode }) {
  return (
    <div className="flex flex-col items-center justify-center py-16 text-gray-400 gap-3">
      {icon && <div className="text-gray-300 opacity-60">{icon}</div>}
      <p className="text-sm">{message}</p>
    </div>
  )
}

// ── Tabs ──────────────────────────────────────────────────────────────────────
export function Tabs({
  tabs, active, onChange,
}: {
  tabs: { id: string; label: string; count?: number }[]
  active: string
  onChange: (id: string) => void
}) {
  return (
    <div className="flex gap-0.5 bg-gray-100 rounded-lg p-1 border border-gray-200 w-fit">
      {tabs.map(t => (
        <button
          key={t.id}
          onClick={() => onChange(t.id)}
          className={clsx(
            'px-3 py-1.5 text-xs rounded-md font-medium transition-all flex items-center gap-1.5 whitespace-nowrap',
            active === t.id
              ? 'bg-white text-gray-900 shadow-sm border border-gray-200'
              : 'text-gray-500 hover:text-gray-700',
          )}
        >
          {t.label}
          {t.count !== undefined && (
            <span className={clsx(
              'px-1.5 py-0.5 rounded-full text-xs tabular-nums',
              active === t.id ? 'bg-blue-600 text-white' : 'bg-gray-200 text-gray-600',
            )}>
              {t.count}
            </span>
          )}
        </button>
      ))}
    </div>
  )
}

// ── Duration / TimeAgo ────────────────────────────────────────────────────────
export function Duration({ seconds }: { seconds?: number | null }) {
  if (!seconds) return <span className="text-gray-400">—</span>
  if (seconds < 60) return <span className="tabular-nums text-gray-700">{seconds.toFixed(1)}s</span>
  const m = Math.floor(seconds / 60)
  const s = Math.round(seconds % 60)
  if (m < 60) return <span className="tabular-nums text-gray-700">{m}m {s}s</span>
  return <span className="tabular-nums text-gray-700">{Math.floor(m / 60)}h {m % 60}m</span>
}

export function TimeAgo({ ts }: { ts?: string | null }) {
  if (!ts) return <span className="text-gray-400">—</span>
  const diff = Date.now() - new Date(ts).getTime()
  const s = Math.floor(diff / 1000)
  if (s < 5) return <span className="text-gray-600">just now</span>
  if (s < 60) return <span className="text-gray-600">{s}s ago</span>
  if (s < 3600) return <span className="text-gray-600">{Math.floor(s / 60)}m ago</span>
  if (s < 86400) return <span className="text-gray-600">{Math.floor(s / 3600)}h ago</span>
  return <span className="text-gray-600">{Math.floor(s / 86400)}d ago</span>
}

// ── Code block ────────────────────────────────────────────────────────────────
export function Code({ children, className }: { children: string; className?: string }) {
  return (
    <pre className={clsx(
      'bg-gray-950 border border-gray-800 rounded-lg p-4 text-xs text-gray-300 font-mono',
      'overflow-auto whitespace-pre-wrap break-all', className,
    )}>
      {children}
    </pre>
  )
}

// ── Section Header ────────────────────────────────────────────────────────────
export function PageHeader({
  title, subtitle, actions,
}: {
  title: string; subtitle?: string; actions?: React.ReactNode
}) {
  return (
    <div className="flex items-start justify-between mb-6">
      <div>
        <h1 className="text-xl font-bold text-gray-900">{title}</h1>
        {subtitle && <p className="text-sm text-gray-500 mt-0.5">{subtitle}</p>}
      </div>
      {actions && <div className="flex items-center gap-2">{actions}</div>}
    </div>
  )
}

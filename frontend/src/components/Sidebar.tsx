import React from 'react'
import { NavLink } from 'react-router-dom'
import clsx from 'clsx'
import {
  LayoutDashboard, GitBranch, Play, CheckSquare, Bell,
  Database, Rocket, Search, History, Server, Zap,
  ChevronLeft, Wifi, WifiOff, LogOut,
} from 'lucide-react'
import { useStore } from '../store/useStore'

const NAV_SECTIONS = [
  {
    label: 'Monitor',
    items: [
      { to: '/',            icon: LayoutDashboard, label: 'Dashboard' },
      { to: '/runs',        icon: Play,            label: 'Pipeline Runs' },
      { to: '/tasks',       icon: CheckSquare,     label: 'Tasks' },
      { to: '/alerts',      icon: Bell,            label: 'Alerts' },
    ],
  },
  {
    label: 'Configure',
    items: [
      { to: '/pipelines',   icon: GitBranch,       label: 'Pipelines' },
      { to: '/connections', icon: Database,        label: 'Connections' },
    ],
  },
  {
    label: 'Deploy & Analyze',
    items: [
      { to: '/deployments', icon: Rocket,          label: 'Deployments' },
      { to: '/eks',         icon: Server,          label: 'EKS Jobs' },
      { to: '/query',       icon: Search,          label: 'Query Explorer' },
      { to: '/audit',       icon: History,         label: 'Audit Trail' },
    ],
  },
]

export function Sidebar() {
  const { sidebarOpen, setSidebar, wsConnected, user, setToken, setUser } = useStore()

  function logout() {
    setToken(null)
    setUser(null)
  }

  return (
    <aside className={clsx(
      'flex flex-col bg-slate-900 border-r border-slate-800 transition-all duration-200 flex-shrink-0 h-full',
      sidebarOpen ? 'w-56' : 'w-14',
    )}>
      {/* Logo / header */}
      <div className={clsx(
        'flex items-center border-b border-slate-800 flex-shrink-0',
        sidebarOpen ? 'px-4 py-4 gap-3' : 'px-0 py-4 justify-center',
      )}>
        <div className="w-7 h-7 bg-blue-600 rounded-lg flex items-center justify-center flex-shrink-0 shadow-md shadow-blue-600/30">
          <Zap size={14} className="text-white" />
        </div>
        {sidebarOpen && (
          <div className="flex-1 min-w-0">
            <span className="text-sm font-bold text-white block truncate">NextGenDatabridge</span>
            <span className="text-xs text-slate-500 block truncate">NextGen Platform</span>
          </div>
        )}
        {sidebarOpen && (
          <button
            onClick={() => setSidebar(false)}
            className="text-slate-500 hover:text-slate-200 p-1 rounded flex-shrink-0 transition-colors"
          >
            <ChevronLeft size={14} />
          </button>
        )}
      </div>

      {/* Collapsed: expand button */}
      {!sidebarOpen && (
        <button
          onClick={() => setSidebar(true)}
          className="mx-auto mt-2 w-8 h-6 flex items-center justify-center text-slate-500 hover:text-slate-200 transition-colors"
        >
          <ChevronLeft size={14} className="rotate-180" />
        </button>
      )}

      {/* Navigation */}
      <nav className="flex-1 py-3 overflow-y-auto space-y-4">
        {NAV_SECTIONS.map(section => (
          <div key={section.label}>
            {sidebarOpen && (
              <p className="px-4 mb-1.5 text-xs font-semibold text-slate-500 uppercase tracking-widest">
                {section.label}
              </p>
            )}
            {section.items.map(({ to, icon: Icon, label }) => (
              <NavLink
                key={to}
                to={to}
                end={to === '/'}
                className={({ isActive }) => clsx(
                  'flex items-center gap-3 transition-colors relative',
                  sidebarOpen ? 'mx-2 px-3 py-2 rounded-lg text-sm' : 'mx-0 px-0 py-2 justify-center w-full',
                  isActive
                    ? 'bg-blue-600/15 text-blue-400'
                    : 'text-slate-400 hover:text-slate-100 hover:bg-slate-800/80',
                )}
              >
                {({ isActive }) => (
                  <>
                    {isActive && sidebarOpen && (
                      <span className="absolute left-0 top-1/2 -translate-y-1/2 w-0.5 h-5 bg-blue-500 rounded-r -ml-2" />
                    )}
                    <Icon size={16} className="flex-shrink-0" />
                    {sidebarOpen && <span className="truncate">{label}</span>}
                  </>
                )}
              </NavLink>
            ))}
          </div>
        ))}
      </nav>

      {/* Footer */}
      <div className="border-t border-slate-800 flex-shrink-0">
        <div className={clsx(
          'flex items-center gap-2 px-4 py-2',
          !sidebarOpen && 'justify-center px-0',
        )}>
          {wsConnected
            ? <Wifi size={12} className="text-emerald-400 flex-shrink-0" />
            : <WifiOff size={12} className="text-slate-600 flex-shrink-0" />}
          {sidebarOpen && (
            <span className="text-xs text-slate-500">
              {wsConnected ? 'Live' : 'Offline'} · {user?.sub ?? 'guest'}
            </span>
          )}
        </div>
        <button
          onClick={logout}
          className={clsx(
            'flex items-center gap-2 text-slate-500 hover:text-slate-200 hover:bg-slate-800/60 transition-colors w-full text-xs py-2.5',
            sidebarOpen ? 'px-4' : 'justify-center',
          )}
        >
          <LogOut size={13} className="flex-shrink-0" />
          {sidebarOpen && 'Sign out'}
        </button>
      </div>
    </aside>
  )
}

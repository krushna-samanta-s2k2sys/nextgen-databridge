import React from 'react'
import { NavLink } from 'react-router-dom'
import clsx from 'clsx'
import {
  LayoutDashboard, Settings2, Activity, Database,
  Zap, ChevronLeft, Wifi, WifiOff, LogOut,
} from 'lucide-react'
import { useStore } from '../store/useStore'

const NAV_ITEMS = [
  { to: '/',          icon: LayoutDashboard, label: 'Dashboard'       },
  { to: '/pipelines', icon: Settings2,       label: 'Pipelines'       },
  { to: '/runs',      icon: Activity,        label: 'Pipeline Runs'   },
  { to: '/query',     icon: Database,        label: 'Query Editor'    },
]

export function Sidebar() {
  const { sidebarOpen, setSidebar, wsConnected, user, setToken, setUser } = useStore()

  return (
    <aside className={clsx(
      'flex flex-col bg-slate-900 border-r border-slate-800 transition-all duration-200 flex-shrink-0 h-full',
      sidebarOpen ? 'w-52' : 'w-14',
    )}>
      {/* Logo */}
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
            <span className="text-xs text-slate-500 block truncate">Monitor &amp; Explore</span>
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

      {!sidebarOpen && (
        <button
          onClick={() => setSidebar(true)}
          className="mx-auto mt-2 w-8 h-6 flex items-center justify-center text-slate-500 hover:text-slate-200 transition-colors"
        >
          <ChevronLeft size={14} className="rotate-180" />
        </button>
      )}

      {/* Navigation */}
      <nav className="flex-1 py-4 px-2 space-y-0.5">
        {NAV_ITEMS.map(({ to, icon: Icon, label }) => (
          <NavLink
            key={to}
            to={to}
            end={to === '/'}
            className={({ isActive }) => clsx(
              'flex items-center gap-3 rounded-lg py-2.5 text-sm transition-colors',
              sidebarOpen ? 'px-3' : 'justify-center px-0',
              isActive
                ? 'bg-blue-600/15 text-blue-400 font-medium'
                : 'text-slate-400 hover:text-slate-100 hover:bg-slate-800/70',
            )}
          >
            <Icon size={16} className="flex-shrink-0" />
            {sidebarOpen && <span className="truncate">{label}</span>}
          </NavLink>
        ))}
      </nav>

      {/* Footer */}
      <div className="border-t border-slate-800 flex-shrink-0">
        <div className={clsx(
          'flex items-center gap-2 px-4 py-2',
          !sidebarOpen && 'justify-center px-0',
        )}>
          {wsConnected
            ? <Wifi size={11} className="text-emerald-400 flex-shrink-0" />
            : <WifiOff size={11} className="text-slate-600 flex-shrink-0" />}
          {sidebarOpen && (
            <span className="text-xs text-slate-500">
              {wsConnected ? 'Live' : 'Offline'} · {user?.sub ?? 'guest'}
            </span>
          )}
        </div>
        <button
          onClick={() => { setToken(null); setUser(null) }}
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

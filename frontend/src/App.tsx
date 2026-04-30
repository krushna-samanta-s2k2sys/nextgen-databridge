import React, { useEffect, useState } from 'react'
import { BrowserRouter, Routes, Route, Navigate, useNavigate } from 'react-router-dom'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { Toaster } from 'react-hot-toast'
import { Zap, Eye, EyeOff, AlertCircle } from 'lucide-react'
import { Sidebar } from './components/Sidebar'
import { useStore } from './store/useStore'
import { useWebSocket } from './hooks/useWebSocket'
import { me, login } from './api/client'
import { Spinner } from './components/ui'

import Dashboard from './pages/Dashboard'
import { ConfiguredPipelines, PipelineConfigDetail } from './pages/ConfiguredPipelines'
import { Runs, RunDetail } from './pages/Runs'
import QueryEditor from './pages/QueryEditor'
import ConfigReference from './pages/ConfigReference'

const qc = new QueryClient({
  defaultOptions: { queries: { retry: 1, staleTime: 10_000 } },
})

// ── Login Page ────────────────────────────────────────────────────────────────
function LoginPage() {
  const { setToken, setUser } = useStore()
  const nav = useNavigate()
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [showPw, setShowPw] = useState(false)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  async function handleLogin(e: React.FormEvent) {
    e.preventDefault()
    setLoading(true)
    setError('')
    try {
      const data = await login(username, password)
      setToken(data.token || data.access_token)
      const user = await me()
      setUser(user)
      nav('/')
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Invalid credentials')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="min-h-screen bg-zinc-950 flex items-center justify-center p-4">
      <div className="w-full max-w-sm">
        <div className="flex items-center justify-center gap-3 mb-8">
          <div className="w-10 h-10 bg-blue-600 rounded-xl flex items-center justify-center shadow-lg shadow-blue-600/30">
            <Zap size={20} className="text-white" />
          </div>
          <div>
            <h1 className="text-lg font-bold text-zinc-100 leading-tight">NextGenDatabridge</h1>
            <p className="text-xs text-zinc-600">Monitor &amp; Explore</p>
          </div>
        </div>

        <div className="bg-zinc-900 border border-zinc-800 rounded-2xl p-6 shadow-2xl">
          <h2 className="text-sm font-semibold text-zinc-200 mb-5">Sign in to continue</h2>
          <form onSubmit={handleLogin} className="space-y-4">
            <div>
              <label className="text-xs font-medium text-zinc-500 block mb-1">Username</label>
              <input
                value={username}
                onChange={e => setUsername(e.target.value)}
                placeholder="admin"
                autoFocus
                required
                className="w-full bg-zinc-950 border border-zinc-700 rounded-lg px-3 py-2.5 text-sm text-zinc-100 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent placeholder:text-zinc-700"
              />
            </div>
            <div>
              <label className="text-xs font-medium text-zinc-500 block mb-1">Password</label>
              <div className="relative">
                <input
                  type={showPw ? 'text' : 'password'}
                  value={password}
                  onChange={e => setPassword(e.target.value)}
                  placeholder="••••••••"
                  required
                  className="w-full bg-zinc-950 border border-zinc-700 rounded-lg px-3 py-2.5 pr-10 text-sm text-zinc-100 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent placeholder:text-zinc-700"
                />
                <button
                  type="button"
                  onClick={() => setShowPw(!showPw)}
                  className="absolute right-3 top-1/2 -translate-y-1/2 text-zinc-600 hover:text-zinc-400"
                >
                  {showPw ? <EyeOff size={15}/> : <Eye size={15}/>}
                </button>
              </div>
            </div>

            {error && (
              <div className="flex items-center gap-2 text-xs text-red-400 bg-red-500/10 border border-red-500/20 rounded-lg px-3 py-2">
                <AlertCircle size={13} className="flex-shrink-0"/>
                {error}
              </div>
            )}

            <button
              type="submit"
              disabled={loading || !username || !password}
              className="w-full bg-blue-600 hover:bg-blue-500 disabled:opacity-40 disabled:cursor-not-allowed text-white rounded-lg py-2.5 text-sm font-medium transition-colors flex items-center justify-center gap-2"
            >
              {loading ? <><Spinner size="sm"/> Signing in…</> : 'Sign in'}
            </button>
          </form>
        </div>

        <p className="text-center text-xs text-zinc-700 mt-4">
          NextGenDatabridge Platform · S&amp;P NextGen Framework
        </p>
      </div>
    </div>
  )
}

// ── App Shell ─────────────────────────────────────────────────────────────────
function AppShell() {
  const { setUser, token } = useStore()
  useWebSocket()

  useEffect(() => {
    if (token) me().then(setUser).catch(() => {})
  }, [token])

  return (
    <div className="flex h-screen overflow-hidden bg-gray-50">
      <Sidebar />
      <main className="flex-1 overflow-hidden">
        <Routes>
          <Route path="/"                element={<Dashboard />} />
          <Route path="/pipelines"       element={<ConfiguredPipelines />} />
          <Route path="/pipelines/:id"   element={<PipelineConfigDetail />} />
          <Route path="/runs"            element={<Runs />} />
          <Route path="/runs/:runId"     element={<RunDetail />} />
          <Route path="/query"           element={<QueryEditor />} />
          <Route path="/reference"      element={<ConfigReference />} />
          <Route path="*"               element={<Navigate to="/" />} />
        </Routes>
      </main>
    </div>
  )
}

function InnerApp() {
  const { token } = useStore()
  return (
    <Routes>
      <Route path="/login" element={token ? <Navigate to="/" replace /> : <LoginPage />} />
      <Route path="/*"     element={token ? <AppShell /> : <Navigate to="/login" replace />} />
    </Routes>
  )
}

export default function App() {
  return (
    <QueryClientProvider client={qc}>
      <BrowserRouter>
        <InnerApp />
        <Toaster
          position="bottom-right"
          toastOptions={{
            style: {
              background: '#18181b',
              color: '#f4f4f5',
              border: '1px solid #3f3f46',
              fontSize: '13px',
              borderRadius: '10px',
            },
            success: { iconTheme: { primary: '#34d399', secondary: '#18181b' } },
            error:   { iconTheme: { primary: '#f87171', secondary: '#18181b' } },
          }}
        />
      </BrowserRouter>
    </QueryClientProvider>
  )
}

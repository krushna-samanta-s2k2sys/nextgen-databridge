import React, { useState, useEffect, useCallback } from 'react'
import { useLocation } from 'react-router-dom'
import Editor from '@monaco-editor/react'
import { useQuery, useMutation } from '@tanstack/react-query'
import toast from 'react-hot-toast'
import { queryDuckDB, getDuckDBFiles } from '../api/client'
import { Spinner } from '../components/ui'
import {
  Database, Search, RefreshCw, Play, Download, Copy,
  CheckCircle, ChevronDown, ChevronRight, XCircle,
  TableProperties, FileText, Hash, Type as TypeIcon,
  Clock, Rows, BarChart2,
} from 'lucide-react'
import clsx from 'clsx'

// ── Types ─────────────────────────────────────────────────────────────────────
interface DuckDBFile {
  s3_path: string
  pipeline_id: string
  run_id: string
  task_id: string
  table_name?: string
  row_count?: number
}

interface SchemaRow {
  column_name: string
  column_type: string
}

interface QueryResult {
  columns: string[]
  rows: any[][]
  row_count: number
  duration_ms: number
}

// ── Helpers ───────────────────────────────────────────────────────────────────
function formatRunId(runId: string): string {
  // e.g. "2024-04-29T07:00:00+00:00__scheduled" → "Apr 29 07:00"
  try {
    const dt = new Date(runId.split('__')[0] || runId.split('_')[0] || runId)
    if (!isNaN(dt.getTime())) return dt.toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })
  } catch {}
  return runId.slice(-20)
}

function typeIcon(colType: string) {
  const t = colType?.toLowerCase() ?? ''
  if (t.includes('int') || t.includes('double') || t.includes('float') || t.includes('decimal') || t.includes('numeric')) {
    return <Hash size={10} className="text-blue-500 flex-shrink-0" />
  }
  if (t.includes('timestamp') || t.includes('date') || t.includes('time')) {
    return <Clock size={10} className="text-purple-500 flex-shrink-0" />
  }
  return <TypeIcon size={10} className="text-gray-400 flex-shrink-0" />
}

const SNIPPETS = [
  { label: 'All rows',      sql: (t: string) => `SELECT *\nFROM ${t}\nLIMIT 100` },
  { label: 'Row count',     sql: (t: string) => `SELECT COUNT(*) AS total_rows\nFROM ${t}` },
  { label: 'Null check',    sql: (t: string) => `-- Replace column_name with any column\nSELECT COUNT(*) AS nulls\nFROM ${t}\nWHERE column_name IS NULL` },
  { label: 'Column stats',  sql: (t: string) => `SUMMARIZE SELECT * FROM ${t}` },
  { label: 'Distinct vals', sql: (t: string) => `-- Replace column_name with any column\nSELECT column_name, COUNT(*) AS cnt\nFROM ${t}\nGROUP BY 1\nORDER BY cnt DESC\nLIMIT 50` },
]

// ── File browser ──────────────────────────────────────────────────────────────
function FileBrowser({
  selectedPath, onSelect, onRefresh,
}: {
  selectedPath?: string
  onSelect: (f: DuckDBFile) => void
  onRefresh: () => void
}) {
  const [search,       setSearch]       = useState('')
  const [expanded,     setExpanded]     = useState<Record<string, boolean>>({})

  const { data, isLoading, isError, refetch } = useQuery({
    queryKey: ['duckdb-files'],
    queryFn:  () => getDuckDBFiles(),
    staleTime: 30_000,
    refetchInterval: 60_000,
  })

  const files: DuckDBFile[] = data?.files ?? []

  // Group by pipeline → run
  const grouped: Record<string, Record<string, DuckDBFile[]>> = {}
  files.forEach(f => {
    const pid = f.pipeline_id || 'unknown'
    const rid = f.run_id       || 'unknown'
    if (!grouped[pid]) grouped[pid] = {}
    if (!grouped[pid][rid]) grouped[pid][rid] = []
    grouped[pid][rid].push(f)
  })

  // Filter by search
  const filteredPipelines = Object.entries(grouped).filter(([pid]) =>
    !search || pid.toLowerCase().includes(search.toLowerCase()),
  )

  function togglePipeline(pid: string) {
    setExpanded(e => ({ ...e, [pid]: !e[pid] }))
  }
  function toggleRun(key: string) {
    setExpanded(e => ({ ...e, [key]: !e[key] }))
  }

  // Auto-expand pipeline that has the selected file
  useEffect(() => {
    if (!selectedPath) return
    const f = files.find(f => f.s3_path === selectedPath)
    if (f) {
      setExpanded(e => ({
        ...e,
        [f.pipeline_id]: true,
        [`${f.pipeline_id}__${f.run_id}`]: true,
      }))
    }
  }, [selectedPath, files.length])

  function handleRefresh() { refetch(); onRefresh() }

  return (
    <div className="flex flex-col h-full w-56 flex-shrink-0 bg-white border-r border-gray-200">
      {/* Header */}
      <div className="px-3 py-2.5 border-b border-gray-100 flex items-center gap-2">
        <div className="relative flex-1">
          <Search size={11} className="absolute left-2.5 top-1/2 -translate-y-1/2 text-gray-400" />
          <input
            value={search}
            onChange={e => setSearch(e.target.value)}
            placeholder="Filter pipeline…"
            className="w-full bg-gray-50 border border-gray-200 rounded-md pl-7 pr-2 py-1.5 text-xs text-gray-700 focus:outline-none focus:ring-1 focus:ring-blue-500 placeholder:text-gray-400"
          />
        </div>
        <button
          onClick={handleRefresh}
          className="text-gray-400 hover:text-gray-600 p-1 rounded hover:bg-gray-100 flex-shrink-0 transition-colors"
          title="Refresh file list"
        >
          <RefreshCw size={11} />
        </button>
      </div>

      {/* Tree */}
      <div className="flex-1 overflow-y-auto text-xs">
        {isLoading && <div className="flex justify-center py-8"><Spinner size="sm" /></div>}
        {isError && <p className="text-red-500 px-3 py-4 text-xs">Failed to load files</p>}

        {!isLoading && filteredPipelines.length === 0 && (
          <div className="flex flex-col items-center py-10 gap-2">
            <Database size={20} className="text-gray-200" />
            <p className="text-gray-400 text-xs text-center px-2">
              {search ? 'No pipelines match filter' : 'No DuckDB outputs yet'}
            </p>
          </div>
        )}

        {filteredPipelines.map(([pid, runs]) => {
          const isPipelineOpen = !!expanded[pid]
          return (
            <div key={pid}>
              {/* Pipeline row */}
              <div
                onClick={() => togglePipeline(pid)}
                className="flex items-center gap-1.5 px-3 py-2 cursor-pointer hover:bg-gray-50 select-none border-b border-gray-50"
              >
                {isPipelineOpen
                  ? <ChevronDown size={11} className="text-gray-400 flex-shrink-0" />
                  : <ChevronRight size={11} className="text-gray-400 flex-shrink-0" />}
                <Database size={11} className="text-blue-500 flex-shrink-0" />
                <span className="font-mono font-semibold text-gray-700 truncate">{pid}</span>
              </div>

              {isPipelineOpen && Object.entries(runs).map(([rid, taskFiles]) => {
                const runKey   = `${pid}__${rid}`
                const isRunOpen = !!expanded[runKey]
                return (
                  <div key={rid}>
                    {/* Run row */}
                    <div
                      onClick={() => toggleRun(runKey)}
                      className="flex items-center gap-1.5 pl-6 pr-3 py-1.5 cursor-pointer hover:bg-gray-50 select-none border-b border-gray-50"
                    >
                      {isRunOpen
                        ? <ChevronDown size={10} className="text-gray-300 flex-shrink-0" />
                        : <ChevronRight size={10} className="text-gray-300 flex-shrink-0" />}
                      <Clock size={10} className="text-gray-400 flex-shrink-0" />
                      <span className="text-gray-500 truncate font-mono" title={rid}>{formatRunId(rid)}</span>
                    </div>

                    {/* File rows */}
                    {isRunOpen && taskFiles.map(f => {
                      const fname      = f.s3_path.split('/').slice(-1)[0]
                      const isSelected = f.s3_path === selectedPath
                      return (
                        <div
                          key={f.s3_path}
                          onClick={() => onSelect(f)}
                          className={clsx(
                            'flex items-start gap-1.5 pl-10 pr-3 py-2 cursor-pointer border-b border-gray-50 transition-colors',
                            isSelected
                              ? 'bg-blue-50 border-l-2 border-l-blue-500'
                              : 'hover:bg-gray-50',
                          )}
                        >
                          <FileText size={10} className={clsx('flex-shrink-0 mt-0.5', isSelected ? 'text-blue-500' : 'text-gray-300')} />
                          <div className="min-w-0 flex-1">
                            <p className={clsx('font-mono truncate leading-tight', isSelected ? 'text-blue-700 font-medium' : 'text-gray-700')}>
                              {f.task_id}
                            </p>
                            {f.row_count != null && (
                              <p className="text-gray-400 mt-0.5 flex items-center gap-1">
                                <Rows size={9} />
                                {f.row_count.toLocaleString()} rows
                              </p>
                            )}
                          </div>
                        </div>
                      )
                    })}
                  </div>
                )
              })}
            </div>
          )
        })}
      </div>

      <div className="border-t border-gray-100 px-3 py-2">
        <p className="text-xs text-gray-400">{files.length} file{files.length !== 1 ? 's' : ''}</p>
      </div>
    </div>
  )
}

// ── Schema panel ──────────────────────────────────────────────────────────────
function SchemaPanel({ schema, loading }: { schema: SchemaRow[]; loading: boolean }) {
  if (loading) return (
    <div className="flex items-center gap-2 px-4 py-3 bg-gray-50 border-b border-gray-200">
      <Spinner size="sm" />
      <span className="text-xs text-gray-500">Loading schema…</span>
    </div>
  )
  if (schema.length === 0) return null

  return (
    <div className="bg-gray-50 border-b border-gray-200 px-4 py-2 flex items-start gap-3 overflow-x-auto flex-shrink-0">
      <div className="flex items-center gap-1 text-xs text-gray-500 flex-shrink-0 pt-0.5">
        <TableProperties size={11} />
        <span className="font-semibold">Schema</span>
        <span className="text-gray-400">({schema.length} cols)</span>
      </div>
      <div className="flex items-start gap-2 flex-wrap">
        {schema.map(col => (
          <div
            key={col.column_name}
            className="flex items-center gap-1 bg-white border border-gray-200 rounded px-2 py-1 text-xs whitespace-nowrap shadow-sm"
          >
            {typeIcon(col.column_type)}
            <span className="font-mono text-gray-700 font-medium">{col.column_name}</span>
            <span className="text-gray-400">{col.column_type}</span>
          </div>
        ))}
      </div>
    </div>
  )
}

// ── Results table ─────────────────────────────────────────────────────────────
function ResultsTable({ result }: { result: QueryResult }) {
  const [copied, setCopied] = useState(false)

  function copyTSV() {
    const header = result.columns.join('\t')
    const rows   = result.rows.map(r => r.map(c => c ?? '').join('\t')).join('\n')
    navigator.clipboard.writeText(`${header}\n${rows}`)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  function downloadCSV() {
    const header = result.columns.join(',')
    const rows   = result.rows.map(r =>
      r.map(c => c === null ? '' : String(c).includes(',') ? `"${c}"` : c).join(',')
    ).join('\n')
    const blob = new Blob([`${header}\n${rows}`], { type: 'text/csv' })
    const url  = URL.createObjectURL(blob)
    const a    = document.createElement('a')
    a.href = url
    a.download = `query_${Date.now()}.csv`
    a.click()
    URL.revokeObjectURL(url)
  }

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      {/* Result meta bar */}
      <div className="flex items-center justify-between px-4 py-2 border-b border-gray-200 bg-white flex-shrink-0">
        <div className="flex items-center gap-4">
          <div className="flex items-center gap-1.5 text-xs">
            <Rows size={11} className="text-gray-400" />
            <span className="font-semibold text-gray-700">{result.row_count.toLocaleString()}</span>
            <span className="text-gray-400">rows</span>
          </div>
          <div className="flex items-center gap-1.5 text-xs">
            <BarChart2 size={11} className="text-gray-400" />
            <span className="text-gray-400">{result.columns.length} cols</span>
          </div>
          <div className="flex items-center gap-1.5 text-xs">
            <Clock size={11} className="text-gray-400" />
            <span className="text-gray-400">{result.duration_ms}ms</span>
          </div>
        </div>
        <div className="flex items-center gap-1.5">
          <button
            onClick={copyTSV}
            className="flex items-center gap-1 text-xs text-gray-600 hover:text-gray-800 bg-white hover:bg-gray-50 border border-gray-300 px-2.5 py-1.5 rounded-lg transition-colors shadow-sm"
          >
            {copied ? <CheckCircle size={11} className="text-emerald-500" /> : <Copy size={11} />}
            {copied ? 'Copied!' : 'Copy TSV'}
          </button>
          <button
            onClick={downloadCSV}
            className="flex items-center gap-1 text-xs text-white bg-blue-600 hover:bg-blue-500 px-2.5 py-1.5 rounded-lg transition-colors shadow-sm"
          >
            <Download size={11} /> Export CSV
          </button>
        </div>
      </div>

      {/* Table */}
      <div className="flex-1 overflow-auto">
        {result.row_count === 0 ? (
          <div className="flex flex-col items-center justify-center h-full gap-2 text-gray-300">
            <Database size={24} />
            <p className="text-sm text-gray-400">Query returned 0 rows</p>
          </div>
        ) : (
          <table className="w-full text-xs border-collapse">
            <thead className="sticky top-0 bg-gray-50 shadow-sm z-10">
              <tr>
                <th className="px-3 py-2 text-center text-gray-300 font-medium border-b border-r border-gray-200 w-10 bg-gray-50">#</th>
                {result.columns.map(c => (
                  <th key={c} className="px-3 py-2 text-left text-gray-600 font-semibold border-b border-r border-gray-200 whitespace-nowrap bg-gray-50 font-mono">
                    {c}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {result.rows.map((row, i) => (
                <tr key={i} className={clsx('border-b border-gray-100', i % 2 === 0 ? 'bg-white' : 'bg-gray-50/30')}>
                  <td className="px-3 py-2 text-gray-300 font-mono text-right border-r border-gray-100 select-none">{i + 1}</td>
                  {row.map((cell, j) => (
                    <td
                      key={j}
                      className="px-3 py-2 font-mono whitespace-nowrap max-w-xs truncate border-r border-gray-100"
                      title={cell === null ? 'NULL' : String(cell)}
                    >
                      {cell === null
                        ? <span className="text-gray-300 italic not-italic text-xs">NULL</span>
                        : typeof cell === 'number'
                          ? <span className="text-blue-700">{cell}</span>
                          : typeof cell === 'boolean'
                            ? <span className={cell ? 'text-emerald-600' : 'text-red-500'}>{String(cell)}</span>
                            : <span className="text-gray-700">{String(cell)}</span>
                      }
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}

// ── Main Query Editor ─────────────────────────────────────────────────────────
export default function QueryEditor() {
  const location = useLocation()

  const [selectedFile, setSelectedFile] = useState<DuckDBFile | null>(null)
  const [sql,          setSql]          = useState('-- Select a file on the left, then write SQL\n-- Ctrl+Enter to run\n\nSELECT *\nFROM result\nLIMIT 100')
  const [result,       setResult]       = useState<QueryResult | null>(null)
  const [schema,       setSchema]       = useState<SchemaRow[]>([])
  const [schemaLoading, setSchemaLoading] = useState(false)
  const [rowLimit,     setRowLimit]     = useState('500')
  const [filesKey,     setFilesKey]     = useState(0)

  // Auto-select from URL params (linked from RunDetail)
  useEffect(() => {
    const p = new URLSearchParams(location.search)
    const path = p.get('path')
    if (!path) return
    setSelectedFile({
      s3_path:     path,
      pipeline_id: p.get('pipeline') ?? '',
      run_id:      p.get('run')      ?? '',
      task_id:     p.get('task')     ?? '',
      table_name:  p.get('table')    ?? 'result',
    })
    const tbl = p.get('table') || p.get('task') || 'result'
    setSql(`SELECT *\nFROM ${tbl}\nLIMIT 100`)
  }, [location.search])

  // Load schema when file changes: SHOW TABLES → discover real name → DESCRIBE
  useEffect(() => {
    if (!selectedFile) { setSchema([]); return }
    setSchemaLoading(true)
    setSchema([])

    const headers = {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${localStorage.getItem('df_token') ?? ''}`,
    }
    const baseBody = {
      pipeline_id: selectedFile.pipeline_id,
      run_id:      selectedFile.run_id,
      task_id:     selectedFile.task_id,
      duckdb_path: selectedFile.s3_path,
      limit:       200,
    }

    // Step 1: discover the actual table name inside the DuckDB file
    fetch('/api/query', {
      method: 'POST',
      headers,
      body: JSON.stringify({ ...baseBody, sql: 'SHOW TABLES', limit: 10 }),
    })
      .then(r => r.json())
      .then(res => {
        const nameIdx = (res?.columns ?? []).indexOf('name')
        let tbl = selectedFile.table_name || 'result'
        if (nameIdx >= 0 && res.rows?.length > 0) {
          tbl = res.rows[0][nameIdx]
        }
        // Update SQL editor with real table name
        setSql(`SELECT *\nFROM ${tbl}\nLIMIT 100`)
        setSelectedFile(prev => prev ? { ...prev, table_name: tbl } : prev)

        // Step 2: describe the discovered table
        return fetch('/api/query', {
          method: 'POST',
          headers,
          body: JSON.stringify({ ...baseBody, sql: `DESCRIBE ${tbl}`, limit: 200 }),
        })
      })
      .then(r => r.json())
      .then(res => {
        if (res?.columns && res?.rows) {
          const colIdx = res.columns.indexOf('column_name')
          const typIdx = res.columns.indexOf('column_type')
          if (colIdx >= 0 && typIdx >= 0) {
            setSchema(res.rows.map((r: any[]) => ({
              column_name: r[colIdx],
              column_type: r[typIdx],
            })))
          }
        }
      })
      .catch(() => {})
      .finally(() => setSchemaLoading(false))
  }, [selectedFile?.s3_path])

  const queryMut = useMutation({
    mutationFn: () => queryDuckDB({
      pipeline_id: selectedFile?.pipeline_id ?? '',
      run_id:      selectedFile?.run_id      ?? '',
      task_id:     selectedFile?.task_id     ?? '',
      duckdb_path: selectedFile?.s3_path     ?? '',
      sql,
      limit: parseInt(rowLimit, 10),
    }),
    onSuccess: (res) => {
      setResult(res)
      toast.success(`${res.row_count.toLocaleString()} rows · ${res.duration_ms}ms`)
    },
    onError: (e: any) => {
      const msg = e.response?.data?.detail ?? e.message ?? 'Query failed'
      toast.error(msg)
    },
  })

  const runQuery = useCallback(() => {
    if (!selectedFile) { toast.error('Select a file first'); return }
    queryMut.mutate()
  }, [selectedFile, sql, rowLimit])

  function handleFileSelect(f: DuckDBFile) {
    setSelectedFile(f)
    setResult(null)
    setSql('-- Discovering table…')
  }

  function applySnippet(fn: (t: string) => string) {
    const tbl = selectedFile?.table_name || 'result'
    setSql(fn(tbl))
  }

  const fileName   = selectedFile?.s3_path?.split('/').slice(-1)[0] ?? ''
  const tableName  = selectedFile?.table_name || 'result'

  return (
    <div className="flex flex-col h-full overflow-hidden bg-gray-50">
      {/* Top bar */}
      <div className="flex items-center justify-between px-5 py-3 border-b border-gray-200 bg-white flex-shrink-0 shadow-sm">
        <div className="flex items-center gap-3">
          <Database size={16} className="text-blue-600" />
          <div>
            <h1 className="text-sm font-bold text-gray-900">Query Editor</h1>
            <p className="text-xs text-gray-400">Ad-hoc SQL over any DuckDB pipeline output</p>
          </div>
        </div>
        {selectedFile && (
          <div className="flex items-center gap-2 bg-blue-50 border border-blue-200 rounded-lg px-3 py-1.5">
            <Database size={11} className="text-blue-500" />
            <div className="text-xs">
              <span className="font-mono text-blue-700 font-semibold">{selectedFile.pipeline_id}</span>
              <span className="text-blue-400 mx-1">›</span>
              <span className="font-mono text-blue-600">{selectedFile.task_id}</span>
              {selectedFile.run_id && (
                <span className="text-blue-400 ml-1 hidden lg:inline">· {formatRunId(selectedFile.run_id)}</span>
              )}
            </div>
          </div>
        )}
      </div>

      {/* Body: file browser + editor */}
      <div className="flex flex-1 overflow-hidden">
        {/* File browser */}
        <FileBrowser
          selectedPath={selectedFile?.s3_path}
          onSelect={handleFileSelect}
          onRefresh={() => setFilesKey(k => k + 1)}
        />

        {/* Editor + results */}
        <div className="flex-1 flex flex-col overflow-hidden">
          {/* Schema panel */}
          <SchemaPanel schema={schema} loading={schemaLoading} />

          {/* SQL editor section */}
          <div className="bg-white border-b border-gray-200 flex-shrink-0">
            {/* Editor toolbar */}
            <div className="flex items-center justify-between px-4 py-2 border-b border-gray-100">
              {/* Snippets */}
              <div className="flex items-center gap-1.5 flex-wrap">
                <span className="text-xs text-gray-400 mr-1">Quick:</span>
                {SNIPPETS.map(s => (
                  <button
                    key={s.label}
                    onClick={() => applySnippet(s.sql)}
                    disabled={!selectedFile}
                    className="text-xs text-gray-600 hover:text-blue-700 hover:bg-blue-50 border border-gray-200 hover:border-blue-200 px-2 py-0.5 rounded transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
                  >
                    {s.label}
                  </button>
                ))}
              </div>
              {/* Run controls */}
              <div className="flex items-center gap-2 flex-shrink-0">
                <div className="flex items-center gap-1.5">
                  <span className="text-xs text-gray-400">Limit:</span>
                  <select
                    value={rowLimit}
                    onChange={e => setRowLimit(e.target.value)}
                    className="bg-white border border-gray-300 rounded px-2 py-0.5 text-xs text-gray-700 focus:outline-none focus:ring-1 focus:ring-blue-500"
                  >
                    {['100', '500', '1000', '5000', '10000'].map(n => (
                      <option key={n} value={n}>{Number(n).toLocaleString()}</option>
                    ))}
                  </select>
                </div>
                <span className="text-xs text-gray-300 hidden xl:block">Ctrl+Enter</span>
                <button
                  onClick={runQuery}
                  disabled={!selectedFile || queryMut.isPending}
                  className="flex items-center gap-1.5 text-xs font-medium text-white bg-blue-600 hover:bg-blue-500 disabled:opacity-40 disabled:cursor-not-allowed px-3 py-1.5 rounded-lg transition-colors shadow-sm"
                >
                  {queryMut.isPending
                    ? <><Spinner size="sm" /> Running…</>
                    : <><Play size={12} /> Run Query</>
                  }
                </button>
              </div>
            </div>

            {/* Monaco */}
            {!selectedFile && (
              <div className="px-4 py-2 bg-amber-50 border-b border-amber-100">
                <p className="text-xs text-amber-700 flex items-center gap-1.5">
                  <XCircle size={11} /> Select a pipeline output from the panel on the left
                </p>
              </div>
            )}
            <Editor
              height="200px"
              language="sql"
              theme="vs-dark"
              value={sql}
              onChange={v => setSql(v ?? '')}
              options={{
                minimap: { enabled: false },
                fontSize: 13,
                lineNumbers: 'on',
                scrollBeyondLastLine: false,
                wordWrap: 'on',
                tabSize: 2,
                automaticLayout: true,
                padding: { top: 10, bottom: 10 },
                suggestOnTriggerCharacters: true,
              }}
              onMount={(editor, monaco) => {
                editor.addCommand(
                  monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter,
                  () => { if (selectedFile) queryMut.mutate() },
                )
              }}
            />
          </div>

          {/* Results area */}
          <div className="flex-1 overflow-hidden flex flex-col">
            {queryMut.isPending && (
              <div className="flex items-center justify-center flex-1 gap-3 text-gray-500">
                <Spinner size="md" />
                <span className="text-sm">Executing query…</span>
              </div>
            )}

            {!queryMut.isPending && !result && (
              <div className="flex flex-col items-center justify-center flex-1 gap-3 text-gray-300">
                <Play size={32} strokeWidth={1.5} />
                <div className="text-center">
                  <p className="text-sm font-medium text-gray-400">
                    {selectedFile ? `Table: ${tableName}` : 'No file selected'}
                  </p>
                  <p className="text-xs text-gray-300 mt-1">
                    {selectedFile ? 'Click "Run Query" or press Ctrl+Enter' : 'Pick a pipeline output from the left panel'}
                  </p>
                </div>
              </div>
            )}

            {!queryMut.isPending && result && (
              <ResultsTable result={result} />
            )}
          </div>
        </div>
      </div>
    </div>
  )
}

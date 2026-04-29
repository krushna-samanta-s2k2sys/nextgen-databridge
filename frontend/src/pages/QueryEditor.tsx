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
  Hash, Type as TypeIcon, Clock, Rows, BarChart2,
  Columns, X, Plus, Link2, HelpCircle,
} from 'lucide-react'
import clsx from 'clsx'

// ── Types ─────────────────────────────────────────────────────────────────────
interface DuckDBFile {
  s3_path: string
  pipeline_id: string
  run_id: string
  task_id: string
  table_name?: string | null
  row_count?: number
}

interface SchemaRow { column_name: string; column_type: string }
interface QueryResult { columns: string[]; rows: any[][]; row_count: number; duration_ms: number }

// ── Helpers ───────────────────────────────────────────────────────────────────
function sanitizeAlias(s: string): string {
  return s.replace(/[^a-z0-9_]/gi, '_').toLowerCase()
}

function fileAlias(f: DuckDBFile): string {
  return sanitizeAlias(f.task_id || 'table')
}

function formatRunId(runId: string): string {
  try {
    const dt = new Date(runId.split('__')[0] || runId.split('_')[0] || runId)
    if (!isNaN(dt.getTime()))
      return dt.toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })
  } catch {}
  return runId.slice(-20)
}

function typeIcon(colType: string) {
  const t = colType?.toLowerCase() ?? ''
  if (t.includes('int') || t.includes('double') || t.includes('float') || t.includes('decimal') || t.includes('numeric'))
    return <Hash size={10} className="text-blue-500 flex-shrink-0" />
  if (t.includes('timestamp') || t.includes('date') || t.includes('time'))
    return <Clock size={10} className="text-purple-500 flex-shrink-0" />
  return <TypeIcon size={10} className="text-gray-400 flex-shrink-0" />
}

const SNIPPETS = [
  { label: 'All rows',      sql: (t: string, multi: boolean) =>
      multi ? `SELECT *\nFROM ${t}\nLIMIT 100` : `SELECT *\nFROM ${t}\nLIMIT 100` },
  { label: 'Row count',     sql: (t: string) => `SELECT COUNT(*) AS total_rows\nFROM ${t}` },
  { label: 'Column stats',  sql: (t: string) => `SUMMARIZE SELECT * FROM ${t}` },
  { label: 'Null check',    sql: (t: string) => `-- Replace column_name below\nSELECT COUNT(*) AS nulls\nFROM ${t}\nWHERE column_name IS NULL` },
  { label: 'Distinct vals', sql: (t: string) => `-- Replace column_name below\nSELECT column_name, COUNT(*) AS cnt\nFROM ${t}\nGROUP BY 1\nORDER BY cnt DESC\nLIMIT 50` },
]

// ── File browser ──────────────────────────────────────────────────────────────
function FileBrowser({
  selectedPaths, onToggle, onRefresh,
}: {
  selectedPaths: string[]
  onToggle: (f: DuckDBFile) => void
  onRefresh: () => void
}) {
  const [search,   setSearch]   = useState('')
  const [expanded, setExpanded] = useState<Record<string, boolean>>({})

  const { data, isLoading, isError, refetch } = useQuery({
    queryKey: ['duckdb-files'],
    queryFn:  () => getDuckDBFiles(),
    staleTime: 30_000,
    refetchInterval: 60_000,
  })

  const files: DuckDBFile[] = data?.files ?? []

  const grouped: Record<string, Record<string, DuckDBFile[]>> = {}
  files.forEach(f => {
    const pid = f.pipeline_id || 'unknown'
    const rid = f.run_id       || 'unknown'
    if (!grouped[pid]) grouped[pid] = {}
    if (!grouped[pid][rid]) grouped[pid][rid] = []
    grouped[pid][rid].push(f)
  })

  const filteredPipelines = Object.entries(grouped).filter(([pid]) =>
    !search || pid.toLowerCase().includes(search.toLowerCase()),
  )

  useEffect(() => {
    if (!selectedPaths.length) return
    const f = files.find(f => f.s3_path === selectedPaths[0])
    if (f) setExpanded(e => ({
      ...e,
      [f.pipeline_id]: true,
      [`${f.pipeline_id}__${f.run_id}`]: true,
    }))
  }, [selectedPaths[0], files.length])

  return (
    <div className="flex flex-col h-full w-56 flex-shrink-0 bg-white border-r border-gray-200">
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
        <button onClick={() => { refetch(); onRefresh() }}
          className="text-gray-400 hover:text-gray-600 p-1 rounded hover:bg-gray-100 flex-shrink-0 transition-colors">
          <RefreshCw size={11} />
        </button>
      </div>

      <div className="flex-1 overflow-y-auto text-xs">
        {isLoading && <div className="flex justify-center py-8"><Spinner size="sm" /></div>}
        {isError && <p className="text-red-500 px-3 py-4 text-xs">Failed to load files</p>}
        {!isLoading && filteredPipelines.length === 0 && (
          <div className="flex flex-col items-center py-10 gap-2">
            <Database size={20} className="text-gray-200" />
            <p className="text-gray-400 text-xs text-center px-2">
              {search ? 'No match' : 'No DuckDB outputs yet'}
            </p>
          </div>
        )}

        {filteredPipelines.map(([pid, runs]) => {
          const isOpen = !!expanded[pid]
          return (
            <div key={pid}>
              <div onClick={() => setExpanded(e => ({ ...e, [pid]: !e[pid] }))}
                className="flex items-center gap-1.5 px-3 py-2 cursor-pointer hover:bg-gray-50 select-none border-b border-gray-50">
                {isOpen ? <ChevronDown size={11} className="text-gray-400 flex-shrink-0" />
                        : <ChevronRight size={11} className="text-gray-400 flex-shrink-0" />}
                <Database size={11} className="text-blue-500 flex-shrink-0" />
                <span className="font-mono font-semibold text-gray-700 truncate">{pid}</span>
              </div>

              {isOpen && Object.entries(runs).map(([rid, taskFiles]) => {
                const runKey = `${pid}__${rid}`
                const isRunOpen = !!expanded[runKey]
                return (
                  <div key={rid}>
                    <div onClick={() => setExpanded(e => ({ ...e, [runKey]: !e[runKey] }))}
                      className="flex items-center gap-1.5 pl-6 pr-3 py-1.5 cursor-pointer hover:bg-gray-50 select-none border-b border-gray-50">
                      {isRunOpen ? <ChevronDown size={10} className="text-gray-300 flex-shrink-0" />
                                 : <ChevronRight size={10} className="text-gray-300 flex-shrink-0" />}
                      <Clock size={10} className="text-gray-400 flex-shrink-0" />
                      <span className="text-gray-500 truncate font-mono" title={rid}>{formatRunId(rid)}</span>
                    </div>

                    {isRunOpen && taskFiles.map(f => {
                      const selIdx    = selectedPaths.indexOf(f.s3_path)
                      const isSelected = selIdx >= 0
                      return (
                        <div key={f.s3_path} onClick={() => onToggle(f)}
                          className={clsx(
                            'flex items-start gap-1.5 pl-10 pr-3 py-2 cursor-pointer border-b border-gray-50 transition-colors',
                            isSelected ? 'bg-blue-50 border-l-2 border-l-blue-500' : 'hover:bg-gray-50',
                          )}>
                          {/* Selection indicator */}
                          <div className={clsx(
                            'flex-shrink-0 w-4 h-4 rounded-full border flex items-center justify-center mt-0.5 text-xs font-bold',
                            isSelected
                              ? 'bg-blue-500 border-blue-500 text-white'
                              : 'border-gray-300 text-gray-300',
                          )}>
                            {isSelected ? selIdx + 1 : <Plus size={8} />}
                          </div>
                          <div className="min-w-0 flex-1">
                            <p className={clsx('font-mono truncate leading-tight text-xs',
                              isSelected ? 'text-blue-700 font-medium' : 'text-gray-700')}>
                              {f.task_id}
                            </p>
                            {f.row_count != null && (
                              <p className="text-gray-400 mt-0.5 flex items-center gap-1 text-xs">
                                <Rows size={9} />{f.row_count.toLocaleString()} rows
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

// ── Schema panel (right, collapsible) ─────────────────────────────────────────
function SchemaPanel({
  files, schemas, loadingFor,
}: {
  files: DuckDBFile[]
  schemas: Record<string, SchemaRow[]>
  loadingFor: Set<string>
}) {
  const [collapsed, setCollapsed] = useState<Record<string, boolean>>({})

  if (files.length === 0) return (
    <div className="flex flex-col items-center justify-center h-full gap-2 text-gray-300 px-4">
      <Columns size={24} strokeWidth={1.5} />
      <p className="text-xs text-center text-gray-400">Select a file to see its schema</p>
    </div>
  )

  return (
    <div className="h-full overflow-y-auto text-xs">
      {files.map((f, idx) => {
        const alias    = fileAlias(f)
        const cols     = schemas[f.s3_path] ?? []
        const loading  = loadingFor.has(f.s3_path)
        const isOpen   = !collapsed[f.s3_path]
        return (
          <div key={f.s3_path} className="border-b border-gray-100">
            <div
              onClick={() => setCollapsed(c => ({ ...c, [f.s3_path]: !c[f.s3_path] }))}
              className="flex items-center gap-2 px-3 py-2 cursor-pointer hover:bg-gray-50 select-none sticky top-0 bg-white z-10"
            >
              <div className="w-4 h-4 rounded-full bg-blue-500 flex items-center justify-center text-white font-bold text-xs flex-shrink-0">
                {idx + 1}
              </div>
              {isOpen
                ? <ChevronDown size={11} className="text-gray-400 flex-shrink-0" />
                : <ChevronRight size={11} className="text-gray-400 flex-shrink-0" />}
              <span className="font-mono font-semibold text-gray-700 truncate flex-1">{alias}</span>
              {loading && <Spinner size="sm" />}
              {!loading && <span className="text-gray-400 flex-shrink-0">{cols.length} cols</span>}
            </div>
            {isOpen && (
              <div className="pb-2">
                {cols.length === 0 && !loading && (
                  <p className="px-4 py-2 text-gray-400 italic">No schema loaded</p>
                )}
                {cols.map(col => (
                  <div key={col.column_name}
                    className="flex items-center gap-1.5 px-4 py-1 hover:bg-gray-50">
                    {typeIcon(col.column_type)}
                    <span className="font-mono text-gray-700 truncate flex-1">{col.column_name}</span>
                    <span className="text-gray-400 flex-shrink-0 text-xs">{col.column_type.split('(')[0]}</span>
                  </div>
                ))}
              </div>
            )}
          </div>
        )
      })}
    </div>
  )
}

// ── Results table ─────────────────────────────────────────────────────────────
function ResultsTable({ result }: { result: QueryResult }) {
  const [copied, setCopied] = useState(false)

  function copyTSV() {
    const rows = result.rows.map(r => r.map(c => c ?? '').join('\t')).join('\n')
    navigator.clipboard.writeText(`${result.columns.join('\t')}\n${rows}`)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  function downloadCSV() {
    const rows = result.rows.map(r =>
      r.map(c => c === null ? '' : String(c).includes(',') ? `"${c}"` : c).join(','),
    ).join('\n')
    const blob = new Blob([`${result.columns.join(',')}\n${rows}`], { type: 'text/csv' })
    const url  = URL.createObjectURL(blob)
    const a    = document.createElement('a')
    a.href = url; a.download = `query_${Date.now()}.csv`; a.click()
    URL.revokeObjectURL(url)
  }

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
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
          <button onClick={copyTSV}
            className="flex items-center gap-1 text-xs text-gray-600 hover:text-gray-800 bg-white hover:bg-gray-50 border border-gray-300 px-2.5 py-1.5 rounded-lg transition-colors shadow-sm">
            {copied ? <CheckCircle size={11} className="text-emerald-500" /> : <Copy size={11} />}
            {copied ? 'Copied!' : 'Copy TSV'}
          </button>
          <button onClick={downloadCSV}
            className="flex items-center gap-1 text-xs text-white bg-blue-600 hover:bg-blue-500 px-2.5 py-1.5 rounded-lg transition-colors shadow-sm">
            <Download size={11} /> Export CSV
          </button>
        </div>
      </div>
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
                  <th key={c} className="px-3 py-2 text-left text-gray-600 font-semibold border-b border-r border-gray-200 whitespace-nowrap bg-gray-50 font-mono">{c}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {result.rows.map((row, i) => (
                <tr key={i} className={clsx('border-b border-gray-100', i % 2 === 0 ? 'bg-white' : 'bg-gray-50/30')}>
                  <td className="px-3 py-2 text-gray-300 font-mono text-right border-r border-gray-100 select-none">{i + 1}</td>
                  {row.map((cell, j) => (
                    <td key={j} className="px-3 py-2 font-mono whitespace-nowrap max-w-xs truncate border-r border-gray-100"
                      title={cell === null ? 'NULL' : String(cell)}>
                      {cell === null
                        ? <span className="text-gray-300 italic text-xs">NULL</span>
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

// ── Documentation Overlay ────────────────────────────────────────────────────
function DocsOverlay({ onClose }: { onClose: () => void }) {
  const section = 'mb-5'
  const h2 = 'text-xs font-bold text-gray-700 uppercase tracking-wider mb-2 flex items-center gap-1.5'
  const code = 'block bg-gray-900 text-gray-100 rounded-lg px-3 py-2.5 text-xs font-mono leading-relaxed whitespace-pre overflow-x-auto mt-1.5'
  const p = 'text-xs text-gray-600 mb-1 leading-relaxed'

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-6 bg-black/50" onClick={onClose}>
      <div
        className="bg-white rounded-2xl shadow-2xl w-full max-w-2xl max-h-[85vh] flex flex-col"
        onClick={e => e.stopPropagation()}
      >
        <div className="flex items-center justify-between px-5 py-4 border-b border-gray-200 flex-shrink-0">
          <div className="flex items-center gap-2">
            <HelpCircle size={15} className="text-blue-600" />
            <h3 className="text-sm font-semibold text-gray-900">Query Editor Guide</h3>
          </div>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600 p-1 rounded hover:bg-gray-100">
            <X size={16} />
          </button>
        </div>

        <div className="flex-1 overflow-y-auto px-6 py-5">

          <div className={section}>
            <p className={h2}><Database size={12} /> Selecting Files</p>
            <p className={p}>Click any file in the left panel to select it. Files are grouped by pipeline → run → task.</p>
            <p className={p}>Select <strong>multiple files</strong> to enable cross-file JOINs. Selected files appear as numbered chips above the editor.</p>
            <p className={p}>Click a chip's × to deselect. The "Clear all" button resets the entire selection.</p>
          </div>

          <div className={section}>
            <p className={h2}><Database size={12} /> Single-file Query</p>
            <p className={p}>After selecting a file, the editor auto-populates with a simple SELECT. The table name is discovered via <code className="bg-gray-100 px-1 rounded">SHOW TABLES</code> and shown in the Schema panel.</p>
            <pre className={code}>{`SELECT * FROM inventory_valuation LIMIT 100

-- Filter
SELECT StockItemName, QuantityOnHand
FROM inventory_valuation
WHERE QuantityOnHand > 0
ORDER BY QuantityOnHand DESC
LIMIT 100`}</pre>
          </div>

          <div className={section}>
            <p className={h2}><Link2 size={12} /> Multi-file JOINs</p>
            <p className={p}>Each file is ATTACHed to an in-memory DuckDB using its <strong>task_id</strong> as the alias. Reference tables as <code className="bg-gray-100 px-1 rounded">alias.table_name</code>.</p>
            <p className={p}>The comment headers in the editor tell you the exact alias and full reference for each selected file.</p>
            <pre className={code}>{`-- [1] alias: extract_stock_holdings   table: extract_stock_holdings.raw_stock_holdings
-- [2] alias: transform_inventory_valuation   table: transform_inventory_valuation.inventory_valuation

SELECT
  iv.StockItemID,
  iv.StockItemName,
  h.QuantityOnHand,
  iv.StockCostValue
FROM extract_stock_holdings.raw_stock_holdings h
JOIN transform_inventory_valuation.inventory_valuation iv
  ON h.StockItemID = iv.StockItemID
WHERE h.QuantityOnHand > 0
ORDER BY iv.StockCostValue DESC
LIMIT 100`}</pre>
          </div>

          <div className={section}>
            <p className={h2}><Columns size={12} /> Schema Discovery</p>
            <p className={p}>Use the <strong>Schema</strong> button to toggle the schema panel. You can also run these directly in the editor:</p>
            <pre className={code}>{`-- List all tables in the selected file
SHOW TABLES

-- Column names and types
DESCRIBE inventory_valuation

-- Column statistics (min, max, avg, nulls, distinct)
SUMMARIZE SELECT * FROM inventory_valuation`}</pre>
          </div>

          <div className={section}>
            <p className={h2}><BarChart2 size={12} /> Useful Patterns</p>
            <pre className={code}>{`-- Row count
SELECT COUNT(*) AS total FROM table_name

-- Distinct values with frequency
SELECT column, COUNT(*) AS cnt
FROM table_name
GROUP BY column
ORDER BY cnt DESC
LIMIT 50

-- Null check
SELECT COUNT(*) FILTER (WHERE column IS NULL) AS nulls,
       COUNT(*) AS total
FROM table_name

-- Recent rows (if date column exists)
SELECT * FROM table_name
WHERE date_column >= '2025-01-01'
ORDER BY date_column DESC
LIMIT 100`}</pre>
          </div>

          <div className={section}>
            <p className={h2}><Download size={12} /> Export &amp; Shortcuts</p>
            <p className={p}>After running a query, use <strong>TSV</strong> or <strong>CSV</strong> buttons to download results.</p>
            <p className={p}><kbd className="bg-gray-100 border border-gray-300 rounded px-1.5 py-0.5 text-xs font-mono">Ctrl+Enter</kbd> — run the current query</p>
            <p className={p}>Row limit is configurable (100 – 10,000) in the toolbar dropdown.</p>
          </div>

        </div>
      </div>
    </div>
  )
}

// ── Main Query Editor ─────────────────────────────────────────────────────────
export default function QueryEditor() {
  const location = useLocation()

  const [selectedFiles,  setSelectedFiles]  = useState<DuckDBFile[]>([])
  const [fileSchemas,    setFileSchemas]     = useState<Record<string, SchemaRow[]>>({})
  const [schemaLoadingFor, setSchemaLoadingFor] = useState<Set<string>>(new Set())
  const [showSchema,     setShowSchema]     = useState(false)
  const [showDocs,       setShowDocs]       = useState(false)
  const [sql,            setSql]            = useState('-- Select files on the left, then write SQL\n-- Ctrl+Enter to run\n')
  const [result,         setResult]         = useState<QueryResult | null>(null)
  const [rowLimit,       setRowLimit]       = useState('500')

  // ── Auto-select from URL params (linked from RunDetail) ───────────────────
  useEffect(() => {
    const p = new URLSearchParams(location.search)
    const path = p.get('path')
    if (!path) return
    const file: DuckDBFile = {
      s3_path:     path,
      pipeline_id: p.get('pipeline') ?? '',
      run_id:      p.get('run')      ?? '',
      task_id:     p.get('task')     ?? '',
      table_name:  p.get('table')    ?? null,
    }
    setSelectedFiles([file])
    setSql('-- Discovering table…')
  }, [location.search])

  // ── Load schema for each newly selected file (SHOW TABLES → DESCRIBE) ─────
  useEffect(() => {
    selectedFiles.forEach(f => {
      if (f.s3_path in fileSchemas) return  // already loaded
      if (schemaLoadingFor.has(f.s3_path)) return  // already in flight

      setSchemaLoadingFor(s => new Set([...s, f.s3_path]))

      const headers = {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${localStorage.getItem('df_token') ?? ''}`,
      }
      const base = {
        pipeline_id: f.pipeline_id, run_id: f.run_id, task_id: f.task_id,
        duckdb_path: f.s3_path, limit: 200,
      }

      fetch('/api/query', { method: 'POST', headers, body: JSON.stringify({ ...base, sql: 'SHOW TABLES', limit: 10 }) })
        .then(r => r.json())
        .then(res => {
          const nameIdx = (res?.columns ?? []).indexOf('name')
          let tbl = f.table_name || 'result'
          if (nameIdx >= 0 && res.rows?.length > 0) tbl = res.rows[0][nameIdx]

          // Store discovered table name on the file object
          setSelectedFiles(prev =>
            prev.map(p => p.s3_path === f.s3_path ? { ...p, table_name: tbl } : p),
          )

          return fetch('/api/query', {
            method: 'POST', headers,
            body: JSON.stringify({ ...base, sql: `DESCRIBE ${tbl}`, limit: 200 }),
          })
        })
        .then(r => r.json())
        .then(res => {
          if (res?.columns && res?.rows) {
            const ci = res.columns.indexOf('column_name')
            const ti = res.columns.indexOf('column_type')
            if (ci >= 0 && ti >= 0) {
              setFileSchemas(prev => ({
                ...prev,
                [f.s3_path]: res.rows.map((r: any[]) => ({
                  column_name: r[ci], column_type: r[ti],
                })),
              }))
            }
          }
        })
        .catch(() => {})
        .finally(() => setSchemaLoadingFor(s => { const n = new Set(s); n.delete(f.s3_path); return n }))
    })
  }, [selectedFiles.map(f => f.s3_path).join(',')])

  // ── Auto-update SQL when selection changes ────────────────────────────────
  useEffect(() => {
    if (selectedFiles.length === 0) { setSql('-- Select files on the left, then write SQL\n-- Ctrl+Enter to run\n'); return }
    // Wait for at least the first file to have a table name
    const firstTable = selectedFiles[0]?.table_name
    if (!firstTable) return

    if (selectedFiles.length === 1) {
      setSql(`SELECT *\nFROM ${firstTable}\nLIMIT 100`)
    } else {
      const header = selectedFiles.map((f, i) =>
        `-- [${i + 1}] alias: ${fileAlias(f)}${f.table_name ? `  table: ${fileAlias(f)}.${f.table_name}` : ''}`
      ).join('\n')
      const alias1 = fileAlias(selectedFiles[0])
      const alias2 = fileAlias(selectedFiles[1])
      setSql(
        `${header}\n\nSELECT *\nFROM ${alias1}.${firstTable}\nJOIN ${alias2}.${selectedFiles[1].table_name ?? '???'} ON ???\nLIMIT 100`,
      )
    }
  }, [selectedFiles.map(f => f.s3_path + ':' + (f.table_name ?? '')).join(',')])

  // ── File toggle ───────────────────────────────────────────────────────────
  function toggleFile(f: DuckDBFile) {
    setSelectedFiles(prev => {
      const idx = prev.findIndex(p => p.s3_path === f.s3_path)
      if (idx >= 0) {
        const next = prev.filter((_, i) => i !== idx)
        // Remove schema for deselected file
        setFileSchemas(s => { const n = { ...s }; delete n[f.s3_path]; return n })
        return next
      }
      return [...prev, f]
    })
    setResult(null)
  }

  function removeFile(s3_path: string) {
    setSelectedFiles(prev => prev.filter(f => f.s3_path !== s3_path))
    setFileSchemas(prev => { const n = { ...prev }; delete n[s3_path]; return n })
    setResult(null)
  }

  function clearAll() {
    setSelectedFiles([])
    setFileSchemas({})
    setResult(null)
    setSql('-- Select files on the left, then write SQL\n-- Ctrl+Enter to run\n')
  }

  // ── Snippets ──────────────────────────────────────────────────────────────
  function applySnippet(fn: (t: string, multi: boolean) => string) {
    const primary = selectedFiles[0]
    const tbl     = primary?.table_name || 'result'
    setSql(fn(tbl, selectedFiles.length > 1))
  }

  // ── Query execution ───────────────────────────────────────────────────────
  const queryMut = useMutation({
    mutationFn: () => {
      const [primary, ...rest] = selectedFiles
      return queryDuckDB({
        pipeline_id: primary?.pipeline_id ?? '',
        run_id:      primary?.run_id      ?? '',
        task_id:     primary?.task_id     ?? '',
        duckdb_path: primary?.s3_path     ?? '',
        additional_paths: rest.length > 0
          ? rest.map(f => ({ path: f.s3_path, alias: fileAlias(f) }))
          : undefined,
        sql,
        limit: parseInt(rowLimit, 10),
      })
    },
    onSuccess: (res) => {
      setResult(res)
      toast.success(`${res.row_count.toLocaleString()} rows · ${res.duration_ms}ms`)
    },
    onError: (e: any) => {
      toast.error(e.response?.data?.detail ?? e.message ?? 'Query failed')
    },
  })

  const runQuery = useCallback(() => {
    if (!selectedFiles.length) { toast.error('Select at least one file'); return }
    queryMut.mutate()
  }, [selectedFiles, sql, rowLimit])

  const primaryTable = selectedFiles[0]?.table_name || 'result'

  return (
    <div className="flex flex-col h-full overflow-hidden bg-gray-50">
      {/* Top bar */}
      <div className="flex items-center justify-between px-5 py-3 border-b border-gray-200 bg-white flex-shrink-0 shadow-sm">
        <div className="flex items-center gap-3">
          <Database size={16} className="text-blue-600" />
          <div>
            <h1 className="text-sm font-bold text-gray-900">Query Editor</h1>
            <p className="text-xs text-gray-400">Ad-hoc SQL over DuckDB pipeline outputs · supports cross-file JOINs</p>
          </div>
        </div>
        {selectedFiles.length > 0 && (
          <button onClick={clearAll}
            className="text-xs text-gray-500 hover:text-red-600 flex items-center gap-1 transition-colors">
            <X size={11} /> Clear all
          </button>
        )}
      </div>

      {/* Body */}
      <div className="flex flex-1 overflow-hidden">
        {/* File browser */}
        <FileBrowser
          selectedPaths={selectedFiles.map(f => f.s3_path)}
          onToggle={toggleFile}
          onRefresh={() => setResult(null)}
        />

        {/* Center: editor + results */}
        <div className="flex-1 flex flex-col overflow-hidden min-w-0">
          {/* Selected files chips */}
          {selectedFiles.length > 0 && (
            <div className="flex items-center gap-2 px-4 py-2 bg-blue-50/60 border-b border-blue-100 flex-shrink-0 flex-wrap">
              <Link2 size={11} className="text-blue-500 flex-shrink-0" />
              <span className="text-xs text-blue-600 font-medium flex-shrink-0">
                {selectedFiles.length === 1 ? 'Querying:' : `Joining ${selectedFiles.length} tables:`}
              </span>
              {selectedFiles.map((f, i) => (
                <div key={f.s3_path}
                  className="flex items-center gap-1 bg-white border border-blue-200 rounded-full px-2.5 py-0.5 text-xs text-blue-700 shadow-sm">
                  <span className="w-4 h-4 rounded-full bg-blue-500 text-white flex items-center justify-center text-xs font-bold flex-shrink-0">
                    {i + 1}
                  </span>
                  <span className="font-mono">{f.table_name || f.task_id}</span>
                  {selectedFiles.length > 1 && (
                    <span className="text-blue-400 font-mono text-xs">({fileAlias(f)})</span>
                  )}
                  <button onClick={() => removeFile(f.s3_path)}
                    className="text-blue-300 hover:text-red-500 ml-0.5 transition-colors">
                    <X size={10} />
                  </button>
                </div>
              ))}
            </div>
          )}

          {/* Editor toolbar */}
          <div className="flex items-center justify-between px-4 py-2 border-b border-gray-200 bg-white flex-shrink-0">
            <div className="flex items-center gap-1.5 flex-wrap">
              <span className="text-xs text-gray-400 mr-1">Quick:</span>
              {SNIPPETS.map(s => (
                <button key={s.label} onClick={() => applySnippet(s.sql)}
                  disabled={!selectedFiles.length}
                  className="text-xs text-gray-600 hover:text-blue-700 hover:bg-blue-50 border border-gray-200 hover:border-blue-200 px-2 py-0.5 rounded transition-colors disabled:opacity-40 disabled:cursor-not-allowed">
                  {s.label}
                </button>
              ))}
            </div>
            <div className="flex items-center gap-2 flex-shrink-0">
              <div className="flex items-center gap-1.5">
                <span className="text-xs text-gray-400">Limit:</span>
                <select value={rowLimit} onChange={e => setRowLimit(e.target.value)}
                  className="bg-white border border-gray-300 rounded px-2 py-0.5 text-xs text-gray-700 focus:outline-none focus:ring-1 focus:ring-blue-500">
                  {['100', '500', '1000', '5000', '10000'].map(n => (
                    <option key={n} value={n}>{Number(n).toLocaleString()}</option>
                  ))}
                </select>
              </div>
              <button
                onClick={() => setShowSchema(s => !s)}
                className={clsx(
                  'flex items-center gap-1 text-xs px-2.5 py-1.5 rounded-lg border transition-colors',
                  showSchema
                    ? 'bg-blue-600 text-white border-blue-600'
                    : 'bg-white text-gray-600 border-gray-300 hover:bg-gray-50',
                )}
              >
                <Columns size={11} /> Schema
              </button>
              <button
                onClick={() => setShowDocs(true)}
                className="flex items-center gap-1 text-xs px-2.5 py-1.5 rounded-lg border border-gray-300 bg-white text-gray-600 hover:bg-gray-50 transition-colors"
                title="Query Editor Guide"
              >
                <HelpCircle size={11} /> Docs
              </button>
              <span className="text-xs text-gray-300 hidden xl:block">Ctrl+Enter</span>
              <button onClick={runQuery}
                disabled={!selectedFiles.length || queryMut.isPending}
                className="flex items-center gap-1.5 text-xs font-medium text-white bg-blue-600 hover:bg-blue-500 disabled:opacity-40 disabled:cursor-not-allowed px-3 py-1.5 rounded-lg transition-colors shadow-sm">
                {queryMut.isPending
                  ? <><Spinner size="sm" /> Running…</>
                  : <><Play size={12} /> Run Query</>
                }
              </button>
            </div>
          </div>

          {/* Monaco */}
          {!selectedFiles.length && (
            <div className="px-4 py-2 bg-amber-50 border-b border-amber-100 flex-shrink-0">
              <p className="text-xs text-amber-700 flex items-center gap-1.5">
                <XCircle size={11} /> Select a pipeline output from the panel on the left
              </p>
            </div>
          )}
          <div className="flex-shrink-0 border-b border-gray-200 bg-white">
            <Editor
              height="200px"
              language="sql"
              theme="vs-dark"
              value={sql}
              onChange={v => setSql(v ?? '')}
              options={{
                minimap: { enabled: false }, fontSize: 13, lineNumbers: 'on',
                scrollBeyondLastLine: false, wordWrap: 'on', tabSize: 2,
                automaticLayout: true, padding: { top: 10, bottom: 10 },
              }}
              onMount={(editor, monaco) => {
                editor.addCommand(
                  monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter,
                  () => { if (selectedFiles.length) queryMut.mutate() },
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
                    {selectedFiles.length
                      ? selectedFiles.length > 1
                        ? `${selectedFiles.length} tables selected — write a JOIN query`
                        : `Table: ${primaryTable}`
                      : 'No file selected'}
                  </p>
                  <p className="text-xs text-gray-300 mt-1">
                    {selectedFiles.length
                      ? 'Click "Run Query" or press Ctrl+Enter'
                      : 'Pick a pipeline output from the left panel'}
                  </p>
                </div>
              </div>
            )}
            {!queryMut.isPending && result && <ResultsTable result={result} />}
          </div>
        </div>

        {/* Schema panel (right, collapsible) */}
        {showSchema && (
          <div className="w-72 flex-shrink-0 border-l border-gray-200 bg-white flex flex-col overflow-hidden">
            <div className="px-3 py-2.5 border-b border-gray-100 flex items-center justify-between bg-gray-50">
              <div className="flex items-center gap-1.5">
                <Columns size={12} className="text-blue-500" />
                <span className="text-xs font-semibold text-gray-700">Schema</span>
                {selectedFiles.length > 0 && (
                  <span className="text-xs text-gray-400">· {selectedFiles.length} table{selectedFiles.length > 1 ? 's' : ''}</span>
                )}
              </div>
              <button onClick={() => setShowSchema(false)}
                className="text-gray-400 hover:text-gray-600 p-1 rounded hover:bg-gray-100 transition-colors">
                <X size={12} />
              </button>
            </div>
            <SchemaPanel
              files={selectedFiles}
              schemas={fileSchemas}
              loadingFor={schemaLoadingFor}
            />
          </div>
        )}
      </div>
    </div>

    {showDocs && <DocsOverlay onClose={() => setShowDocs(false)} />}
  )
}

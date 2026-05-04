import React, { useState } from 'react'
import {
  ArrowRight, CheckCircle2, Shield, GitBranch,
  ChevronRight, ChevronDown, Package,
  AlertTriangle, RefreshCw, Server, Database, Trash2,
  Key, Lock, GitMerge, Play, Upload, Info,
  CheckCircle, Clock, Zap,
} from 'lucide-react'
import clsx from 'clsx'
import { PageHeader, Tabs } from '../components/ui'

// ── Shared primitives ─────────────────────────────────────────────────────────

function EnvBadge({ env }: { env: 'dev' | 'staging' | 'production' }) {
  return (
    <span className={clsx(
      'inline-flex items-center px-2 py-0.5 rounded text-xs font-medium font-mono',
      env === 'dev'        && 'bg-blue-50 text-blue-700',
      env === 'staging'    && 'bg-amber-50 text-amber-700',
      env === 'production' && 'bg-emerald-50 text-emerald-700',
    )}>{env}</span>
  )
}

function Mono({ children }: { children: React.ReactNode }) {
  return (
    <code className="font-mono text-xs bg-blue-50 border border-blue-200 rounded px-1.5 py-0.5 text-blue-700">
      {children}
    </code>
  )
}

function CodeBlock({ children }: { children: string }) {
  return (
    <pre className="bg-gray-950 border border-gray-800 rounded-lg p-4 text-xs font-mono text-gray-300 overflow-x-auto whitespace-pre leading-relaxed">
      {children}
    </pre>
  )
}

function Pill({
  children,
  color = 'gray',
}: { children: React.ReactNode; color?: 'gray' | 'blue' | 'amber' | 'emerald' | 'purple' | 'sky' | 'rose' }) {
  return (
    <span className={clsx(
      'inline-flex items-center px-2 py-0.5 rounded text-xs font-medium',
      color === 'gray'    && 'bg-gray-100 text-gray-600',
      color === 'blue'    && 'bg-blue-50 text-blue-700',
      color === 'amber'   && 'bg-amber-50 text-amber-700',
      color === 'emerald' && 'bg-emerald-50 text-emerald-700',
      color === 'purple'  && 'bg-purple-50 text-purple-700',
      color === 'sky'     && 'bg-sky-50 text-sky-700',
      color === 'rose'    && 'bg-rose-50 text-rose-700',
    )}>{children}</span>
  )
}

function SectionTitle({ children, className }: { children: React.ReactNode; className?: string }) {
  return <h3 className={clsx('text-sm font-semibold text-gray-900 mb-3', className)}>{children}</h3>
}

function CardSubTitle({ children }: { children: React.ReactNode }) {
  return (
    <h4 className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-2 mt-4 first:mt-0">
      {children}
    </h4>
  )
}

function Prose({ children }: { children: React.ReactNode }) {
  return <p className="text-xs text-gray-600 leading-relaxed">{children}</p>
}

function Divider() { return <div className="border-t border-gray-200 my-5" /> }

function FlowStep({
  n, title, desc, env, color = 'bg-gray-500', last,
}: {
  n: number | string; title: string; desc: React.ReactNode
  env?: 'dev' | 'staging' | 'production'; color?: string; last?: boolean
}) {
  return (
    <div className="flex gap-3">
      <div className="flex flex-col items-center flex-shrink-0">
        <div className={clsx('w-6 h-6 rounded-full flex items-center justify-center text-xs font-bold text-white', color)}>
          {n}
        </div>
        {!last && <div className="w-px flex-1 bg-gray-200 mt-1 mb-1 min-h-[14px]" />}
      </div>
      <div className={clsx('pb-4', last && 'pb-0')}>
        <div className="flex items-center gap-2 flex-wrap">
          <span className="text-sm font-medium text-gray-900">{title}</span>
          {env && <EnvBadge env={env} />}
        </div>
        <div className="text-xs text-gray-600 mt-0.5 leading-relaxed">{desc}</div>
      </div>
    </div>
  )
}

function FlagCell({ v }: { v: boolean }) {
  return v
    ? <span className="text-emerald-600 font-semibold">true</span>
    : <span className="text-gray-400">false</span>
}

function RunCell({ runs }: { runs: boolean }) {
  return runs
    ? <span className="inline-flex items-center gap-1 text-emerald-600 text-xs"><CheckCircle2 size={11} />runs</span>
    : <span className="text-gray-400 text-xs">skipped</span>
}

// ── GitHub Actions-specific primitives ────────────────────────────────────────

function TriggerBadge({ label, auto }: { label: string; auto?: boolean }) {
  return (
    <span className={clsx(
      'inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium border',
      auto
        ? 'bg-emerald-50 text-emerald-700 border-emerald-200'
        : 'bg-violet-50 text-violet-700 border-violet-200',
    )}>
      {auto ? <Zap size={10} /> : <Play size={10} />}
      {label}
    </span>
  )
}

function InputRow({ name, required, options, desc }: {
  name: string; required: boolean; options?: string[]; desc: string
}) {
  return (
    <tr className="border-b border-gray-100">
      <td className="py-2 pr-3 align-top">
        <code className="text-xs text-blue-700 bg-blue-50 px-1.5 py-0.5 rounded">{name}</code>
        {required && <span className="ml-1 text-red-500 text-xs">*</span>}
      </td>
      <td className="py-2 pr-3 align-top text-xs text-gray-700">{desc}</td>
      <td className="py-2 align-top text-xs text-gray-500">{options ? options.join(' · ') : '—'}</td>
    </tr>
  )
}

function JobChain({ jobs }: { jobs: { id: string; label: string; note?: string; gate?: boolean; skip?: string }[] }) {
  return (
    <div className="flex flex-wrap items-center gap-1.5">
      {jobs.map((j, i) => (
        <React.Fragment key={j.id}>
          {i > 0 && <ArrowRight size={12} className="text-gray-400 flex-shrink-0" />}
          <div className="flex items-center gap-1">
            <div className={clsx(
              'px-2 py-1 rounded text-xs border',
              j.gate
                ? 'bg-amber-50 border-amber-200 text-amber-700'
                : 'bg-gray-100 border-gray-300 text-gray-700',
            )}>
              {j.gate && <Lock size={9} className="inline mr-1 text-amber-600" />}
              {j.label}
            </div>
            {j.note && <span className="text-xs text-gray-400 italic hidden xl:inline">{j.note}</span>}
            {j.skip && <span className="text-xs text-gray-400">({j.skip})</span>}
          </div>
        </React.Fragment>
      ))}
    </div>
  )
}

function NoteBox({ type, children }: { type: 'info' | 'warn'; children: React.ReactNode }) {
  return (
    <div className={clsx(
      'flex gap-2 rounded-lg px-3 py-2.5 text-xs border mt-3',
      type === 'warn'
        ? 'bg-amber-50 border-amber-200 text-amber-800'
        : 'bg-blue-50 border-blue-200 text-blue-800',
    )}>
      {type === 'warn'
        ? <AlertTriangle size={13} className="text-amber-600 flex-shrink-0 mt-0.5" />
        : <Info size={13} className="text-blue-600 flex-shrink-0 mt-0.5" />}
      <span>{children}</span>
    </div>
  )
}

function WorkflowCard({
  file, name, icon: Icon, color, trigger, auto, description, prerequisite,
  inputs, jobsNode, notes,
}: {
  file: string; name: string; icon: React.ElementType; color: string
  trigger: string; auto?: boolean; description: string; prerequisite?: string
  inputs?: React.ReactNode; jobsNode: React.ReactNode; notes?: React.ReactNode
}) {
  const [open, setOpen] = useState(false)
  return (
    <div className="bg-white border border-gray-200 rounded-xl overflow-hidden shadow-sm">
      <button
        onClick={() => setOpen(v => !v)}
        className="w-full flex items-center gap-3 px-5 py-4 text-left hover:bg-gray-50 transition-colors"
      >
        <div className={clsx('w-9 h-9 rounded-lg flex items-center justify-center flex-shrink-0', color)}>
          <Icon size={16} className="text-white" />
        </div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <span className="text-sm font-semibold text-gray-900">{name}</span>
            <code className="text-xs text-gray-500 bg-gray-100 px-1.5 py-0.5 rounded">{file}</code>
            <TriggerBadge label={trigger} auto={auto} />
          </div>
          <p className="text-xs text-gray-500 mt-0.5 line-clamp-1">{description}</p>
        </div>
        {open
          ? <ChevronDown size={15} className="text-gray-400 flex-shrink-0" />
          : <ChevronRight size={15} className="text-gray-400 flex-shrink-0" />}
      </button>

      {open && (
        <div className="px-5 pb-5 border-t border-gray-200">
          <p className="text-sm text-gray-700 mt-4 leading-relaxed">{description}</p>
          {prerequisite && (
            <NoteBox type="info"><strong>Prerequisite:</strong> {prerequisite}</NoteBox>
          )}
          {inputs && (
            <>
              <CardSubTitle>Inputs</CardSubTitle>
              <div className="rounded-lg overflow-hidden border border-gray-200">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="bg-gray-50 border-b border-gray-200">
                      <th className="text-left text-xs text-gray-500 px-3 py-2 font-medium w-36">Parameter</th>
                      <th className="text-left text-xs text-gray-500 px-3 py-2 font-medium">Description</th>
                      <th className="text-left text-xs text-gray-500 px-3 py-2 font-medium w-48">Options</th>
                    </tr>
                  </thead>
                  <tbody>{inputs}</tbody>
                </table>
              </div>
            </>
          )}
          <CardSubTitle>Job Flow</CardSubTitle>
          {jobsNode}
          {notes && <div className="mt-1">{notes}</div>}
        </div>
      )}
    </div>
  )
}

// ── Tab: Versioning ───────────────────────────────────────────────────────────

const PERMUTATIONS = [
  { id: 'apps',   label: 'Apps only',          paths: 'backend/, frontend/, or eks/', apps: true,  dags: false, configs: false, imageTag: 'sha-{sha}', border: 'border-blue-200',   accent: 'text-blue-700',   bg: 'bg-blue-50',   note: 'Full EKS build + deploy + smoke test. Most time-consuming (~8–12 min). Image tag recorded in manifest.' },
  { id: 'dags',   label: 'DAGs only',           paths: 'airflow/',                    apps: false, dags: true,  configs: false, imageTag: 'null',      border: 'border-purple-200', accent: 'text-purple-700', bg: 'bg-purple-50', note: 'Fastest path (~2 min). No Docker build. DAGs uploaded to MWAA S3 bucket. MWAA auto-picks up changes.' },
  { id: 'configs',label: 'Configs only',        paths: 'configs/',                    apps: false, dags: false, configs: true,  imageTag: 'null',      border: 'border-amber-200',  accent: 'text-amber-700',  bg: 'bg-amber-50',  note: 'Fast path (~2 min). JSON pipeline configs uploaded to S3. Pipelines registered/updated via API.' },
  { id: 'ad',     label: 'Apps + DAGs',          paths: 'backend/ + airflow/',         apps: true,  dags: true,  configs: false, imageTag: 'sha-{sha}', border: 'border-sky-200',    accent: 'text-sky-700',    bg: 'bg-sky-50',    note: 'Build and sync-dags run in parallel. Total time driven by the build (~8–12 min).' },
  { id: 'ac',     label: 'Apps + Configs',       paths: 'backend/ + configs/',         apps: true,  dags: false, configs: true,  imageTag: 'sha-{sha}', border: 'border-sky-200',    accent: 'text-sky-700',    bg: 'bg-sky-50',    note: 'Build and sync-configs run in parallel. Config upload completes before EKS rollout finishes.' },
  { id: 'dc',     label: 'DAGs + Configs',       paths: 'airflow/ + configs/',         apps: false, dags: true,  configs: true,  imageTag: 'null',      border: 'border-teal-200',   accent: 'text-teal-700',   bg: 'bg-teal-50',   note: 'No Docker build. Both syncs run in parallel (~2 min). No EKS rollout. Safe to run frequently.' },
  { id: 'all',    label: 'All three areas',      paths: 'backend/ + airflow/ + configs/', apps: true, dags: true, configs: true, imageTag: 'sha-{sha}', border: 'border-emerald-200',accent: 'text-emerald-700',bg: 'bg-emerald-50',note: 'Complete deployment. All jobs run. Time driven by the Docker build. Every artifact updated.' },
  { id: 'manual', label: 'Manual dispatch',      paths: '(any — all flags forced true)',  apps: true, dags: true, configs: true, imageTag: 'sha-{sha}', border: 'border-rose-200',   accent: 'text-rose-700',   bg: 'bg-rose-50',   note: 'workflow_dispatch always sets all three flags to true regardless of what actually changed.' },
]

function VersioningTab() {
  const [expanded, setExpanded] = useState<string | null>(null)
  return (
    <div className="space-y-6">
      <div className="bg-white border border-gray-200 rounded-xl p-5 shadow-sm">
        <SectionTitle>What Is a Version?</SectionTitle>
        <Prose>Every successful deployment to dev creates an immutable record in AWS SSM Parameter Store called
          a <strong className="text-gray-800">version manifest</strong>. A version captures: the exact git SHA deployed,
          which artifact categories changed (EKS apps, MWAA DAGs, pipeline configs), the Docker image tag if apps were
          built, and full audit metadata. Versions are the atomic unit of promotion — you always promote a named, tracked
          version, never a branch or a loose image tag.</Prose>
      </div>

      <div className="bg-white border border-gray-200 rounded-xl p-5 shadow-sm">
        <SectionTitle>Version ID Format</SectionTitle>
        <div className="inline-flex items-center gap-0.5 font-mono text-base bg-gray-50 border border-gray-200 rounded-lg px-4 py-2.5 mb-3">
          <span className="text-gray-400">v</span>
          <span className="text-blue-600">20260504</span>
          <span className="text-gray-400">-</span>
          <span className="text-amber-600">143021</span>
          <span className="text-gray-400">-</span>
          <span className="text-emerald-600">b9138fb</span>
        </div>
        <div className="grid grid-cols-3 gap-3 text-xs">
          {[
            { color: 'blue',    label: 'YYYYMMDD', desc: 'UTC date the record-version job completed' },
            { color: 'amber',   label: 'HHMMSS',   desc: 'UTC time — makes two same-day deploys unique' },
            { color: 'emerald', label: 'sha7',      desc: 'First 7 chars of the deployed git commit SHA' },
          ].map(({ color, label, desc }) => (
            <div key={label} className={clsx('rounded-lg p-3 border',
              color === 'blue'    && 'bg-blue-50 border-blue-200',
              color === 'amber'   && 'bg-amber-50 border-amber-200',
              color === 'emerald' && 'bg-emerald-50 border-emerald-200',
            )}>
              <div className={clsx('font-semibold mb-1',
                color === 'blue'    && 'text-blue-700',
                color === 'amber'   && 'text-amber-700',
                color === 'emerald' && 'text-emerald-700',
              )}>{label}</div>
              <div className="text-gray-600">{desc}</div>
            </div>
          ))}
        </div>
      </div>

      <div className="bg-white border border-gray-200 rounded-xl p-5 shadow-sm">
        <SectionTitle>How Changes Are Detected</SectionTitle>
        <Prose>The <Mono>guard</Mono> job diffs <Mono>HEAD^</Mono> vs <Mono>HEAD</Mono> using
          <Mono>git diff --name-only</Mono> and sets three boolean flags that drive every downstream job's
          <Mono>if:</Mono> condition. Manual dispatch always forces all three to true.</Prose>
        <div className="mt-3 overflow-x-auto">
          <table className="w-full text-xs border-collapse">
            <thead>
              <tr className="border-b border-gray-200">
                <th className="text-left text-gray-500 font-medium py-2 pr-4">Flag</th>
                <th className="text-left text-gray-500 font-medium py-2 pr-4">Triggered when changed paths match</th>
                <th className="text-left text-gray-500 font-medium py-2">Jobs gated on this flag</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {[
                { flag: 'deploy_apps',  pattern: '^(backend|frontend|eks/)', jobs: 'build (×3), build-meta, deploy-apps, smoke-test' },
                { flag: 'sync_dags',    pattern: '^airflow/',                jobs: 'sync-dags' },
                { flag: 'sync_configs', pattern: '^configs/',                jobs: 'sync-configs' },
              ].map(({ flag, pattern, jobs }) => (
                <tr key={flag}>
                  <td className="py-2 pr-4"><code className="text-blue-700 bg-blue-50 px-1.5 py-0.5 rounded text-xs">{flag}</code></td>
                  <td className="py-2 pr-4 font-mono text-gray-500 text-xs">{pattern}</td>
                  <td className="py-2 text-gray-600 text-xs">{jobs}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <div className="bg-white border border-gray-200 rounded-xl p-5 shadow-sm">
        <SectionTitle>Version Manifest Schema</SectionTitle>
        <CodeBlock>{`{
  "version_id":  "v20260504-143021-b9138fb",           // unique identifier
  "sha":         "b9138fbfee2c4d5e6f7a8b9c0d1e2f3a",   // full git commit SHA
  "created_at":  "2026-05-04T14:30:21Z",
  "created_by":  "krushna-samanta-s2k2sys",
  "run_id":      "9876543210",
  "run_url":     "https://github.com/.../actions/runs/9876543210",
  "image_tag":   "sha-b9138fbfee2c4d5e6f7a8b9c0d1e2f3a",  // null when apps=false
  "artifacts": {
    "apps":    true,   // EKS images built + deployed + smoke-tested
    "dags":    false,  // airflow/ files synced to MWAA S3 bucket
    "configs": true    // configs/ files uploaded to pipeline-configs S3
  }
}`}</CodeBlock>
      </div>

      <div className="bg-white border border-gray-200 rounded-xl p-5 shadow-sm">
        <SectionTitle>SSM Parameter Layout</SectionTitle>
        <CodeBlock>{`/nextgen-databridge/
├── versions/                                  ← IMMUTABLE — written once, never changed
│   ├── v20260504-143021-b9138fb               ← full manifest JSON
│   ├── v20260503-091500-a1b2c3d
│   └── v20260501-080000-deadbeef
│
├── deploy/                                    ← MUTABLE — updated on each deployment
│   ├── dev/
│   │   ├── latest-version  = "v20260504-143021-b9138fb"
│   │   └── last-deployment = { deployed_at, deployed_by, run_id, ... }
│   ├── staging/
│   │   ├── latest-version  = "v20260503-091500-a1b2c3d"
│   │   └── last-deployment = { ... }
│   └── production/
│       ├── latest-version  = "v20260501-080000-deadbeef"
│       └── last-deployment = { ... }
│
└── image-tag/                                 ← BACKWARDS COMPAT
    ├── dev         = "sha-b9138fbfee..."
    ├── staging     = "sha-a1b2c3d..."
    └── production  = "sha-deadbeef..."`}</CodeBlock>
      </div>

      <Divider />

      <SectionTitle>All 8 Change Permutations</SectionTitle>
      <Prose>The combination of changed paths determines which artifact flags are set, which jobs run, and what
        the version manifest records. Click any row to see the exact manifest and job breakdown.</Prose>

      <div className="bg-white border border-gray-200 rounded-xl overflow-hidden shadow-sm">
        <table className="w-full text-xs">
          <thead>
            <tr className="bg-gray-50 border-b border-gray-200">
              <th className="text-left text-gray-500 font-semibold px-4 py-3">Permutation</th>
              <th className="text-center text-gray-500 font-semibold px-3 py-3">apps</th>
              <th className="text-center text-gray-500 font-semibold px-3 py-3">dags</th>
              <th className="text-center text-gray-500 font-semibold px-3 py-3">configs</th>
              <th className="text-left text-gray-500 font-semibold px-3 py-3">image_tag</th>
              <th className="text-left text-gray-500 font-semibold px-3 py-3">~Duration</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {PERMUTATIONS.map(p => (
              <tr key={p.id} className="hover:bg-gray-50 cursor-pointer transition-colors"
                onClick={() => setExpanded(expanded === p.id ? null : p.id)}>
                <td className="py-3 px-4">
                  <div className="flex items-center gap-2">
                    <ChevronRight size={12} className={clsx('text-gray-400 transition-transform', expanded === p.id && 'rotate-90')} />
                    <span className={clsx('font-medium', p.accent)}>{p.label}</span>
                  </div>
                  <div className="text-gray-400 font-mono pl-5 mt-0.5 text-xs">{p.paths}</div>
                </td>
                <td className="py-3 px-3 text-center"><FlagCell v={p.apps} /></td>
                <td className="py-3 px-3 text-center"><FlagCell v={p.dags} /></td>
                <td className="py-3 px-3 text-center"><FlagCell v={p.configs} /></td>
                <td className="py-3 px-3 font-mono text-gray-600">{p.imageTag}</td>
                <td className="py-3 px-3 text-gray-500">{p.apps ? '8–12 min' : '2–3 min'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {PERMUTATIONS.map(p => expanded !== p.id ? null : (
        <div key={p.id} className={clsx('bg-white border-2 rounded-xl p-5 shadow-sm', p.border)}>
          <div className="flex items-center gap-2 mb-3">
            <span className={clsx('text-sm font-semibold', p.accent)}>{p.label}</span>
            <Pill>deploy-dev.yml</Pill>
          </div>
          <div className="mb-3 text-xs text-gray-600 leading-relaxed">{p.note}</div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
            <div>
              <div className="text-xs font-semibold text-gray-700 mb-2">Jobs that run in deploy-dev.yml</div>
              <div className="space-y-1 text-xs">
                {[
                  { name: 'guard',              runs: true },
                  { name: 'validate-infra',     runs: true },
                  { name: 'build (×3 matrix)',  runs: p.apps },
                  { name: 'build-meta',         runs: p.apps },
                  { name: 'sync-dags',          runs: p.dags },
                  { name: 'sync-configs',       runs: p.configs },
                  { name: 'deploy-apps',        runs: p.apps },
                  { name: 'smoke-test',         runs: p.apps },
                  { name: 'register-pipelines', runs: p.apps || p.configs },
                  { name: 'record-version',     runs: true },
                ].map(({ name, runs }) => (
                  <div key={name} className="flex items-center gap-2">
                    <RunCell runs={runs} />
                    <span className={clsx('font-mono', runs ? 'text-gray-700' : 'text-gray-300')}>{name}</span>
                  </div>
                ))}
              </div>
            </div>
            <div>
              <div className="text-xs font-semibold text-gray-700 mb-2">What promote.yml does for this version</div>
              <div className="space-y-1 text-xs">
                {[
                  { name: 'copy-images (ECR)',         runs: p.apps },
                  { name: 'validate-infra',             runs: true },
                  { name: 'deploy → sync DAGs',         runs: p.dags },
                  { name: 'deploy → upload configs',    runs: p.configs },
                  { name: 'deploy → kubectl apply',     runs: p.apps },
                  { name: 'deploy → wait for rollout',  runs: p.apps },
                  { name: 'smoke-test',                 runs: p.apps },
                  { name: 'record-deployment',          runs: true },
                ].map(({ name, runs }) => (
                  <div key={name} className="flex items-center gap-2">
                    <RunCell runs={runs} />
                    <span className={clsx('font-mono', runs ? 'text-gray-700' : 'text-gray-300')}>{name}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>
          <div className="text-xs font-semibold text-gray-700 mb-2">Exact version manifest written to SSM</div>
          <CodeBlock>{`{
  "version_id":  "v20260504-143021-b9138fb",
  "sha":         "b9138fbfee2c4d5e6f7a8b9c0d1e2f3a",
  "image_tag":   ${p.apps ? '"sha-b9138fbfee2c4d5e6f7a8b9c0d1e2f3a"' : 'null'},
  "artifacts": {
    "apps":    ${p.apps},${!p.apps ? '   // copy-images and kubectl apply SKIPPED in promote.yml' : '   // images: api, ui, transform in ECR'}
    "dags":    ${p.dags},${!p.dags ? '   // sync-dags step SKIPPED in promote.yml' : '   // airflow/dags/ synced to MWAA S3 bucket'}
    "configs": ${p.configs}${!p.configs ? '   // config upload SKIPPED in promote.yml' : '   // configs/ uploaded to pipeline-configs S3'}
  }
}`}</CodeBlock>
        </div>
      ))}
    </div>
  )
}

// ── Tab: Promotion Flow ───────────────────────────────────────────────────────

function PromotionTab() {
  return (
    <div className="space-y-6">
      <div className="bg-white border border-gray-200 rounded-xl p-5 shadow-sm">
        <SectionTitle>The Promotion Model</SectionTitle>
        <Prose>Code moves from dev to production through <strong className="text-gray-800">two explicit, manual
          promotions</strong>. Each promotion targets a specific version manifest stored in SSM — never a branch,
          a PR, or an image tag directly. No infrastructure is provisioned during promotions.</Prose>
        <div className="mt-4 flex flex-wrap items-center gap-2 text-xs">
          {[
            { label: 'PR merge / manual',             bg: 'bg-gray-100 border border-gray-300 text-gray-700' },
            null,
            { label: 'deploy-dev.yml',                bg: 'bg-blue-600 text-white' },
            null,
            { label: 'dev',                           bg: 'bg-blue-100 border border-blue-300 text-blue-900' },
            null,
            { label: 'promote.yml',                   bg: 'bg-gray-100 border border-gray-300 text-gray-700' },
            null,
            { label: 'staging ✓ approval',            bg: 'bg-amber-100 border border-amber-300 text-amber-900' },
            null,
            { label: 'promote.yml',                   bg: 'bg-gray-100 border border-gray-300 text-gray-700' },
            null,
            { label: 'production ✓ chain + approval', bg: 'bg-emerald-100 border border-emerald-300 text-emerald-900' },
          ].map((item, i) =>
            item === null
              ? <ArrowRight key={i} size={13} className="text-gray-400 flex-shrink-0" />
              : <div key={i} className={clsx('px-3 py-1.5 rounded-lg font-medium', item.bg)}>{item.label}</div>
          )}
        </div>
        <div className="mt-3 grid grid-cols-1 md:grid-cols-3 gap-3 text-xs">
          <div className="bg-blue-50 border border-blue-200 rounded-lg p-3">
            <div className="font-semibold text-blue-700 mb-1">Dev deployment</div>
            <div className="text-gray-600">Automatic on PR merge to main, or manual dispatch. Creates a version manifest. No approval required.</div>
          </div>
          <div className="bg-amber-50 border border-amber-200 rounded-lg p-3">
            <div className="font-semibold text-amber-700 mb-1">Dev → Staging</div>
            <div className="text-gray-600">Manual trigger only. No chain validation. Requires approval via GitHub Environment "staging".</div>
          </div>
          <div className="bg-emerald-50 border border-emerald-200 rounded-lg p-3">
            <div className="font-semibold text-emerald-700 mb-1">Staging → Production</div>
            <div className="text-gray-600">Manual trigger only. Chain validation enforced. Requires approval via GitHub Environment "production".</div>
          </div>
        </div>
      </div>

      <div className="bg-white border border-gray-200 rounded-xl p-5 shadow-sm">
        <SectionTitle>Step 1 — Resolve: Version Manifest + Chain Validation</SectionTitle>
        <div className="space-y-0">
          <FlowStep n="a" title="Resolve version_id" color="bg-gray-400"
            desc={<>If you specified a <Mono>version_id</Mono> input it's used directly. Otherwise reads <Mono>/nextgen-databridge/deploy/{'{source_env}'}/latest-version</Mono> from SSM. Fails if no version deployed to source environment yet.</>} />
          <FlowStep n="b" title="Chain validation (production only)" color="bg-gray-400"
            desc={<>Reads <Mono>/nextgen-databridge/deploy/staging/latest-version</Mono>. Must equal the version_id being promoted. If staging is on any different version the job fails with a descriptive error — before any images are copied or approval is sought.</>} />
          <FlowStep n="c" title="Load manifest" color="bg-gray-400" last
            desc={<>Reads the immutable manifest from <Mono>/nextgen-databridge/versions/{'{version_id}'}</Mono>. Extracts <Mono>sha</Mono>, <Mono>image_tag</Mono>, and the three artifact flags. All become job outputs via <Mono>needs.resolve.outputs.*</Mono>.</>} />
        </div>
      </div>

      <div className="bg-white border border-gray-200 rounded-xl p-5 shadow-sm">
        <SectionTitle>Step 2 — Copy Images + Validate Infra (Parallel)</SectionTitle>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div>
            <div className="text-xs font-semibold text-gray-800 mb-2 flex items-center gap-2">
              <Package size={13} className="text-rose-500" /> copy-images
              <Pill>only if apps=true</Pill>
            </div>
            <Prose>Assumes the dev IAM role, pulls SHA-tagged images for api, ui, and transform from dev ECR.
              Then assumes the target env IAM role and pushes with two tags: the immutable <Mono>sha-{'{sha}'}</Mono>
              and the mutable <Mono>{'{env}'}</Mono> alias. Images are <strong className="text-gray-800">never
              rebuilt</strong> — exact bytes from the dev smoke test are promoted.</Prose>
          </div>
          <div>
            <div className="text-xs font-semibold text-gray-800 mb-2 flex items-center gap-2">
              <CheckCircle2 size={13} className="text-emerald-600" /> validate-infra
              <Pill>always runs</Pill>
            </div>
            <Prose>Assumes the target env IAM role. Confirms EKS cluster is ACTIVE and the
              <Mono>nextgen-databridge/connections/audit_db</Mono> secret exists. Fails fast before the
              approval gate — no point spending reviewer time on a broken environment.</Prose>
          </div>
        </div>
      </div>

      <div className="bg-white border border-amber-200 rounded-xl p-5 shadow-sm">
        <SectionTitle>Step 3 — GitHub Environment Approval Gate</SectionTitle>
        <Prose>The <Mono>deploy</Mono> job is annotated with <Mono>environment: {'{env}'}</Mono>. GitHub pauses
          the workflow and notifies all Required Reviewers. After approval the deploy job proceeds. After rejection
          or timeout the run is cancelled and SSM is not updated — the environment is untouched.</Prose>
        <div className="mt-3 grid grid-cols-1 md:grid-cols-2 gap-3 text-xs">
          <div className="bg-amber-50 border border-amber-200 rounded-lg p-3">
            <div className="font-semibold text-amber-700 mb-1">Staging environment</div>
            <div className="text-gray-600">Configure Required Reviewers in GitHub Settings → Environments → staging.</div>
          </div>
          <div className="bg-emerald-50 border border-emerald-200 rounded-lg p-3">
            <div className="font-semibold text-emerald-700 mb-1">Production environment</div>
            <div className="text-gray-600">Configure more reviewers and a branch restriction to <Mono>main</Mono>. Chain validation already ran before this gate.</div>
          </div>
        </div>
      </div>

      <div className="bg-white border border-gray-200 rounded-xl p-5 shadow-sm">
        <SectionTitle>Step 4 — Selective Deployment (Checkout Exact SHA)</SectionTitle>
        <Prose>The deploy job checks out the <strong className="text-gray-800">exact git SHA from the version
          manifest</strong> — not main HEAD. Each deployment step is individually conditional on the artifact flags.</Prose>
        <div className="mt-4 overflow-x-auto">
          <table className="w-full text-xs border-collapse">
            <thead>
              <tr className="bg-gray-50 border-b border-gray-200">
                <th className="text-left text-gray-500 font-semibold px-3 py-2">Deploy step</th>
                <th className="text-center text-gray-500 font-semibold px-2 py-2">apps=T<br/>dags=F cfg=F</th>
                <th className="text-center text-gray-500 font-semibold px-2 py-2">apps=F<br/>dags=T cfg=F</th>
                <th className="text-center text-gray-500 font-semibold px-2 py-2">apps=F<br/>dags=F cfg=T</th>
                <th className="text-center text-gray-500 font-semibold px-2 py-2">apps=T<br/>dags=T cfg=T</th>
                <th className="text-center text-gray-500 font-semibold px-2 py-2">apps=F<br/>dags=T cfg=T</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100 text-center">
              {[
                { step: 'sync DAGs to MWAA',         r: [false, true,  false, true,  true ] },
                { step: 'upload pipeline configs',    r: [false, false, true,  true,  true ] },
                { step: 'apply kubectl manifests',    r: [true,  false, false, true,  false] },
                { step: 'wait for rollout',           r: [true,  false, false, true,  false] },
                { step: 'rollback on failure',        r: [true,  false, false, true,  false] },
              ].map(({ step, r }) => (
                <tr key={step}>
                  <td className="text-left py-2 px-3 font-mono text-gray-600">{step}</td>
                  {r.map((runs, i) => <td key={i} className="py-2 px-2"><RunCell runs={runs} /></td>)}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <div className="bg-white border border-emerald-200 rounded-xl p-5 shadow-sm">
        <div className="flex items-center gap-2 mb-3">
          <Shield size={14} className="text-emerald-600" />
          <SectionTitle className="mb-0">Chain Validation — All Scenarios</SectionTitle>
        </div>
        <div className="space-y-3">
          {[
            { scenario: 'Happy path', state: 'staging/latest-version = v20260504-… and you promote v20260504-…', result: 'Pass', detail: 'Versions match. Chain validated. Approval gate reached.', ok: true },
            { scenario: 'Staging has newer version', state: 'staging/latest-version = v20260505-… and you promote v20260504-…', result: 'Fail', detail: 'Staging has moved on. Promote v20260505-… instead, or re-deploy v20260504-… to staging first.', ok: false },
            { scenario: 'Staging has older version', state: 'staging/latest-version = v20260503-… and you promote v20260504-…', result: 'Fail', detail: 'The version has never been tested on staging. Deploy v20260504-… to staging first.', ok: false },
            { scenario: 'Staging never deployed', state: 'staging/latest-version = (parameter does not exist)', result: 'Fail', detail: 'SSM get-parameter fails. Error: "No version found for staging." Deploy to staging first.', ok: false },
            { scenario: 'Explicit matching version', state: 'You specify version_id=v20260504-… and staging/latest-version = v20260504-…', result: 'Pass', detail: 'Explicit version_id triggers the same chain check. Passes because versions match.', ok: true },
          ].map(({ scenario, state, result, detail, ok }) => (
            <div key={scenario} className={clsx('rounded-lg p-3 border text-xs', ok ? 'bg-emerald-50 border-emerald-200' : 'bg-red-50 border-red-200')}>
              <div className="flex items-center gap-2 mb-1">
                <span className="font-semibold text-gray-900">{scenario}</span>
                <span className={clsx('font-semibold', ok ? 'text-emerald-700' : 'text-red-700')}>{result}</span>
              </div>
              <div className="text-gray-500 mb-1">{state}</div>
              <div className="text-gray-700">{detail}</div>
            </div>
          ))}
        </div>
      </div>

      <div className="bg-white border border-gray-200 rounded-xl p-5 shadow-sm">
        <SectionTitle>Rollback Behaviour</SectionTitle>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-xs">
          <div>
            <div className="font-semibold text-gray-800 mb-1.5 flex items-center gap-1.5">
              <RefreshCw size={12} className="text-amber-600" /> When kubectl rollout fails
            </div>
            <div className="text-gray-600 leading-relaxed">The <Mono>rollback on failure</Mono> step runs
              <Mono>kubectl rollout undo</Mono> for both api and ui. The SSM pointer is <strong className="text-gray-800">
              not updated</strong> because <Mono>record-deployment</Mono> only runs when deploy succeeded.</div>
          </div>
          <div>
            <div className="font-semibold text-gray-800 mb-1.5 flex items-center gap-1.5">
              <AlertTriangle size={12} className="text-rose-500" /> What is NOT rolled back
            </div>
            <div className="text-gray-600 leading-relaxed">DAG files and pipeline configs already synced to S3
              are not reverted automatically. Re-deploy a previous version with <Mono>dags=true</Mono> or
              <Mono>configs=true</Mono> to overwrite with correct files.</div>
          </div>
          <div>
            <div className="font-semibold text-gray-800 mb-1.5 flex items-center gap-1.5">
              <CheckCircle2 size={12} className="text-emerald-600" /> SSM is always consistent
            </div>
            <div className="text-gray-600 leading-relaxed"><Mono>record-deployment</Mono> only writes to SSM
              after deploy AND smoke-test succeed. A failed run always leaves the SSM pointer at the last
              known-good version.</div>
          </div>
          <div>
            <div className="font-semibold text-gray-800 mb-1.5 flex items-center gap-1.5">
              <GitBranch size={12} className="text-blue-600" /> Manual rollback procedure
            </div>
            <div className="text-gray-600 leading-relaxed">Run promote.yml, specify the older <Mono>version_id</Mono>
              explicitly, and approve. Chain validation still applies — you must have the old version live on
              staging first.</div>
          </div>
        </div>
      </div>
    </div>
  )
}

// ── Tab: GitHub Actions ───────────────────────────────────────────────────────

function GitHubActionsTab() {
  const [sub, setSub] = useState<'all' | 'cicd' | 'infra' | 'utils'>('all')
  const subTabs: { id: typeof sub; label: string }[] = [
    { id: 'all',   label: 'All Workflows' },
    { id: 'cicd',  label: 'CI / CD' },
    { id: 'infra', label: 'Infrastructure' },
    { id: 'utils', label: 'Utilities' },
  ]

  return (
    <div className="space-y-5">
      <div className="bg-white border border-gray-200 rounded-xl p-5 shadow-sm">
        <SectionTitle>Workflow Lifecycle Overview</SectionTitle>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {[
            { phase: '1. Bootstrap (once per env)', color: 'border-gray-200 bg-gray-50', steps: ['infra-core.yml → apply', 'infra-audit-db.yml → apply', 'infra-source-db.yml → apply'], desc: 'Creates all AWS resources for the environment.' },
            { phase: '2. Develop & Deploy (ongoing)', color: 'border-blue-200 bg-blue-50', steps: ['ci.yml (on each PR)', 'deploy-dev.yml (on merge)', 'promote.yml staging (manual)', 'promote.yml production (manual)'], desc: 'Continuous cycle on every merge to main.' },
            { phase: '3. Operations (on-demand)', color: 'border-gray-200 bg-gray-50', steps: ['developer-access.yml', 'infra-core.yml plan', 'cleanup.yml (last resort)'], desc: 'Operational and maintenance tasks.' },
          ].map(p => (
            <div key={p.phase} className={clsx('rounded-lg p-4 border', p.color)}>
              <div className="text-sm font-semibold text-gray-900 mb-1">{p.phase}</div>
              <div className="text-xs text-gray-500 mb-3">{p.desc}</div>
              <ul className="space-y-1">
                {p.steps.map(s => (
                  <li key={s} className="flex items-start gap-2 text-xs text-gray-700">
                    <CheckCircle size={11} className="text-emerald-500 flex-shrink-0 mt-0.5" />
                    <code className="text-gray-700">{s}</code>
                  </li>
                ))}
              </ul>
            </div>
          ))}
        </div>
      </div>

      <div className="flex gap-2">
        {subTabs.map(t => (
          <button key={t.id} onClick={() => setSub(t.id)}
            className={clsx('px-3 py-1.5 rounded-lg text-xs font-medium transition-colors',
              sub === t.id
                ? 'bg-blue-600 text-white shadow-sm'
                : 'bg-white border border-gray-200 text-gray-600 hover:text-gray-900 hover:bg-gray-50',
            )}>
            {t.label}
          </button>
        ))}
      </div>

      <div className="space-y-4">

        {(sub === 'all' || sub === 'cicd') && (
          <>
            {sub === 'all' && <div className="text-xs font-semibold text-gray-400 uppercase tracking-wider mt-2">CI / CD</div>}

            <WorkflowCard file="ci.yml" name="Pull Request Checks" icon={GitMerge} color="bg-blue-600"
              trigger="Pull request → main" auto
              description="Runs on every pull request to main. Validates Terraform syntax, scans for security issues, lints Python, runs unit tests, builds all three Docker images and scans them with Trivy, and confirms the Kubernetes manifest has all required placeholders. All jobs run in parallel. None of these jobs deploy anything."
              jobsNode={
                <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                  {[
                    { label: 'Terraform Validate (×4)', desc: 'init -backend=false + validate + fmt check for core, mwaa, wwi, audit-db', color: 'bg-purple-50 border-l-purple-500' },
                    { label: 'Terraform Security (tfsec)', desc: 'Aqua Security tfsec scan across infra/terraform/. soft_fail=true — warns without blocking', color: 'bg-purple-50 border-l-purple-500' },
                    { label: 'Python Lint (ruff)', desc: 'ruff check on backend/, airflow/, eks/, tests/ — ignores E501', color: 'bg-blue-50 border-l-blue-500' },
                    { label: 'Python Tests (pytest)', desc: 'pytest tests/unit/ with coverage. Gracefully skips if no tests found', color: 'bg-blue-50 border-l-blue-500' },
                    { label: 'Docker Build & Scan (×3)', desc: 'Builds api/ui/transform for linux/amd64. Trivy CRITICAL+HIGH scan, SARIF uploaded to GitHub Security tab', color: 'bg-cyan-50 border-l-cyan-500' },
                    { label: 'K8s Manifest Validate', desc: 'Checks all 9 __PLACEHOLDER__ tokens exist, substitutes them, parses with PyYAML', color: 'bg-cyan-50 border-l-cyan-500' },
                  ].map(j => (
                    <div key={j.label} className={clsx('rounded-lg p-3 border-l-2', j.color)}>
                      <div className="text-xs font-semibold text-gray-800 mb-1">{j.label}</div>
                      <div className="text-xs text-gray-600">{j.desc}</div>
                    </div>
                  ))}
                </div>
              }
              notes={
                <>
                  <NoteBox type="info">Concurrency group <code className="text-xs font-mono">ci-{'${{ github.head_ref }}'}</code> — a new push to a PR branch cancels the in-flight run for that branch only.</NoteBox>
                  <NoteBox type="info">Docker images are built with <code className="text-xs font-mono">push: false</code>. They are scanned but never pushed to ECR. The real push happens in deploy-dev.yml after merge.</NoteBox>
                </>
              }
            />

            <WorkflowCard file="deploy-dev.yml" name="Dev Deployment" icon={Upload} color="bg-emerald-600"
              trigger="PR merged → main  ·  manual" auto
              description="Fires automatically when a pull request merges to main, or manually for on-demand deploys. Detects what changed, skips unchanged artifacts, builds and pushes Docker images to ECR, deploys to EKS, syncs DAGs to MWAA, uploads pipeline configs to S3, registers pipeline definitions via the REST API, and writes an immutable version manifest to SSM."
              inputs={<InputRow name="reason" required={false} desc="Audit trail note for manual dispatches." />}
              jobsNode={
                <div className="space-y-4">
                  <JobChain jobs={[{ id: 'g', label: 'guard', note: 'change detection' }, { id: 'vi', label: 'validate-infra' }]} />
                  <div className="text-xs text-gray-500 ml-4">Three parallel branches after validate-infra:</div>
                  <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
                    {[
                      { chain: ['build (×3)', 'build-meta', 'deploy-apps', 'smoke-test'], note: 'if apps changed', bg: 'bg-blue-50 border-blue-200' },
                      { chain: ['sync-dags'], note: 'if dags changed OR apps changed', bg: 'bg-violet-50 border-violet-200' },
                      { chain: ['sync-configs'], note: 'if configs changed', bg: 'bg-amber-50 border-amber-200' },
                    ].map(b => (
                      <div key={b.chain[0]} className={clsx('rounded-lg p-3 border', b.bg)}>
                        <div className="text-xs text-gray-500 mb-2 italic">{b.note}</div>
                        <div className="flex flex-wrap gap-1 items-center">
                          {b.chain.map((j, i) => (
                            <React.Fragment key={j}>
                              {i > 0 && <ArrowRight size={10} className="text-gray-400" />}
                              <code className="text-xs bg-white border border-gray-300 text-gray-700 px-1.5 py-0.5 rounded">{j}</code>
                            </React.Fragment>
                          ))}
                        </div>
                      </div>
                    ))}
                  </div>
                  <JobChain jobs={[{ id: 'rp', label: 'register-pipelines' }, { id: 'rv', label: 'record-version', note: 'SSM manifest' }, { id: 'sum', label: 'summary' }]} />
                </div>
              }
              notes={
                <>
                  <NoteBox type="info">Concurrency group <code className="text-xs font-mono">deploy-dev</code> is shared — a new merge cancels any in-flight dev deploy, preventing two deploys from racing to update the same cluster.</NoteBox>
                  <NoteBox type="warn">The RDS SG must allow port 5432 from the runner IP for register-pipelines to sync historical MWAA runs. The job opens the rule temporarily and removes it via <code className="text-xs font-mono">trap cleanup EXIT</code>.</NoteBox>
                </>
              }
            />

            <WorkflowCard file="promote.yml" name="Promotion (Staging / Production)" icon={GitBranch} color="bg-violet-600"
              trigger="Manual only"
              description="Promotes a versioned dev deployment to staging or production. Reads the immutable version manifest from SSM, validates the promotion chain (staging→prod requires staging to be running that exact version), copies ECR images across accounts, waits for a GitHub Environment approval, deploys only the artifacts recorded in the manifest, runs smoke tests if apps were deployed, and writes an audit record. Infrastructure is never touched."
              inputs={
                <>
                  <InputRow name="environment" required desc="Target environment." options={['staging', 'production']} />
                  <InputRow name="version_id" required={false} desc="Version to promote (e.g. v20260504-145230-abc1234). Leave blank to auto-resolve from source environment." />
                </>
              }
              jobsNode={
                <div className="space-y-3">
                  <JobChain jobs={[{ id: 'r', label: 'resolve', note: 'load manifest + chain validation' }]} />
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-3 ml-4">
                    <div className="bg-blue-50 rounded-lg p-3 border border-blue-200">
                      <div className="text-xs text-gray-500 mb-1 italic">if deploy_apps == true</div>
                      <code className="text-xs bg-white border border-gray-300 text-gray-700 px-1.5 py-0.5 rounded">copy-images</code>
                      <span className="text-xs text-gray-500 ml-1">dev ECR → target ECR</span>
                    </div>
                    <div className="bg-gray-50 rounded-lg p-3 border border-gray-200">
                      <div className="text-xs text-gray-500 mb-1 italic">always</div>
                      <code className="text-xs bg-white border border-gray-300 text-gray-700 px-1.5 py-0.5 rounded">validate-infra</code>
                      <span className="text-xs text-gray-500 ml-1">EKS + secret exist in target</span>
                    </div>
                  </div>
                  <JobChain jobs={[
                    { id: 'd', label: 'deploy', gate: true, note: 'env approval' },
                    { id: 's', label: 'smoke-test', skip: 'if apps=false' },
                    { id: 'rd', label: 'record-deployment' },
                    { id: 'sum', label: 'summary' },
                  ]} />
                </div>
              }
              notes={
                <>
                  <NoteBox type="warn"><strong>Chain enforcement:</strong> Promoting to production fails instantly if staging is not already running that exact version — before any approval gate is shown.</NoteBox>
                  <NoteBox type="info"><strong>Exact SHA checkout:</strong> The deploy job checks out the git SHA recorded in the manifest, not main HEAD — guaranteeing identical code to what passed dev smoke tests.</NoteBox>
                </>
              }
            />
          </>
        )}

        {(sub === 'all' || sub === 'infra') && (
          <>
            {sub === 'all' && <div className="text-xs font-semibold text-gray-400 uppercase tracking-wider mt-6">Infrastructure</div>}

            <WorkflowCard file="infra-core.yml" name="Core Infrastructure" icon={Server} color="bg-gray-600"
              trigger="Manual only"
              description="Provisions VPC (subnets, NAT gateways, routing), EKS cluster and node groups, ECR repositories (api/ui/transform), S3 buckets (MWAA DAGs, DuckDB store, pipeline configs, artifacts), ElastiCache Redis, and Amazon MWAA. Supports a disaster recovery region (us-west-2) via the 'dr' environment. Does not manage any database."
              inputs={
                <>
                  <InputRow name="environment" required options={['dev', 'staging', 'production', 'dr']} desc="Target environment. 'dr' deploys to us-west-2." />
                  <InputRow name="action" required options={['plan', 'apply', 'destroy']} desc="plan = show Terraform plan only. apply = apply the plan. destroy = plan + apply with -destroy." />
                </>
              }
              jobsNode={
                <div className="space-y-3">
                  <JobChain jobs={[{ id: 's', label: 'setup' }, { id: 'b', label: 'bootstrap', note: 'S3 state bucket + DynamoDB lock table' }]} />
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-3 ml-4">
                    <div className="bg-gray-50 rounded-lg p-3 border border-gray-200">
                      <code className="text-xs text-gray-700">plan-core</code>
                      <span className="text-xs text-gray-500 ml-1">VPC + EKS + ECR + S3 + Redis</span>
                    </div>
                    <div className="bg-gray-50 rounded-lg p-3 border border-gray-200">
                      <code className="text-xs text-gray-700">check-core</code>
                      <span className="text-xs text-gray-500 ml-1">is EKS ACTIVE? (gates plan-mwaa)</span>
                    </div>
                  </div>
                  <JobChain jobs={[
                    { id: 'ac', label: 'apply-core', gate: true, note: 'env approval if action!=plan' },
                    { id: 'pm', label: 'plan-mwaa' },
                    { id: 'am', label: 'apply-mwaa', gate: true },
                    { id: 'sum', label: 'summary' },
                  ]} />
                </div>
              }
              notes={
                <>
                  <NoteBox type="info">After apply-core, pipeline configs are uploaded to the S3 bucket. After apply-mwaa, <code className="text-xs font-mono">airflow/deploy_to_mwaa.sh</code> seeds the MWAA bucket with DAGs.</NoteBox>
                  <NoteBox type="warn">plan-mwaa is gated on check-core: running it against a fresh (post-cleanup) environment fails because MWAA Terraform uses data sources that query the VPC and EKS cluster.</NoteBox>
                </>
              }
            />

            <WorkflowCard file="infra-audit-db.yml" name="Audit Database (PostgreSQL)" icon={Database} color="bg-blue-700"
              trigger="Manual only"
              prerequisite="infra-core.yml must be applied first — VPC and security groups must exist."
              description="Manages the PostgreSQL RDS instance storing pipeline audit data. After a successful apply, temporarily opens port 5432 to the runner IP, runs scripts/create_audit_tables.py to create all tables and indexes, then removes the security group rule via trap cleanup EXIT."
              inputs={
                <>
                  <InputRow name="environment" required options={['dev', 'staging', 'production', 'dr']} desc="Target environment." />
                  <InputRow name="action" required options={['plan', 'apply', 'destroy']} desc="plan does not require a password." />
                  <InputRow name="db_password" required={false} desc="PostgreSQL password for 'airflow' user. Leave blank to use DB_PASSWORD repository secret." />
                </>
              }
              jobsNode={
                <JobChain jobs={[
                  { id: 's', label: 'setup' },
                  { id: 'p', label: 'plan', note: 'tf plan for audit-db module' },
                  { id: 'a', label: 'apply / destroy', gate: true, note: '+ create-audit-schema with temp SG rule' },
                  { id: 'sum', label: 'summary' },
                ]} />
              }
              notes={
                <NoteBox type="warn">If both the input and repo secret are blank and action is apply/destroy, the plan step fails early. Never set a placeholder value — configure the <code className="text-xs font-mono">DB_PASSWORD</code> repo secret instead.</NoteBox>
              }
            />

            <WorkflowCard file="infra-source-db.yml" name="Source Database (SQL Server)" icon={Database} color="bg-orange-600"
              trigger="Manual only"
              prerequisite="infra-core.yml must be applied first. VPC, artifacts S3 bucket, and IAM role for RDS S3 restore must exist."
              description="Manages the RDS SQL Server instance used as ETL source. After apply, temporarily opens port 1433 then runs scripts/setup_databases.py which restores WideWorldImporters from S3 and creates all TargetDB tables and views."
              inputs={
                <>
                  <InputRow name="environment" required options={['dev', 'staging', 'production', 'dr']} desc="Target environment." />
                  <InputRow name="action" required options={['plan', 'apply', 'destroy']} desc="plan does not require a password." />
                  <InputRow name="mssql_password" required={false} desc="SQL Server password for 'sqladmin' user. Leave blank to use MSSQL_PASSWORD repository secret." />
                </>
              }
              jobsNode={
                <JobChain jobs={[
                  { id: 's', label: 'setup' },
                  { id: 'p', label: 'plan', note: 'tf plan for wwi module' },
                  { id: 'a', label: 'apply / destroy', gate: true, note: '+ WideWorldImporters restore + TargetDB schema' },
                  { id: 'sum', label: 'summary' },
                ]} />
              }
              notes={
                <NoteBox type="info">The WideWorldImporters .bak file must be pre-uploaded to the artifacts S3 bucket. The SQL Server endpoint is written to Secrets Manager by Terraform and read by ETL DAGs at runtime — no hardcoded hostnames.</NoteBox>
              }
            />
          </>
        )}

        {(sub === 'all' || sub === 'utils') && (
          <>
            {sub === 'all' && <div className="text-xs font-semibold text-gray-400 uppercase tracking-wider mt-6">Utilities</div>}

            <WorkflowCard file="cleanup.yml" name="Emergency Cleanup" icon={Trash2} color="bg-red-600"
              trigger="Manual only  ·  requires env name confirmation"
              description="Bypasses Terraform and deletes all NextGenDatabridge AWS resources directly via AWS CLI. Use when the normal destroy workflow fails. All steps are idempotent — re-running is always safe. Protected by a GitHub Environment approval AND a typed confirmation check."
              inputs={
                <>
                  <InputRow name="environment" required options={['dev', 'staging', 'production']} desc="Environment to destroy." />
                  <InputRow name="confirm" required desc="Must exactly match the environment name. Any mismatch fails the first step before anything is deleted." />
                </>
              }
              jobsNode={
                <div className="space-y-1.5">
                  {[
                    { n: 1,  label: 'Safety confirmation check', desc: 'Fails immediately if confirm ≠ environment. Nothing is touched.' },
                    { n: 2,  label: 'Delete K8s LoadBalancer services', desc: 'Removes NLB-backed K8s services → AWS decommissions the NLBs. Waits 90s for ENIs.' },
                    { n: 3,  label: 'Delete MWAA (async)', desc: 'Initiates deletion early — MWAA takes ~20 min. Continues without waiting.' },
                    { n: 4,  label: 'Delete RDS SQL Server', desc: 'Disables deletion protection, skip-final-snapshot, waits for completion.' },
                    { n: 5,  label: 'Delete RDS PostgreSQL', desc: 'Same pattern as SQL Server.' },
                    { n: 6,  label: 'Delete ElastiCache Redis', desc: 'Deletes the replication group without retaining primary cluster.' },
                    { n: 7,  label: 'Wait for MWAA deletion', desc: 'Polls up to 30 min (60 × 30s). Continues once status is NOT_FOUND.' },
                    { n: 8,  label: 'Delete EKS node groups + cluster', desc: 'Lists and deletes all node groups, removes access entries, then deletes cluster.' },
                    { n: 9,  label: 'Delete S3 buckets', desc: 'Paginated Python loop deletes all versions/markers before deleting bucket.' },
                    { n: 10, label: 'Delete Secrets / ECR / IAM / SSM', desc: 'force-delete-without-recovery for secrets; --force for ECR; detaches policies before deleting IAM roles.' },
                    { n: 11, label: 'Delete RDS/ElastiCache subnet groups', desc: 'Subnet groups, option groups for SQL Server, ElastiCache subnet group.' },
                    { n: 12, label: 'Delete security groups', desc: 'Revokes all inbound rules first to clear cross-SG references, then deletes.' },
                    { n: 13, label: 'Delete VPC and networking', desc: 'Order: NAT gateways → wait 90s → EIPs → IGW → available ENIs → subnets → route tables → VPC.' },
                  ].map(s => (
                    <div key={s.n} className="flex gap-3 bg-gray-50 rounded-lg px-3 py-2 border border-gray-100">
                      <span className="text-gray-400 text-xs w-5 flex-shrink-0 mt-0.5">{s.n}.</span>
                      <div>
                        <div className="text-xs font-semibold text-gray-800">{s.label}</div>
                        <div className="text-xs text-gray-500 mt-0.5">{s.desc}</div>
                      </div>
                    </div>
                  ))}
                </div>
              }
              notes={
                <>
                  <NoteBox type="warn"><strong>Common failure: VPC won't delete</strong> means in-use ENIs remain. Re-run the workflow; if ENIs persist after a second run, remove them manually from EC2 → Network Interfaces console.</NoteBox>
                  <NoteBox type="warn">All resources and data are permanently deleted. There is no undo. Configure Required Reviewers on the GitHub Environments to prevent accidental deletion.</NoteBox>
                </>
              }
            />

            <WorkflowCard file="developer-access.yml" name="Developer Access Toggle" icon={Key} color="bg-teal-600"
              trigger="Manual only"
              description="Temporarily opens or closes direct developer access to RDS databases and web services. For databases it adds or revokes a security group ingress rule. For web-api it outputs the current NLB hostnames (always internet-facing — no SG change needed)."
              inputs={
                <>
                  <InputRow name="action" required options={['enable', 'disable']} desc="enable adds the ingress rule. disable revokes it." />
                  <InputRow name="environment" required options={['dev', 'staging', 'production']} desc="Target environment." />
                  <InputRow name="resources" required options={['all', 'postgres', 'sqlserver', 'web-api']} desc="Which resources to toggle." />
                  <InputRow name="cidr" required={false} desc="Your IP/CIDR (e.g. 203.0.113.1/32). For enable: auto-detected from runner egress IP if blank. For disable: revokes all non-VPC rules if blank." />
                </>
              }
              jobsNode={
                <div className="space-y-3">
                  <JobChain jobs={[
                    { id: 'r', label: 'Resolve role' },
                    { id: 'c', label: 'Resolve CIDR', note: 'auto-detect or use input' },
                    { id: 'pg', label: 'Toggle PostgreSQL', skip: 'postgres or all' },
                    { id: 'ss', label: 'Toggle SQL Server', skip: 'sqlserver or all' },
                    { id: 'we', label: 'Get Web & API endpoints', skip: 'web-api or all' },
                  ]} />
                  <NoteBox type="warn"><strong>Remember to disable access</strong> when done. Rules are tagged <code className="text-xs font-mono">developer-access=true</code> and <code className="text-xs font-mono">added-by</code> for auditability.</NoteBox>
                  <NoteBox type="info">The auto-detect CIDR feature uses the GitHub Actions <em>runner's</em> egress IP — not your laptop's. For GitHub-hosted runners, always provide your public IP explicitly.</NoteBox>
                </div>
              }
            />
          </>
        )}
      </div>
    </div>
  )
}

// ── Main page ─────────────────────────────────────────────────────────────────

const TABS = [
  { id: 'versioning',     label: 'Versioning'     },
  { id: 'promotion',      label: 'Promotion Flow' },
  { id: 'github-actions', label: 'GitHub Actions' },
]

export default function DeploymentInstruction() {
  const [tab, setTab] = useState('versioning')
  return (
    <div className="flex flex-col h-full overflow-hidden">
      <div className="px-6 pt-5 pb-0 flex-shrink-0 bg-gray-50">
        <PageHeader
          title="Deployment Process"
          subtitle="Version management, promotion flow, and GitHub Actions reference"
        />
        <div className="mt-4">
          <Tabs tabs={TABS} active={tab} onChange={setTab} />
        </div>
      </div>
      <div className="flex-1 overflow-y-auto px-6 py-5 bg-gray-50">
        {tab === 'versioning'     && <VersioningTab />}
        {tab === 'promotion'      && <PromotionTab />}
        {tab === 'github-actions' && <GitHubActionsTab />}
      </div>
    </div>
  )
}

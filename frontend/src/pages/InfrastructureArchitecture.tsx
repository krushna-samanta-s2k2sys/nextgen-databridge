import React, { useState } from 'react'
import {
  Server, Cloud, Database, Package, Key, Lock, HardDrive,
  GitBranch, ArrowRight, CheckCircle2, Shield, Tag,
  Cpu, Zap, AlertTriangle, ChevronRight, Globe,
  Settings, Activity, FileText, RefreshCw, Rocket,
  GitCommit, Layers, Box, Terminal,
} from 'lucide-react'
import clsx from 'clsx'
import { PageHeader, Tabs } from '../components/ui'

// ── Reusable primitives ───────────────────────────────────────────────────────

function EnvBadge({ env }: { env: 'dev' | 'staging' | 'production' }) {
  return (
    <span className={clsx(
      'inline-flex items-center px-2 py-0.5 rounded text-xs font-medium font-mono',
      env === 'dev'        && 'bg-blue-500/15 text-blue-400',
      env === 'staging'    && 'bg-amber-500/15 text-amber-400',
      env === 'production' && 'bg-emerald-500/15 text-emerald-400',
    )}>
      {env}
    </span>
  )
}

function SectionTitle({ children, className }: { children: React.ReactNode; className?: string }) {
  return (
    <h3 className={clsx('text-sm font-semibold text-zinc-100 mb-3', className)}>
      {children}
    </h3>
  )
}

function Prose({ children }: { children: React.ReactNode }) {
  return <p className="text-xs text-zinc-400 leading-relaxed">{children}</p>
}

function Mono({ children }: { children: React.ReactNode }) {
  return <code className="font-mono text-xs bg-zinc-800 border border-zinc-700 rounded px-1.5 py-0.5 text-zinc-300">{children}</code>
}

function CodeBlock({ children }: { children: string }) {
  return (
    <pre className="bg-zinc-950 border border-zinc-800 rounded-lg p-4 text-xs font-mono text-zinc-300 overflow-x-auto whitespace-pre leading-relaxed">
      {children}
    </pre>
  )
}

function ServiceCard({
  icon: Icon,
  iconBg,
  title,
  subtitle,
  children,
}: {
  icon: React.ElementType
  iconBg: string
  title: string
  subtitle?: string
  children: React.ReactNode
}) {
  return (
    <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-4">
      <div className="flex items-start gap-3 mb-3">
        <div className={clsx('w-8 h-8 rounded-lg flex items-center justify-center flex-shrink-0', iconBg)}>
          <Icon size={15} className="text-white" />
        </div>
        <div>
          <div className="text-sm font-semibold text-zinc-100">{title}</div>
          {subtitle && <div className="text-xs text-zinc-500 mt-0.5">{subtitle}</div>}
        </div>
      </div>
      <div className="space-y-1.5 text-xs text-zinc-400">{children}</div>
    </div>
  )
}

function InfoRow({ label, value, mono }: { label: string; value: React.ReactNode; mono?: boolean }) {
  return (
    <div className="flex items-start gap-2 py-0.5">
      <span className="text-zinc-500 min-w-[100px] flex-shrink-0">{label}</span>
      <span className={clsx('text-zinc-300', mono && 'font-mono')}>{value}</span>
    </div>
  )
}

function Divider() {
  return <div className="border-t border-zinc-800 my-5" />
}

function FlowStep({
  n,
  title,
  desc,
  env,
  last,
}: {
  n: number
  title: string
  desc: React.ReactNode
  env?: 'dev' | 'staging' | 'production'
  last?: boolean
}) {
  return (
    <div className="flex gap-3">
      <div className="flex flex-col items-center flex-shrink-0">
        <div className="w-6 h-6 rounded-full bg-blue-600 flex items-center justify-center text-xs font-bold text-white">
          {n}
        </div>
        {!last && <div className="w-px flex-1 bg-zinc-800 mt-1 mb-1 min-h-[16px]" />}
      </div>
      <div className={clsx('pb-4', last && 'pb-0')}>
        <div className="flex items-center gap-2">
          <span className="text-sm font-medium text-zinc-100">{title}</span>
          {env && <EnvBadge env={env} />}
        </div>
        <div className="text-xs text-zinc-400 mt-1 leading-relaxed">{desc}</div>
      </div>
    </div>
  )
}

function Pill({
  children,
  color = 'zinc',
}: {
  children: React.ReactNode
  color?: 'zinc' | 'blue' | 'amber' | 'emerald' | 'purple' | 'sky'
}) {
  return (
    <span className={clsx(
      'inline-flex items-center px-2 py-0.5 rounded text-xs font-medium',
      color === 'zinc'    && 'bg-zinc-800 text-zinc-300',
      color === 'blue'    && 'bg-blue-500/15 text-blue-400',
      color === 'amber'   && 'bg-amber-500/15 text-amber-400',
      color === 'emerald' && 'bg-emerald-500/15 text-emerald-400',
      color === 'purple'  && 'bg-purple-500/15 text-purple-400',
      color === 'sky'     && 'bg-sky-500/15 text-sky-400',
    )}>
      {children}
    </span>
  )
}

// ── Tab: Architecture ─────────────────────────────────────────────────────────

function ArchitectureTab() {
  return (
    <div className="space-y-6">

      {/* High-level overview */}
      <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5">
        <SectionTitle>Platform Overview</SectionTitle>
        <Prose>
          NextGenDatabridge is an AWS-native data pipeline platform. Pipeline DAGs run on Amazon MWAA
          (managed Airflow). Extract and transform tasks execute as ephemeral Kubernetes pods on EKS,
          using DuckDB for in-memory transforms. The monitoring UI and REST API are long-running EKS
          deployments. All configuration and infrastructure are managed as code — pipeline configs in
          JSON, infrastructure in Terraform, CI/CD in GitHub Actions.
        </Prose>
      </div>

      {/* Data flow diagram */}
      <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5">
        <SectionTitle>Data Flow</SectionTitle>
        <div className="flex flex-wrap items-center gap-2 text-xs">
          {[
            { label: 'SQL Server (RDS)', color: 'bg-sky-600' },
            null,
            { label: 'sql_extract task', color: 'bg-blue-700' },
            null,
            { label: 'DuckDB (EKS Job)', color: 'bg-violet-700' },
            null,
            { label: 'S3 DuckDB Store', color: 'bg-amber-700' },
            null,
            { label: 'load_target task', color: 'bg-blue-700' },
            null,
            { label: 'Target DB (RDS)', color: 'bg-sky-600' },
          ].map((item, i) =>
            item === null ? (
              <ArrowRight key={i} size={14} className="text-zinc-600 flex-shrink-0" />
            ) : (
              <div key={i} className={clsx('px-3 py-1.5 rounded-lg text-white font-medium', item.color)}>
                {item.label}
              </div>
            )
          )}
        </div>
        <Prose>
          MWAA orchestrates each DAG. The sql_extract operator streams batches from SQL Server into a
          DuckDB file on S3. The duckdb_transform operator runs SQL against those files inside an EKS
          job pod. The load_target operator writes the result to the target database. All intermediate
          state lives in S3 — no data crosses EKS node boundaries.
        </Prose>
      </div>

      {/* AWS services grid */}
      <SectionTitle>AWS Services</SectionTitle>
      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">

        <ServiceCard icon={Server} iconBg="bg-blue-700" title="Amazon EKS" subtitle="nextgen-databridge">
          <InfoRow label="Services" value="api, ui, transform (job)" />
          <InfoRow label="Namespace" value="nextgen-databridge" mono />
          <InfoRow label="API" value="FastAPI — /api/*, /health" />
          <InfoRow label="UI" value="React — served by Nginx" />
          <InfoRow label="Transform" value="Ephemeral DuckDB executor pods" />
          <InfoRow label="Images" value="ECR — api, ui, transform" />
        </ServiceCard>

        <ServiceCard icon={Cloud} iconBg="bg-indigo-700" title="Amazon MWAA" subtitle="Managed Apache Airflow">
          <InfoRow label="Cluster" value="nextgen-databridge" mono />
          <InfoRow label="DAG source" value="s3://nextgen-databridge-mwaa-{env}/dags/" mono />
          <InfoRow label="Scheduling" value="Cron-based; per pipeline config" />
          <InfoRow label="Operators" value="sql_extract, duckdb_transform, load_target, data_quality, api_call, stored_proc, autosys_job, notification" />
          <InfoRow label="Auth" value="MWAA web-login token (CI) / Basic auth (local)" />
        </ServiceCard>

        <ServiceCard icon={Database} iconBg="bg-sky-700" title="RDS SQL Server" subtitle="WideWorldImporters source">
          <InfoRow label="Identifier" value="nextgen-databridge-sqlserver" mono />
          <InfoRow label="Engine" value="SQL Server SE 15.00" />
          <InfoRow label="Dev instance" value="db.t3.xlarge" />
          <InfoRow label="Prod instance" value="db.r6g.xlarge (multi-AZ)" />
          <InfoRow label="Used by" value="wwi_* pipelines (source)" />
          <InfoRow label="Schema" value="WideWorldImporters / TargetDB" mono />
        </ServiceCard>

        <ServiceCard icon={Database} iconBg="bg-teal-700" title="RDS PostgreSQL" subtitle="Audit & metadata database">
          <InfoRow label="Secret" value="nextgen-databridge/connections/audit_db" mono />
          <InfoRow label="Used by" value="nextgen-databridge-api (EKS)" />
          <InfoRow label="Stores" value="Pipeline runs, task logs, connections, deployments" />
          <InfoRow label="Access" value="VPC only; CI uses temp SG rule" />
        </ServiceCard>

        <ServiceCard icon={HardDrive} iconBg="bg-amber-700" title="Amazon S3" subtitle="Five purpose-specific buckets">
          <InfoRow label="mwaa-{env}" value="DAG files synced by GitHub Actions" mono />
          <InfoRow label="pipeline-configs-{env}" value="Pipeline JSON configs" mono />
          <InfoRow label="duckdb-store-{env}" value="DuckDB output files" mono />
          <InfoRow label="artifacts-{env}" value="SQL backup, build artifacts" mono />
          <InfoRow label="terraform-state" value="Shared Terraform backend" mono />
        </ServiceCard>

        <ServiceCard icon={Package} iconBg="bg-rose-700" title="Amazon ECR" subtitle="Container image registry">
          <InfoRow label="nextgen-databridge/api" value="FastAPI backend" mono />
          <InfoRow label="nextgen-databridge/ui" value="React frontend (Nginx)" mono />
          <InfoRow label="nextgen-databridge/transform" value="DuckDB executor" mono />
          <InfoRow label="Tag format" value="sha-{full-sha} + {env} alias" />
          <InfoRow label="Promotion" value="Images are copied across accounts — never rebuilt" />
        </ServiceCard>

        <ServiceCard icon={Key} iconBg="bg-yellow-700" title="AWS SSM Parameter Store" subtitle="Deployment tracking">
          <InfoRow label="/versions/{id}" value="Immutable version manifests" mono />
          <InfoRow label="/deploy/{env}/latest-version" value="Current version pointer" mono />
          <InfoRow label="/deploy/{env}/last-deployment" value="Audit record" mono />
          <InfoRow label="/image-tag/{env}" value="Backwards-compat image tag" mono />
        </ServiceCard>

        <ServiceCard icon={Lock} iconBg="bg-slate-600" title="AWS Secrets Manager" subtitle="Connection credentials">
          <InfoRow label="wwi_sqlserver" value="SQL Server source credentials" />
          <InfoRow label="wwi_sqlserver_target" value="Target DB credentials" />
          <InfoRow label="audit_db" value="PostgreSQL connection URL" />
          <InfoRow label="exchange_rate_api" value="External API key" />
          <InfoRow label="autosys, downstream_api" value="Service credentials" />
        </ServiceCard>

        <ServiceCard icon={Shield} iconBg="bg-green-700" title="IAM / OIDC" subtitle="GitHub Actions authentication">
          <InfoRow label="Dev role" value="nextgen-databridge-github-actions-dev" mono />
          <InfoRow label="Staging role" value="nextgen-databridge-github-actions-staging" mono />
          <InfoRow label="Prod role" value="nextgen-databridge-github-actions-production" mono />
          <InfoRow label="Auth method" value="GitHub OIDC — no long-lived secrets" />
          <InfoRow label="Scope" value="Each role is scoped to its account only" />
        </ServiceCard>

      </div>

      <Divider />

      {/* Terraform modules */}
      <SectionTitle>Terraform Modules</SectionTitle>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
        {[
          { path: 'infra/terraform/core',         desc: 'VPC, EKS cluster, ECR repos, IAM roles, S3 buckets, OIDC provider' },
          { path: 'infra/terraform/mwaa',          desc: 'MWAA Airflow environment, DAG bucket, IAM execution role' },
          { path: 'infra/terraform/audit-db',      desc: 'PostgreSQL RDS, security group, Secrets Manager entry' },
          { path: 'infra/terraform/wwi',           desc: 'SQL Server RDS, option group, Secrets Manager entries for source + target' },
          { path: 'infra/terraform/duckdb-store',  desc: 'S3 buckets for DuckDB intermediate files and pipeline config storage' },
        ].map(({ path, desc }) => (
          <div key={path} className="bg-zinc-900 border border-zinc-800 rounded-lg p-3 flex gap-3">
            <Layers size={14} className="text-zinc-500 mt-0.5 flex-shrink-0" />
            <div>
              <div className="font-mono text-xs text-zinc-300 mb-0.5">{path}</div>
              <div className="text-xs text-zinc-500">{desc}</div>
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

// ── Tab: Environments ─────────────────────────────────────────────────────────

function EnvironmentsTab() {
  return (
    <div className="space-y-6">

      {/* Three environment cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        {/* Dev */}
        <div className="bg-zinc-900 border border-blue-500/30 rounded-xl p-4">
          <div className="flex items-center gap-2 mb-3">
            <div className="w-2 h-2 rounded-full bg-blue-500" />
            <span className="text-sm font-semibold text-zinc-100">Development</span>
            <EnvBadge env="dev" />
          </div>
          <div className="space-y-2 text-xs">
            <InfoRow label="AWS Account"  value="877707676590" mono />
            <InfoRow label="EKS Cluster"  value="nextgen-databridge" mono />
            <InfoRow label="MWAA Env"     value="nextgen-databridge" mono />
            <InfoRow label="Trigger"      value="PR merge to main (auto) or manual dispatch" />
            <InfoRow label="Approval"     value="None — deploys automatically" />
            <InfoRow label="RDS type"     value="db.t3.xlarge" />
            <InfoRow label="Multi-AZ"     value="No" />
            <InfoRow label="Deletion protection" value="No" />
          </div>
          <div className="mt-3 pt-3 border-t border-zinc-800">
            <div className="text-xs text-blue-400">
              Every successful deploy creates a new version manifest. All promotions start from a dev version.
            </div>
          </div>
        </div>

        {/* Staging */}
        <div className="bg-zinc-900 border border-amber-500/30 rounded-xl p-4">
          <div className="flex items-center gap-2 mb-3">
            <div className="w-2 h-2 rounded-full bg-amber-500" />
            <span className="text-sm font-semibold text-zinc-100">Staging</span>
            <EnvBadge env="staging" />
          </div>
          <div className="space-y-2 text-xs">
            <InfoRow label="AWS Account"  value="STAGING_ACCOUNT_ID" mono />
            <InfoRow label="EKS Cluster"  value="nextgen-databridge-staging" mono />
            <InfoRow label="MWAA Env"     value="nextgen-databridge-staging" mono />
            <InfoRow label="Trigger"      value="Manual — promote.yml workflow" />
            <InfoRow label="Approval"     value="Required reviewers (GitHub Environment)" />
            <InfoRow label="Infra"        value="Managed separately via infra-*.yml" />
            <InfoRow label="Multi-AZ"     value="Configurable" />
            <InfoRow label="Source"       value="Any version deployed to dev" />
          </div>
          <div className="mt-3 pt-3 border-t border-zinc-800">
            <div className="text-xs text-amber-400">
              Must successfully host a version before that version can be promoted to production.
            </div>
          </div>
        </div>

        {/* Production */}
        <div className="bg-zinc-900 border border-emerald-500/30 rounded-xl p-4">
          <div className="flex items-center gap-2 mb-3">
            <div className="w-2 h-2 rounded-full bg-emerald-500" />
            <span className="text-sm font-semibold text-zinc-100">Production</span>
            <EnvBadge env="production" />
          </div>
          <div className="space-y-2 text-xs">
            <InfoRow label="AWS Account"  value="PROD_ACCOUNT_ID" mono />
            <InfoRow label="EKS Cluster"  value="nextgen-databridge-production" mono />
            <InfoRow label="MWAA Env"     value="nextgen-databridge-production" mono />
            <InfoRow label="Trigger"      value="Manual — promote.yml workflow" />
            <InfoRow label="Approval"     value="Required reviewers + main-branch restriction" />
            <InfoRow label="RDS type"     value="db.r6g.xlarge" />
            <InfoRow label="Multi-AZ"     value="Yes" />
            <InfoRow label="Deletion protection" value="Yes" />
          </div>
          <div className="mt-3 pt-3 border-t border-zinc-800">
            <div className="text-xs text-emerald-400">
              Chain validation enforced: only the version currently on staging may be promoted here.
            </div>
          </div>
        </div>
      </div>

      <Divider />

      {/* Resource naming */}
      <SectionTitle>Resource Naming Convention</SectionTitle>
      <Prose>
        All environment-specific resources follow the pattern <Mono>nextgen-databridge-{'{resource}'}-{'{env}'}</Mono>.
        The exception is the dev environment, where some resources omit the env suffix (the first environment to be provisioned).
      </Prose>
      <div className="mt-3 overflow-x-auto">
        <table className="w-full text-xs border-collapse">
          <thead>
            <tr className="border-b border-zinc-800">
              <th className="text-left text-zinc-500 font-medium py-2 pr-4">Resource</th>
              <th className="text-left text-zinc-500 font-medium py-2 pr-4">dev</th>
              <th className="text-left text-zinc-500 font-medium py-2 pr-4">staging</th>
              <th className="text-left text-zinc-500 font-medium py-2">production</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-zinc-900">
            {[
              ['EKS cluster',        'nextgen-databridge',               'nextgen-databridge-staging',       'nextgen-databridge-production'],
              ['MWAA environment',   'nextgen-databridge',               'nextgen-databridge-staging',       'nextgen-databridge-production'],
              ['DAG S3 bucket',      'nextgen-databridge-mwaa-dev',      'nextgen-databridge-mwaa-staging',  'nextgen-databridge-mwaa-production'],
              ['Config S3 bucket',   'nextgen-databridge-pipeline-configs-dev',  '…-staging',              '…-production'],
              ['DuckDB S3 bucket',   'nextgen-databridge-duckdb-store-dev',      '…-staging',              '…-production'],
              ['Artifacts bucket',   'nextgen-databridge-artifacts-dev', '…-staging',                        '…-production'],
              ['ECR (images)',       'nextgen-databridge/{svc}',         'nextgen-databridge/{svc}',         'nextgen-databridge/{svc}'],
            ].map(([resource, dev, stg, prod]) => (
              <tr key={resource} className="hover:bg-zinc-900/50">
                <td className="py-2 pr-4 text-zinc-400">{resource}</td>
                <td className="py-2 pr-4 font-mono text-blue-400">{dev}</td>
                <td className="py-2 pr-4 font-mono text-amber-400">{stg}</td>
                <td className="py-2 font-mono text-emerald-400">{prod}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <Divider />

      {/* What differs per environment */}
      <SectionTitle>Infrastructure Separation</SectionTitle>
      <Prose>
        Each environment is an independent AWS account with its own VPC, EKS cluster, MWAA environment,
        and RDS instances. ECR repositories exist in each account independently — Docker images are
        copied from dev ECR during promotion; they are never rebuilt. Terraform state is per-environment
        and stored in the shared <Mono>nextgen-databridge-terraform-state</Mono> S3 bucket with isolated
        key prefixes. No cross-account data access occurs at runtime — only CI/CD roles have cross-account
        permissions (limited to ECR image copy and SSM reads).
      </Prose>
    </div>
  )
}

// ── Tab: Versioning ───────────────────────────────────────────────────────────

function VersioningTab() {
  return (
    <div className="space-y-6">

      <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5">
        <SectionTitle>What Is a Version?</SectionTitle>
        <Prose>
          Every successful deployment to dev creates an immutable version record in AWS SSM Parameter
          Store. A version captures the exact git SHA deployed, which artifacts changed (EKS apps, MWAA
          DAGs, or pipeline configs), the Docker image tag (if apps were built), and full audit metadata.
          Versions are the unit of promotion — you promote a named version, not a branch or tag.
        </Prose>
      </div>

      {/* Version ID */}
      <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5">
        <SectionTitle>Version ID Format</SectionTitle>
        <div className="flex items-center gap-2 mb-3">
          <div className="font-mono text-base text-zinc-100 bg-zinc-950 border border-zinc-700 rounded-lg px-4 py-2.5 inline-block">
            v<span className="text-blue-400">20260504</span>-<span className="text-amber-400">143021</span>-<span className="text-emerald-400">b9138fb</span>
          </div>
        </div>
        <div className="grid grid-cols-3 gap-3 text-xs">
          <div className="bg-blue-500/10 border border-blue-500/20 rounded-lg p-3">
            <div className="font-semibold text-blue-400 mb-1">YYYYMMDD</div>
            <div className="text-zinc-400">UTC date when deploy-dev.yml ran</div>
          </div>
          <div className="bg-amber-500/10 border border-amber-500/20 rounded-lg p-3">
            <div className="font-semibold text-amber-400 mb-1">HHMMSS</div>
            <div className="text-zinc-400">UTC time — makes same-day versions unique</div>
          </div>
          <div className="bg-emerald-500/10 border border-emerald-500/20 rounded-lg p-3">
            <div className="font-semibold text-emerald-400 mb-1">sha7</div>
            <div className="text-zinc-400">First 7 characters of the git commit SHA</div>
          </div>
        </div>
      </div>

      {/* When is a version created */}
      <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5">
        <SectionTitle>When Is a Version Created?</SectionTitle>
        <Prose>
          The <Mono>record-version</Mono> job at the end of <Mono>deploy-dev.yml</Mono> creates a version
          manifest after any successful deployment to dev. The conditions:
        </Prose>
        <div className="mt-3 grid grid-cols-1 md:grid-cols-3 gap-3 text-xs">
          <div className="bg-zinc-950 border border-zinc-800 rounded-lg p-3">
            <div className="flex items-center gap-1.5 mb-1.5">
              <Pill color="blue">apps=true</Pill>
            </div>
            <div className="text-zinc-400">
              Docker images were built, deployed to EKS, and the API health check passed (smoke-test succeeded).
              The image tag is recorded in the manifest.
            </div>
          </div>
          <div className="bg-zinc-950 border border-zinc-800 rounded-lg p-3">
            <div className="flex items-center gap-1.5 mb-1.5">
              <Pill color="purple">dags=true</Pill>
            </div>
            <div className="text-zinc-400">
              Files under <Mono>airflow/</Mono> changed and the sync-dags job successfully uploaded them to
              the MWAA S3 bucket.
            </div>
          </div>
          <div className="bg-zinc-950 border border-zinc-800 rounded-lg p-3">
            <div className="flex items-center gap-1.5 mb-1.5">
              <Pill color="amber">configs=true</Pill>
            </div>
            <div className="text-zinc-400">
              Files under <Mono>configs/</Mono> changed and the sync-configs job uploaded them to the
              pipeline-configs S3 bucket.
            </div>
          </div>
        </div>
        <div className="mt-3 text-xs text-zinc-500">
          A version is only created when at least one artifact flag is <Mono>true</Mono>.
          If nothing deployed (all jobs skipped), no version is recorded.
        </div>
      </div>

      {/* SSM layout */}
      <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5">
        <SectionTitle>SSM Parameter Store Layout</SectionTitle>
        <CodeBlock>{`/nextgen-databridge/
├── versions/
│   └── v20260504-143021-b9138fb    ← immutable; one entry per version
│   └── v20260503-091500-a1b2c3d    ← previous version still readable
│
├── deploy/
│   ├── dev/
│   │   ├── latest-version          ← "v20260504-143021-b9138fb"  (mutable)
│   │   └── last-deployment         ← audit JSON  (mutable)
│   ├── staging/
│   │   ├── latest-version          ← "v20260503-091500-a1b2c3d"  (mutable)
│   │   └── last-deployment         ← audit JSON  (mutable)
│   └── production/
│       ├── latest-version          ← "v20260501-080000-deadbeef"  (mutable)
│       └── last-deployment         ← audit JSON  (mutable)
│
└── image-tag/                      ← backwards-compat (legacy consumers)
    ├── dev                         ← "sha-b9138fbfee..."
    ├── staging                     ← "sha-a1b2c3d..."
    └── production                  ← "sha-deadbeef..."`}</CodeBlock>
        <div className="mt-2 text-xs text-zinc-500">
          Version manifests (<Mono>/versions/*</Mono>) are written once and never overwritten.
          The pointer parameters (<Mono>/deploy/*/latest-version</Mono>) are updated on each
          successful deployment.
        </div>
      </div>

      {/* Manifest structure */}
      <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5">
        <SectionTitle>Version Manifest Structure</SectionTitle>
        <CodeBlock>{`{
  "version_id":  "v20260504-143021-b9138fb",
  "sha":         "b9138fbfee2c4d5e6f7a8b9c0d1e2f3a4b5c6d7",
  "created_at":  "2026-05-04T14:30:21Z",
  "created_by":  "krushna-samanta-s2k2sys",
  "run_id":      "9876543210",
  "run_url":     "https://github.com/org/nextgen-databridge/actions/runs/9876543210",
  "image_tag":   "sha-b9138fbfee2c4d5e6f7a8b9c0d1e2f3a4b5c6d7",
  "artifacts": {
    "apps":    true,   // EKS images were built + deployed + health-checked
    "dags":    false,  // no changes under airflow/
    "configs": true    // pipeline configs were updated
  }
}`}</CodeBlock>
        <div className="mt-2 text-xs text-zinc-500">
          <Mono>image_tag</Mono> is <Mono>null</Mono> when <Mono>artifacts.apps</Mono> is <Mono>false</Mono>.
          In that case the copy-images and K8s rollout steps in promote.yml are skipped automatically.
        </div>
      </div>

    </div>
  )
}

// ── Tab: Promotion Flow ───────────────────────────────────────────────────────

function PromotionTab() {
  return (
    <div className="space-y-6">

      {/* Visual pipeline */}
      <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5">
        <SectionTitle>End-to-End Pipeline</SectionTitle>
        <div className="flex flex-wrap items-center gap-2 text-xs">
          {[
            { label: 'PR merge / manual', bg: 'bg-zinc-700', env: null },
            null,
            { label: 'deploy-dev.yml', bg: 'bg-zinc-700', env: null },
            null,
            { label: 'dev', bg: 'bg-blue-700', env: 'dev' as const },
            null,
            { label: 'promote → staging', bg: 'bg-zinc-700', env: null },
            null,
            { label: 'staging', bg: 'bg-amber-700', env: null },
            null,
            { label: 'promote → prod', bg: 'bg-zinc-700', env: null },
            null,
            { label: 'production', bg: 'bg-emerald-700', env: null },
          ].map((item, i) =>
            item === null ? (
              <ArrowRight key={i} size={14} className="text-zinc-600 flex-shrink-0" />
            ) : (
              <div key={i} className={clsx('px-3 py-1.5 rounded-lg text-white font-medium', item.bg)}>
                {item.label}
              </div>
            )
          )}
        </div>
        <div className="mt-3 text-xs text-zinc-500">
          Every promotion is manual and requires explicit approval via GitHub Environments.
          Code is never automatically deployed to staging or production.
          Infra changes are handled by separate <Mono>infra-*.yml</Mono> workflows and do not run during promotions.
        </div>
      </div>

      {/* dev deployment */}
      <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5">
        <div className="flex items-center gap-2 mb-4">
          <Tag size={14} className="text-blue-400" />
          <SectionTitle className="mb-0">Step 1 — Deploy to Dev</SectionTitle>
          <EnvBadge env="dev" />
          <Pill color="blue">deploy-dev.yml</Pill>
        </div>
        <div className="space-y-0">
          <FlowStep n={1} title="Trigger" desc="PR merged to main (automatic) OR workflow_dispatch (manual). Concurrent runs are cancelled — only the newest deployment proceeds." />
          <FlowStep n={2} title="Detect changes" desc={<>The guard job diffs HEAD^ vs HEAD. Sets three flags: <Mono>deploy_apps</Mono> (backend/frontend/eks/), <Mono>sync_dags</Mono> (airflow/), <Mono>sync_configs</Mono> (configs/). Manual dispatch always sets all three to true.</>} />
          <FlowStep n={3} title="Validate infra" desc="Confirms EKS cluster is ACTIVE, ECR repos exist, and the audit-db secret is present. Fails fast before any build work." />
          <FlowStep n={4} title="Build Docker images (if apps changed)" desc="Matrix build — api, ui, transform run in parallel. Each image is pushed as sha-{sha} and dev aliases to ECR. Build cache is stored in GitHub Actions cache." />
          <FlowStep n={5} title="Sync DAGs / Sync Configs (if changed)" desc="These run in parallel with the build. DAGs are synced to the MWAA S3 bucket. Pipeline configs (JSON) are uploaded to the pipeline-configs S3 bucket. MWAA automatically picks up DAG changes within minutes." />
          <FlowStep n={6} title="Deploy EKS applications (if apps changed)" desc={<>Kubernetes manifests in <Mono>eks/manifests/platform.yaml</Mono> are applied with <Mono>sed</Mono> token substitution for ACCOUNT_ID, IMAGE_TAG, etc. Waits for rollout of both api and ui deployments.</>} />
          <FlowStep n={7} title="Smoke test (if apps deployed)" desc="Waits for pods to be Ready, then polls the /health endpoint up to 10 times (15s apart). Fails the pipeline if health check never returns 200." />
          <FlowStep n={8} title="Record version" desc="Generates VERSION_ID (v{date}-{time}-{sha7}), writes the immutable manifest to SSM, updates /deploy/dev/latest-version pointer. This is what promote.yml reads." last />
        </div>
      </div>

      {/* dev → staging */}
      <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5">
        <div className="flex items-center gap-2 mb-4">
          <Rocket size={14} className="text-amber-400" />
          <SectionTitle className="mb-0">Step 2 — Promote Dev → Staging</SectionTitle>
          <EnvBadge env="staging" />
          <Pill color="zinc">promote.yml</Pill>
        </div>
        <div className="space-y-0">
          <FlowStep n={1} title="Trigger" desc="Manually run promote.yml, select environment=staging. Optionally specify a version_id — if blank, auto-resolves to /deploy/dev/latest-version." />
          <FlowStep n={2} title="Resolve version manifest" desc="Reads the version manifest from SSM. Extracts sha, image_tag, and artifact flags (apps/dags/configs). No chain validation for dev→staging." />
          <FlowStep n={3} title="Copy ECR images (only if apps=true)" desc="Assumes the dev IAM role, pulls the SHA-tagged images from dev ECR. Then assumes the staging role and pushes to staging ECR. Images get two tags: sha-{sha} and staging." />
          <FlowStep n={4} title="Validate infra" desc="Assumes the staging IAM role. Confirms the staging EKS cluster is ACTIVE and audit_db secret exists. Runs in parallel with copy-images." />
          <FlowStep n={5} title="Approval gate" desc="GitHub Environment 'staging' gate triggers. Required reviewers must approve before the deploy job continues. Approval window is configurable in GitHub Environment settings." env="staging" />
          <FlowStep n={6} title="Deploy (checkout exact SHA)" desc={<>Checks out the exact commit SHA from the version manifest — not the current main HEAD. Then selectively: syncs DAGs if <Mono>dags=true</Mono>, uploads configs if <Mono>configs=true</Mono>, applies kubectl manifests if <Mono>apps=true</Mono>. Waits for rollout. Auto-rolls back on failure.</>} />
          <FlowStep n={7} title="Smoke test (only if apps=true)" desc="Same health-check pattern as dev — waits for pods, polls /health." />
          <FlowStep n={8} title="Record deployment" desc="Updates /deploy/staging/latest-version to this version_id. Writes an audit record. This pointer is read during chain validation for the staging→prod promotion." last />
        </div>
      </div>

      {/* staging → prod */}
      <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5">
        <div className="flex items-center gap-2 mb-4">
          <Shield size={14} className="text-emerald-400" />
          <SectionTitle className="mb-0">Step 3 — Promote Staging → Production</SectionTitle>
          <EnvBadge env="production" />
          <Pill color="zinc">promote.yml</Pill>
        </div>

        {/* Chain validation callout */}
        <div className="mb-4 bg-emerald-500/10 border border-emerald-500/25 rounded-lg p-3 flex gap-2.5">
          <CheckCircle2 size={14} className="text-emerald-400 mt-0.5 flex-shrink-0" />
          <div className="text-xs text-emerald-300">
            <span className="font-semibold">Chain validation</span> — Before any deployment work begins,
            the resolve job reads <Mono>/deploy/staging/latest-version</Mono> and verifies it equals the
            version being promoted. If staging is running a different version, the job fails with a clear
            error. This ensures production only ever receives a version that has been verified on staging.
          </div>
        </div>

        <div className="space-y-0">
          <FlowStep n={1} title="Trigger" desc="Manually run promote.yml, select environment=production. Same version_id auto-resolve or explicit input." />
          <FlowStep n={2} title="Chain validation (production only)" desc={<>Reads <Mono>/nextgen-databridge/deploy/staging/latest-version</Mono>. Must match the requested version_id exactly. If staging has moved to a newer version or the version was never deployed to staging, the workflow fails here — before any images are copied or reviewer time is spent.</>} />
          <FlowStep n={3} title="Copy ECR images (only if apps=true)" desc="Same as staging: pulls from dev ECR (images are not re-copied from staging ECR; dev ECR is always the source of truth), pushes to production ECR." />
          <FlowStep n={4} title="Validate infra" desc="Confirms production EKS cluster is ACTIVE and audit_db secret is present in the production account." />
          <FlowStep n={5} title="Approval gate" desc="GitHub Environment 'production' gate triggers. Should be configured with stricter reviewer requirements and a deployment branch restriction to main." env="production" />
          <FlowStep n={6} title="Deploy (checkout exact SHA)" desc="Identical selective deployment logic as staging. Checks out the exact SHA, applies only the artifacts present in the version manifest." />
          <FlowStep n={7} title="Smoke test (only if apps=true)" desc="Health check against the production API." />
          <FlowStep n={8} title="Record deployment" desc="Updates /deploy/production/latest-version to this version_id." last />
        </div>
      </div>

      {/* EKS image lifecycle */}
      <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5">
        <div className="flex items-center gap-2 mb-3">
          <Package size={14} className="text-rose-400" />
          <SectionTitle className="mb-0">EKS Image Lifecycle</SectionTitle>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-xs">
          <div>
            <div className="text-zinc-300 font-semibold mb-1.5">When are images built?</div>
            <Prose>
              Images are built once — in deploy-dev.yml — only when files under <Mono>backend/</Mono>,
              <Mono>frontend/</Mono>, or <Mono>eks/</Mono> change. They are never rebuilt during promotion.
              The exact same bytes that passed the dev smoke test are promoted to staging and then production.
            </Prose>
          </div>
          <div>
            <div className="text-zinc-300 font-semibold mb-1.5">Image tagging</div>
            <Prose>
              Every image gets two ECR tags: an immutable <Mono>sha-{'{sha}'}</Mono> tag (used by manifests
              and version records) and a mutable <Mono>{'{env}'}</Mono> alias (dev/staging/production) that
              always points to the currently-deployed image in that environment.
            </Prose>
          </div>
          <div>
            <div className="text-zinc-300 font-semibold mb-1.5">How are images deployed?</div>
            <Prose>
              The <Mono>eks/manifests/platform.yaml</Mono> template uses <Mono>__IMAGE_TAG__</Mono> and
              <Mono>__REGISTRY__</Mono> placeholders. GitHub Actions substitutes these with the version's
              image tag and the target account's ECR registry before running <Mono>kubectl apply</Mono>.
            </Prose>
          </div>
          <div>
            <div className="text-zinc-300 font-semibold mb-1.5">Rollback</div>
            <Prose>
              If the <Mono>kubectl rollout status</Mono> wait times out or fails, the deploy job runs
              <Mono>kubectl rollout undo</Mono> to revert both api and ui deployments to the previously
              running ReplicaSet. The version is not recorded in SSM if deploy fails.
            </Prose>
          </div>
        </div>
      </div>

      {/* MWAA deployment */}
      <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5">
        <div className="flex items-center gap-2 mb-3">
          <Cloud size={14} className="text-indigo-400" />
          <SectionTitle className="mb-0">MWAA DAG Deployment</SectionTitle>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-xs">
          <div>
            <div className="text-zinc-300 font-semibold mb-1.5">How DAGs are deployed</div>
            <Prose>
              The <Mono>airflow/deploy_to_mwaa.sh</Mono> script syncs the <Mono>airflow/dags/</Mono>
              directory to <Mono>s3://nextgen-databridge-mwaa-{'{env}'}/dags/</Mono> using <Mono>aws s3 sync</Mono>.
              MWAA continuously polls its DAG bucket and automatically picks up new or changed files.
              There is no restart or re-provision required.
            </Prose>
          </div>
          <div>
            <div className="text-zinc-300 font-semibold mb-1.5">When DAGs are deployed</div>
            <Prose>
              In dev: whenever files under <Mono>airflow/</Mono> change (detected by the guard job) or when
              <Mono>deploy_apps=true</Mono> (full redeploy). In staging/production: only when the version
              manifest has <Mono>artifacts.dags=true</Mono> — a version without DAG changes skips the
              MWAA sync step entirely.
            </Prose>
          </div>
          <div>
            <div className="text-zinc-300 font-semibold mb-1.5">Pipeline configs</div>
            <Prose>
              JSON pipeline configs from <Mono>configs/pipelines/</Mono> are uploaded to the
              pipeline-configs S3 bucket and also registered in the PostgreSQL audit database via the
              FastAPI REST API (create or update per pipeline_id). MWAA reads configs at DAG run time,
              not at import time.
            </Prose>
          </div>
          <div>
            <div className="text-zinc-300 font-semibold mb-1.5">DAG registration</div>
            <Prose>
              After config upload, the register-pipelines job calls the platform API to upsert each
              pipeline definition. It also unpauses DAGs via the MWAA Airflow REST API so newly-deployed
              pipelines begin running on their configured cron schedule.
            </Prose>
          </div>
        </div>
      </div>

    </div>
  )
}

// ── Main page ─────────────────────────────────────────────────────────────────

const TABS = [
  { id: 'architecture', label: 'Architecture'    },
  { id: 'environments', label: 'Environments'    },
  { id: 'versioning',   label: 'Versioning'      },
  { id: 'promotion',    label: 'Promotion Flow'  },
]

export default function InfrastructureArchitecture() {
  const [activeTab, setActiveTab] = useState('architecture')

  return (
    <div className="flex flex-col h-full overflow-hidden bg-zinc-950">
      {/* Header */}
      <div className="px-6 pt-5 pb-0 flex-shrink-0">
        <PageHeader
          title="Infrastructure & Deployment"
          subtitle="Architecture overview, environment layout, version management, and promotion flow"
        />
        <div className="mt-4">
          <Tabs tabs={TABS} active={activeTab} onChange={setActiveTab} />
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-y-auto px-6 py-5">
        {activeTab === 'architecture' && <ArchitectureTab />}
        {activeTab === 'environments' && <EnvironmentsTab />}
        {activeTab === 'versioning'   && <VersioningTab />}
        {activeTab === 'promotion'    && <PromotionTab />}
      </div>
    </div>
  )
}

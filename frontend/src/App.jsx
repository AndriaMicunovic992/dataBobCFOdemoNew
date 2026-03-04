/**
 * DataBobIQ — single-file React application
 *
 * Views
 * -----
 * UploadScreen  → shown when no datasets exist
 * SchemaView    → column role editor + relationship manager
 * ActualsView   → baseline chart + paginated data table
 * ScenariosView → what-if scenario CRUD + comparison chart
 * ChatPanel     → SSE-streaming AI assistant (slide-in sidebar)
 */

import React, {
  useState, useEffect, useRef, useCallback, useMemo
} from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer,
} from 'recharts'
import * as api from './api.js'

// ── Design tokens ─────────────────────────────────────────────────────────────

const C = {
  primary:    '#6abbd9',
  primaryDk:  '#4a9ab8',
  primaryLt:  '#e8f5fb',
  bg:         '#f8fafc',
  surface:    '#ffffff',
  border:     '#e2e8f0',
  text:       '#1e293b',
  text2:      '#64748b',
  error:      '#ef4444',
  success:    '#22c55e',
  warning:    '#f59e0b',
}

const SCENARIO_COLORS = ['#6abbd9', '#f59e0b', '#22c55e', '#a78bfa', '#f87171', '#38bdf8']
const ROLES = ['key', 'attribute', 'measure', 'time', 'ignore']
const ROLE_COLOR = {
  key: '#a78bfa', attribute: C.primary, measure: C.success,
  time: C.warning, ignore: '#94a3b8',
}

// ── Utility helpers ───────────────────────────────────────────────────────────

const fmt = (n) =>
  n == null ? '–' : new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 }).format(n)

const fmtCompact = (n) => {
  if (n == null) return '–'
  const abs = Math.abs(n)
  if (abs >= 1e9) return `${(n / 1e9).toFixed(1)}B`
  if (abs >= 1e6) return `${(n / 1e6).toFixed(1)}M`
  if (abs >= 1e3) return `${(n / 1e3).toFixed(0)}K`
  return String(Math.round(n))
}

const fmtPct = (n) =>
  n == null ? '–' : `${n > 0 ? '+' : ''}${n.toFixed(1)}%`

/** Group baseline rows by a dimension column and aggregate the value column. */
function groupRows(rows, columns, groupCol, valueCol, limit = 20) {
  const gIdx = columns.indexOf(groupCol)
  const vIdx = columns.indexOf(valueCol)
  if (gIdx === -1 || vIdx === -1) return []
  const map = new Map()
  for (const row of rows) {
    const key = String(row[gIdx] ?? '(null)')
    const val = Number(row[vIdx]) || 0
    map.set(key, (map.get(key) ?? 0) + val)
  }
  return Array.from(map, ([name, value]) => ({ name, value }))
    .sort((a, b) => Math.abs(b.value) - Math.abs(a.value))
    .slice(0, limit)
}

/**
 * Apply scenario rules client-side.
 * multiplier: row_value *= factor
 * offset: each matching row gets += total_offset / matching_row_count
 */
function applyRules(rows, columns, rules, valueCol) {
  const vIdx = columns.indexOf(valueCol)
  if (vIdx === -1 || !rules?.length) return rows

  // Pre-compute match counts for offset rules
  const matchCounts = rules.map((rule) => {
    if (rule.type !== 'offset') return 0
    let count = 0
    for (const row of rows) {
      if (rowMatchesFilters(row, columns, rule.filters)) count++
    }
    return count
  })

  return rows.map((row) => {
    let value = Number(row[vIdx]) || 0
    for (let i = 0; i < rules.length; i++) {
      const rule = rules[i]
      if (!rowMatchesFilters(row, columns, rule.filters)) continue
      if (rule.type === 'multiplier') {
        value *= Number(rule.factor) || 1
      } else {
        value += (Number(rule.offset) || 0) / (matchCounts[i] || 1)
      }
    }
    const newRow = [...row]
    newRow[vIdx] = value
    return newRow
  })
}

function rowMatchesFilters(row, columns, filters) {
  if (!filters) return true
  for (const [col, vals] of Object.entries(filters)) {
    const idx = columns.indexOf(col)
    if (idx === -1) continue
    if (!vals.map(String).includes(String(row[idx]))) return false
  }
  return true
}

function sumColumn(rows, columns, valueCol) {
  const vIdx = columns.indexOf(valueCol)
  if (vIdx === -1) return null
  return rows.reduce((s, r) => s + (Number(r[vIdx]) || 0), 0)
}

// ── Shared UI primitives ──────────────────────────────────────────────────────

function Card({ children, style }) {
  return (
    <div style={{
      background: C.surface, border: `1px solid ${C.border}`,
      borderRadius: 12, padding: 20, ...style,
    }}>
      {children}
    </div>
  )
}

function Btn({ children, onClick, variant = 'primary', small, style, disabled }) {
  const variants = {
    primary:   { background: C.primary,    color: '#fff' },
    secondary: { background: C.bg,         color: C.text,  border: `1px solid ${C.border}` },
    danger:    { background: '#fee2e2',     color: C.error },
    ghost:     { background: 'transparent', color: C.text2, border: `1px solid ${C.border}` },
  }
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      style={{
        padding: small ? '5px 12px' : '8px 18px',
        borderRadius: 8, border: 'none', cursor: disabled ? 'not-allowed' : 'pointer',
        fontFamily: 'inherit', fontWeight: 600, fontSize: small ? 12 : 14,
        transition: 'opacity .15s', opacity: disabled ? .5 : 1,
        ...variants[variant], ...style,
      }}
    >
      {children}
    </button>
  )
}

function Badge({ children, color = C.primary }) {
  return (
    <span style={{
      background: color + '22', color, border: `1px solid ${color}55`,
      borderRadius: 4, padding: '2px 7px', fontSize: 11, fontWeight: 600,
    }}>
      {children}
    </span>
  )
}

function Tag({ children }) {
  return (
    <span style={{
      background: C.primaryLt, color: C.primaryDk,
      borderRadius: 4, padding: '1px 7px', fontSize: 11, fontWeight: 600,
    }}>
      {children}
    </span>
  )
}

function Spinner() {
  return <span style={{ color: C.primary }}>⏳</span>
}

// ── UploadScreen ──────────────────────────────────────────────────────────────

function UploadScreen({ onUpload }) {
  const [dragging, setDragging] = useState(false)
  const [uploading, setUploading] = useState(false)
  const [progress, setProgress]  = useState('')
  const [error, setError]        = useState(null)
  const inputRef = useRef(null)

  const handleFile = async (file) => {
    if (!file) return
    setUploading(true)
    setError(null)
    setProgress('Uploading file…')
    try {
      const datasets = await api.uploadFile(file)
      setProgress('Processing schema…')
      onUpload(datasets)
    } catch (e) {
      setError(e.message)
    } finally {
      setUploading(false)
      setProgress('')
    }
  }

  return (
    <div style={{
      minHeight: '100vh', background: C.bg,
      display: 'flex', flexDirection: 'column', alignItems: 'center',
      justifyContent: 'center', padding: 24,
    }}>
      {/* Logo */}
      <div style={{ marginBottom: 48, textAlign: 'center' }}>
        <div style={{ fontSize: 36, fontWeight: 800, color: C.text, letterSpacing: -1 }}>
          data<span style={{ color: C.primary }}>Bob</span>IQ
        </div>
        <div style={{ color: C.text2, marginTop: 6, fontSize: 15 }}>
          AI-powered financial analysis
        </div>
      </div>

      {/* Drop zone */}
      <div
        style={{
          width: 500, maxWidth: '92vw',
          border: `2px dashed ${dragging ? C.primary : C.border}`,
          borderRadius: 20, padding: '52px 40px', textAlign: 'center',
          background: dragging ? C.primaryLt : C.surface,
          transition: 'all .2s', cursor: uploading ? 'default' : 'pointer',
          boxShadow: '0 4px 24px rgba(0,0,0,.06)',
        }}
        onClick={() => !uploading && inputRef.current?.click()}
        onDragOver={(e) => { e.preventDefault(); setDragging(true) }}
        onDragLeave={() => setDragging(false)}
        onDrop={(e) => { e.preventDefault(); setDragging(false); handleFile(e.dataTransfer.files[0]) }}
      >
        <div style={{ fontSize: 48, marginBottom: 16 }}>
          {uploading ? '⏳' : '📂'}
        </div>

        {uploading ? (
          <div>
            <div style={{ color: C.primary, fontWeight: 700, fontSize: 16, marginBottom: 8 }}>
              {progress}
            </div>
            <div style={{ color: C.text2, fontSize: 13 }}>
              Parsing columns and detecting schema…
            </div>
          </div>
        ) : (
          <>
            <div style={{ fontWeight: 700, fontSize: 18, color: C.text, marginBottom: 8 }}>
              Drag & drop your financial data
            </div>
            <div style={{ color: C.text2, fontSize: 14, marginBottom: 20 }}>
              or click to browse files
            </div>
            <div style={{ display: 'flex', gap: 8, justifyContent: 'center', flexWrap: 'wrap' }}>
              {['.xlsx', '.xls', '.csv', '.tsv'].map((ext) => <Tag key={ext}>{ext}</Tag>)}
            </div>
          </>
        )}
      </div>

      {error && (
        <div style={{
          marginTop: 20, padding: '12px 20px', borderRadius: 10,
          background: '#fee2e2', color: C.error, fontSize: 14,
          maxWidth: 500, width: '100%',
        }}>
          ⚠️ {error}
        </div>
      )}

      <input
        ref={inputRef} type="file" style={{ display: 'none' }}
        accept=".xlsx,.xls,.csv,.tsv"
        onChange={(e) => handleFile(e.target.files?.[0])}
      />
    </div>
  )
}

// ── SchemaView ────────────────────────────────────────────────────────────────

function SchemaView({ schema, allSchemas, onRefresh }) {
  const { dataset, columns, relationships } = schema
  const [saving, setSaving] = useState({})
  const [showAddRel, setShowAddRel] = useState(false)
  const [newRel, setNewRel] = useState({
    source_dataset_id: dataset.id, source_column: '',
    target_dataset_id: '',         target_column: '',
  })
  const [relError, setRelError] = useState(null)

  const updateCol = async (col, patch) => {
    setSaving((s) => ({ ...s, [col.id]: true }))
    try {
      await api.updateColumnRole(dataset.id, col.id, patch)
      onRefresh()
    } finally {
      setSaving((s) => ({ ...s, [col.id]: false }))
    }
  }

  const handleAddRel = async () => {
    setRelError(null)
    try {
      await api.createRelationship(newRel)
      setShowAddRel(false)
      setNewRel({ source_dataset_id: dataset.id, source_column: '', target_dataset_id: '', target_column: '' })
      onRefresh()
    } catch (e) { setRelError(e.message) }
  }

  const handleDelRel = async (id) => {
    if (!confirm('Delete this relationship?')) return
    await api.deleteRelationship(id)
    onRefresh()
  }

  const otherSchemas = allSchemas.filter((s) => s.dataset.id !== dataset.id)
  const targetSchema = allSchemas.find((s) => s.dataset.id === newRel.target_dataset_id)

  return (
    <div style={{ padding: '24px 28px' }}>
      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 24 }}>
        <h2 style={{ margin: 0, fontSize: 20, fontWeight: 800, color: C.text }}>
          Schema — {dataset.name}
        </h2>
        <Badge>{(dataset.row_count ?? 0).toLocaleString()} rows</Badge>
        {dataset.ai_analyzed && <Badge color={C.success}>AI analysed</Badge>}
      </div>

      {/* Column table */}
      <Card style={{ padding: 0, overflow: 'hidden', marginBottom: 28 }}>
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
            <thead>
              <tr style={{ background: C.bg, borderBottom: `1px solid ${C.border}` }}>
                {['Column name', 'Display name', 'Type', 'Role', 'Unique vals', 'Sample values'].map((h) => (
                  <th key={h} style={{ padding: '10px 14px', textAlign: 'left', fontWeight: 700, color: C.text2, fontSize: 11, whiteSpace: 'nowrap' }}>
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {columns.map((col, i) => (
                <tr key={col.id} style={{ borderBottom: `1px solid ${C.border}`, background: i % 2 === 0 ? C.surface : C.bg }}>
                  <td style={{ padding: '7px 14px', fontFamily: 'monospace', fontSize: 12, color: C.text }}>
                    {col.column_name}
                  </td>
                  <td style={{ padding: '7px 14px' }}>
                    <input
                      key={col.display_name}
                      defaultValue={col.display_name}
                      onBlur={(e) => {
                        if (e.target.value !== col.display_name)
                          updateCol(col, { display_name: e.target.value })
                      }}
                      style={{
                        border: `1px solid ${C.border}`, borderRadius: 6,
                        padding: '4px 8px', fontSize: 12, width: 160,
                      }}
                    />
                  </td>
                  <td style={{ padding: '7px 14px' }}>
                    <Tag>{col.data_type}</Tag>
                  </td>
                  <td style={{ padding: '7px 14px' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                      <select
                        value={col.column_role}
                        onChange={(e) => updateCol(col, { column_role: e.target.value })}
                        style={{
                          border: `1px solid ${C.border}`, borderRadius: 6,
                          padding: '4px 8px', fontSize: 12,
                          color: ROLE_COLOR[col.column_role] ?? C.text,
                          fontWeight: 600,
                        }}
                      >
                        {ROLES.map((r) => <option key={r} value={r}>{r}</option>)}
                      </select>
                      {col.ai_suggestion?.suggested_role && col.ai_suggestion.suggested_role !== col.column_role && (
                        <span
                          title={col.ai_suggestion.reasoning ?? 'AI suggestion'}
                          onClick={() => updateCol(col, { column_role: col.ai_suggestion.suggested_role })}
                          style={{ cursor: 'pointer' }}
                        >
                          <Badge color={C.warning}>AI: {col.ai_suggestion.suggested_role} ✓</Badge>
                        </span>
                      )}
                      {saving[col.id] && <Spinner />}
                    </div>
                  </td>
                  <td style={{ padding: '7px 14px', color: C.text2, fontSize: 12 }}>
                    {col.unique_count?.toLocaleString() ?? '–'}
                  </td>
                  <td style={{ padding: '7px 14px', color: C.text2, fontSize: 12, maxWidth: 220 }}>
                    {(col.sample_values ?? []).slice(0, 5).join(', ')}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Card>

      {/* Relationships */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 14 }}>
        <h3 style={{ margin: 0, fontSize: 16, fontWeight: 700, color: C.text }}>Relationships</h3>
        <Btn small onClick={() => setShowAddRel((v) => !v)}>+ Add relationship</Btn>
      </div>

      {showAddRel && (
        <Card style={{ marginBottom: 16 }}>
          <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', alignItems: 'flex-end' }}>
            <div>
              <label style={{ fontSize: 11, fontWeight: 700, color: C.text2, display: 'block', marginBottom: 4 }}>Source column</label>
              <select
                value={newRel.source_column}
                onChange={(e) => setNewRel((r) => ({ ...r, source_column: e.target.value }))}
                style={{ border: `1px solid ${C.border}`, borderRadius: 6, padding: '6px 10px' }}
              >
                <option value="">Select column…</option>
                {columns.map((c) => <option key={c.id} value={c.column_name}>{c.column_name}</option>)}
              </select>
            </div>
            <div>
              <label style={{ fontSize: 11, fontWeight: 700, color: C.text2, display: 'block', marginBottom: 4 }}>Target dataset</label>
              <select
                value={newRel.target_dataset_id}
                onChange={(e) => setNewRel((r) => ({ ...r, target_dataset_id: e.target.value, target_column: '' }))}
                style={{ border: `1px solid ${C.border}`, borderRadius: 6, padding: '6px 10px' }}
              >
                <option value="">Select dataset…</option>
                {otherSchemas.map((s) => <option key={s.dataset.id} value={s.dataset.id}>{s.dataset.name}</option>)}
              </select>
            </div>
            {targetSchema && (
              <div>
                <label style={{ fontSize: 11, fontWeight: 700, color: C.text2, display: 'block', marginBottom: 4 }}>Target column</label>
                <select
                  value={newRel.target_column}
                  onChange={(e) => setNewRel((r) => ({ ...r, target_column: e.target.value }))}
                  style={{ border: `1px solid ${C.border}`, borderRadius: 6, padding: '6px 10px' }}
                >
                  <option value="">Select column…</option>
                  {targetSchema.columns.map((c) => <option key={c.id} value={c.column_name}>{c.column_name}</option>)}
                </select>
              </div>
            )}
            <Btn
              onClick={handleAddRel}
              disabled={!newRel.source_column || !newRel.target_dataset_id || !newRel.target_column}
            >
              Create
            </Btn>
            <Btn variant="ghost" onClick={() => { setShowAddRel(false); setRelError(null) }}>Cancel</Btn>
          </div>
          {relError && <div style={{ color: C.error, fontSize: 13, marginTop: 10 }}>⚠️ {relError}</div>}
        </Card>
      )}

      {relationships.length === 0 && !showAddRel && (
        <div style={{ color: C.text2, fontSize: 13, padding: '12px 0' }}>
          No relationships defined. Add relationships to enable server-side joins with dimension tables.
        </div>
      )}

      {relationships.length > 0 && (
        <Card style={{ padding: 0, overflow: 'hidden' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
            <thead>
              <tr style={{ background: C.bg, borderBottom: `1px solid ${C.border}` }}>
                {['Source', 'Target', 'Coverage', ''].map((h) => (
                  <th key={h} style={{ padding: '10px 14px', textAlign: 'left', fontWeight: 700, color: C.text2, fontSize: 11 }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {relationships.map((rel) => {
                const srcDs = allSchemas.find((s) => s.dataset.id === rel.source_dataset_id)
                const tgtDs = allSchemas.find((s) => s.dataset.id === rel.target_dataset_id)
                const pct = rel.coverage_pct ?? 0
                const barColor = pct > 80 ? C.success : pct > 50 ? C.warning : C.error
                return (
                  <tr key={rel.id} style={{ borderBottom: `1px solid ${C.border}` }}>
                    <td style={{ padding: '9px 14px' }}>
                      <span style={{ fontWeight: 700 }}>{srcDs?.dataset.name ?? rel.source_dataset_id}</span>
                      <code style={{ color: C.primary, fontSize: 11, marginLeft: 6 }}>.{rel.source_column}</code>
                    </td>
                    <td style={{ padding: '9px 14px' }}>
                      <span style={{ fontWeight: 700 }}>{tgtDs?.dataset.name ?? rel.target_dataset_id}</span>
                      <code style={{ color: C.primary, fontSize: 11, marginLeft: 6 }}>.{rel.target_column}</code>
                    </td>
                    <td style={{ padding: '9px 14px' }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                        <div style={{ width: 80, height: 5, background: C.border, borderRadius: 3, overflow: 'hidden' }}>
                          <div style={{ height: '100%', width: `${pct}%`, background: barColor }} />
                        </div>
                        <span style={{ fontSize: 12, color: C.text2 }}>{pct}%</span>
                      </div>
                    </td>
                    <td style={{ padding: '9px 14px' }}>
                      <Btn small variant="danger" onClick={() => handleDelRel(rel.id)}>Delete</Btn>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </Card>
      )}
    </div>
  )
}

// ── ActualsView ───────────────────────────────────────────────────────────────

const PAGE_SIZE = 50

function ActualsView({ schema, baseline, baselineLoading, onLoadBaseline }) {
  const { dataset, columns } = schema
  const [groupCol, setGroupCol] = useState(null)
  const [valueCol, setValueCol] = useState(null)
  const [page, setPage]         = useState(0)

  const measureCols = columns.filter((c) => c.column_role === 'measure')
  const dimCols     = columns.filter((c) => ['key', 'attribute', 'time'].includes(c.column_role))

  // Auto-pick first available measure + dimension when baseline first loads
  useEffect(() => {
    if (!baseline) return
    const bCols = baseline.columns
    if (!valueCol) {
      const m = measureCols.find((c) => bCols.includes(c.column_name))
      if (m) setValueCol(m.column_name)
    }
    if (!groupCol) {
      const d = dimCols.find((c) => bCols.includes(c.column_name))
      if (d) setGroupCol(d.column_name)
    }
  }, [baseline])

  const chartData = useMemo(() => {
    if (!baseline || !groupCol || !valueCol) return []
    return groupRows(baseline.data, baseline.columns, groupCol, valueCol)
  }, [baseline, groupCol, valueCol])

  const total = useMemo(() => {
    if (!baseline || !valueCol) return null
    return sumColumn(baseline.data, baseline.columns, valueCol)
  }, [baseline, valueCol])

  const totalPages = baseline ? Math.ceil(baseline.data.length / PAGE_SIZE) : 0
  const pagedRows  = useMemo(() => {
    if (!baseline) return []
    return baseline.data.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE)
  }, [baseline, page])

  const colLabel = (name) => columns.find((c) => c.column_name === name)?.display_name ?? name
  const isMeasure = (name) => columns.find((c) => c.column_name === name)?.column_role === 'measure'

  return (
    <div style={{ padding: '24px 28px' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 14, marginBottom: 24 }}>
        <h2 style={{ margin: 0, fontSize: 20, fontWeight: 800, color: C.text }}>
          Actuals — {dataset.name}
        </h2>
        {!baseline && (
          <Btn onClick={() => onLoadBaseline(schema)} disabled={baselineLoading}>
            {baselineLoading ? '⏳ Loading…' : 'Load data'}
          </Btn>
        )}
        {baseline && (
          <Btn small variant="ghost" onClick={() => onLoadBaseline(schema)} disabled={baselineLoading}>
            ↺ Reload
          </Btn>
        )}
      </div>

      {/* Empty / loading states */}
      {!baseline && !baselineLoading && (
        <div style={{ textAlign: 'center', paddingTop: 80, color: C.text2 }}>
          <div style={{ fontSize: 48, marginBottom: 12 }}>📊</div>
          <div style={{ fontWeight: 700, fontSize: 16, marginBottom: 6 }}>No data loaded</div>
          <div style={{ fontSize: 13 }}>Click "Load data" to fetch the enriched baseline from the server.</div>
        </div>
      )}
      {baselineLoading && (
        <div style={{ textAlign: 'center', paddingTop: 80, color: C.primary }}>
          <div style={{ fontSize: 48, marginBottom: 12 }}>⏳</div>
          <div style={{ fontWeight: 700 }}>Loading baseline…</div>
        </div>
      )}

      {baseline && (
        <>
          {/* Controls + summary card */}
          <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', alignItems: 'flex-end', marginBottom: 20 }}>
            <div>
              <label style={{ fontSize: 11, fontWeight: 700, color: C.text2, display: 'block', marginBottom: 4 }}>Group by</label>
              <select
                value={groupCol ?? ''}
                onChange={(e) => { setGroupCol(e.target.value); setPage(0) }}
                style={{ border: `1px solid ${C.border}`, borderRadius: 6, padding: '6px 10px' }}
              >
                {dimCols.filter((c) => baseline.columns.includes(c.column_name)).map((c) => (
                  <option key={c.column_name} value={c.column_name}>{c.display_name}</option>
                ))}
                {baseline.columns
                  .filter((c) => !columns.some((dc) => dc.column_name === c))
                  .map((c) => <option key={c} value={c}>{c}</option>)}
              </select>
            </div>
            <div>
              <label style={{ fontSize: 11, fontWeight: 700, color: C.text2, display: 'block', marginBottom: 4 }}>Measure</label>
              <select
                value={valueCol ?? ''}
                onChange={(e) => setValueCol(e.target.value)}
                style={{ border: `1px solid ${C.border}`, borderRadius: 6, padding: '6px 10px' }}
              >
                {measureCols.filter((c) => baseline.columns.includes(c.column_name)).map((c) => (
                  <option key={c.column_name} value={c.column_name}>{c.display_name}</option>
                ))}
              </select>
            </div>

            {total != null && (
              <Card style={{ padding: '10px 20px', marginLeft: 'auto' }}>
                <div style={{ fontSize: 11, color: C.text2, fontWeight: 700, marginBottom: 2 }}>
                  {colLabel(valueCol)} — total
                </div>
                <div style={{ fontSize: 24, fontWeight: 800, color: C.primary }}>{fmtCompact(total)}</div>
                <div style={{ fontSize: 11, color: C.text2 }}>{baseline.data.length.toLocaleString()} rows</div>
              </Card>
            )}
          </div>

          {/* Chart */}
          {chartData.length > 0 && (
            <Card style={{ marginBottom: 20 }}>
              <div style={{ fontWeight: 700, fontSize: 14, color: C.text, marginBottom: 12 }}>
                {colLabel(valueCol)} by {colLabel(groupCol)} — top {chartData.length}
              </div>
              <ResponsiveContainer width="100%" height={280}>
                <BarChart data={chartData} margin={{ top: 0, right: 10, left: 10, bottom: 80 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke={C.border} />
                  <XAxis
                    dataKey="name"
                    tick={{ fontSize: 11, fill: C.text2 }}
                    angle={-35} textAnchor="end" interval={0}
                  />
                  <YAxis tickFormatter={fmtCompact} tick={{ fontSize: 11, fill: C.text2 }} />
                  <Tooltip
                    formatter={(v) => [fmt(v), colLabel(valueCol)]}
                    contentStyle={{ fontFamily: 'Plus Jakarta Sans, sans-serif', fontSize: 13, borderRadius: 8 }}
                  />
                  <Bar dataKey="value" fill={C.primary} radius={[4, 4, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </Card>
          )}

          {/* Data table */}
          <Card style={{ padding: 0, overflow: 'hidden' }}>
            <div style={{ padding: '10px 14px', borderBottom: `1px solid ${C.border}`, fontSize: 12, fontWeight: 700, color: C.text2 }}>
              Rows {page * PAGE_SIZE + 1}–{Math.min((page + 1) * PAGE_SIZE, baseline.data.length)} of {baseline.data.length.toLocaleString()}
            </div>
            <div style={{ overflowX: 'auto' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
                <thead>
                  <tr style={{ background: C.bg, borderBottom: `1px solid ${C.border}` }}>
                    {baseline.columns.map((c) => (
                      <th key={c} style={{ padding: '8px 12px', textAlign: 'left', fontWeight: 700, color: C.text2, fontSize: 11, whiteSpace: 'nowrap' }}>
                        {colLabel(c)}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {pagedRows.map((row, i) => (
                    <tr key={i} style={{ borderBottom: `1px solid ${C.border}`, background: i % 2 ? C.bg : C.surface }}>
                      {row.map((cell, j) => {
                        const isNum = isMeasure(baseline.columns[j])
                        return (
                          <td key={j} style={{
                            padding: '6px 12px', color: C.text, whiteSpace: 'nowrap',
                            textAlign: isNum ? 'right' : 'left',
                          }}>
                            {isNum && cell != null ? fmt(cell) : String(cell ?? '–')}
                          </td>
                        )
                      })}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {totalPages > 1 && (
              <div style={{ padding: '10px 14px', display: 'flex', gap: 8, justifyContent: 'center', alignItems: 'center' }}>
                <Btn small variant="ghost" disabled={page === 0} onClick={() => setPage((p) => p - 1)}>← Prev</Btn>
                <span style={{ fontSize: 12, color: C.text2 }}>Page {page + 1} / {totalPages}</span>
                <Btn small variant="ghost" disabled={page >= totalPages - 1} onClick={() => setPage((p) => p + 1)}>Next →</Btn>
              </div>
            )}
          </Card>
        </>
      )}
    </div>
  )
}

// ── ScenariosView ─────────────────────────────────────────────────────────────

const DEFAULT_RULE = { name: '', type: 'multiplier', factor: 1.05, offset: 0, filters: {} }

function ScenariosView({ schema, baseline, scenarios, onScenariosChange }) {
  const { dataset, columns } = schema
  const [activeId, setActiveId]       = useState(null)
  const [showNewForm, setShowNewForm] = useState(false)
  const [newName, setNewName]         = useState('')
  const [showAddRule, setShowAddRule] = useState(false)
  const [newRule, setNewRule]         = useState(DEFAULT_RULE)
  const [saving, setSaving]           = useState(false)

  const active = scenarios.find((s) => s.id === activeId) ?? scenarios[0]

  const measureCols = columns.filter((c) => c.column_role === 'measure')
  const dimCols     = columns.filter((c) => ['key', 'attribute'].includes(c.column_role))

  const valueCol = useMemo(() => {
    if (!baseline) return null
    return measureCols.find((c) => baseline.columns.includes(c.column_name))?.column_name ?? null
  }, [baseline, measureCols])

  const scenarioRows = useMemo(() => {
    if (!baseline || !active || !valueCol) return null
    return applyRules(baseline.data, baseline.columns, active.rules ?? [], valueCol)
  }, [baseline, active, valueCol])

  const compareData = useMemo(() => {
    if (!baseline || !scenarioRows || !valueCol) return []
    const groupColName = dimCols.find((c) => baseline.columns.includes(c.column_name))?.column_name
    if (!groupColName) return []
    const base = groupRows(baseline.data, baseline.columns, groupColName, valueCol, 15)
    const scen = groupRows(scenarioRows, baseline.columns, groupColName, valueCol, 15)
    const scenMap = new Map(scen.map((r) => [r.name, r.value]))
    return base.map((r) => ({ name: r.name, Baseline: r.value, Scenario: scenMap.get(r.name) ?? 0 }))
  }, [baseline, scenarioRows, valueCol, dimCols])

  const totalBase = useMemo(() => baseline && valueCol ? sumColumn(baseline.data, baseline.columns, valueCol) : null, [baseline, valueCol])
  const totalScen = useMemo(() => scenarioRows && valueCol ? sumColumn(scenarioRows, baseline.columns, valueCol) : null, [scenarioRows, baseline, valueCol])
  const delta    = totalBase != null && totalScen != null ? totalScen - totalBase : null
  const deltaPct = delta != null && totalBase ? (delta / Math.abs(totalBase)) * 100 : null

  const createScenario = async () => {
    if (!newName.trim()) return
    setSaving(true)
    try {
      const sc = await api.createScenario({
        name: newName.trim(),
        dataset_id: dataset.id,
        rules: [],
        color: SCENARIO_COLORS[scenarios.length % SCENARIO_COLORS.length],
      })
      onScenariosChange([...scenarios, sc])
      setActiveId(sc.id)
      setNewName('')
      setShowNewForm(false)
    } finally { setSaving(false) }
  }

  const saveRules = async (scenarioId, rules) => {
    const saved = await api.updateScenario(scenarioId, { rules })
    onScenariosChange(scenarios.map((s) => s.id === saved.id ? saved : s))
  }

  const addRule = async () => {
    if (!active) return
    const rule = { ...newRule }
    if (rule.type === 'multiplier') delete rule.offset
    else delete rule.factor
    await saveRules(active.id, [...(active.rules ?? []), rule])
    setShowAddRule(false)
    setNewRule(DEFAULT_RULE)
  }

  const deleteRule = async (idx) => {
    if (!active) return
    await saveRules(active.id, (active.rules ?? []).filter((_, i) => i !== idx))
  }

  const deleteScenario = async (id) => {
    if (!confirm('Delete this scenario?')) return
    await api.deleteScenario(id)
    onScenariosChange(scenarios.filter((s) => s.id !== id))
    if (activeId === id) setActiveId(null)
  }

  return (
    <div style={{ padding: '24px 28px' }}>
      <h2 style={{ margin: '0 0 24px', fontSize: 20, fontWeight: 800, color: C.text }}>
        What-If Scenarios
      </h2>
      <div style={{ display: 'flex', gap: 24, alignItems: 'flex-start' }}>

        {/* Left: scenario list */}
        <div style={{ width: 220, flexShrink: 0 }}>
          <Btn small onClick={() => setShowNewForm((v) => !v)} style={{ width: '100%', marginBottom: 12 }}>
            + New scenario
          </Btn>
          {showNewForm && (
            <div style={{ marginBottom: 12 }}>
              <input
                value={newName}
                onChange={(e) => setNewName(e.target.value)}
                onKeyDown={(e) => e.key === 'Enter' && createScenario()}
                placeholder="Scenario name"
                autoFocus
                style={{
                  width: '100%', boxSizing: 'border-box', marginBottom: 6,
                  border: `1px solid ${C.border}`, borderRadius: 6, padding: '7px 10px', fontSize: 13,
                }}
              />
              <div style={{ display: 'flex', gap: 6 }}>
                <Btn small onClick={createScenario} disabled={saving || !newName.trim()}>Create</Btn>
                <Btn small variant="ghost" onClick={() => setShowNewForm(false)}>Cancel</Btn>
              </div>
            </div>
          )}
          {scenarios.map((sc) => (
            <div
              key={sc.id}
              onClick={() => setActiveId(sc.id)}
              style={{
                padding: '10px 12px', borderRadius: 10, cursor: 'pointer', marginBottom: 6,
                background: sc.id === active?.id ? C.primaryLt : C.bg,
                border: `1px solid ${sc.id === active?.id ? C.primary : C.border}`,
              }}
            >
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 7, fontWeight: 700, fontSize: 13, color: C.text }}>
                  <span style={{ width: 9, height: 9, borderRadius: '50%', background: sc.color ?? C.primary, display: 'inline-block', flexShrink: 0 }} />
                  {sc.name}
                </div>
                <button
                  onClick={(e) => { e.stopPropagation(); deleteScenario(sc.id) }}
                  style={{ background: 'none', border: 'none', cursor: 'pointer', color: C.text2, fontSize: 16, lineHeight: 1 }}
                >×</button>
              </div>
              <div style={{ fontSize: 11, color: C.text2, marginTop: 3, paddingLeft: 16 }}>
                {(sc.rules ?? []).length} rule{sc.rules?.length !== 1 ? 's' : ''}
              </div>
            </div>
          ))}
          {scenarios.length === 0 && !showNewForm && (
            <div style={{ color: C.text2, fontSize: 12, lineHeight: 1.6 }}>
              Create a scenario to explore what-if adjustments.
            </div>
          )}
        </div>

        {/* Right: scenario detail */}
        {active ? (
          <div style={{ flex: 1 }}>
            {/* Summary cards */}
            {baseline && valueCol && (
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 12, marginBottom: 20 }}>
                {[
                  { label: 'Baseline', value: totalBase, color: C.text2, isPct: false },
                  { label: `${active.name}`, value: totalScen, color: active.color ?? C.primary, isPct: false },
                  { label: 'Delta', value: delta, color: delta != null && delta >= 0 ? C.success : C.error, isPct: false },
                  { label: 'Delta %', value: deltaPct, color: delta != null && delta >= 0 ? C.success : C.error, isPct: true },
                ].map((item) => (
                  <Card key={item.label} style={{ padding: '12px 16px' }}>
                    <div style={{ fontSize: 10, color: C.text2, fontWeight: 700, marginBottom: 4, textTransform: 'uppercase', letterSpacing: .5 }}>{item.label}</div>
                    <div style={{ fontSize: 22, fontWeight: 800, color: item.color }}>
                      {item.isPct ? fmtPct(item.value) : fmtCompact(item.value)}
                    </div>
                  </Card>
                ))}
              </div>
            )}

            {/* Comparison chart */}
            {compareData.length > 0 && (
              <Card style={{ marginBottom: 20 }}>
                <div style={{ fontWeight: 700, fontSize: 14, color: C.text, marginBottom: 12 }}>
                  Baseline vs {active.name}
                </div>
                <ResponsiveContainer width="100%" height={240}>
                  <BarChart data={compareData} margin={{ top: 0, right: 10, left: 10, bottom: 60 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke={C.border} />
                    <XAxis dataKey="name" tick={{ fontSize: 11, fill: C.text2 }} angle={-30} textAnchor="end" interval={0} />
                    <YAxis tickFormatter={fmtCompact} tick={{ fontSize: 11, fill: C.text2 }} />
                    <Tooltip
                      formatter={(v) => [fmt(v)]}
                      contentStyle={{ fontFamily: 'Plus Jakarta Sans, sans-serif', fontSize: 12, borderRadius: 8 }}
                    />
                    <Legend wrapperStyle={{ fontSize: 12, paddingTop: 8 }} />
                    <Bar dataKey="Baseline" fill={C.border} radius={[3, 3, 0, 0]} />
                    <Bar dataKey="Scenario" fill={active.color ?? C.primary} radius={[3, 3, 0, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </Card>
            )}

            {/* Rules editor */}
            <Card>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 14 }}>
                <div style={{ fontWeight: 700, fontSize: 14, color: C.text }}>Rules</div>
                <Btn small onClick={() => setShowAddRule((v) => !v)}>+ Add rule</Btn>
              </div>

              {showAddRule && (
                <div style={{ background: C.bg, border: `1px solid ${C.border}`, borderRadius: 10, padding: 16, marginBottom: 16 }}>
                  <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', marginBottom: 12 }}>
                    <div style={{ flex: '2 1 160px' }}>
                      <label style={{ fontSize: 11, fontWeight: 700, color: C.text2, display: 'block', marginBottom: 4 }}>Rule name</label>
                      <input
                        value={newRule.name}
                        onChange={(e) => setNewRule((r) => ({ ...r, name: e.target.value }))}
                        placeholder="e.g. Cost increase Q1"
                        style={{ width: '100%', boxSizing: 'border-box', border: `1px solid ${C.border}`, borderRadius: 6, padding: '6px 10px', fontSize: 13 }}
                      />
                    </div>
                    <div>
                      <label style={{ fontSize: 11, fontWeight: 700, color: C.text2, display: 'block', marginBottom: 4 }}>Type</label>
                      <select
                        value={newRule.type}
                        onChange={(e) => setNewRule((r) => ({ ...r, type: e.target.value }))}
                        style={{ border: `1px solid ${C.border}`, borderRadius: 6, padding: '6px 10px' }}
                      >
                        <option value="multiplier">Multiplier (%)</option>
                        <option value="offset">Offset (amount)</option>
                      </select>
                    </div>
                    <div>
                      <label style={{ fontSize: 11, fontWeight: 700, color: C.text2, display: 'block', marginBottom: 4 }}>
                        {newRule.type === 'multiplier' ? 'Factor (e.g. 1.05 = +5%)' : 'Offset amount'}
                      </label>
                      <input
                        type="number"
                        step={newRule.type === 'multiplier' ? 0.01 : 1000}
                        value={newRule.type === 'multiplier' ? newRule.factor : newRule.offset}
                        onChange={(e) => {
                          const v = parseFloat(e.target.value)
                          setNewRule((r) => newRule.type === 'multiplier' ? { ...r, factor: v } : { ...r, offset: v })
                        }}
                        style={{ width: 130, border: `1px solid ${C.border}`, borderRadius: 6, padding: '6px 10px', fontSize: 13 }}
                      />
                    </div>
                  </div>
                  <div style={{ display: 'flex', gap: 8 }}>
                    <Btn small onClick={addRule}>Add rule</Btn>
                    <Btn small variant="ghost" onClick={() => { setShowAddRule(false); setNewRule(DEFAULT_RULE) }}>Cancel</Btn>
                  </div>
                </div>
              )}

              {(active.rules ?? []).length === 0 && !showAddRule ? (
                <div style={{ color: C.text2, fontSize: 13 }}>
                  No rules yet. Add a rule to define the what-if adjustment.
                </div>
              ) : (
                (active.rules ?? []).map((rule, i) => (
                  <div key={i} style={{
                    display: 'flex', alignItems: 'center', gap: 12, padding: '9px 14px',
                    background: C.bg, borderRadius: 8, marginBottom: 8, border: `1px solid ${C.border}`,
                  }}>
                    <div style={{ flex: 1 }}>
                      <span style={{ fontWeight: 700, fontSize: 13, color: C.text }}>
                        {rule.name || `Rule ${i + 1}`}
                      </span>
                      <span style={{ marginLeft: 10, fontSize: 12, color: C.text2 }}>
                        {rule.type === 'multiplier'
                          ? `× ${rule.factor ?? 1}  (${(((rule.factor ?? 1) - 1) * 100).toFixed(1)}%)`
                          : `+ ${fmt(rule.offset ?? 0)}`}
                      </span>
                      {rule._preview?.affected_rows != null && (
                        <span style={{ marginLeft: 10, fontSize: 11, color: C.text2 }}>
                          ~{rule._preview.affected_rows} rows
                        </span>
                      )}
                    </div>
                    <Btn small variant="danger" onClick={() => deleteRule(i)}>×</Btn>
                  </div>
                ))
              )}
            </Card>
          </div>
        ) : (
          <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', color: C.text2, padding: 60 }}>
            <div style={{ textAlign: 'center' }}>
              <div style={{ fontSize: 48, marginBottom: 12 }}>🎯</div>
              <div style={{ fontWeight: 700, fontSize: 16, marginBottom: 6 }}>Select a scenario</div>
              <div style={{ fontSize: 13 }}>Create a scenario on the left to start exploring what-if adjustments.</div>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

// ── ChatPanel ─────────────────────────────────────────────────────────────────

function ChatPanel({ schema, isOpen, onClose, onRuleCreated }) {
  const [messages, setMessages]   = useState([])
  const [input, setInput]         = useState('')
  const [streaming, setStreaming] = useState(false)
  const endRef  = useRef(null)
  const abortRef = useRef(null)

  useEffect(() => {
    endRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages])

  const send = async () => {
    const text = input.trim()
    if (!text || streaming) return
    setInput('')

    const userMsg = { role: 'user', content: text, id: Date.now() }
    setMessages((prev) => [...prev, userMsg])
    setStreaming(true)

    // Build history for the API (role + string content only)
    const history = messages
      .filter((m) => m.role !== 'system' && typeof m.content === 'string')
      .map((m) => ({ role: m.role, content: m.content }))

    const asstId = Date.now() + 1
    setMessages((prev) => [...prev, { role: 'assistant', content: '', id: asstId, toolCalls: [] }])

    const ac = new AbortController()
    abortRef.current = ac

    try {
      const stream = api.streamChat(text, schema.dataset.id, history, ac.signal)
      for await (const event of stream) {
        if (event.type === 'text_delta') {
          setMessages((prev) => prev.map((m) =>
            m.id === asstId ? { ...m, content: m.content + event.text } : m
          ))
        } else if (event.type === 'tool_executing') {
          setMessages((prev) => prev.map((m) =>
            m.id === asstId
              ? { ...m, toolCalls: [...(m.toolCalls ?? []), { ...event, status: 'running' }] }
              : m
          ))
        } else if (event.type === 'tool_result') {
          setMessages((prev) => prev.map((m) =>
            m.id === asstId
              ? { ...m, toolCalls: (m.toolCalls ?? []).map((tc) =>
                  tc.tool === event.tool && tc.status === 'running'
                    ? { ...tc, status: 'done', result: event.result }
                    : tc
                )}
              : m
          ))
        } else if (event.type === 'scenario_rule') {
          onRuleCreated?.(event.rule)
          setMessages((prev) => prev.map((m) =>
            m.id === asstId
              ? { ...m, createdRule: event.rule }
              : m
          ))
        } else if (event.type === 'error') {
          setMessages((prev) => prev.map((m) =>
            m.id === asstId ? { ...m, error: event.message } : m
          ))
        } else if (event.type === 'done') {
          break
        }
      }
    } catch (e) {
      if (e.name !== 'AbortError') {
        setMessages((prev) => prev.map((m) =>
          m.id === asstId ? { ...m, error: e.message } : m
        ))
      }
    } finally {
      setStreaming(false)
      abortRef.current = null
    }
  }

  if (!isOpen) return null

  const toolIcon = { query_data: '🔍', create_scenario_rule: '🎯', list_dimension_values: '📋' }

  return (
    <div style={{
      position: 'fixed', right: 0, top: 0, bottom: 0, width: 400,
      background: C.surface, borderLeft: `1px solid ${C.border}`,
      display: 'flex', flexDirection: 'column', zIndex: 100,
      boxShadow: '-6px 0 32px rgba(0,0,0,.08)',
    }}>
      {/* Header */}
      <div style={{
        padding: '14px 18px', borderBottom: `1px solid ${C.border}`,
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        background: C.surface,
      }}>
        <div>
          <div style={{ fontWeight: 800, color: C.text, fontSize: 15 }}>AI Assistant</div>
          <div style={{ fontSize: 11, color: C.text2, marginTop: 1 }}>{schema?.dataset?.name}</div>
        </div>
        <div style={{ display: 'flex', gap: 8 }}>
          <Btn small variant="ghost" onClick={() => setMessages([])}>Clear</Btn>
          <button
            onClick={onClose}
            style={{ background: 'none', border: 'none', cursor: 'pointer', fontSize: 20, color: C.text2, lineHeight: 1 }}
          >×</button>
        </div>
      </div>

      {/* Messages */}
      <div style={{ flex: 1, overflowY: 'auto', padding: '16px 14px' }}>
        {messages.length === 0 && (
          <div style={{ textAlign: 'center', paddingTop: 48, color: C.text2 }}>
            <div style={{ fontSize: 36, marginBottom: 10 }}>🤖</div>
            <div style={{ fontWeight: 700, marginBottom: 6 }}>Ask me anything</div>
            <div style={{ fontSize: 12, lineHeight: 1.6 }}>
              I can query your data, explain trends, and create what-if scenario rules.
            </div>
          </div>
        )}

        {messages.map((msg) => (
          <div
            key={msg.id}
            style={{
              marginBottom: 14,
              display: 'flex',
              justifyContent: msg.role === 'user' ? 'flex-end' : 'flex-start',
            }}
          >
            <div style={{
              maxWidth: '88%',
              background: msg.role === 'user' ? C.primary : C.bg,
              color: msg.role === 'user' ? '#fff' : C.text,
              borderRadius: msg.role === 'user' ? '16px 16px 4px 16px' : '4px 16px 16px 16px',
              padding: '10px 14px', fontSize: 13, lineHeight: 1.55,
            }}>
              {msg.content && (
                <div style={{ whiteSpace: 'pre-wrap' }}>{msg.content}</div>
              )}

              {/* Tool call cards */}
              {(msg.toolCalls ?? []).map((tc, i) => (
                <div key={i} style={{
                  marginTop: 8, background: C.surface, borderRadius: 8,
                  border: `1px solid ${C.border}`, padding: '8px 12px', fontSize: 12,
                }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 4 }}>
                    <span>{toolIcon[tc.tool] ?? '🔧'}</span>
                    <span style={{ fontWeight: 700, color: C.primary }}>{tc.tool}</span>
                    {tc.status === 'running' && <span style={{ color: C.text2 }}>running…</span>}
                    {tc.status === 'done'    && <span style={{ color: C.success }}>✓ done</span>}
                  </div>
                  <div style={{ fontFamily: 'monospace', fontSize: 11, color: C.text2, wordBreak: 'break-all' }}>
                    {JSON.stringify(tc.input).slice(0, 150)}
                    {JSON.stringify(tc.input).length > 150 ? '…' : ''}
                  </div>
                  {tc.result?.row_count != null && (
                    <div style={{ marginTop: 4, color: C.text, fontSize: 12 }}>
                      {tc.result.row_count} row{tc.result.row_count !== 1 ? 's' : ''} returned
                    </div>
                  )}
                </div>
              ))}

              {/* Scenario rule created notification */}
              {msg.createdRule && (
                <div style={{
                  marginTop: 8, background: C.success + '18', border: `1px solid ${C.success}44`,
                  borderRadius: 8, padding: '8px 12px', fontSize: 12,
                }}>
                  <span style={{ color: C.success, fontWeight: 700 }}>✓ Rule created: </span>
                  {msg.createdRule.name ?? 'Unnamed'} added to active scenario
                </div>
              )}

              {msg.error && (
                <div style={{ marginTop: 8, color: C.error, fontSize: 12 }}>
                  ⚠️ {msg.error}
                </div>
              )}
            </div>
          </div>
        ))}
        <div ref={endRef} />
      </div>

      {/* Input */}
      <div style={{
        padding: '12px 14px', borderTop: `1px solid ${C.border}`,
        display: 'flex', gap: 8, alignItems: 'flex-end',
      }}>
        <textarea
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); send() } }}
          placeholder="Ask about your data… (Enter to send)"
          disabled={streaming}
          rows={2}
          style={{
            flex: 1, resize: 'none', border: `1px solid ${C.border}`, borderRadius: 8,
            padding: '8px 12px', fontSize: 13, outline: 'none', color: C.text,
            lineHeight: 1.5,
          }}
        />
        <Btn
          onClick={streaming ? () => { abortRef.current?.abort(); setStreaming(false) } : send}
          style={{ height: 38, width: 38, padding: 0, fontSize: 18, flexShrink: 0 }}
          disabled={!streaming && !input.trim()}
        >
          {streaming ? '⏹' : '↑'}
        </Btn>
      </div>
    </div>
  )
}

// ── App (root) ────────────────────────────────────────────────────────────────

export default function App() {
  const qc = useQueryClient()
  const [activeDatasetId, setActiveDatasetId] = useState(null)
  const [view, setView]                       = useState('schema')
  const [chatOpen, setChatOpen]               = useState(false)
  const [baselines, setBaselines]             = useState({})      // datasetId → baseline
  const [baselineLoading, setBaselineLoading] = useState({})
  const [scenarios, setScenarios]             = useState([])

  const { data: allSchemas = [], isLoading: schemasLoading, refetch: refetchSchemas } = useQuery({
    queryKey: ['datasets'],
    queryFn: api.getDatasets,
    staleTime: 30_000,
  })

  // Auto-select first dataset on load
  useEffect(() => {
    if (allSchemas.length > 0 && !activeDatasetId) {
      setActiveDatasetId(allSchemas[0].dataset.id)
    }
  }, [allSchemas])

  // Load scenarios whenever active dataset changes
  useEffect(() => {
    if (!activeDatasetId) return
    api.getScenarios(activeDatasetId).then(setScenarios).catch(console.error)
  }, [activeDatasetId])

  const activeSchema = allSchemas.find((s) => s.dataset.id === activeDatasetId) ?? null

  const loadBaseline = useCallback(async (schema) => {
    const { dataset, relationships } = schema
    setBaselineLoading((b) => ({ ...b, [dataset.id]: true }))
    try {
      const relIds = relationships
        .filter((r) => r.source_dataset_id === dataset.id || r.target_dataset_id === dataset.id)
        .map((r) => r.id)
      const bl = await api.getBaseline(dataset.id, relIds)
      setBaselines((b) => ({ ...b, [dataset.id]: bl }))
    } catch (e) {
      alert('Failed to load baseline: ' + e.message)
    } finally {
      setBaselineLoading((b) => ({ ...b, [dataset.id]: false }))
    }
  }, [])

  const switchToView = (tab) => {
    setView(tab)
    if (tab === 'actuals' && activeSchema && !baselines[activeDatasetId]) {
      loadBaseline(activeSchema)
    }
  }

  const handleUpload = async () => {
    const { data } = await refetchSchemas()
    if (data?.length > 0) setActiveDatasetId(data[0].dataset.id)
  }

  const handleUploadNew = () => {
    const inp = document.createElement('input')
    inp.type = 'file'
    inp.accept = '.xlsx,.xls,.csv,.tsv'
    inp.onchange = async (e) => {
      const file = e.target.files?.[0]
      if (!file) return
      try { await api.uploadFile(file); refetchSchemas() }
      catch (err) { alert(err.message) }
    }
    inp.click()
  }

  // When Claude creates a scenario rule, add it to the first scenario (or note it)
  const handleRuleCreated = useCallback((rule) => {
    if (scenarios.length === 0) return
    const sc = scenarios[0]
    const updated = { ...sc, rules: [...(sc.rules ?? []), rule] }
    api.updateScenario(sc.id, { rules: updated.rules })
      .then((saved) => setScenarios((prev) => prev.map((s) => s.id === saved.id ? saved : s)))
      .catch(console.error)
  }, [scenarios])

  // ── Render ──

  if (!schemasLoading && allSchemas.length === 0) {
    return <UploadScreen onUpload={handleUpload} />
  }

  const TABS = [
    { id: 'schema',    label: 'Schema' },
    { id: 'actuals',   label: 'Actuals' },
    { id: 'scenarios', label: 'Scenarios' },
  ]

  return (
    <div style={{
      fontFamily: 'Plus Jakarta Sans, sans-serif',
      background: C.bg, minHeight: '100vh',
      paddingRight: chatOpen ? 400 : 0,
      transition: 'padding-right .25s',
    }}>
      {/* ── Header ── */}
      <div style={{
        background: C.surface, borderBottom: `1px solid ${C.border}`,
        height: 56, padding: '0 20px',
        display: 'flex', alignItems: 'center', gap: 16,
        position: 'sticky', top: 0, zIndex: 50,
      }}>
        {/* Logo */}
        <div style={{ fontWeight: 800, fontSize: 20, color: C.text, letterSpacing: -0.5, flexShrink: 0 }}>
          data<span style={{ color: C.primary }}>Bob</span>IQ
        </div>

        {/* Dataset selector */}
        {allSchemas.length > 0 && (
          <select
            value={activeDatasetId ?? ''}
            onChange={(e) => {
              setActiveDatasetId(e.target.value)
              setView('schema')
            }}
            style={{
              border: `1px solid ${C.border}`, borderRadius: 8,
              padding: '5px 10px', fontSize: 13, background: C.surface,
              color: C.text, maxWidth: 200, fontFamily: 'inherit', fontWeight: 600,
            }}
          >
            {allSchemas.map((s) => (
              <option key={s.dataset.id} value={s.dataset.id}>{s.dataset.name}</option>
            ))}
          </select>
        )}

        <Btn small variant="ghost" onClick={handleUploadNew}>+ Upload</Btn>

        {/* Nav tabs */}
        <div style={{ display: 'flex', gap: 2, marginLeft: 8 }}>
          {TABS.map(({ id, label }) => (
            <button
              key={id}
              onClick={() => switchToView(id)}
              style={{
                padding: '6px 14px', borderRadius: 8, border: 'none', cursor: 'pointer',
                fontFamily: 'inherit', fontWeight: 700, fontSize: 13,
                background: view === id ? C.primaryLt : 'transparent',
                color: view === id ? C.primaryDk : C.text2,
                transition: 'all .15s',
              }}
            >
              {label}
            </button>
          ))}
        </div>

        <div style={{ flex: 1 }} />

        {/* Chat toggle */}
        <Btn
          small
          variant={chatOpen ? 'primary' : 'ghost'}
          onClick={() => setChatOpen((v) => !v)}
        >
          💬 {chatOpen ? 'Close' : 'Ask AI'}
        </Btn>
      </div>

      {/* ── Main content ── */}
      {schemasLoading ? (
        <div style={{ textAlign: 'center', paddingTop: 100, color: C.primary }}>
          <div style={{ fontSize: 48, marginBottom: 12 }}>⏳</div>
          <div style={{ fontWeight: 700 }}>Loading datasets…</div>
        </div>
      ) : activeSchema ? (
        <>
          {view === 'schema' && (
            <SchemaView
              schema={activeSchema}
              allSchemas={allSchemas}
              onRefresh={() => refetchSchemas()}
            />
          )}
          {view === 'actuals' && (
            <ActualsView
              schema={activeSchema}
              baseline={baselines[activeDatasetId]}
              baselineLoading={!!baselineLoading[activeDatasetId]}
              onLoadBaseline={loadBaseline}
            />
          )}
          {view === 'scenarios' && (
            <ScenariosView
              schema={activeSchema}
              baseline={baselines[activeDatasetId]}
              scenarios={scenarios}
              onScenariosChange={setScenarios}
            />
          )}
        </>
      ) : null}

      {/* ── Chat panel ── */}
      {activeSchema && (
        <ChatPanel
          schema={activeSchema}
          isOpen={chatOpen}
          onClose={() => setChatOpen(false)}
          onRuleCreated={handleRuleCreated}
        />
      )}
    </div>
  )
}

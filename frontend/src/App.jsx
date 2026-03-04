import React, { useState, useEffect, useRef, useMemo, useCallback } from "react";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, Cell } from "recharts";
import _ from "lodash";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import {
  uploadFile, getDatasets, getBaseline, deleteDataset,
  updateColumnRole as apiUpdateColumnRole,
  createRelationship as apiCreateRelationship,
  updateRelationship as apiUpdateRelationship,
  deleteRelationship as apiDeleteRelationship,
  streamChat,
} from "./api.js";

// ─── THEME ──────────────────────────────────────────────────────
const FONT_URL = "https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:ital,wght@0,300;0,400;0,500;0,600;0,700;0,800;1,400&family=IBM+Plex+Mono:wght@400;500&display=swap";
const C = {
  brand: "#6abbd9",
  brandLight: "#6abbd915",
  brandMid: "#6abbd930",
  brandDark: "#3a8aa8",
  bg: "#fafbfc",
  white: "#ffffff",
  surface: "#ffffff",
  surfaceHover: "#f4f7f9",
  border: "#e8ecf0",
  borderLight: "#f0f2f5",
  text: "#1a2b3c",
  textSec: "#5a6b7c",
  textMuted: "#8a96a3",
  green: "#22a06b",
  greenBg: "#22a06b12",
  red: "#cf1322",
  redBg: "#cf132212",
  amber: "#d97706",
  amberBg: "#d9770612",
  purple: "#7c4dff",
  purpleBg: "#7c4dff12",
};
const SC_COLORS = ["#6abbd9", "#7c4dff", "#f97316", "#ec4899", "#22a06b", "#eab308"];
const ROLE_COLORS = { key: C.amber, measure: C.brand, attribute: C.textMuted, time: C.green, ignore: C.border };

const fmt = n => (n == null || isNaN(n)) ? "" : new Intl.NumberFormat("de-DE", { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(n);
const fmtS = n => { if (n == null || isNaN(n)) return ""; const a = Math.abs(n); if (a >= 1e6) return (n / 1e6).toFixed(1) + "M"; if (a >= 1e3) return (n / 1e3).toFixed(1) + "K"; return n.toFixed(0); };

// ─── STYLES ─────────────────────────────────────────────────────
const S = {
  card: {
    background: C.white, border: `1px solid ${C.border}`, borderRadius: 12,
    padding: 20, marginBottom: 14, boxShadow: "0 1px 3px rgba(0,0,0,0.04)"
  },
  cardT: {
    fontSize: 11, fontWeight: 700, color: C.textMuted, textTransform: "uppercase",
    letterSpacing: "0.6px", marginBottom: 12
  },
  th: {
    textAlign: "left", padding: "8px 10px", borderBottom: `2px solid ${C.border}`,
    color: C.textMuted, fontWeight: 600, fontSize: 10, textTransform: "uppercase",
    letterSpacing: "0.5px", position: "sticky", top: 0, background: C.white, zIndex: 1
  },
  td: { padding: "6px 10px", borderBottom: `1px solid ${C.borderLight}`, fontSize: 12, color: C.text },
  mono: { fontFamily: "'IBM Plex Mono', monospace", fontSize: 11, fontWeight: 500 },
  badge: (color) => ({
    display: "inline-flex", alignItems: "center", gap: 4, padding: "2px 8px",
    borderRadius: 20, fontSize: 10, fontWeight: 600, background: color + "14",
    color: color, border: `1px solid ${color}25`
  }),
  tag: (color = C.brand) => ({
    display: "inline-flex", alignItems: "center", gap: 5, padding: "4px 10px",
    borderRadius: 6, fontSize: 11, fontWeight: 500, background: color + "12",
    color: color, border: `1px solid ${color}22`, cursor: "pointer", userSelect: "none"
  }),
  btn: (variant = "primary", small = false) => ({
    padding: small ? "5px 12px" : "8px 16px", borderRadius: 8, border: "none",
    cursor: "pointer", fontSize: small ? 11 : 13, fontWeight: 600,
    fontFamily: "'Plus Jakarta Sans', sans-serif", transition: "all .15s",
    background: variant === "primary" ? C.brand : variant === "danger" ? C.red
      : variant === "active" ? C.brandLight : C.white,
    color: variant === "primary" ? "#fff" : variant === "danger" ? "#fff"
      : variant === "active" ? C.brand : C.textSec,
    border: variant === "primary" || variant === "danger" ? "none" : `1px solid ${C.border}`,
  }),
  input: {
    background: C.white, border: `1px solid ${C.border}`, borderRadius: 8,
    padding: "7px 12px", color: C.text, fontSize: 12,
    fontFamily: "'Plus Jakarta Sans', sans-serif", outline: "none", width: "100%",
    transition: "border-color .15s",
  },
  select: {
    background: C.white, border: `1px solid ${C.border}`, borderRadius: 8,
    padding: "7px 12px", color: C.text, fontSize: 12,
    fontFamily: "'Plus Jakarta Sans', sans-serif", outline: "none",
  },
  dropdown: {
    position: "absolute", top: "100%", left: 0, zIndex: 60, background: C.white,
    border: `1px solid ${C.border}`, borderRadius: 10, padding: 8, marginTop: 4,
    minWidth: 220, maxHeight: 240, overflow: "auto",
    boxShadow: "0 12px 32px rgba(0,0,0,0.12), 0 2px 6px rgba(0,0,0,0.06)"
  },
};

// ─── DATA ENGINE ────────────────────────────────────────────────
const ROLE_OPTIONS = ["key", "measure", "attribute", "time", "ignore"];

function autoRole(name, vals) {
  const c = vals.filter(v => v != null);
  if (!c.length) return "ignore";
  const nr = c.filter(v => typeof v === "number").length / c.length;
  const isId = name.toLowerCase().includes("_id") || name.toLowerCase().endsWith("nr");
  const isDate = c.some(v => typeof v === "string" && /^\d{4}-\d{2}/.test(v));
  if (isDate) return "time";
  if (isId) return "key";
  if (nr > 0.8 && !isId && name !== "entry_count") return "measure";
  return "attribute";
}

function discoverSchema(tables) {
  const schema = {};
  for (const [name, table] of Object.entries(tables)) {
    const { headers, data } = table;
    const columns = headers.map(h => {
      const vals = data.slice(0, 200).map(r => r[h]);
      return { name: h, role: autoRole(h, vals), uniqueCount: new Set(vals.filter(v => v != null).map(String)).size };
    });
    schema[name] = {
      columns,
      isFact: columns.some(c => c.role === "measure") && columns.filter(c => c.role === "key").length >= 2,
      rowCount: data.length
    };
  }
  return schema;
}

function autoDiscoverRelationships(tables, schema) {
  const rels = [];
  const ns = Object.keys(schema);
  for (let i = 0; i < ns.length; i++) {
    for (let j = i + 1; j < ns.length; j++) {
      const shared = schema[ns[i]].columns.map(c => c.name)
        .filter(c => schema[ns[j]].columns.some(d => d.name === c) && c.includes("_id"));
      for (const col of shared) {
        const v1 = new Set(tables[ns[i]].data.map(r => r[col]).filter(v => v != null).map(String));
        const v2 = new Set(tables[ns[j]].data.map(r => r[col]).filter(v => v != null).map(String));
        const ov = [...v1].filter(v => v2.has(v)).length;
        rels.push({
          id: `${ns[i]}-${ns[j]}-${col}`,
          from: ns[i], to: ns[j], fromCol: col, toCol: col,
          coverage: Math.round(ov / Math.min(v1.size || 1, v2.size || 1) * 100),
          overlapCount: ov
        });
      }
    }
  }
  return rels;
}

function buildBaseline(tables, schema, relationships) {
  const fn = Object.entries(schema).find(([, s]) => s.isFact)?.[0];
  if (!fn) return [];
  const lk = {};
  for (const rel of relationships) {
    const dimName = rel.from === fn ? rel.to : rel.to === fn ? rel.from : null;
    if (!dimName || !tables[dimName]) continue;
    const dimKeyCol = rel.from === fn ? rel.toCol : rel.fromCol;
    const factKeyCol = rel.from === fn ? rel.fromCol : rel.toCol;
    if (lk[dimName]) continue;
    const map = {};
    for (const row of tables[dimName].data) map[row[dimKeyCol]] = row;
    const dimCols = schema[dimName]?.columns.filter(c => c.role === "attribute" || c.role === "time") || [];
    lk[dimName] = { map, factKeyCol, dimCols };
  }
  return tables[fn].data.map(row => {
    const e = { ...row };
    if (row.period) { e._year = row.period.slice(0, 4); e._month = row.period.slice(5, 7); e._period = row.period; }
    for (const [, l] of Object.entries(lk)) {
      const dr = l.map[row[l.factKeyCol]];
      if (dr) for (const dc of l.dimCols) e[dc.name] = dr[dc.name];
    }
    return e;
  });
}

function getDimFields(bl) {
  if (!bl.length) return [];
  const nums = new Set(["amount", "entry_count"]);
  const skip = new Set(["company_id"]);
  return Object.keys(bl[0]).filter(k => !nums.has(k) && !skip.has(k) && typeof bl[0][k] !== "number").sort();
}
function getMeasureFields(bl, schema = null) {
  if (!bl.length) return [];
  if (schema) {
    const measureNames = new Set(
      Object.values(schema).flatMap(t =>
        t.columns.filter(c => c.role === "measure").map(c => c.name)
      )
    );
    const found = Object.keys(bl[0]).filter(k => measureNames.has(k));
    if (found.length) return found;
  }
  // Fallback: heuristic for numeric non-id columns
  return Object.keys(bl[0]).filter(k => typeof bl[0][k] === "number" && k !== "entry_count" && !k.endsWith("_id"));
}
function getUniq(bl, f) { return [...new Set(bl.map(r => r[f]).filter(v => v != null))].sort(); }
function applyFilters(data, filters) {
  return data.filter(r => {
    for (const [f, vs] of Object.entries(filters)) { if (vs.length && !vs.includes(r[f])) return false; }
    return true;
  });
}

function computePivot(data, rowFs, colF, valF, sortMode = "value_desc") {
  if (!rowFs.length || !valF) return { rows: [], colKeys: [] };
  const groups = {};
  const colKeysSet = new Set();
  for (const r of data) {
    const rk = rowFs.map(f => r[f] ?? "—").join(" | ");
    const ck = colF ? (r[colF] ?? "—") : null;
    if (ck) colKeysSet.add(ck);
    if (!groups[rk]) { groups[rk] = { _key: rk, _total: 0 }; rowFs.forEach(f => groups[rk][f] = r[f] ?? "—"); }
    const v = +r[valF] || 0;
    groups[rk]._total += v;
    if (ck) { groups[rk][ck] = (groups[rk][ck] || 0) + v; }
  }
  let rows = Object.values(groups);
  const firstF = rowFs[0];
  const isTime = firstF && (firstF.includes("period") || firstF.includes("_year") || firstF.includes("_month") || firstF.includes("date") || firstF === "period");
  if (sortMode === "none") {
    // no sort - caller handles it
  } else if (sortMode === "time_asc" || (sortMode === "auto" && isTime)) {
    rows.sort((a, b) => String(a[firstF] ?? "").localeCompare(String(b[firstF] ?? "")));
  } else if (sortMode === "time_desc") {
    rows.sort((a, b) => String(b[firstF] ?? "").localeCompare(String(a[firstF] ?? "")));
  } else {
    rows.sort((a, b) => b._total - a._total);
  }
  return { rows, colKeys: [...colKeysSet].sort() };
}

function applyRules(data, rules) {
  let res = data.map(r => ({ ...r }));
  for (const rule of rules) {
    const matchIdx = [];
    res.forEach((r, i) => {
      let m = true;
      for (const [k, v] of Object.entries(rule.filters || {})) {
        if (!v || (Array.isArray(v) && v.length === 0)) continue;
        if (Array.isArray(v)) { if (!v.includes(r[k])) m = false; }
        else { if (r[k] !== v) m = false; }
      }
      if (rule.periodFrom && (r._period || r.period) < rule.periodFrom) m = false;
      if (rule.periodTo && (r._period || r.period) > rule.periodTo) m = false;
      if (m) matchIdx.push(i);
    });
    if (rule.type === "multiplier") {
      for (const i of matchIdx) res[i] = { ...res[i], amount: Math.round(res[i].amount * rule.factor * 100) / 100 };
    } else if (rule.type === "offset" && matchIdx.length > 0) {
      // Count distinct periods among matching rows
      const periodCounts = {};
      for (const i of matchIdx) {
        const p = res[i]._period || res[i].period || "all";
        periodCounts[p] = (periodCounts[p] || 0) + 1;
      }
      const numPeriods = Object.keys(periodCounts).length;
      const perPeriod = rule.offset / (numPeriods || 1);
      // Within each period, split evenly across matching rows
      for (const i of matchIdx) {
        const p = res[i]._period || res[i].period || "all";
        const rowsInPeriod = periodCounts[p] || 1;
        const share = perPeriod / rowsInPeriod;
        res[i] = { ...res[i], amount: Math.round((res[i].amount + share) * 100) / 100 };
      }
    }
  }
  return res;
}

// ─── FIELD MANAGER ──────────────────────────────────────────────
function FieldManager({ label, allFields, selected, onChange, color = C.brand, single = false }) {
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState("");
  const ref = useRef(null);
  useEffect(() => {
    const h = e => { if (ref.current && !ref.current.contains(e.target)) setOpen(false); };
    document.addEventListener("mousedown", h);
    return () => document.removeEventListener("mousedown", h);
  }, []);
  const available = allFields.filter(f => single ? true : !selected.includes(f))
    .filter(f => !search || f.toLowerCase().includes(search.toLowerCase()));

  return (
    <div style={{ marginBottom: 4 }}>
      <div style={{ fontSize: 10, color: C.textMuted, textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: 5, fontWeight: 600 }}>{label}</div>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 4, alignItems: "center" }}>
        {(single ? [selected].filter(Boolean) : selected).map(f => (
          <span key={f} style={S.tag(color)}>
            {f.replace(/_/g, " ")}
            <span onClick={() => onChange(single ? "" : selected.filter(x => x !== f))} style={{ cursor: "pointer", opacity: 0.5, fontSize: 13, lineHeight: 1 }}>×</span>
          </span>
        ))}
        <div ref={ref} style={{ position: "relative" }}>
          <button onClick={() => { setOpen(!open); setSearch(""); }} style={{ ...S.btn("ghost", true), padding: "4px 8px", fontSize: 11, color: color, border: `1px dashed ${color}44` }}>+</button>
          {open && (
            <div style={S.dropdown}>
              <input autoFocus style={{ ...S.input, marginBottom: 6, fontSize: 11 }} placeholder="Search fields..." value={search} onChange={e => setSearch(e.target.value)} />
              {available.length === 0 && <div style={{ fontSize: 11, color: C.textMuted, padding: 6 }}>No fields</div>}
              {available.map(f => (
                <div key={f} onClick={() => { onChange(single ? f : [...selected, f]); if (single) setOpen(false); }}
                  style={{ padding: "6px 8px", borderRadius: 6, cursor: "pointer", fontSize: 12, color: C.text }}
                  onMouseEnter={e => e.target.style.background = C.surfaceHover}
                  onMouseLeave={e => e.target.style.background = ""}>
                  {f.replace(/_/g, " ")}
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

// ─── FILTER MANAGER ─────────────────────────────────────────────
function FilterManager({ baseline, allFields, filters, setFilters }) {
  const [addOpen, setAddOpen] = useState(false);
  const [addSearch, setAddSearch] = useState("");
  const [expandedF, setExpandedF] = useState(null);
  const [valSearch, setValSearch] = useState("");
  const activeFilterFields = Object.keys(filters);
  const availableFs = allFields.filter(f => !activeFilterFields.includes(f))
    .filter(f => !addSearch || f.toLowerCase().includes(addSearch.toLowerCase()));
  const addRef = useRef(null);
  useEffect(() => {
    const h = e => { if (addRef.current && !addRef.current.contains(e.target)) setAddOpen(false); };
    document.addEventListener("mousedown", h);
    return () => document.removeEventListener("mousedown", h);
  }, []);

  return (
    <div style={{ marginBottom: 4 }}>
      <div style={{ fontSize: 10, color: C.textMuted, textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: 5, fontWeight: 600 }}>Filters</div>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 4, alignItems: "center" }}>
        {activeFilterFields.map(f => {
          const vals = filters[f] || [];
          const expanded = expandedF === f;
          const allVals = getUniq(baseline, f);
          const fVals = valSearch ? allVals.filter(v => String(v).toLowerCase().includes(valSearch.toLowerCase())) : allVals;
          return (
            <div key={f} style={{ position: "relative" }}>
              <span onClick={() => { setExpandedF(expanded ? null : f); setValSearch(""); }}
                style={{ ...S.tag(vals.length ? C.amber : C.textMuted) }}>
                {f.replace(/_/g, " ")}{vals.length ? ` (${vals.length})` : ""}
                <span onClick={e => { e.stopPropagation(); const nf = { ...filters }; delete nf[f]; setFilters(nf); setExpandedF(null); }}
                  style={{ cursor: "pointer", opacity: .5, fontSize: 13 }}>×</span>
              </span>
              {expanded && (
                <div style={S.dropdown}>
                  <input autoFocus style={{ ...S.input, marginBottom: 6, fontSize: 11 }} placeholder="Search values..." value={valSearch} onChange={e => setValSearch(e.target.value)} />
                  {fVals.slice(0, 80).map(v => {
                    const ch = vals.includes(v);
                    return (
                      <label key={String(v)} style={{ display: "flex", alignItems: "center", gap: 6, padding: "4px 4px", fontSize: 12, color: C.text, cursor: "pointer", borderRadius: 4 }}>
                        <input type="checkbox" checked={ch} onChange={() => setFilters({ ...filters, [f]: ch ? vals.filter(x => x !== v) : [...vals, v] })}
                          style={{ accentColor: C.brand }} />
                        {String(v)}
                      </label>
                    );
                  })}
                </div>
              )}
            </div>
          );
        })}
        <div ref={addRef} style={{ position: "relative" }}>
          <button onClick={() => { setAddOpen(!addOpen); setAddSearch(""); }}
            style={{ ...S.btn("ghost", true), padding: "4px 8px", fontSize: 11, color: C.amber, border: `1px dashed ${C.amber}44` }}>+ filter</button>
          {addOpen && (
            <div style={S.dropdown}>
              <input autoFocus style={{ ...S.input, marginBottom: 6, fontSize: 11 }} placeholder="Search fields..." value={addSearch} onChange={e => setAddSearch(e.target.value)} />
              {availableFs.map(f => (
                <div key={f} onClick={() => { setFilters({ ...filters, [f]: [] }); setAddOpen(false); }}
                  style={{ padding: "6px 8px", borderRadius: 6, cursor: "pointer", fontSize: 12, color: C.text }}
                  onMouseEnter={e => e.target.style.background = C.surfaceHover}
                  onMouseLeave={e => e.target.style.background = ""}>
                  {f.replace(/_/g, " ")}
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

// ─── PIVOT TABLE ────────────────────────────────────────────────
function PivotTableView({ data, rowFs, colF, valF, colorFn }) {
  const [sortCol, setSortCol] = useState(null);
  const [sortDir, setSortDir] = useState("asc");
  const { rows: rawRows, colKeys } = useMemo(() => computePivot(data, rowFs, colF, valF, "none"), [data, rowFs, colF, valF]);

  // Default: auto sort by first field if time-like, else by _total desc
  const rows = useMemo(() => {
    const arr = [...rawRows];
    const col = sortCol;
    const dir = sortDir;
    if (!col) {
      const f0 = rowFs[0];
      const isTime = f0 && (f0.includes("period") || f0.includes("_year") || f0.includes("_month") || f0 === "period");
      if (isTime) arr.sort((a, b) => String(a[f0] ?? "").localeCompare(String(b[f0] ?? "")));
      else arr.sort((a, b) => b._total - a._total);
      return arr;
    }
    arr.sort((a, b) => {
      const av = a[col] ?? a._total ?? 0;
      const bv = b[col] ?? b._total ?? 0;
      if (typeof av === "number" && typeof bv === "number") return dir === "asc" ? av - bv : bv - av;
      return dir === "asc" ? String(av).localeCompare(String(bv)) : String(bv).localeCompare(String(av));
    });
    return arr;
  }, [rawRows, sortCol, sortDir, rowFs]);

  function toggleSort(col) {
    if (sortCol === col) setSortDir(d => d === "asc" ? "desc" : "asc");
    else { setSortCol(col); setSortDir(typeof (rawRows[0]?.[col]) === "number" ? "desc" : "asc"); }
  }

  const arrow = col => sortCol === col ? (sortDir === "asc" ? " ↑" : " ↓") : "";
  const thClick = (col, align = "left") => ({
    ...S.th, textAlign: align, cursor: "pointer", userSelect: "none",
    color: sortCol === col ? C.brand : S.th.color,
  });

  if (!rowFs.length || !valF) return <div style={{ color: C.textMuted, fontSize: 12, padding: 20, textAlign: "center" }}>Add row fields and a measure.</div>;
  const hasCols = colF && colKeys.length > 0;
  return (
    <div style={{ maxHeight: 420, overflow: "auto" }}>
      <table style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead><tr>
          {rowFs.map(f => <th key={f} style={thClick(f)} onClick={() => toggleSort(f)}>{f.replace(/_/g, " ")}{arrow(f)}</th>)}
          {hasCols ? colKeys.map(ck => <th key={ck} style={thClick(ck, "right")} onClick={() => toggleSort(ck)}>{String(ck)}{arrow(ck)}</th>)
            : <th style={thClick("_total", "right")} onClick={() => toggleSort("_total")}>{valF}{arrow("_total")}</th>}
          {hasCols && <th style={thClick("_total", "right")} onClick={() => toggleSort("_total")}>Total{arrow("_total")}</th>}
        </tr></thead>
        <tbody>
          {rows.slice(0, 120).map((r, i) => (
            <tr key={i} style={{ background: i % 2 === 0 ? "" : C.bg }}>
              {rowFs.map(f => <td key={f} style={S.td}>{String(r[f] ?? "—")}</td>)}
              {hasCols ? colKeys.map(ck => {
                const v = r[ck] || 0;
                return <td key={ck} style={{ ...S.td, ...S.mono, textAlign: "right", color: colorFn ? colorFn(v) : v >= 0 ? C.green : C.red }}>{fmt(v)}</td>;
              }) : <td style={{ ...S.td, ...S.mono, textAlign: "right", color: colorFn ? colorFn(r._total) : r._total >= 0 ? C.green : C.red }}>{fmt(r._total)}</td>}
              {hasCols && <td style={{ ...S.td, ...S.mono, textAlign: "right", fontWeight: 600, color: colorFn ? colorFn(r._total) : r._total >= 0 ? C.green : C.red }}>{fmt(r._total)}</td>}
            </tr>
          ))}
        </tbody>
        <tfoot><tr style={{ background: C.bg }}>
          <td colSpan={rowFs.length} style={{ ...S.th, fontWeight: 700, color: C.text, borderTop: `2px solid ${C.border}` }}>Total</td>
          {hasCols ? colKeys.map(ck => {
            const t = rows.reduce((s, r) => s + (r[ck] || 0), 0);
            return <td key={ck} style={{ ...S.th, ...S.mono, textAlign: "right", fontWeight: 700, color: C.text, borderTop: `2px solid ${C.border}` }}>{fmt(t)}</td>;
          }) : <td style={{ ...S.th, ...S.mono, textAlign: "right", fontWeight: 700, color: C.text, borderTop: `2px solid ${C.border}` }}>{fmt(rows.reduce((s, r) => s + r._total, 0))}</td>}
          {hasCols && <td style={{ ...S.th, ...S.mono, textAlign: "right", fontWeight: 700, color: C.text, borderTop: `2px solid ${C.border}` }}>{fmt(rows.reduce((s, r) => s + r._total, 0))}</td>}
        </tr></tfoot>
      </table>
    </div>
  );
}

// ─── PIVOT CHART ────────────────────────────────────────────────
function PivotChartView({ data, rowFs, colF, valF, scenarioData }) {
  const chartData = useMemo(() => {
    if (!rowFs.length || !valF) return [];
    const { rows } = computePivot(data, rowFs, null, valF, "auto");
    const main = rows.slice(0, 25).map(r => ({ ...r, label: rowFs.map(f => r[f]).join(" | ") }));
    if (!scenarioData || !Object.keys(scenarioData).length) return main.map(r => ({ ...r, Actuals: r._total }));
    const scPivots = {};
    for (const [name, sd] of Object.entries(scenarioData)) {
      const { rows: sr } = computePivot(sd, rowFs, null, valF, "auto");
      const map = {}; for (const r of sr) map[r._key] = r._total;
      scPivots[name] = map;
    }
    return main.map(r => {
      const out = { ...r, Actuals: r._total };
      for (const [name, map] of Object.entries(scPivots)) out[name] = map[r._key] || 0;
      return out;
    });
  }, [data, rowFs, colF, valF, scenarioData]);
  if (!chartData.length) return null;
  const hasScen = scenarioData && Object.keys(scenarioData).length > 0;
  const labelF = rowFs[rowFs.length - 1];
  return (
    <ResponsiveContainer width="100%" height={280}>
      <BarChart data={chartData} margin={{ top: 8, right: 12, left: 12, bottom: 60 }}>
        <CartesianGrid strokeDasharray="3 3" stroke={C.border} />
        <XAxis dataKey={labelF} tick={{ fill: C.textMuted, fontSize: 10 }} angle={-30} textAnchor="end" interval={0} height={65} />
        <YAxis tick={{ fill: C.textMuted, fontSize: 10 }} tickFormatter={fmtS} />
        <Tooltip contentStyle={{ background: C.white, border: `1px solid ${C.border}`, borderRadius: 8, fontSize: 11, boxShadow: "0 4px 12px rgba(0,0,0,0.08)" }} formatter={v => [fmt(v), ""]} />
        {hasScen ? (
          <>
            <Legend wrapperStyle={{ fontSize: 11 }} />
            <Bar dataKey="Actuals" fill={C.textMuted} radius={[4, 4, 0, 0]} opacity={0.35} />
            {Object.keys(scenarioData).map((name, i) => (
              <Bar key={name} dataKey={name} fill={SC_COLORS[i % SC_COLORS.length]} radius={[4, 4, 0, 0]} />
            ))}
          </>
        ) : (
          <Bar dataKey="Actuals" radius={[4, 4, 0, 0]}>
            {chartData.map((r, i) => <Cell key={i} fill={r.Actuals >= 0 ? C.green : C.red} opacity={0.75} />)}
          </Bar>
        )}
      </BarChart>
    </ResponsiveContainer>
  );
}

// ─── WATERFALL CHART ────────────────────────────────────────────
function WaterfallChart({ baseline, scenarioData, scenarioName, scenarioColor, rowFs, valF, waterfallField }) {
  const data = useMemo(() => {
    if (!waterfallField || !scenarioData) return [];
    // Group baseline and scenario by waterfallField
    const groupBy = (arr) => {
      const g = {};
      for (const r of arr) {
        const k = String(r[waterfallField] ?? "Other");
        g[k] = (g[k] || 0) + (r[valF] || 0);
      }
      return g;
    };
    const baseG = groupBy(baseline);
    const scG = groupBy(scenarioData);
    const allKeys = [...new Set([...Object.keys(baseG), ...Object.keys(scG)])];

    // Build waterfall items: only where there's a change
    const items = [];
    for (const k of allKeys) {
      const bv = baseG[k] || 0;
      const sv = scG[k] || 0;
      const delta = sv - bv;
      if (Math.abs(delta) > 0.01) items.push({ key: k, delta });
    }
    // Sort by absolute delta descending
    items.sort((a, b) => Math.abs(b.delta) - Math.abs(a.delta));

    const baseTotal = baseline.reduce((s, r) => s + (r[valF] || 0), 0);
    const scTotal = scenarioData.reduce((s, r) => s + (r[valF] || 0), 0);

    // Build waterfall bars
    const bars = [];
    bars.push({ name: "Actuals", value: baseTotal, isTotal: true, delta: 0, bottom: 0 });
    let running = baseTotal;
    for (const item of items.slice(0, 15)) {
      const bottom = item.delta >= 0 ? running : running + item.delta;
      bars.push({ name: item.key.length > 20 ? item.key.slice(0, 18) + "…" : item.key, value: item.delta, isTotal: false, delta: item.delta, bottom });
      running += item.delta;
    }
    // If there are more items, aggregate as "Other"
    if (items.length > 15) {
      const rest = items.slice(15).reduce((s, i) => s + i.delta, 0);
      if (Math.abs(rest) > 0.01) {
        const bottom = rest >= 0 ? running : running + rest;
        bars.push({ name: "Other", value: rest, isTotal: false, delta: rest, bottom });
        running += rest;
      }
    }
    bars.push({ name: scenarioName, value: scTotal, isTotal: true, delta: 0, bottom: 0 });
    return bars;
  }, [baseline, scenarioData, waterfallField, valF, scenarioName]);

  if (!data.length) return null;

  return (
    <ResponsiveContainer width="100%" height={300}>
      <BarChart data={data} margin={{ top: 8, right: 12, left: 12, bottom: 80 }}>
        <CartesianGrid strokeDasharray="3 3" stroke={C.border} />
        <XAxis dataKey="name" tick={{ fill: C.textMuted, fontSize: 10 }} angle={-35} textAnchor="end" interval={0} height={80} />
        <YAxis tick={{ fill: C.textMuted, fontSize: 10 }} tickFormatter={fmtS} />
        <Tooltip
          contentStyle={{ background: C.white, border: `1px solid ${C.border}`, borderRadius: 8, fontSize: 11, boxShadow: "0 4px 12px rgba(0,0,0,0.08)" }}
          formatter={(v, name) => {
            if (name === "bottom") return [null, null];
            return [fmt(v), ""];
          }}
        />
        <Bar dataKey="bottom" stackId="a" fill="transparent" />
        <Bar dataKey="value" stackId="a" radius={[3, 3, 0, 0]}>
          {data.map((d, i) => (
            <Cell key={i} fill={d.isTotal ? scenarioColor || C.brand : d.delta >= 0 ? C.green : C.red} opacity={d.isTotal ? 0.7 : 0.85} />
          ))}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  );
}

// ─── COMPARISON TABLE ───────────────────────────────────────────
function ComparisonTable({ baseline, scenarioOutputs, rowFs, colF, valF, scenarios }) {
  const [sortCol, setSortCol] = useState(null);
  const [sortDir, setSortDir] = useState("asc");
  const hasCol = colF && colF.length > 0;

  // Compute column keys
  const colKeys = useMemo(() => {
    if (!hasCol) return [];
    const keys = new Set();
    baseline.forEach(r => { if (r[colF] != null) keys.add(r[colF]); });
    return [...keys].sort();
  }, [baseline, colF, hasCol]);

  // Build raw data with column-pivoted values
  const rawData = useMemo(() => {
    if (!hasCol) {
      // Simple mode: no column pivot
      const { rows: actRows } = computePivot(baseline, rowFs, null, valF, "none");
      const scPivots = {};
      for (const sc of scenarios) {
        const { rows: sr } = computePivot(scenarioOutputs[sc.name] || [], rowFs, null, valF, "none");
        const map = {}; for (const r of sr) map[r._key] = r._total;
        scPivots[sc.name] = { map };
      }
      return actRows.map(r => {
        const out = { ...r };
        for (const sc of scenarios) {
          out["sc_" + sc.name] = scPivots[sc.name]?.map[r._key] || 0;
          out["var_" + sc.name] = (scPivots[sc.name]?.map[r._key] || 0) - r._total;
        }
        return out;
      });
    }
    // Column pivot mode: group by rowFs, then for each colKey compute actuals + scenarios
    const groupData = (data) => {
      const groups = {};
      for (const r of data) {
        const rk = rowFs.map(f => r[f] ?? "—").join(" | ");
        const ck = r[colF] ?? "—";
        if (!groups[rk]) { groups[rk] = { _key: rk }; rowFs.forEach(f => groups[rk][f] = r[f] ?? "—"); }
        groups[rk]["col_" + ck] = (groups[rk]["col_" + ck] || 0) + (r[valF] || 0);
        groups[rk]._total = (groups[rk]._total || 0) + (r[valF] || 0);
      }
      return groups;
    };
    const actG = groupData(baseline);
    const scGroups = {};
    for (const sc of scenarios) scGroups[sc.name] = groupData(scenarioOutputs[sc.name] || []);

    return Object.values(actG).map(r => {
      const out = { ...r };
      // For each colKey and scenario, add values
      for (const ck of colKeys) {
        out["act_" + ck] = r["col_" + ck] || 0;
        for (const sc of scenarios) {
          const sr = scGroups[sc.name]?.[r._key];
          out["sc_" + sc.name + "_" + ck] = sr?.["col_" + ck] || 0;
          out["var_" + sc.name + "_" + ck] = (sr?.["col_" + ck] || 0) - (r["col_" + ck] || 0);
        }
      }
      // Totals per scenario
      for (const sc of scenarios) {
        const sr = scGroups[sc.name]?.[r._key];
        out["sc_" + sc.name] = sr?._total || 0;
        out["var_" + sc.name] = (sr?._total || 0) - (r._total || 0);
      }
      return out;
    });
  }, [baseline, scenarioOutputs, rowFs, colF, valF, scenarios, hasCol, colKeys]);

  const data = useMemo(() => {
    const arr = [...rawData];
    if (!sortCol) {
      const f0 = rowFs[0];
      const isTime = f0 && (f0.includes("period") || f0.includes("_year") || f0.includes("_month") || f0 === "period");
      if (isTime) arr.sort((a, b) => String(a[f0] ?? "").localeCompare(String(b[f0] ?? "")));
      else arr.sort((a, b) => (b._total || 0) - (a._total || 0));
      return arr;
    }
    arr.sort((a, b) => {
      const av = a[sortCol] ?? 0; const bv = b[sortCol] ?? 0;
      if (typeof av === "number" && typeof bv === "number") return sortDir === "asc" ? av - bv : bv - av;
      return sortDir === "asc" ? String(av).localeCompare(String(bv)) : String(bv).localeCompare(String(av));
    });
    return arr;
  }, [rawData, sortCol, sortDir, rowFs]);

  function toggleSort(col) {
    if (sortCol === col) setSortDir(d => d === "asc" ? "desc" : "asc");
    else { setSortCol(col); setSortDir(typeof (rawData[0]?.[col]) === "number" ? "desc" : "asc"); }
  }
  const arrow = col => sortCol === col ? (sortDir === "asc" ? " ↑" : " ↓") : "";
  const thClick = (col, align = "left", extra = {}) => ({
    ...S.th, textAlign: align, cursor: "pointer", userSelect: "none",
    color: sortCol === col ? C.brand : S.th.color, ...extra,
  });

  if (!data.length) return null;

  // Column pivot mode
  if (hasCol && colKeys.length > 0) {
    return (
      <div style={{ maxHeight: 480, overflow: "auto" }}>
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead>
            {/* Top header row: column field values as groups */}
            <tr>
              <th colSpan={rowFs.length} style={{ ...S.th, borderBottom: "none" }}></th>
              {colKeys.map(ck => (
                <th key={ck} colSpan={1 + scenarios.length * 2} style={{ ...S.th, textAlign: "center", borderBottom: "none", color: C.brand, fontSize: 11, fontWeight: 700 }}>{String(ck)}</th>
              ))}
              <th colSpan={1 + scenarios.length * 2} style={{ ...S.th, textAlign: "center", borderBottom: "none", color: C.text, fontWeight: 700, fontSize: 11 }}>Total</th>
            </tr>
            {/* Sub header row: Actuals, Scenarios, Deltas */}
            <tr>
              {rowFs.map(f => <th key={f} style={thClick(f)} onClick={() => toggleSort(f)}>{f.replace(/_/g, " ")}{arrow(f)}</th>)}
              {colKeys.map(ck => (
                <React.Fragment key={ck}>
                  <th style={thClick("act_" + ck, "right")} onClick={() => toggleSort("act_" + ck)}>Act{arrow("act_" + ck)}</th>
                  {scenarios.map(sc => (
                    <React.Fragment key={sc.id}>
                      <th style={thClick("sc_" + sc.name + "_" + ck, "right", { color: sortCol === "sc_" + sc.name + "_" + ck ? C.brand : sc.color })} onClick={() => toggleSort("sc_" + sc.name + "_" + ck)}>{sc.name.slice(0, 6)}{arrow("sc_" + sc.name + "_" + ck)}</th>
                      <th style={thClick("var_" + sc.name + "_" + ck, "right", { color: sortCol === "var_" + sc.name + "_" + ck ? C.brand : sc.color })} onClick={() => toggleSort("var_" + sc.name + "_" + ck)}>Δ{arrow("var_" + sc.name + "_" + ck)}</th>
                    </React.Fragment>
                  ))}
                </React.Fragment>
              ))}
              <th style={thClick("_total", "right")} onClick={() => toggleSort("_total")}>Act{arrow("_total")}</th>
              {scenarios.map(sc => (
                <React.Fragment key={sc.id}>
                  <th style={thClick("sc_" + sc.name, "right", { color: sortCol === "sc_" + sc.name ? C.brand : sc.color })} onClick={() => toggleSort("sc_" + sc.name)}>{sc.name.slice(0, 6)}{arrow("sc_" + sc.name)}</th>
                  <th style={thClick("var_" + sc.name, "right", { color: sortCol === "var_" + sc.name ? C.brand : sc.color })} onClick={() => toggleSort("var_" + sc.name)}>Δ{arrow("var_" + sc.name)}</th>
                </React.Fragment>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.slice(0, 80).map((r, i) => (
              <tr key={i} style={{ background: i % 2 === 0 ? "" : C.bg }}>
                {rowFs.map(f => <td key={f} style={S.td}>{String(r[f] ?? "—")}</td>)}
                {colKeys.map(ck => (
                  <React.Fragment key={ck}>
                    <td style={{ ...S.td, ...S.mono, textAlign: "right", color: (r["act_" + ck] || 0) >= 0 ? C.green : C.red }}>{fmt(r["act_" + ck])}</td>
                    {scenarios.map(sc => {
                      const sv = r["sc_" + sc.name + "_" + ck] || 0;
                      const dv = r["var_" + sc.name + "_" + ck] || 0;
                      return (
                        <React.Fragment key={sc.id}>
                          <td style={{ ...S.td, ...S.mono, textAlign: "right", color: sc.color }}>{fmt(sv)}</td>
                          <td style={{ ...S.td, ...S.mono, textAlign: "right", color: dv >= 0 ? C.green : C.red }}>{dv >= 0 ? "+" : ""}{fmt(dv)}</td>
                        </React.Fragment>
                      );
                    })}
                  </React.Fragment>
                ))}
                <td style={{ ...S.td, ...S.mono, textAlign: "right", fontWeight: 600, color: (r._total || 0) >= 0 ? C.green : C.red }}>{fmt(r._total)}</td>
                {scenarios.map(sc => {
                  const sv = r["sc_" + sc.name] || 0;
                  const dv = r["var_" + sc.name] || 0;
                  return (
                    <React.Fragment key={sc.id}>
                      <td style={{ ...S.td, ...S.mono, textAlign: "right", fontWeight: 600, color: sc.color }}>{fmt(sv)}</td>
                      <td style={{ ...S.td, ...S.mono, textAlign: "right", fontWeight: 600, color: dv >= 0 ? C.green : C.red }}>{dv >= 0 ? "+" : ""}{fmt(dv)}</td>
                    </React.Fragment>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    );
  }

  // Simple mode (no column pivot)
  return (
    <div style={{ maxHeight: 450, overflow: "auto" }}>
      <table style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead><tr>
          {rowFs.map(f => <th key={f} style={thClick(f)} onClick={() => toggleSort(f)}>{f.replace(/_/g, " ")}{arrow(f)}</th>)}
          <th style={thClick("_total", "right")} onClick={() => toggleSort("_total")}>Actuals{arrow("_total")}</th>
          {scenarios.map(sc => <th key={sc.id} style={thClick("sc_" + sc.name, "right", { color: sortCol === "sc_" + sc.name ? C.brand : sc.color })} onClick={() => toggleSort("sc_" + sc.name)}>{sc.name}{arrow("sc_" + sc.name)}</th>)}
          {scenarios.map(sc => <th key={"v" + sc.id} style={thClick("var_" + sc.name, "right", { color: sortCol === "var_" + sc.name ? C.brand : sc.color })} onClick={() => toggleSort("var_" + sc.name)}>Δ {sc.name}{arrow("var_" + sc.name)}</th>)}
        </tr></thead>
        <tbody>
          {data.slice(0, 120).map((r, i) => (
            <tr key={i} style={{ background: i % 2 === 0 ? "" : C.bg }}>
              {rowFs.map(f => <td key={f} style={S.td}>{String(r[f] ?? "—")}</td>)}
              <td style={{ ...S.td, ...S.mono, textAlign: "right", color: r._total >= 0 ? C.green : C.red }}>{fmt(r._total)}</td>
              {scenarios.map(sc => <td key={sc.id} style={{ ...S.td, ...S.mono, textAlign: "right", color: sc.color }}>{fmt(r["sc_" + sc.name])}</td>)}
              {scenarios.map(sc => {
                const v = r["var_" + sc.name];
                return <td key={"v" + sc.id} style={{ ...S.td, ...S.mono, textAlign: "right", color: v >= 0 ? C.green : C.red }}>{v >= 0 ? "+" : ""}{fmt(v)}</td>;
              })}
            </tr>
          ))}
        </tbody>
        <tfoot><tr style={{ background: C.bg }}>
          <td colSpan={rowFs.length} style={{ ...S.th, fontWeight: 700, color: C.text, borderTop: `2px solid ${C.border}` }}>Total</td>
          <td style={{ ...S.th, ...S.mono, textAlign: "right", fontWeight: 700, color: C.text, borderTop: `2px solid ${C.border}` }}>{fmt(data.reduce((s, r) => s + (r._total || 0), 0))}</td>
          {scenarios.map(sc => <td key={sc.id} style={{ ...S.th, ...S.mono, textAlign: "right", fontWeight: 700, color: sc.color, borderTop: `2px solid ${C.border}` }}>{fmt(data.reduce((s, r) => s + (r["sc_" + sc.name] || 0), 0))}</td>)}
          {scenarios.map(sc => {
            const v = data.reduce((s, r) => s + (r["var_" + sc.name] || 0), 0);
            return <td key={"v" + sc.id} style={{ ...S.th, ...S.mono, textAlign: "right", fontWeight: 700, color: v >= 0 ? C.green : C.red, borderTop: `2px solid ${C.border}` }}>{v >= 0 ? "+" : ""}{fmt(v)}</td>;
          })}
        </tr></tfoot>
      </table>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// SCHEMA VIEW (with editable roles + editable relationships)
// ═══════════════════════════════════════════════════════════════
function SchemaView({ tables, schema, setSchema, relationships, setRelationships, onOpenUpload }) {
  const [addRelOpen, setAddRelOpen] = useState(false);
  const [newRel, setNewRel] = useState({ from: "", to: "", fromCol: "", toCol: "" });
  const tableNames = Object.keys(schema);

  function changeRole(tn, cn, nr) {
    setSchema(p => {
      const n = { ...p };
      n[tn] = { ...n[tn], columns: n[tn].columns.map(c => c.name === cn ? { ...c, role: nr } : c) };
      n[tn].isFact = n[tn].columns.some(c => c.role === "measure") && n[tn].columns.filter(c => c.role === "key").length >= 2;
      return n;
    });
  }

  function addRelationship() {
    if (!newRel.from || !newRel.to || !newRel.fromCol || !newRel.toCol) return;
    const v1 = new Set((tables[newRel.from]?.data ?? []).map(r => r[newRel.fromCol]).filter(v => v != null).map(String));
    const v2 = new Set((tables[newRel.to]?.data ?? []).map(r => r[newRel.toCol]).filter(v => v != null).map(String));
    const ov = [...v1].filter(v => v2.has(v)).length;
    setRelationships(p => [...p, {
      id: `${newRel.from}-${newRel.to}-${newRel.fromCol}-${Date.now()}`,
      ...newRel,
      coverage: Math.round(ov / Math.min(v1.size || 1, v2.size || 1) * 100),
      overlapCount: ov
    }]);
    setNewRel({ from: "", to: "", fromCol: "", toCol: "" });
    setAddRelOpen(false);
  }

  function removeRel(id) { setRelationships(p => p.filter(r => r.id !== id)); }

  function updateRelCol(id, side, col) {
    setRelationships(p => p.map(r => {
      if (r.id !== id) return r;
      const updated = { ...r, [side]: col };
      const v1 = new Set((tables[updated.from]?.data ?? []).map(row => row[updated.fromCol]).filter(v => v != null).map(String));
      const v2 = new Set((tables[updated.to]?.data ?? []).map(row => row[updated.toCol]).filter(v => v != null).map(String));
      const ov = [...v1].filter(v => v2.has(v)).length;
      return { ...updated, coverage: Math.round(ov / Math.min(v1.size || 1, v2.size || 1) * 100), overlapCount: ov };
    }));
  }

  return (
    <div>
      <div style={{ marginBottom: 20, display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
        <div>
          <h2 style={{ fontSize: 20, fontWeight: 700, color: C.text, marginBottom: 4 }}>Data Model</h2>
          <p style={{ color: C.textSec, fontSize: 13 }}>Auto-discovered schema. Edit roles and relationships below.</p>
        </div>
        {onOpenUpload && (
          <button onClick={onOpenUpload} style={S.btn("primary", true)}>+ Upload Data</button>
        )}
      </div>

      {/* Relationships */}
      <div style={S.card}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
          <div style={S.cardT}>Relationships</div>
          <button onClick={() => setAddRelOpen(!addRelOpen)} style={S.btn("primary", true)}>+ Add Relationship</button>
        </div>

        {addRelOpen && (
          <div style={{ background: C.bg, borderRadius: 8, padding: 14, border: `1px solid ${C.border}`, marginBottom: 12 }}>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr auto", gap: 8, alignItems: "end" }}>
              <div>
                <label style={{ fontSize: 10, color: C.textMuted, fontWeight: 600 }}>From Table</label>
                <select style={{ ...S.select, width: "100%" }} value={newRel.from} onChange={e => setNewRel(p => ({ ...p, from: e.target.value, fromCol: "" }))}>
                  <option value="">Select...</option>
                  {tableNames.map(t => <option key={t} value={t}>{t}</option>)}
                </select>
              </div>
              <div>
                <label style={{ fontSize: 10, color: C.textMuted, fontWeight: 600 }}>From Column</label>
                <select style={{ ...S.select, width: "100%" }} value={newRel.fromCol} onChange={e => setNewRel(p => ({ ...p, fromCol: e.target.value }))}>
                  <option value="">Select...</option>
                  {newRel.from && schema[newRel.from]?.columns.map(c => <option key={c.name} value={c.name}>{c.name}</option>)}
                </select>
              </div>
              <div>
                <label style={{ fontSize: 10, color: C.textMuted, fontWeight: 600 }}>To Table</label>
                <select style={{ ...S.select, width: "100%" }} value={newRel.to} onChange={e => setNewRel(p => ({ ...p, to: e.target.value, toCol: "" }))}>
                  <option value="">Select...</option>
                  {tableNames.filter(t => t !== newRel.from).map(t => <option key={t} value={t}>{t}</option>)}
                </select>
              </div>
              <div>
                <label style={{ fontSize: 10, color: C.textMuted, fontWeight: 600 }}>To Column</label>
                <select style={{ ...S.select, width: "100%" }} value={newRel.toCol} onChange={e => setNewRel(p => ({ ...p, toCol: e.target.value }))}>
                  <option value="">Select...</option>
                  {newRel.to && schema[newRel.to]?.columns.map(c => <option key={c.name} value={c.name}>{c.name}</option>)}
                </select>
              </div>
              <button onClick={addRelationship} style={S.btn("primary", true)}>Add</button>
            </div>
          </div>
        )}

        <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
          {relationships.map(rel => (
            <div key={rel.id} style={{ background: C.bg, borderRadius: 8, padding: "10px 14px", border: `1px solid ${C.border}`, display: "flex", alignItems: "center", gap: 10 }}>
              <span style={{ ...S.badge(C.brand), minWidth: 80, justifyContent: "center" }}>{rel.from}</span>
              <select style={{ ...S.select, fontSize: 11, padding: "3px 6px" }} value={rel.fromCol} onChange={e => updateRelCol(rel.id, "fromCol", e.target.value)}>
                {schema[rel.from]?.columns.map(c => <option key={c.name} value={c.name}>{c.name}</option>)}
              </select>
              <span style={{ color: C.textMuted, fontSize: 18, fontWeight: 300 }}>=</span>
              <span style={{ ...S.badge(C.purple), minWidth: 80, justifyContent: "center" }}>{rel.to}</span>
              <select style={{ ...S.select, fontSize: 11, padding: "3px 6px" }} value={rel.toCol} onChange={e => updateRelCol(rel.id, "toCol", e.target.value)}>
                {schema[rel.to]?.columns.map(c => <option key={c.name} value={c.name}>{c.name}</option>)}
              </select>
              <div style={{ flex: 1 }} />
              <span style={{ ...S.mono, fontSize: 10, color: rel.coverage > 80 ? C.green : rel.coverage > 50 ? C.amber : C.red }}>{rel.coverage}%</span>
              <span style={{ fontSize: 10, color: C.textMuted }}>{rel.overlapCount} matches</span>
              <span onClick={() => removeRel(rel.id)} style={{ cursor: "pointer", color: C.textMuted, fontSize: 14, padding: "2px 4px" }}>×</span>
            </div>
          ))}
        </div>
      </div>

      {/* Tables */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(260px, 1fr))", gap: 14 }}>
        {Object.entries(schema).map(([name, info]) => (
          <div key={name} style={{ ...S.card, borderColor: info.isFact ? C.brand + "44" : C.border, minWidth: 0, overflow: "hidden" }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 6 }}>
              <span style={{ fontSize: 15, fontWeight: 700, color: C.text }}>{name}</span>
              <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
                {info.aiAnalyzing && (
                  <span style={{ ...S.badge(C.amber), fontSize: 9, animation: "pulse 1.5s infinite" }}>⏳ Analyzing…</span>
                )}
                <span style={S.badge(info.isFact ? C.brand : C.purple)}>{info.isFact ? "FACT" : "DIMENSION"}</span>
              </div>
            </div>
            {info.aiNotes?.description && (
              <div style={{ fontSize: 11, color: C.textSec, marginBottom: 6, fontStyle: "italic" }}>{info.aiNotes.description}</div>
            )}
            <div style={{ fontSize: 12, color: C.textMuted, marginBottom: 10 }}>{info.rowCount} rows</div>
            <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead><tr>
                <th style={S.th}>Column</th>
                <th style={S.th}>Role</th>
                <th style={S.th}>Unique</th>
              </tr></thead>
              <tbody>{info.columns.map(col => (
                <tr key={col.name}>
                  <td style={{ ...S.td, ...S.mono, fontSize: 11 }}>{col.name}</td>
                  <td style={S.td}>
                    <select value={col.role} onChange={e => changeRole(name, col.name, e.target.value)}
                      style={{ ...S.select, padding: "2px 8px", fontSize: 10, background: ROLE_COLORS[col.role] + "12", color: ROLE_COLORS[col.role], border: `1px solid ${ROLE_COLORS[col.role]}30`, borderRadius: 20, fontWeight: 600 }}>
                      {ROLE_OPTIONS.map(r => <option key={r} value={r}>{r.toUpperCase()}</option>)}
                    </select>
                    {col.aiSuggestion?.reasoning && col.aiSuggestion.suggested_role !== col.role && (
                      <div title={col.aiSuggestion.reasoning} style={{ fontSize: 9, color: C.amber, cursor: "help", marginTop: 2 }}>
                        AI: {col.aiSuggestion.suggested_role} ⓘ
                      </div>
                    )}
                  </td>
                  <td style={{ ...S.td, color: C.textMuted, fontSize: 11 }}>{col.uniqueCount}</td>
                </tr>
              ))}</tbody>
            </table>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// ACTUALS VIEW
// ═══════════════════════════════════════════════════════════════
function ActualsView({ baseline, schema }) {
  const dims = useMemo(() => getDimFields(baseline), [baseline]);
  const measures = useMemo(() => getMeasureFields(baseline, schema), [baseline, schema]);
  const [rowFs, setRowFs] = useState(() => []);
  const [colF, setColF] = useState("");
  const [valF, setValF] = useState(() => "");
  const [filters, setFilters] = useState({});
  const filtered = useMemo(() => applyFilters(baseline, filters), [baseline, filters]);

  return (
    <div>
      <div style={{ marginBottom: 20 }}>
        <h2 style={{ fontSize: 20, fontWeight: 700, color: C.text, marginBottom: 4 }}>Actuals</h2>
        <p style={{ color: C.textSec, fontSize: 13 }}>{filtered.length} entries after filters</p>
      </div>
      <div style={S.card}>
        <div style={{ display: "grid", gridTemplateColumns: "2fr 2fr 1fr", gap: 14, marginBottom: 10 }}>
          <FieldManager label="Row Fields" allFields={dims} selected={rowFs} onChange={setRowFs} color={C.brand} />
          <FieldManager label="Column Field" allFields={dims.filter(f => !rowFs.includes(f))} selected={colF} onChange={setColF} color={C.purple} single />
          <FieldManager label="Value" allFields={measures} selected={valF} onChange={setValF} color={C.green} single />
        </div>
        <FilterManager baseline={baseline} allFields={dims} filters={filters} setFilters={setFilters} />
      </div>
      <div style={S.card}>
        <div style={S.cardT}>Pivot Table</div>
        <PivotTableView data={filtered} rowFs={rowFs} colF={colF} valF={valF} />
      </div>
      <div style={S.card}>
        <div style={S.cardT}>Chart</div>
        <PivotChartView data={filtered} rowFs={rowFs} colF={colF} valF={valF} />
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// SCENARIOS VIEW
// ═══════════════════════════════════════════════════════════════
// ─── RULE FILTER HELPERS (for inline rule editing) ──────────────
function RuleFilterTag({ dim, activeVals, baseline, onChange, onRemove }) {
  const [expanded, setExpanded] = useState(false);
  const [search, setSearch] = useState("");
  const ref = useRef(null);
  useEffect(() => {
    const h = e => { if (ref.current && !ref.current.contains(e.target)) setExpanded(false); };
    document.addEventListener("mousedown", h);
    return () => document.removeEventListener("mousedown", h);
  }, []);
  const allVals = getUniq(baseline, dim);
  const filtered = search ? allVals.filter(v => String(v).toLowerCase().includes(search.toLowerCase())) : allVals;
  return (
    <div ref={ref} style={{ position: "relative" }}>
      <span onClick={() => { setExpanded(!expanded); setSearch(""); }}
        style={{ ...S.tag(activeVals.length ? C.amber : C.textMuted) }}>
        {dim.replace(/_/g, " ")}{activeVals.length ? ` (${activeVals.length})` : ""}
        <span onClick={e => { e.stopPropagation(); onRemove(); }}
          style={{ cursor: "pointer", opacity: .5, fontSize: 13 }}>×</span>
      </span>
      {expanded && (
        <div style={S.dropdown}>
          <input autoFocus style={{ ...S.input, marginBottom: 6, fontSize: 11 }}
            placeholder="Search values..." value={search}
            onChange={e => setSearch(e.target.value)} />
          {filtered.slice(0, 80).map(v => {
            const ch = activeVals.includes(v);
            return (
              <label key={String(v)} style={{ display: "flex", alignItems: "center", gap: 6, padding: "4px 4px", fontSize: 12, color: C.text, cursor: "pointer" }}>
                <input type="checkbox" checked={ch} style={{ accentColor: C.brand }}
                  onChange={() => onChange(ch ? activeVals.filter(x => x !== v) : [...activeVals, v])} />
                {String(v)}
              </label>
            );
          })}
        </div>
      )}
    </div>
  );
}

function RuleFilterAdd({ dims, existingFilters, onAdd }) {
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState("");
  const ref = useRef(null);
  useEffect(() => {
    const h = e => { if (ref.current && !ref.current.contains(e.target)) setOpen(false); };
    document.addEventListener("mousedown", h);
    return () => document.removeEventListener("mousedown", h);
  }, []);
  const available = dims.filter(f => !existingFilters.includes(f))
    .filter(f => !search || f.toLowerCase().includes(search.toLowerCase()));
  return (
    <div ref={ref} style={{ position: "relative" }}>
      <button onClick={() => { setOpen(!open); setSearch(""); }}
        style={{ ...S.btn("ghost", true), padding: "4px 8px", fontSize: 11, color: C.amber, border: `1px dashed ${C.amber}44` }}>+ filter</button>
      {open && (
        <div style={S.dropdown}>
          <input autoFocus style={{ ...S.input, marginBottom: 6, fontSize: 11 }}
            placeholder="Search fields..." value={search}
            onChange={e => setSearch(e.target.value)} />
          {available.map(f => (
            <div key={f} onClick={() => { onAdd(f); setOpen(false); }}
              style={{ padding: "6px 8px", borderRadius: 6, cursor: "pointer", fontSize: 12, color: C.text }}
              onMouseEnter={e => e.target.style.background = C.surfaceHover}
              onMouseLeave={e => e.target.style.background = ""}>
              {f.replace(/_/g, " ")}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function ScenariosView({ baseline, scenarios, setScenarios, schema }) {
  const dims = useMemo(() => getDimFields(baseline), [baseline]);
  const measures = useMemo(() => getMeasureFields(baseline, schema), [baseline, schema]);
  const periods = useMemo(() => getUniq(baseline, "_period"), [baseline]);

  const [active, setActive] = useState(new Set());
  const [editId, setEditId] = useState(null);
  const [rowFs, setRowFs] = useState(() => []);
  const [colF, setColF] = useState("");
  const [valF, setValF] = useState(() => "");
  const [filters, setFilters] = useState({});
  const [newRule, setNewRule] = useState({ name: "", type: "multiplier", factor: 1.05, offset: 0, filters: {}, periodFrom: "", periodTo: "" });
  const [ruleFilterFields, setRuleFilterFields] = useState([]);
  const [ruleFilterSearch, setRuleFilterSearch] = useState("");
  const [ruleFilterOpen, setRuleFilterOpen] = useState(false);
  const [ruleFilterExpanded, setRuleFilterExpanded] = useState(null);
  const [ruleValSearch, setRuleValSearch] = useState("");
  const ruleFilterRef = useRef(null);
  const [waterfallField, setWaterfallField] = useState("");

  const toggle = id => setActive(p => { const n = new Set(p); n.has(id) ? n.delete(id) : n.add(id); return n; });
  const filtered = useMemo(() => applyFilters(baseline, filters), [baseline, filters]);
  const scOutputs = useMemo(() => {
    const o = {};
    for (const sc of scenarios) if (active.has(sc.id)) o[sc.name] = applyFilters(applyRules(baseline, sc.rules), filters);
    return o;
  }, [scenarios, active, baseline, filters]);
  const editSc = scenarios.find(s => s.id === editId);

  function addScenario() {
    const id = Date.now();
    setScenarios(p => [...p, { id, name: `Scenario ${p.length + 1}`, rules: [], color: SC_COLORS[p.length % SC_COLORS.length] }]);
    setEditId(id); setActive(p => new Set([...p, id]));
  }
  function delScenario(id) { setScenarios(p => p.filter(s => s.id !== id)); setActive(p => { const n = new Set(p); n.delete(id); return n; }); if (editId === id) setEditId(null); }
  function renameScenario(id, newName) { if (newName.trim()) setScenarios(p => p.map(s => s.id === id ? { ...s, name: newName.trim() } : s)); }
  function addRule() {
    if (!editId || !newRule.name) return;
    setScenarios(p => p.map(s => s.id !== editId ? s : { ...s, rules: [...s.rules, { ...newRule, id: Date.now() }] }));
    setNewRule({ name: "", type: "multiplier", factor: 1.05, offset: 0, filters: {}, periodFrom: "", periodTo: "" });
    setRuleFilterFields([]);
  }
  function rmRule(rid) { setScenarios(p => p.map(s => s.id !== editId ? s : { ...s, rules: s.rules.filter(r => r.id !== rid) })); }
  function updateRule(rid, updates) { setScenarios(p => p.map(s => s.id !== editId ? s : { ...s, rules: s.rules.map(r => r.id === rid ? { ...r, ...updates } : r) })); }
  const [editingRuleId, setEditingRuleId] = useState(null);

  const variance = useMemo(() => {
    if (!active.size) return [];
    const at = filtered.reduce((s, r) => s + (r[valF] || 0), 0);
    return scenarios.filter(sc => active.has(sc.id)).map(sc => {
      const sd = scOutputs[sc.name] || [];
      const st = sd.reduce((s, r) => s + (r[valF] || 0), 0);
      return { name: sc.name, color: sc.color, total: st, variance: st - at, pct: at ? ((st - at) / Math.abs(at)) * 100 : 0 };
    });
  }, [active, scenarios, scOutputs, filtered, valF]);

  return (
    <div>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 20 }}>
        <div>
          <h2 style={{ fontSize: 20, fontWeight: 700, color: C.text, marginBottom: 4 }}>Scenarios</h2>
          <p style={{ color: C.textSec, fontSize: 13 }}>{active.size} active · {scenarios.length} total</p>
        </div>
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          <button style={S.btn("primary")} onClick={addScenario}>+ New Scenario</button>
        </div>
      </div>

      {scenarios.length > 0 && (
        <div style={{ display: "flex", gap: 8, marginBottom: 16, flexWrap: "wrap" }}>
          {scenarios.map(sc => (
            <div key={sc.id} style={{ display: "flex", alignItems: "center", gap: 2 }}>
              <button onClick={() => toggle(sc.id)}
                style={{ display: "inline-flex", alignItems: "center", gap: 8, padding: "10px 20px", borderRadius: 10, cursor: "pointer", fontSize: 14, fontWeight: 600, fontFamily: "'Plus Jakarta Sans', sans-serif", background: active.has(sc.id) ? sc.color + "15" : C.white, border: `2px solid ${active.has(sc.id) ? sc.color : C.border}`, color: active.has(sc.id) ? sc.color : C.textMuted, transition: "all .15s" }}>
                <span style={{ width: 10, height: 10, borderRadius: "50%", background: sc.color, flexShrink: 0 }} />
                {sc.name}
                <span style={{ fontSize: 11, opacity: 0.6 }}>({sc.rules.length})</span>
              </button>
              <span onClick={() => setEditId(editId === sc.id ? null : sc.id)} style={{ padding: "6px 8px", cursor: "pointer", color: editId === sc.id ? C.brand : C.textMuted, fontSize: 15 }}>✎</span>
              <span onClick={() => delScenario(sc.id)} style={{ padding: "6px 6px", cursor: "pointer", color: C.textMuted, fontSize: 15 }}>×</span>
            </div>
          ))}
        </div>
      )}

      <div style={S.card}>
        <div style={{ display: "grid", gridTemplateColumns: "2fr 2fr 1fr", gap: 14, marginBottom: 10 }}>
          <FieldManager label="Row Fields" allFields={dims} selected={rowFs} onChange={setRowFs} color={C.brand} />
          <FieldManager label="Column Field" allFields={dims.filter(f => !rowFs.includes(f))} selected={colF} onChange={setColF} color={C.purple} single />
          <FieldManager label="Value" allFields={measures} selected={valF} onChange={setValF} color={C.green} single />
        </div>
        <FilterManager baseline={baseline} allFields={dims} filters={filters} setFilters={setFilters} />
      </div>

      {editSc && (
        <div style={{ ...S.card, borderColor: editSc.color + "44", borderWidth: 2 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 12 }}>
            <span style={{ fontSize: 11, fontWeight: 700, color: editSc.color, textTransform: "uppercase", letterSpacing: "0.6px" }}>Editing:</span>
            <input
              value={editSc.name}
              onChange={e => renameScenario(editSc.id, e.target.value)}
              style={{ ...S.input, fontSize: 14, fontWeight: 700, color: editSc.color, border: `1px solid ${editSc.color}33`, background: editSc.color + "08", padding: "4px 10px", borderRadius: 6, width: "auto", minWidth: 120, maxWidth: 300 }}
            />
          </div>
          {editSc.rules.map(rule => {
            const isEditing = editingRuleId === rule.id;
            return (
              <div key={rule.id} style={{ background: C.bg, borderRadius: 8, border: `1px solid ${isEditing ? editSc.color + "44" : C.border}`, marginBottom: 4, overflow: "hidden" }}>
                {/* Collapsed summary row */}
                <div style={{ padding: "8px 12px", display: "flex", justifyContent: "space-between", alignItems: "center", cursor: "pointer" }}
                  onClick={() => setEditingRuleId(isEditing ? null : rule.id)}>
                  <div style={{ display: "flex", flexWrap: "wrap", alignItems: "center", gap: 6 }}>
                    <span style={{ fontSize: 12, color: isEditing ? editSc.color : C.textMuted }}>{isEditing ? "▾" : "▸"}</span>
                    <span style={{ fontWeight: 600, fontSize: 12 }}>{rule.name}</span>
                    <span style={S.badge(rule.type === "multiplier" ? C.brand : C.amber)}>{rule.type === "multiplier" ? `×${rule.factor}` : `+${fmt(rule.offset)}`}</span>
                    {rule.periodFrom && <span style={{ fontSize: 10, color: C.textMuted }}>{rule.periodFrom} → {rule.periodTo || "∞"}</span>}
                    {Object.entries(rule.filters || {}).filter(([, v]) => v && (!Array.isArray(v) || v.length > 0)).map(([k, v]) =>
                      <span key={k} style={S.badge(C.purple)}>{k}: {Array.isArray(v) ? (v.length > 2 ? v.slice(0, 2).join(", ") + ` +${v.length - 2}` : v.join(", ")) : v}</span>
                    )}
                  </div>
                  <button onClick={e => { e.stopPropagation(); rmRule(rule.id); }} style={{ ...S.btn("danger", true), borderRadius: 6 }}>×</button>
                </div>
                {/* Expanded editor */}
                {isEditing && (
                  <div style={{ padding: "8px 12px 12px", borderTop: `1px solid ${C.border}` }}>
                    <div style={{ display: "grid", gridTemplateColumns: "2fr 1fr 1fr", gap: 8 }}>
                      <div>
                        <label style={{ fontSize: 10, color: C.textMuted, fontWeight: 600 }}>Name</label>
                        <input style={S.input} value={rule.name} onChange={e => updateRule(rule.id, { name: e.target.value })} />
                      </div>
                      <div>
                        <label style={{ fontSize: 10, color: C.textMuted, fontWeight: 600 }}>Type</label>
                        <select style={{ ...S.select, width: "100%" }} value={rule.type} onChange={e => updateRule(rule.id, { type: e.target.value })}>
                          <option value="multiplier">Multiplier (×)</option>
                          <option value="offset">Offset (+/-)</option>
                        </select>
                      </div>
                      <div>
                        <label style={{ fontSize: 10, color: C.textMuted, fontWeight: 600 }}>{rule.type === "multiplier" ? "Factor" : "Total Offset"}</label>
                        {rule.type === "multiplier"
                          ? <input style={S.input} type="number" step="0.01" value={rule.factor} onChange={e => updateRule(rule.id, { factor: parseFloat(e.target.value) || 1 })} />
                          : <input style={S.input} type="number" step="1000" value={rule.offset} onChange={e => updateRule(rule.id, { offset: parseFloat(e.target.value) || 0 })} />}
                      </div>
                    </div>
                    <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8, marginTop: 8 }}>
                      <div>
                        <label style={{ fontSize: 10, color: C.textMuted, fontWeight: 600 }}>Period From</label>
                        <select style={{ ...S.select, width: "100%" }} value={rule.periodFrom || ""} onChange={e => updateRule(rule.id, { periodFrom: e.target.value })}>
                          <option value="">All</option>
                          {periods.map(p => <option key={p} value={p}>{p}</option>)}
                        </select>
                      </div>
                      <div>
                        <label style={{ fontSize: 10, color: C.textMuted, fontWeight: 600 }}>Period To</label>
                        <select style={{ ...S.select, width: "100%" }} value={rule.periodTo || ""} onChange={e => updateRule(rule.id, { periodTo: e.target.value })}>
                          <option value="">All</option>
                          {periods.map(p => <option key={p} value={p}>{p}</option>)}
                        </select>
                      </div>
                    </div>
                    {/* Inline filter editor for existing rule */}
                    <div style={{ marginTop: 8 }}>
                      <div style={{ fontSize: 10, color: C.textMuted, textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: 4, fontWeight: 600 }}>Filters</div>
                      <div style={{ display: "flex", flexWrap: "wrap", gap: 4, alignItems: "center" }}>
                        {Object.keys(rule.filters || {}).map(dim => {
                          const vals = rule.filters[dim] || [];
                          const activeVals = Array.isArray(vals) ? vals : (vals ? [vals] : []);
                          return (
                            <RuleFilterTag key={dim} dim={dim} activeVals={activeVals} baseline={baseline}
                              onChange={nv => updateRule(rule.id, { filters: { ...rule.filters, [dim]: nv } })}
                              onRemove={() => { const nf = { ...rule.filters }; delete nf[dim]; updateRule(rule.id, { filters: nf }); }}
                            />
                          );
                        })}
                        <RuleFilterAdd dims={dims} existingFilters={Object.keys(rule.filters || {})}
                          onAdd={dim => updateRule(rule.id, { filters: { ...rule.filters, [dim]: [] } })}
                        />
                      </div>
                    </div>
                  </div>
                )}
              </div>
            );
          })}

          <div style={{ background: C.bg, borderRadius: 10, padding: 16, border: `1px solid ${C.border}`, marginTop: 8 }}>
            <div style={{ display: "grid", gridTemplateColumns: "2fr 1fr 1fr", gap: 10 }}>
              <div>
                <label style={{ fontSize: 10, color: C.textMuted, fontWeight: 600 }}>Rule Name</label>
                <input style={S.input} value={newRule.name} onChange={e => setNewRule(p => ({ ...p, name: e.target.value }))} placeholder="e.g. Revenue +5%" />
              </div>
              <div>
                <label style={{ fontSize: 10, color: C.textMuted, fontWeight: 600 }}>Type</label>
                <select style={{ ...S.select, width: "100%" }} value={newRule.type} onChange={e => setNewRule(p => ({ ...p, type: e.target.value }))}>
                  <option value="multiplier">Multiplier (×)</option>
                  <option value="offset">Offset (+/-)</option>
                </select>
              </div>
              <div>
                <label style={{ fontSize: 10, color: C.textMuted, fontWeight: 600 }}>{newRule.type === "multiplier" ? "Factor" : "Total Offset"}</label>
                {newRule.type === "multiplier"
                  ? <input style={S.input} type="number" step="0.01" value={newRule.factor} onChange={e => setNewRule(p => ({ ...p, factor: parseFloat(e.target.value) || 1 }))} />
                  : <input style={S.input} type="number" step="1000" value={newRule.offset} onChange={e => setNewRule(p => ({ ...p, offset: parseFloat(e.target.value) || 0 }))} />}
              </div>
            </div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10, marginTop: 10 }}>
              <div>
                <label style={{ fontSize: 10, color: C.textMuted, fontWeight: 600 }}>Period From</label>
                <select style={{ ...S.select, width: "100%" }} value={newRule.periodFrom} onChange={e => setNewRule(p => ({ ...p, periodFrom: e.target.value }))}>
                  <option value="">All</option>
                  {periods.map(p => <option key={p} value={p}>{p}</option>)}
                </select>
              </div>
              <div>
                <label style={{ fontSize: 10, color: C.textMuted, fontWeight: 600 }}>Period To</label>
                <select style={{ ...S.select, width: "100%" }} value={newRule.periodTo} onChange={e => setNewRule(p => ({ ...p, periodTo: e.target.value }))}>
                  <option value="">All</option>
                  {periods.map(p => <option key={p} value={p}>{p}</option>)}
                </select>
              </div>
            </div>

            {/* Rule filters */}
            <div style={{ marginTop: 10 }} ref={ruleFilterRef}>
              <div style={{ fontSize: 10, color: C.textMuted, textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: 5, fontWeight: 600 }}>Rule Filters</div>
              <div style={{ display: "flex", flexWrap: "wrap", gap: 4, alignItems: "center" }}>
                {ruleFilterFields.map(dim => {
                  const vals = newRule.filters[dim] || [];
                  const isArr = Array.isArray(vals);
                  const activeVals = isArr ? vals : (vals ? [vals] : []);
                  const expanded = ruleFilterExpanded === dim;
                  const dimVals = getUniq(baseline, dim);
                  return (
                    <div key={dim} style={{ position: "relative" }}>
                      <span onClick={() => { setRuleFilterExpanded(expanded ? null : dim); setRuleValSearch(""); }}
                        style={{ ...S.tag(activeVals.length ? C.amber : C.textMuted) }}>
                        {dim.replace(/_/g, " ")}{activeVals.length ? ` (${activeVals.length})` : ""}
                        <span onClick={e => { e.stopPropagation(); setRuleFilterFields(p => p.filter(f => f !== dim)); setNewRule(p => { const nf = { ...p.filters }; delete nf[dim]; return { ...p, filters: nf }; }); setRuleFilterExpanded(null); }}
                          style={{ cursor: "pointer", opacity: .5, fontSize: 13 }}>×</span>
                      </span>
                      {expanded && (
                        <div style={S.dropdown}>
                          <input autoFocus style={{ ...S.input, marginBottom: 6, fontSize: 11 }} placeholder="Search values..." value={ruleValSearch} onChange={e => setRuleValSearch(e.target.value)} />
                          {dimVals.filter(v => !ruleValSearch || String(v).toLowerCase().includes(ruleValSearch.toLowerCase())).slice(0, 80).map(v => {
                            const ch = activeVals.includes(v);
                            return (
                              <label key={String(v)} style={{ display: "flex", alignItems: "center", gap: 6, padding: "4px 4px", fontSize: 12, color: C.text, cursor: "pointer" }}>
                                <input type="checkbox" checked={ch} style={{ accentColor: C.brand }} onChange={() => {
                                  const nv = ch ? activeVals.filter(x => x !== v) : [...activeVals, v];
                                  setNewRule(p => ({ ...p, filters: { ...p.filters, [dim]: nv } }));
                                }} />
                                {String(v)}
                              </label>
                            );
                          })}
                        </div>
                      )}
                    </div>
                  );
                })}
                <div style={{ position: "relative" }}>
                  <button onClick={() => { setRuleFilterOpen(!ruleFilterOpen); setRuleFilterSearch(""); }}
                    style={{ ...S.btn("ghost", true), padding: "4px 8px", fontSize: 11, color: C.amber, border: `1px dashed ${C.amber}44` }}>+ filter</button>
                  {ruleFilterOpen && (
                    <div style={S.dropdown}>
                      <input autoFocus style={{ ...S.input, marginBottom: 6, fontSize: 11 }} placeholder="Search fields..." value={ruleFilterSearch} onChange={e => setRuleFilterSearch(e.target.value)} />
                      {dims.filter(f => !ruleFilterFields.includes(f)).filter(f => !ruleFilterSearch || f.toLowerCase().includes(ruleFilterSearch.toLowerCase())).map(f => (
                        <div key={f} onClick={() => { setRuleFilterFields(p => [...p, f]); setRuleFilterOpen(false); }}
                          style={{ padding: "6px 8px", borderRadius: 6, cursor: "pointer", fontSize: 12, color: C.text }}
                          onMouseEnter={e => e.target.style.background = C.surfaceHover}
                          onMouseLeave={e => e.target.style.background = ""}>
                          {f.replace(/_/g, " ")}
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              </div>
            </div>

            <button style={{ ...S.btn("primary"), marginTop: 12 }} onClick={addRule} disabled={!newRule.name}>Add Rule</button>
          </div>
        </div>
      )}

      {active.size > 0 && rowFs.length > 0 && (
        <div style={S.card}>
          <div style={S.cardT}>Comparison Chart</div>
          <PivotChartView data={filtered} rowFs={rowFs} colF={colF} valF={valF} scenarioData={scOutputs} />
        </div>
      )}

      {variance.length > 0 && (
        <div style={S.card}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
            <div style={S.cardT}>Waterfall Analysis</div>
            <FieldManager label="" allFields={dims} selected={waterfallField} onChange={setWaterfallField} color={C.purple} single />
          </div>
          {waterfallField ? (
            <div style={{ display: "grid", gridTemplateColumns: `repeat(${Math.min(scenarios.filter(sc => active.has(sc.id)).length, 2)}, 1fr)`, gap: 14 }}>
              {scenarios.filter(sc => active.has(sc.id)).map(sc => (
                <div key={sc.id}>
                  <div style={{ fontSize: 12, fontWeight: 600, color: sc.color, marginBottom: 6, textAlign: "center" }}>{sc.name}</div>
                  <WaterfallChart baseline={filtered} scenarioData={scOutputs[sc.name]} scenarioName={sc.name} scenarioColor={sc.color} rowFs={rowFs} valF={valF} waterfallField={waterfallField} />
                </div>
              ))}
            </div>
          ) : (
            <div style={{ textAlign: "center", padding: 24, color: C.textMuted, fontSize: 12 }}>Select a field above to break down changes by dimension.</div>
          )}
        </div>
      )}

      {variance.length > 0 && (
        <div style={S.card}>
          <div style={S.cardT}>Variance Summary</div>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead><tr>
              <th style={S.th}>Scenario</th>
              <th style={{ ...S.th, textAlign: "right" }}>Total</th>
              <th style={{ ...S.th, textAlign: "right" }}>Δ Actuals</th>
              <th style={{ ...S.th, textAlign: "right" }}>Δ %</th>
            </tr></thead>
            <tbody>
              <tr><td style={S.td}><span style={{ color: C.textSec }}>● Actuals</span></td>
                <td style={{ ...S.td, ...S.mono, textAlign: "right" }}>{fmt(filtered.reduce((s, r) => s + (r[valF] || 0), 0))}</td>
                <td style={{ ...S.td, textAlign: "right" }}>—</td><td style={{ ...S.td, textAlign: "right" }}>—</td></tr>
              {variance.map(v => (
                <tr key={v.name}>
                  <td style={S.td}><span style={{ color: v.color, fontWeight: 600 }}>● {v.name}</span></td>
                  <td style={{ ...S.td, ...S.mono, textAlign: "right" }}>{fmt(v.total)}</td>
                  <td style={{ ...S.td, ...S.mono, textAlign: "right", color: v.variance >= 0 ? C.green : C.red }}>{v.variance >= 0 ? "+" : ""}{fmt(v.variance)}</td>
                  <td style={{ ...S.td, textAlign: "right", color: v.pct >= 0 ? C.green : C.red, fontWeight: 600 }}>{v.pct >= 0 ? "+" : ""}{v.pct.toFixed(1)}%</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {active.size > 0 && rowFs.length > 0 && valF && (
        <div style={S.card}>
          <div style={S.cardT}>Comparison Table</div>
          <ComparisonTable baseline={filtered} scenarioOutputs={scOutputs} rowFs={rowFs} colF={colF} valF={valF} scenarios={scenarios.filter(sc => active.has(sc.id))} />
        </div>
      )}

      {scenarios.length === 0 && (
        <div style={{ ...S.card, textAlign: "center", padding: 48 }}>
          <div style={{ fontSize: 36, marginBottom: 10 }}>📊</div>
          <div style={{ fontSize: 17, fontWeight: 700, color: C.text, marginBottom: 6 }}>No scenarios yet</div>
          <p style={{ color: C.textMuted, fontSize: 13, marginBottom: 16 }}>Create a scenario to model "what-if" plans against your actuals.</p>
          <button style={S.btn("primary")} onClick={addScenario}>Create First Scenario</button>
        </div>
      )}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// CHAT PANEL
// ═══════════════════════════════════════════════════════════════
function ChatPanel({ baseline, scenarios, setScenarios, setActiveTab, datasetId }) {
  const [messages, setMessages] = useState([
    { role: "assistant", content: "Data loaded. Ask me anything about your data, or say **\"What if…\"** to build a scenario." }
  ]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const endRef = useRef(null);
  const abortRef = useRef(null);
  useEffect(() => { endRef.current?.scrollIntoView({ behavior: "smooth" }); }, [messages]);

  async function send() {
    if (!input.trim() || loading || !datasetId) return;
    const msg = input.trim();
    setInput("");
    setLoading(true);
    const history = messages
      .filter(m => m.role !== "system")
      .map(m => ({ role: m.role, content: m.content }));
    setMessages(p => [...p, { role: "user", content: msg }]);

    // Add a placeholder assistant message to stream into
    setMessages(p => [...p, { role: "assistant", content: "" }]);
    const abortCtrl = new AbortController();
    abortRef.current = abortCtrl;
    const pendingRules = [];

    try {
      for await (const event of streamChat(msg, datasetId, history, abortCtrl.signal)) {
        if (event.type === "text_delta") {
          setMessages(p => {
            const next = [...p];
            next[next.length - 1] = { ...next[next.length - 1], content: next[next.length - 1].content + event.text };
            return next;
          });
        } else if (event.type === "scenario_rule") {
          pendingRules.push({ ...event.rule, id: Date.now() + pendingRules.length });
        } else if (event.type === "done") {
          if (pendingRules.length) {
            setScenarios(p => [...p, {
              id: Date.now(),
              name: `Scenario ${p.length + 1}`,
              rules: pendingRules,
              color: SC_COLORS[p.length % SC_COLORS.length],
            }]);
            setActiveTab("scenarios");
          }
          // Ensure placeholder has content
          setMessages(p => {
            const last = p[p.length - 1];
            if (!last.content) {
              const next = [...p];
              next[next.length - 1] = { ...last, content: pendingRules.length ? "Done! Check the Scenarios tab." : "Could you rephrase?" };
              return next;
            }
            return p;
          });
        } else if (event.type === "error") {
          setMessages(p => { const next = [...p]; next[next.length - 1] = { ...next[next.length - 1], content: `Error: ${event.message}` }; return next; });
        }
      }
    } catch (e) {
      if (e.name !== "AbortError") {
        setMessages(p => { const next = [...p]; next[next.length - 1] = { ...next[next.length - 1], content: "Connection issue. Try again or build scenarios manually." }; return next; });
      }
    }
    setLoading(false);
  }

  return (
    <div style={{ width: 340, borderLeft: `1px solid ${C.border}`, display: "flex", flexDirection: "column", background: C.white, flexShrink: 0 }}>
      <div style={{ padding: "14px 16px", borderBottom: `1px solid ${C.border}`, fontSize: 13, fontWeight: 700, color: C.brand, display: "flex", alignItems: "center", gap: 6 }}>
        <svg width="16" height="16" viewBox="0 0 16 16" fill="none"><circle cx="8" cy="8" r="7" stroke={C.brand} strokeWidth="2" /><path d="M5 8h6M8 5v6" stroke={C.brand} strokeWidth="1.5" strokeLinecap="round" /></svg>
        AI Assistant
      </div>
      <div style={{ flex: 1, overflow: "auto", padding: 12, display: "flex", flexDirection: "column", gap: 8 }}>
        {messages.map((m, i) => (
          <div key={i} style={{ padding: "10px 14px", borderRadius: 10, fontSize: 12, lineHeight: 1.6, background: m.role === "user" ? C.brandLight : C.bg, border: `1px solid ${m.role === "user" ? C.brandMid : C.border}`, alignSelf: m.role === "user" ? "flex-end" : "flex-start", maxWidth: "92%" }}>
            {m.content.split("\n").map((l, j) => {
              let h = l.replace(/\*\*(.*?)\*\*/g, '<b>$1</b>');
              if (l.startsWith("• ") || l.startsWith("- ")) return <div key={j} style={{ paddingLeft: 10, marginBottom: 2 }}><span style={{ color: C.brand, marginRight: 4 }}>·</span><span dangerouslySetInnerHTML={{ __html: h.slice(2) }} /></div>;
              return <div key={j} style={{ marginBottom: l ? 1 : 6 }} dangerouslySetInnerHTML={{ __html: h }} />;
            })}
          </div>
        ))}
        {loading && <div style={{ padding: "10px 14px", borderRadius: 10, background: C.bg, border: `1px solid ${C.border}`, fontSize: 12, color: C.textMuted }}>Thinking...</div>}
        <div ref={endRef} />
      </div>
      <div style={{ padding: 12, borderTop: `1px solid ${C.border}`, display: "flex", gap: 6 }}>
        <input style={{ ...S.input, flex: 1 }} value={input} onChange={e => setInput(e.target.value)} onKeyDown={e => e.key === "Enter" && send()} placeholder="Ask about your data..." />
        <button style={S.btn("primary", true)} onClick={send} disabled={loading}>→</button>
      </div>
    </div>
  );
}

// ─── API ADAPTERS ────────────────────────────────────────────────
// Convert SchemaResponse[] → legacy {schema, relationships} shapes used by components

function apiToSchema(schemaList) {
  const schema = {};
  const _now = Date.now();
  for (const sr of schemaList) {
    const aiAnalyzing = !sr.dataset.ai_analyzed &&
      _now - new Date(sr.dataset.created_at).getTime() < 180_000;
    schema[sr.dataset.name] = {
      id: sr.dataset.id,
      columns: sr.columns.map(c => ({
        id: c.id,
        name: c.column_name,
        role: c.column_role,
        uniqueCount: c.unique_count ?? 0,
        aiSuggestion: c.ai_suggestion ?? null,
      })),
      isFact: sr.columns.some(c => c.column_role === "measure"),
      rowCount: sr.dataset.row_count,
      aiAnalyzed: sr.dataset.ai_analyzed,
      aiAnalyzing,
      aiNotes: sr.dataset.ai_notes ?? null,
    };
  }
  return schema;
}

function apiToRelationships(schemaList) {
  const dsById = Object.fromEntries(schemaList.map(sr => [sr.dataset.id, sr.dataset.name]));
  const seen = new Set();
  const rels = [];
  for (const sr of schemaList) {
    for (const rel of sr.relationships) {
      if (seen.has(rel.id)) continue;
      seen.add(rel.id);
      rels.push({
        id: rel.id,
        from: dsById[rel.source_dataset_id] ?? rel.source_dataset_id,
        to: dsById[rel.target_dataset_id] ?? rel.target_dataset_id,
        fromCol: rel.source_column,
        toCol: rel.target_column,
        coverage: rel.coverage_pct ?? 0,
        overlapCount: rel.overlap_count ?? 0,
      });
    }
  }
  return rels;
}

function apiBaselineToRows(bl) {
  return (bl.data ?? []).map(row => {
    const obj = {};
    (bl.columns ?? []).forEach((col, i) => { obj[col] = row[i]; });
    return obj;
  });
}

// ─── UPLOAD MODAL ────────────────────────────────────────────────
function UploadModal({ isOpen, onClose, onUploaded, schemaList }) {
  const [queue, setQueue] = useState([]); // [{id, file, status, error, datasetIds}]
  const [dragging, setDragging] = useState(false);
  const [deletingId, setDeletingId] = useState(null); // dataset id pending confirm
  const inputRef = useRef(null);
  const uploadingRef = useRef(false);

  // Process upload queue sequentially
  useEffect(() => {
    async function processQueue() {
      if (uploadingRef.current) return;
      const next = queue.find(q => q.status === "queued");
      if (!next) return;
      uploadingRef.current = true;
      setQueue(p => p.map(q => q.id === next.id ? { ...q, status: "uploading" } : q));
      try {
        const result = await uploadFile(next.file);
        const ids = (result ?? []).map(ds => ds.id);
        setQueue(p => p.map(q => q.id === next.id ? { ...q, status: "done", datasetIds: ids } : q));
        onUploaded();
      } catch (e) {
        setQueue(p => p.map(q => q.id === next.id ? { ...q, status: "error", error: e.message ?? "Upload failed" } : q));
      }
      uploadingRef.current = false;
    }
    processQueue();
  }, [queue]);

  function addFiles(files) {
    const newItems = Array.from(files).map(file => ({
      id: `${Date.now()}-${Math.random()}`,
      file,
      status: "queued",
      error: null,
      datasetIds: [],
    }));
    setQueue(p => [...p, ...newItems]);
  }

  async function handleDelete(dsId) {
    try {
      await deleteDataset(dsId);
      onUploaded();
    } catch (e) {
      console.error("Delete failed", e);
    } finally {
      setDeletingId(null);
    }
  }

  if (!isOpen) return null;

  return (
    <div style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,0.35)", zIndex: 1000, display: "flex", alignItems: "center", justifyContent: "center" }}
      onClick={e => { if (e.target === e.currentTarget) onClose(); }}>
      <div style={{ background: C.white, borderRadius: 16, width: 520, maxHeight: "85vh", display: "flex", flexDirection: "column", boxShadow: "0 20px 60px rgba(0,0,0,0.2)", overflow: "hidden" }}>

        {/* Header */}
        <div style={{ padding: "16px 20px", borderBottom: `1px solid ${C.border}`, display: "flex", justifyContent: "space-between", alignItems: "center", flexShrink: 0 }}>
          <span style={{ fontSize: 15, fontWeight: 700, color: C.text }}>Upload Data Files</span>
          <span onClick={onClose} style={{ cursor: "pointer", color: C.textMuted, fontSize: 20, lineHeight: 1, padding: "0 4px" }}>×</span>
        </div>

        <div style={{ overflow: "auto", flex: 1, padding: 20, display: "flex", flexDirection: "column", gap: 16 }}>

          {/* Loaded datasets overview */}
          {schemaList.length > 0 && (
            <div>
              <div style={{ fontSize: 11, fontWeight: 700, color: C.textMuted, textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: 8 }}>Loaded Datasets</div>
              <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                {schemaList.map(sr => {
                  const isDeleting = deletingId === sr.dataset.id;
                  const analyzed = sr.dataset.ai_analyzed;
                  return (
                    <div key={sr.dataset.id} style={{
                      display: "flex", alignItems: "center", gap: 10,
                      padding: "9px 12px", borderRadius: 8,
                      background: isDeleting ? "#fff0f0" : C.bg,
                      border: `1px solid ${isDeleting ? C.red + "44" : C.border}`,
                      fontSize: 12,
                    }}>
                      <div style={{ flex: 1, minWidth: 0 }}>
                        <span style={{ fontWeight: 600, color: C.text }}>{sr.dataset.source_filename ?? sr.dataset.name}</span>
                        <span style={{ color: C.textMuted, marginLeft: 8 }}>{sr.dataset.row_count.toLocaleString()} rows</span>
                      </div>
                      <span style={{ ...S.badge(analyzed ? C.green : (Date.now() - new Date(sr.dataset.created_at).getTime() < 180_000 ? C.amber : C.border)), fontSize: 9 }}>
                        {analyzed ? "✓ Analyzed" : (Date.now() - new Date(sr.dataset.created_at).getTime() < 180_000 ? "⏳ Analyzing…" : "—")}
                      </span>
                      {isDeleting ? (
                        <button onClick={() => handleDelete(sr.dataset.id)} style={{ ...S.btn("danger", true), fontSize: 11, padding: "3px 10px" }}>Confirm</button>
                      ) : (
                        <span onClick={() => setDeletingId(sr.dataset.id)} title="Delete dataset" style={{ cursor: "pointer", color: C.textMuted, fontSize: 15, padding: "2px 4px" }}>🗑</span>
                      )}
                      {isDeleting && (
                        <span onClick={() => setDeletingId(null)} style={{ cursor: "pointer", color: C.textMuted, fontSize: 12 }}>Cancel</span>
                      )}
                    </div>
                  );
                })}
              </div>
            </div>
          )}

          {/* Drop zone */}
          <div
            onDragOver={e => { e.preventDefault(); setDragging(true); }}
            onDragLeave={() => setDragging(false)}
            onDrop={e => { e.preventDefault(); setDragging(false); addFiles(e.dataTransfer.files); }}
            onClick={() => inputRef.current?.click()}
            style={{
              border: `2px dashed ${dragging ? C.brand : C.border}`,
              borderRadius: 12, padding: "32px 20px", textAlign: "center",
              cursor: "pointer", background: dragging ? C.brandLight : C.bg,
              transition: "all .15s", flexShrink: 0,
            }}
          >
            <input ref={inputRef} type="file" multiple accept=".xlsx,.xls,.csv,.tsv"
              style={{ display: "none" }} onChange={e => addFiles(e.target.files)} />
            <div style={{ fontSize: 24, marginBottom: 8 }}>📂</div>
            <div style={{ fontSize: 13, fontWeight: 600, color: C.text, marginBottom: 4 }}>Drop files here or click to browse</div>
            <div style={{ fontSize: 11, color: C.textMuted }}>.xlsx · .xls · .csv · .tsv · multiple files supported</div>
          </div>

          {/* Upload queue */}
          {queue.length > 0 && (
            <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
              <div style={{ fontSize: 11, fontWeight: 700, color: C.textMuted, textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: 4 }}>Upload Queue</div>
              {queue.map(item => {
                const analyzing = item.status === "done" && item.datasetIds.some(id => {
                  const sr = schemaList.find(s => s.dataset.id === id);
                  return sr && !sr.dataset.ai_analyzed &&
                    Date.now() - new Date(sr.dataset.created_at).getTime() < 180_000;
                });
                const fullyDone = item.status === "done" && !analyzing;
                return (
                  <div key={item.id} style={{
                    display: "flex", alignItems: "center", gap: 10,
                    padding: "8px 12px", borderRadius: 8,
                    background: item.status === "error" ? "#fff0f0" : C.bg,
                    border: `1px solid ${item.status === "error" ? C.red + "33" : C.border}`,
                    fontSize: 12,
                  }}>
                    <div style={{ flex: 1, minWidth: 0, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", fontWeight: 500, color: C.text }}>
                      {item.file.name}
                    </div>
                    {item.status === "queued" && <span style={{ color: C.textMuted }}>Queued…</span>}
                    {item.status === "uploading" && <span style={{ color: C.brand, animation: "pulse 1s infinite" }}>Uploading…</span>}
                    {fullyDone && <span style={{ color: C.green }}>✓ Done</span>}
                    {analyzing && <span style={{ color: C.amber, animation: "pulse 1.5s infinite" }}>✓ Uploaded · AI analyzing…</span>}
                    {item.status === "error" && (
                      <span style={{ color: C.red }} title={item.error}>✗ {item.error?.slice(0, 40)}</span>
                    )}
                    <span onClick={() => setQueue(p => p.filter(q => q.id !== item.id && item.status !== "uploading"))}
                      style={{ cursor: item.status === "uploading" ? "not-allowed" : "pointer", color: C.textMuted, opacity: item.status === "uploading" ? 0.3 : 1, fontSize: 14 }}>×</span>
                  </div>
                );
              })}
            </div>
          )}
        </div>

        {/* Footer */}
        <div style={{ padding: "12px 20px", borderTop: `1px solid ${C.border}`, display: "flex", justifyContent: "flex-end", flexShrink: 0 }}>
          <button onClick={onClose} style={S.btn("secondary", true)}>Close</button>
        </div>
      </div>
    </div>
  );
}

// ─── UPLOAD SCREEN ───────────────────────────────────────────────
function UploadScreen({ onUploaded }) {
  const [dragging, setDragging] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [error, setError] = useState(null);
  const inputRef = useRef(null);

  async function handleFile(file) {
    if (!file) return;
    setUploading(true);
    setError(null);
    try {
      await uploadFile(file);
      onUploaded();
    } catch (e) {
      setError(e.message ?? "Upload failed");
      setUploading(false);
    }
  }

  return (
    <div style={{ height: "100vh", display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", background: C.bg, fontFamily: "'Plus Jakarta Sans', sans-serif" }}>
      <link href={FONT_URL} rel="stylesheet" />
      <div style={{ marginBottom: 20, display: "flex", alignItems: "center", gap: 10 }}>
        <div style={{ width: 36, height: 36, borderRadius: 10, background: `linear-gradient(135deg, ${C.brand}, ${C.brandDark})`, display: "flex", alignItems: "center", justifyContent: "center" }}>
          <span style={{ color: "#fff", fontSize: 17, fontWeight: 800 }}>d</span>
        </div>
        <span style={{ fontSize: 22, fontWeight: 800, color: C.text }}>data<span style={{ color: C.brand }}>Bob</span>IQ</span>
      </div>

      <div
        onDragOver={e => { e.preventDefault(); setDragging(true); }}
        onDragLeave={() => setDragging(false)}
        onDrop={e => { e.preventDefault(); setDragging(false); handleFile(e.dataTransfer.files[0]); }}
        onClick={() => inputRef.current?.click()}
        style={{
          width: 420, border: `2px dashed ${dragging ? C.brand : C.border}`,
          borderRadius: 16, padding: "48px 32px", textAlign: "center", cursor: "pointer",
          background: dragging ? C.brandLight : C.white, transition: "all .2s",
        }}
      >
        <input ref={inputRef} type="file" accept=".xlsx,.xls,.csv,.tsv" style={{ display: "none" }}
          onChange={e => handleFile(e.target.files[0])} />
        <div style={{ fontSize: 36, marginBottom: 12 }}>📂</div>
        <div style={{ fontSize: 16, fontWeight: 700, color: C.text, marginBottom: 6 }}>
          {uploading ? "Uploading…" : "Drop your data file here"}
        </div>
        <div style={{ fontSize: 13, color: C.textMuted }}>
          Supports .xlsx · .xls · .csv · .tsv
        </div>
        {error && <div style={{ marginTop: 14, color: C.red, fontSize: 12 }}>{error}</div>}
      </div>
    </div>
  );
}

// ─── LOADING SCREEN ──────────────────────────────────────────────
function LoadingScreen() {
  return (
    <div style={{ height: "100vh", display: "flex", alignItems: "center", justifyContent: "center", background: C.bg, fontFamily: "'Plus Jakarta Sans', sans-serif", color: C.textMuted, fontSize: 14 }}>
      <link href={FONT_URL} rel="stylesheet" />
      Loading…
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// MAIN APP
// ═══════════════════════════════════════════════════════════════
export default function App() {
  const queryClient = useQueryClient();
  const [tab, setTab] = useState("schema");
  const [scenarios, setScenarios] = useState([]);
  const [uploadOpen, setUploadOpen] = useState(false);

  // ── Load datasets from API ──────────────────────────────────────
  const { data: schemaList = [], isLoading } = useQuery({
    queryKey: ["datasets"],
    queryFn: getDatasets,
    staleTime: 30_000,
    refetchInterval: (query) => {
      const list = query.state.data ?? [];
      const now = Date.now();
      return list.some(sr =>
        !sr.dataset.ai_analyzed &&
        now - new Date(sr.dataset.created_at).getTime() < 180_000
      ) ? 3_000 : false;
    },
  });

  // ── Local schema / relationships state (initialised from API) ──
  const [schema, setSchema] = useState({});
  const [relationships, setRelationships] = useState([]);

  useEffect(() => {
    if (schemaList.length) {
      setSchema(apiToSchema(schemaList));
      setRelationships(apiToRelationships(schemaList));
    }
  }, [schemaList]);

  // ── Fact dataset (first dataset with a measure column) ──────────
  const factDataset = useMemo(
    () => schemaList.find(sr => sr.columns.some(c => c.column_role === "measure")) ?? schemaList[0],
    [schemaList]
  );

  // ── Dataset name → id lookup (for relationship creation) ────────
  const dsNameToId = useMemo(
    () => Object.fromEntries(schemaList.map(sr => [sr.dataset.name, sr.dataset.id])),
    [schemaList]
  );

  // ── Baseline from API ───────────────────────────────────────────
  const relIds = useMemo(() => relationships.map(r => r.id), [relationships]);
  const { data: apiBaseline } = useQuery({
    queryKey: ["baseline", factDataset?.dataset.id, relIds],
    queryFn: () => getBaseline(factDataset.dataset.id, relIds),
    enabled: !!factDataset?.dataset.id,
    staleTime: 30_000,
  });
  const baseline = useMemo(() => apiBaseline ? apiBaselineToRows(apiBaseline) : [], [apiBaseline]);

  // ── Schema change handler — persists role changes to API ────────
  function handleSetSchema(updater) {
    setSchema(prev => {
      const next = typeof updater === "function" ? updater(prev) : updater;
      for (const [, tinfo] of Object.entries(next)) {
        const prevCols = prev[tinfo.id === undefined ? "__" : Object.keys(prev).find(k => prev[k].id === tinfo.id) ?? "__"]?.columns ?? [];
        for (const col of tinfo.columns) {
          const prevCol = prevCols.find(c => c.name === col.name);
          if (prevCol && prevCol.role !== col.role && col.id && tinfo.id) {
            apiUpdateColumnRole(tinfo.id, col.id, { column_role: col.role }).catch(console.error);
          }
        }
      }
      return next;
    });
  }

  // ── Relationship change handler — persists to API ───────────────
  function handleSetRelationships(updater) {
    setRelationships(prev => {
      const next = typeof updater === "function" ? updater(prev) : updater;
      const prevIds = new Set(prev.map(r => r.id));
      const nextIds = new Set(next.map(r => r.id));

      // Deleted
      for (const r of prev) {
        if (!nextIds.has(r.id)) {
          apiDeleteRelationship(r.id).then(() => queryClient.invalidateQueries({ queryKey: ["datasets"] })).catch(console.error);
        }
      }
      // Added (local temp ID = string like "gl_entries-accounts-…")
      for (const r of next) {
        if (!prevIds.has(r.id)) {
          const srcId = dsNameToId[r.from];
          const tgtId = dsNameToId[r.to];
          if (srcId && tgtId) {
            apiCreateRelationship({ source_dataset_id: srcId, target_dataset_id: tgtId, source_column: r.fromCol, target_column: r.toCol })
              .then(() => queryClient.invalidateQueries({ queryKey: ["datasets"] }))
              .catch(console.error);
          }
        }
      }
      // Modified
      for (const r of next) {
        if (prevIds.has(r.id)) {
          const p = prev.find(x => x.id === r.id);
          if (p && (p.fromCol !== r.fromCol || p.toCol !== r.toCol)) {
            apiUpdateRelationship(r.id, { source_column: r.fromCol, target_column: r.toCol })
              .then(() => queryClient.invalidateQueries({ queryKey: ["datasets"] }))
              .catch(console.error);
          }
        }
      }
      return next;
    });
  }

  if (isLoading) return <LoadingScreen />;
  if (!schemaList.length) return <UploadScreen onUploaded={() => queryClient.invalidateQueries({ queryKey: ["datasets"] })} />;

  const datasetLabel = factDataset ? `${factDataset.dataset.name} · ${schemaList.length} dataset${schemaList.length !== 1 ? "s" : ""}` : "";

  return (
    <div style={{ fontFamily: "'Plus Jakarta Sans', sans-serif", background: C.bg, color: C.text, height: "100vh", fontSize: 13, display: "flex", flexDirection: "column" }}>
      <link href={FONT_URL} rel="stylesheet" />
      <style>{`
        ::-webkit-scrollbar { width: 5px; }
        ::-webkit-scrollbar-track { background: transparent; }
        ::-webkit-scrollbar-thumb { background: ${C.border}; border-radius: 3px; }
        select option { background: ${C.white}; color: ${C.text}; }
        * { box-sizing: border-box; }
        input:focus, select:focus { border-color: ${C.brand} !important; }
        @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.45} }
      `}</style>

      <div style={{ padding: "0 24px", height: 56, borderBottom: `1px solid ${C.border}`, display: "flex", alignItems: "center", justifyContent: "space-between", background: C.white, flexShrink: 0 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <div style={{ width: 28, height: 28, borderRadius: 8, background: `linear-gradient(135deg, ${C.brand}, ${C.brandDark})`, display: "flex", alignItems: "center", justifyContent: "center" }}>
            <span style={{ color: "#fff", fontSize: 13, fontWeight: 800 }}>d</span>
          </div>
          <span style={{ fontSize: 17, fontWeight: 800, color: C.text, letterSpacing: "-0.3px" }}>
            data<span style={{ color: C.brand }}>Bob</span>IQ
          </span>
        </div>

        <div style={{ display: "flex", gap: 2, background: C.bg, borderRadius: 10, padding: 3 }}>
          {[{ id: "schema", l: "Data Model" }, { id: "actuals", l: "Actuals" }, { id: "scenarios", l: "Scenarios" }].map(t => (
            <button key={t.id} onClick={() => setTab(t.id)} style={{
              padding: "7px 18px", borderRadius: 8, border: "none", cursor: "pointer", fontSize: 13, fontWeight: 600,
              fontFamily: "'Plus Jakarta Sans', sans-serif", transition: "all .15s",
              background: tab === t.id ? C.white : "transparent",
              color: tab === t.id ? C.brand : C.textMuted,
              boxShadow: tab === t.id ? "0 1px 3px rgba(0,0,0,0.08)" : "none",
            }}>
              {t.l}
              {t.id === "scenarios" && scenarios.length > 0 && (
                <span style={{ marginLeft: 6, background: C.brandLight, color: C.brand, borderRadius: 10, padding: "1px 7px", fontSize: 10, fontWeight: 700 }}>{scenarios.length}</span>
              )}
            </button>
          ))}
        </div>

        <div style={{ fontSize: 12, color: C.textMuted, fontWeight: 500 }}>{datasetLabel}</div>
      </div>

      <div style={{ display: "flex", flex: 1, overflow: "hidden" }}>
        <div style={{ flex: 1, overflow: "auto", padding: 24 }}>
          {tab === "schema" && <SchemaView tables={{}} schema={schema} setSchema={handleSetSchema} relationships={relationships} setRelationships={handleSetRelationships} onOpenUpload={() => setUploadOpen(true)} />}
          {tab === "actuals" && <ActualsView baseline={baseline} schema={schema} />}
          {tab === "scenarios" && <ScenariosView baseline={baseline} scenarios={scenarios} setScenarios={setScenarios} schema={schema} />}
        </div>
        <ChatPanel baseline={baseline} scenarios={scenarios} setScenarios={setScenarios} setActiveTab={setTab} datasetId={factDataset?.dataset.id} />
      </div>
      <UploadModal
        isOpen={uploadOpen}
        onClose={() => setUploadOpen(false)}
        onUploaded={() => queryClient.invalidateQueries({ queryKey: ["datasets"] })}
        schemaList={schemaList}
      />
    </div>
  );
}

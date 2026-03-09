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
  getScenarios, createScenario, updateScenario, deleteScenario, computeScenario,
  getKnowledge, createKnowledge, updateKnowledge, deleteKnowledge,
  listModels, createModel, updateModel, deleteModel,
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
const valColor = v => v > 0 ? C.green : v < 0 ? C.red : C.textMuted;

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

function getDimFields(bl) {
  if (!bl.length) return [];
  const nums = new Set(["amount", "entry_count"]);
  const skip = new Set(["company_id", "_data_source", "_row_id", "baseline_amount"]);
  return Object.keys(bl[0]).filter(k =>
    !nums.has(k) && !skip.has(k) && !k.startsWith("baseline_") &&
    typeof bl[0][k] !== "number"
  ).sort();
}
function getMeasureFields(bl, schema = null) {
  if (!bl.length) return [];
  const skip = new Set(["entry_count", "_row_id"]);
  if (schema) {
    const measureNames = new Set(
      Object.values(schema).flatMap(t =>
        t.columns.filter(c => c.role === "measure").map(c => c.name)
      )
    );
    const found = Object.keys(bl[0]).filter(k => measureNames.has(k) && !skip.has(k));
    if (found.length) return found;
  }
  // Fallback: heuristic for numeric non-id columns
  return Object.keys(bl[0]).filter(k =>
    typeof bl[0][k] === "number" && !skip.has(k) && !k.endsWith("_id")
  );
}
function getUniq(bl, f) { return [...new Set(bl.map(r => r[f]).filter(v => v != null))].sort(); }
function generatePeriodRange(from, to) {
  const periods = [];
  try {
    let y = parseInt(from.slice(0, 4));
    let m = parseInt(from.slice(5, 7));
    const yEnd = parseInt(to.slice(0, 4));
    const mEnd = parseInt(to.slice(5, 7));
    while (y < yEnd || (y === yEnd && m <= mEnd)) {
      periods.push(`${String(y).padStart(4, '0')}-${String(m).padStart(2, '0')}`);
      m++;
      if (m > 12) { m = 1; y++; }
      if (periods.length > 120) break;
    }
  } catch (e) { /* invalid format */ }
  return periods;
}
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

function applyRules(data, rules, valF = "amount") {
  if (!valF) return data;
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
      const distribution = rule.distribution || "use_base";
      console.log("[DISTRIBUTION DEBUG multiplier]", { ruleName: rule.name, distribution, rawDistribution: rule.distribution, factor: rule.factor, matchedRows: matchIdx.length });
      if (distribution === "equal") {
        const totalBase = matchIdx.reduce((s, i) => s + Math.abs(+res[i][valF] || 0), 0);
        const totalDelta = totalBase * (rule.factor - 1);
        // Count periods from the rule's range (not from matched data rows)
        const rulePeriodCount = (rule.periodFrom && rule.periodTo)
          ? generatePeriodRange(rule.periodFrom, rule.periodTo).length
          : (() => { const ps = new Set(); for (const i of matchIdx) { ps.add(res[i]._period || res[i].period || res[i].month_year || ""); } return ps.size; })();
        const deltaPerPeriod = rulePeriodCount > 0 ? totalDelta / rulePeriodCount : 0;
        console.log("[DISTRIBUTION] EQUAL multiplier: rulePeriodCount =", rulePeriodCount, "deltaPerPeriod =", deltaPerPeriod);
        // Group matched rows by period and distribute within each period
        const periodGroups = {};
        for (const i of matchIdx) {
          const p = res[i]._period || res[i].period || res[i].month_year || "_no_period";
          if (!periodGroups[p]) periodGroups[p] = [];
          periodGroups[p].push(i);
        }
        for (const [, indices] of Object.entries(periodGroups)) {
          const perRow = indices.length > 0 ? deltaPerPeriod / indices.length : 0;
          for (const i of indices) {
            const cur = +res[i][valF] || 0;
            res[i] = { ...res[i], [valF]: Math.round((cur + perRow) * 100) / 100 };
          }
        }
      } else {
        // Proportional (default): multiply each row by factor
        console.log("[DISTRIBUTION] PROPORTIONAL multiplier: applying factor", rule.factor, "to", matchIdx.length, "rows");
        for (const i of matchIdx) {
          const cur = +res[i][valF] || 0;
          res[i] = { ...res[i], [valF]: Math.round(cur * rule.factor * 100) / 100 };
        }
      }
    } else if (rule.type === "offset" && matchIdx.length > 0) {
      const distribution = rule.distribution || "use_base";
      console.log("[DISTRIBUTION DEBUG]", { ruleName: rule.name, distribution, rawDistribution: rule.distribution, offset: rule.offset, matchedRows: matchIdx.length });
      if (distribution === "equal") {
        // Count periods from the rule's periodFrom→periodTo range
        const rulePeriodCount = (rule.periodFrom && rule.periodTo)
          ? generatePeriodRange(rule.periodFrom, rule.periodTo).length
          : (() => { const ps = new Set(); for (const i of matchIdx) { ps.add(res[i]._period || res[i].period || res[i].month_year || ""); } return ps.size; })();
        const perPeriod = rulePeriodCount > 0 ? rule.offset / rulePeriodCount : 0;
        console.log("[DISTRIBUTION] EQUAL offset: rulePeriodCount =", rulePeriodCount, "perPeriod =", perPeriod);
        // Group matched rows by period — each period gets the same share
        const periodGroups = {};
        for (const i of matchIdx) {
          const p = res[i]._period || res[i].period || res[i].month_year || "_no_period";
          if (!periodGroups[p]) periodGroups[p] = [];
          periodGroups[p].push(i);
        }
        for (const [, indices] of Object.entries(periodGroups)) {
          const perRow = indices.length > 0 ? perPeriod / indices.length : 0;
          for (const i of indices) {
            const cur = +res[i][valF] || 0;
            res[i] = { ...res[i], [valF]: Math.round((cur + perRow) * 100) / 100 };
          }
        }
      } else {
        // Proportional (use_base): distribute based on each row's share of total |value|
        const totalBase = matchIdx.reduce((s, i) => s + Math.abs(+res[i][valF] || 0), 0);
        console.log("[DISTRIBUTION] PROPORTIONAL offset: totalBase =", totalBase);
        if (totalBase === 0) {
          const share = rule.offset / matchIdx.length;
          for (const i of matchIdx) {
            const cur = +res[i][valF] || 0;
            res[i] = { ...res[i], [valF]: Math.round((cur + share) * 100) / 100 };
          }
        } else {
          for (const i of matchIdx) {
            const cur = +res[i][valF] || 0;
            const proportion = Math.abs(cur) / totalBase;
            const rowOffset = rule.offset * proportion;
            res[i] = { ...res[i], [valF]: Math.round((cur + rowOffset) * 100) / 100 };
          }
        }
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
          const allVals = mergeWithCalendar(getUniq(baseline, f), f);
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
                return <td key={ck} style={{ ...S.td, ...S.mono, textAlign: "right", color: colorFn ? colorFn(v) : valColor(v) }}>{fmt(v)}</td>;
              }) : <td style={{ ...S.td, ...S.mono, textAlign: "right", color: colorFn ? colorFn(r._total) : valColor(r._total) }}>{fmt(r._total)}</td>}
              {hasCols && <td style={{ ...S.td, ...S.mono, textAlign: "right", fontWeight: 600, color: colorFn ? colorFn(r._total) : valColor(r._total) }}>{fmt(r._total)}</td>}
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
    const toN = v => { const n = +(v); return isNaN(n) ? 0 : n; };
    const groupBy = (arr) => {
      const g = {};
      for (const r of arr) {
        const k = String(r[waterfallField] ?? "Other");
        g[k] = (g[k] || 0) + toN(r[valF]);
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

    const baseTotal = baseline.reduce((s, r) => s + toN(r[valF]), 0);
    const scTotal = scenarioData.reduce((s, r) => s + toN(r[valF]), 0);

    // Build waterfall bars
    const bars = [];
    bars.push({ name: "Actuals", value: baseTotal, isTotal: true, delta: 0, bottom: 0 });
    let running = baseTotal;
    for (const item of items.slice(0, 15)) {
      const bottom = item.delta >= 0 ? running : running + item.delta;
      bars.push({ name: item.key.length > 20 ? item.key.slice(0, 18) + "…" : item.key, value: Math.abs(item.delta), isTotal: false, delta: item.delta, bottom });
      running += item.delta;
    }
    // If there are more items, aggregate as "Other"
    if (items.length > 15) {
      const rest = items.slice(15).reduce((s, i) => s + i.delta, 0);
      if (Math.abs(rest) > 0.01) {
        const bottom = rest >= 0 ? running : running + rest;
        bars.push({ name: "Other", value: Math.abs(rest), isTotal: false, delta: rest, bottom });
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
const ROW_FIELD_WIDTH = 130;
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

      // Collect ALL unique keys from actuals AND scenario outputs (for future periods)
      const allKeys = new Set(actRows.map(r => r._key));
      for (const sc of scenarios) {
        for (const key of Object.keys(scPivots[sc.name]?.map || {})) allKeys.add(key);
      }
      const actMap = {};
      for (const r of actRows) actMap[r._key] = r;

      return [...allKeys].map(key => {
        const base = actMap[key] || (() => {
          const obj = { _key: key, _total: 0 };
          const parts = key.split(" | ");
          rowFs.forEach((f, i) => { obj[f] = parts[i] || "—"; });
          return obj;
        })();
        const out = { ...base };
        for (const sc of scenarios) {
          out["sc_" + sc.name] = scPivots[sc.name]?.map[key] || 0;
          out["var_" + sc.name] = (scPivots[sc.name]?.map[key] || 0) - (base._total || 0);
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

    // Merge all row keys from actuals AND scenario outputs (future periods)
    const allRowKeys = new Set(Object.keys(actG));
    for (const sc of scenarios) { for (const k of Object.keys(scGroups[sc.name] || {})) allRowKeys.add(k); }

    return [...allRowKeys].map(rk => {
      const r = actG[rk] || (() => {
        const obj = { _key: rk, _total: 0 };
        const parts = rk.split(" | ");
        rowFs.forEach((f, i) => { obj[f] = parts[i] || "—"; });
        return obj;
      })();
      const out = { ...r };
      for (const ck of colKeys) {
        out["act_" + ck] = r["col_" + ck] || 0;
        for (const sc of scenarios) {
          const sr = scGroups[sc.name]?.[rk];
          out["sc_" + sc.name + "_" + ck] = sr?.["col_" + ck] || 0;
          out["var_" + sc.name + "_" + ck] = (sr?.["col_" + ck] || 0) - (r["col_" + ck] || 0);
        }
      }
      for (const sc of scenarios) {
        const sr = scGroups[sc.name]?.[rk];
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
              <th colSpan={rowFs.length} style={{ ...S.th, borderBottom: "none", position: "sticky", left: 0, zIndex: 3, background: C.white }}></th>
              {colKeys.map(ck => (
                <th key={ck} colSpan={1 + scenarios.length * 2} style={{ ...S.th, textAlign: "center", borderBottom: "none", color: C.brand, fontSize: 11, fontWeight: 700 }}>{String(ck)}</th>
              ))}
              <th colSpan={1 + scenarios.length * 2} style={{ ...S.th, textAlign: "center", borderBottom: "none", color: C.text, fontWeight: 700, fontSize: 11 }}>Total</th>
            </tr>
            {/* Sub header row: Actuals, Scenarios, Deltas */}
            <tr>
              {rowFs.map((f, fi) => (
                <th key={f} style={{ ...thClick(f), position: "sticky", left: fi * ROW_FIELD_WIDTH, zIndex: 3, background: C.white, minWidth: ROW_FIELD_WIDTH, maxWidth: ROW_FIELD_WIDTH + 40, boxShadow: fi === rowFs.length - 1 ? "3px 0 6px rgba(0,0,0,0.08)" : "none" }} onClick={() => toggleSort(f)}>{f.replace(/_/g, " ")}{arrow(f)}</th>
              ))}
              {colKeys.map(ck => (
                <React.Fragment key={ck}>
                  <th style={thClick("act_" + ck, "right")} onClick={() => toggleSort("act_" + ck)}>Act{arrow("act_" + ck)}</th>
                  {scenarios.map(sc => (
                    <React.Fragment key={sc.id}>
                      <th style={thClick("sc_" + sc.name + "_" + ck, "right", { color: sortCol === "sc_" + sc.name + "_" + ck ? C.brand : sc.color })} onClick={() => toggleSort("sc_" + sc.name + "_" + ck)}>{sc.name}{arrow("sc_" + sc.name + "_" + ck)}</th>
                      <th style={thClick("var_" + sc.name + "_" + ck, "right", { color: sortCol === "var_" + sc.name + "_" + ck ? C.brand : sc.color })} onClick={() => toggleSort("var_" + sc.name + "_" + ck)}>Δ{arrow("var_" + sc.name + "_" + ck)}</th>
                    </React.Fragment>
                  ))}
                </React.Fragment>
              ))}
              <th style={thClick("_total", "right")} onClick={() => toggleSort("_total")}>Act{arrow("_total")}</th>
              {scenarios.map(sc => (
                <React.Fragment key={sc.id}>
                  <th style={thClick("sc_" + sc.name, "right", { color: sortCol === "sc_" + sc.name ? C.brand : sc.color })} onClick={() => toggleSort("sc_" + sc.name)}>{sc.name}{arrow("sc_" + sc.name)}</th>
                  <th style={thClick("var_" + sc.name, "right", { color: sortCol === "var_" + sc.name ? C.brand : sc.color })} onClick={() => toggleSort("var_" + sc.name)}>Δ{arrow("var_" + sc.name)}</th>
                </React.Fragment>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.slice(0, 80).map((r, i) => (
              <tr key={i} style={{ background: i % 2 === 0 ? "" : C.bg }}>
                {rowFs.map((f, fi) => (
                  <td key={f} style={{ ...S.td, position: "sticky", left: fi * ROW_FIELD_WIDTH, zIndex: 1, background: i % 2 === 0 ? C.white : C.bg, minWidth: ROW_FIELD_WIDTH, maxWidth: ROW_FIELD_WIDTH + 40, boxShadow: fi === rowFs.length - 1 ? "3px 0 6px rgba(0,0,0,0.08)" : "none" }}>{String(r[f] ?? "—")}</td>
                ))}
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
          {rowFs.map((f, fi) => (
            <th key={f} style={{ ...thClick(f), position: "sticky", left: fi * ROW_FIELD_WIDTH, zIndex: 3, background: C.white, minWidth: ROW_FIELD_WIDTH, maxWidth: ROW_FIELD_WIDTH + 40, boxShadow: fi === rowFs.length - 1 ? "3px 0 6px rgba(0,0,0,0.08)" : "none" }} onClick={() => toggleSort(f)}>{f.replace(/_/g, " ")}{arrow(f)}</th>
          ))}
          <th style={thClick("_total", "right")} onClick={() => toggleSort("_total")}>Actuals{arrow("_total")}</th>
          {scenarios.map(sc => <th key={sc.id} style={thClick("sc_" + sc.name, "right", { color: sortCol === "sc_" + sc.name ? C.brand : sc.color })} onClick={() => toggleSort("sc_" + sc.name)}>{sc.name}{arrow("sc_" + sc.name)}</th>)}
          {scenarios.map(sc => <th key={"v" + sc.id} style={thClick("var_" + sc.name, "right", { color: sortCol === "var_" + sc.name ? C.brand : sc.color })} onClick={() => toggleSort("var_" + sc.name)}>Δ {sc.name}{arrow("var_" + sc.name)}</th>)}
        </tr></thead>
        <tbody>
          {data.slice(0, 120).map((r, i) => (
            <tr key={i} style={{ background: i % 2 === 0 ? "" : C.bg }}>
              {rowFs.map((f, fi) => (
                <td key={f} style={{ ...S.td, position: "sticky", left: fi * ROW_FIELD_WIDTH, zIndex: 1, background: i % 2 === 0 ? C.white : C.bg, minWidth: ROW_FIELD_WIDTH, maxWidth: ROW_FIELD_WIDTH + 40, boxShadow: fi === rowFs.length - 1 ? "3px 0 6px rgba(0,0,0,0.08)" : "none" }}>{String(r[f] ?? "—")}</td>
              ))}
              <td style={{ ...S.td, ...S.mono, textAlign: "right", color: valColor(r._total) }}>{fmt(r._total)}</td>
              {scenarios.map(sc => <td key={sc.id} style={{ ...S.td, ...S.mono, textAlign: "right", color: sc.color }}>{fmt(r["sc_" + sc.name])}</td>)}
              {scenarios.map(sc => {
                const v = r["var_" + sc.name];
                return <td key={"v" + sc.id} style={{ ...S.td, ...S.mono, textAlign: "right", color: valColor(v) }}>{v > 0 ? "+" : ""}{fmt(v)}</td>;
              })}
            </tr>
          ))}
        </tbody>
        <tfoot><tr style={{ background: C.bg }}>
          <td colSpan={rowFs.length} style={{ ...S.th, fontWeight: 700, color: C.text, borderTop: `2px solid ${C.border}`, position: "sticky", left: 0, background: C.bg, zIndex: 2, boxShadow: "3px 0 6px rgba(0,0,0,0.08)" }}>Total</td>
          <td style={{ ...S.th, ...S.mono, textAlign: "right", fontWeight: 700, color: C.text, borderTop: `2px solid ${C.border}` }}>{fmt(data.reduce((s, r) => s + (r._total || 0), 0))}</td>
          {scenarios.map(sc => <td key={sc.id} style={{ ...S.th, ...S.mono, textAlign: "right", fontWeight: 700, color: sc.color, borderTop: `2px solid ${C.border}` }}>{fmt(data.reduce((s, r) => s + (r["sc_" + sc.name] || 0), 0))}</td>)}
          {scenarios.map(sc => {
            const v = data.reduce((s, r) => s + (r["var_" + sc.name] || 0), 0);
            return <td key={"v" + sc.id} style={{ ...S.th, ...S.mono, textAlign: "right", fontWeight: 700, color: valColor(v), borderTop: `2px solid ${C.border}` }}>{v > 0 ? "+" : ""}{fmt(v)}</td>;
          })}
        </tr></tfoot>
      </table>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// SCHEMA VIEW (with editable roles + editable relationships)
// ═══════════════════════════════════════════════════════════════
// ─── KNOWLEDGE PANEL ─────────────────────────────────────────────
const KNOWLEDGE_TYPES = {
  relationship:   { label: "Relationships",   icon: "🔗", color: "#6366f1" },
  calculation:    { label: "Calculations",    icon: "🧮", color: "#8b5cf6" },
  transformation: { label: "Transformations", icon: "🔄", color: "#0ea5e9" },
  definition:     { label: "Definitions",     icon: "📖", color: "#2563eb" },
  note:           { label: "Notes",           icon: "📝", color: "#d97706" },
};

const KNOWLEDGE_CONTENT_TEMPLATES = {
  relationship: {
    from_table: "", to_table: "", description: "",
    join_type: "conceptual",
    join_fields: [{ from_field: "", to_field: "", match_type: "exact" }],
    join_possible: false, workaround: "",
  },
  calculation: {
    name: "", formula_display: "", result_type: "currency", result_unit: "EUR",
    components: [{
      id: "comp1", label: "", source_table: "", aggregation: "sum",
      value_column: "amount", sign: "+",
      filters: [{ column: "", operator: "eq", value: "" }],
    }],
    executable: false,
  },
  transformation: {
    name: "", source_table: "", description: "",
    input_grain: "", output_grain: "", operation: "aggregate",
    operation_config: { group_by: [], aggregations: [], filters: [] },
    executable: false,
  },
  definition: {
    term: "", aliases: [],
    applies_to: { table: "", column: "", operator: "eq", value: "" },
    includes_sign_convention: false, sign_convention: "",
  },
  note: {
    subject: "", category: "other", description: "",
    affects: { tables: [], columns: [], values: [] },
    suggested_action: "",
  },
};

function KnowledgeEntryCard({ entry, onConfirm, onEdit, onDelete }) {
  const cfg = KNOWLEDGE_TYPES[entry.entry_type] || { label: "Unknown", icon: "❓", color: "#888" };
  const c = entry.content || {};

  let title = cfg.label;
  if (entry.entry_type === "relationship" && c.from_table && c.to_table) title = `${c.from_table} ↔ ${c.to_table}`;
  else if (entry.entry_type === "calculation" && c.name) title = c.name;
  else if (entry.entry_type === "transformation" && c.name) title = c.name;
  else if (entry.entry_type === "definition" && c.term) title = c.term;
  else if (entry.entry_type === "note" && c.subject) title = c.subject;

  const isSuggested = entry.confidence === "suggested";
  const isRejected = entry.confidence === "rejected";

  return (
    <div style={{
      background: isRejected ? "#fef2f2" : "#fff",
      borderRadius: 8, padding: "10px 14px", marginBottom: 6,
      border: `1px solid ${isSuggested ? "#fbbf2466" : C.border}`,
      opacity: isRejected ? 0.5 : 1,
    }}>
      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 4 }}>
        <span style={{ fontSize: 14 }}>{cfg.icon}</span>
        <span style={{ fontSize: 12, fontWeight: 600, color: cfg.color }}>{title}</span>
        <span style={{
          fontSize: 9, padding: "1px 6px", borderRadius: 8, fontWeight: 600,
          background: (entry.source === "chat_agent" || entry.source === "ai_agent") ? "#dbeafe" : "#f0fdf4",
          color: (entry.source === "chat_agent" || entry.source === "ai_agent") ? "#2563eb" : "#16a34a",
        }}>
          {(entry.source === "chat_agent" || entry.source === "ai_agent") ? "AI" : "Manual"}
        </span>
        {isSuggested && (
          <span style={{ fontSize: 9, padding: "1px 6px", borderRadius: 8, background: "#fef3c7", color: "#d97706", fontWeight: 600 }}>suggested</span>
        )}
        <div style={{ flex: 1 }} />
        {isSuggested && (
          <button onClick={() => onConfirm(entry.id)} style={{
            fontSize: 10, padding: "2px 8px", borderRadius: 6,
            background: "#16a34a", color: "#fff", border: "none", cursor: "pointer",
          }}>✓ Confirm</button>
        )}
        <button onClick={() => onEdit(entry)} style={{
          fontSize: 10, padding: "2px 8px", borderRadius: 6,
          background: "#f3f4f6", color: "#666", border: "none", cursor: "pointer",
        }}>Edit</button>
        <span onClick={() => onDelete(entry.id)} style={{ cursor: "pointer", color: "#9ca3af", fontSize: 14, padding: "0 2px" }}>×</span>
      </div>
      <div style={{ fontSize: 12, color: "#374151", lineHeight: 1.5 }}>{entry.plain_text}</div>
      {entry.entry_type === "relationship" && c.join_fields?.length > 0 && (
        <div style={{ fontSize: 11, color: "#6b7280", marginTop: 4, padding: "4px 8px", background: "#f9fafb", borderRadius: 4 }}>
          Join: {c.join_fields.map(j => `${j.from_field} → ${j.to_field}`).join(", ")}
          {!c.join_possible && " (conceptual — no direct SQL join)"}
        </div>
      )}
      {entry.entry_type === "calculation" && c.formula_display && (
        <div style={{ fontSize: 11, color: "#6b7280", marginTop: 4, padding: "4px 8px", background: "#f9fafb", borderRadius: 4, fontFamily: "monospace" }}>
          {c.formula_display}
          {c.executable === false && <span style={{ fontSize: 9, color: "#9ca3af", marginLeft: 8 }}>(on-demand)</span>}
        </div>
      )}
      {entry.entry_type === "transformation" && (c.input_grain || c.description) && (
        <div style={{ fontSize: 11, color: "#6b7280", marginTop: 4, padding: "4px 8px", background: "#f9fafb", borderRadius: 4 }}>
          {c.input_grain && c.output_grain ? `${c.input_grain} → ${c.output_grain}` : c.description || ""}
          {c.executable === false && <span style={{ fontSize: 9, color: "#9ca3af", marginLeft: 8 }}>(on-demand)</span>}
        </div>
      )}
      {entry.entry_type === "definition" && c.applies_to?.column && (
        <div style={{ fontSize: 11, color: "#6b7280", marginTop: 4, padding: "4px 8px", background: "#f9fafb", borderRadius: 4 }}>
          Filter: {c.applies_to.column} {c.applies_to.operator || "="} {String(c.applies_to.value)}
          {c.aliases?.length > 0 && ` · aliases: ${c.aliases.join(", ")}`}
        </div>
      )}
    </div>
  );
}

function KnowledgeEditModal({ entry, onSave, onClose }) {
  const [plainText, setPlainText] = useState(entry?.plain_text || "");
  const [contentJson, setContentJson] = useState(JSON.stringify(entry?.content || {}, null, 2));
  const [error, setError] = useState(null);

  if (!entry) return null;

  const handleSave = () => {
    try {
      const parsed = JSON.parse(contentJson);
      onSave(entry.id, { plain_text: plainText, content: parsed });
      onClose();
    } catch { setError("Invalid JSON"); }
  };

  return (
    <div style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,0.4)", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 1000 }}>
      <div style={{ background: "#fff", borderRadius: 12, padding: 24, width: 500, maxHeight: "80vh", overflow: "auto", boxShadow: "0 20px 60px rgba(0,0,0,0.2)" }}>
        <div style={{ fontSize: 14, fontWeight: 700, marginBottom: 16 }}>Edit Knowledge Entry</div>
        <label style={{ fontSize: 11, fontWeight: 600, color: "#555" }}>Summary</label>
        <textarea value={plainText} onChange={e => setPlainText(e.target.value)}
          style={{ width: "100%", padding: 8, fontSize: 12, borderRadius: 6, border: "1px solid #ddd", marginBottom: 12, minHeight: 60, fontFamily: "inherit", resize: "vertical" }} />
        <label style={{ fontSize: 11, fontWeight: 600, color: "#555" }}>Structured Content (JSON)</label>
        <textarea value={contentJson} onChange={e => { setContentJson(e.target.value); setError(null); }}
          style={{ width: "100%", padding: 8, fontSize: 11, borderRadius: 6, border: `1px solid ${error ? "#ef4444" : "#ddd"}`, marginBottom: 4, minHeight: 160, fontFamily: "monospace", resize: "vertical" }} />
        {error && <div style={{ color: "#ef4444", fontSize: 11, marginBottom: 8 }}>{error}</div>}
        <div style={{ display: "flex", gap: 8, justifyContent: "flex-end", marginTop: 12 }}>
          <button onClick={onClose} style={{ padding: "6px 16px", fontSize: 12, borderRadius: 6, background: "#f3f4f6", color: "#555", border: "none", cursor: "pointer" }}>Cancel</button>
          <button onClick={handleSave} style={{ padding: "6px 16px", fontSize: 12, borderRadius: 6, background: "#2563eb", color: "#fff", border: "none", cursor: "pointer" }}>Save Changes</button>
        </div>
      </div>
    </div>
  );
}

function AddKnowledgeModal({ datasetId, onSaved, onClose, modelId = null }) {
  const [entryType, setEntryType] = useState("relationship");
  const [plainText, setPlainText] = useState("");
  const [contentJson, setContentJson] = useState(JSON.stringify(KNOWLEDGE_CONTENT_TEMPLATES.relationship, null, 2));
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState(null);

  const handleTypeChange = (type) => {
    setEntryType(type);
    setContentJson(JSON.stringify(KNOWLEDGE_CONTENT_TEMPLATES[type] || {}, null, 2));
    setError(null);
  };

  const handleSave = async () => {
    if (!plainText.trim()) return;
    setSaving(true);
    try {
      const content = JSON.parse(contentJson);
      const created = await createKnowledge(datasetId, {
        entry_type: entryType, content, plain_text: plainText,
        source: "user_manual", confidence: "confirmed",
      }, modelId);
      onSaved(created);
      onClose();
    } catch (e) {
      setError(e.message?.includes("JSON") || contentJson ? "Invalid JSON in content" : e.message);
    } finally { setSaving(false); }
  };

  return (
    <div style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,0.4)", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 1000 }}>
      <div style={{ background: "#fff", borderRadius: 12, padding: 24, width: 520, maxHeight: "80vh", overflow: "auto", boxShadow: "0 20px 60px rgba(0,0,0,0.2)" }}>
        <div style={{ fontSize: 14, fontWeight: 700, marginBottom: 16 }}>Add Knowledge</div>
        <label style={{ fontSize: 11, fontWeight: 600, color: "#555" }}>Type</label>
        <select value={entryType} onChange={e => handleTypeChange(e.target.value)}
          style={{ width: "100%", padding: 6, fontSize: 12, borderRadius: 6, border: "1px solid #ddd", marginBottom: 12, fontFamily: "inherit" }}>
          {Object.entries(KNOWLEDGE_TYPES).map(([k, v]) => (
            <option key={k} value={k}>{v.icon} {v.label}</option>
          ))}
        </select>
        <label style={{ fontSize: 11, fontWeight: 600, color: "#555" }}>Summary</label>
        <textarea value={plainText} onChange={e => setPlainText(e.target.value)}
          placeholder="Brief description..."
          style={{ width: "100%", padding: 8, fontSize: 12, borderRadius: 6, border: "1px solid #ddd", marginBottom: 12, minHeight: 50, fontFamily: "inherit", resize: "vertical" }} />
        <label style={{ fontSize: 11, fontWeight: 600, color: "#555" }}>Structured Content (JSON)</label>
        <textarea value={contentJson} onChange={e => { setContentJson(e.target.value); setError(null); }}
          style={{ width: "100%", padding: 8, fontSize: 11, borderRadius: 6, border: `1px solid ${error ? "#ef4444" : "#ddd"}`, marginBottom: 4, minHeight: 140, fontFamily: "monospace", resize: "vertical" }} />
        {error && <div style={{ color: "#ef4444", fontSize: 11, marginBottom: 8 }}>{error}</div>}
        <div style={{ display: "flex", gap: 8, justifyContent: "flex-end", marginTop: 12 }}>
          <button onClick={onClose} style={{ padding: "6px 16px", fontSize: 12, borderRadius: 6, background: "#f3f4f6", color: "#555", border: "none", cursor: "pointer" }}>Cancel</button>
          <button onClick={handleSave} disabled={saving || !plainText.trim()} style={{
            padding: "6px 16px", fontSize: 12, borderRadius: 6,
            background: "#2563eb", color: "#fff", border: "none",
            cursor: saving ? "wait" : "pointer", opacity: !plainText.trim() ? 0.5 : 1,
          }}>{saving ? "Saving…" : "Save"}</button>
        </div>
      </div>
    </div>
  );
}

function KnowledgePanel({ datasetId, knowledgeRefreshKey, modelId = null }) {
  const [entries, setEntries] = useState([]);
  const [loading, setLoading] = useState(false);
  const [editingEntry, setEditingEntry] = useState(null);
  const [addOpen, setAddOpen] = useState(false);

  useEffect(() => {
    if (!datasetId) return;
    setLoading(true);
    getKnowledge(datasetId, modelId)
      .then(setEntries)
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [datasetId, knowledgeRefreshKey, modelId]);

  async function handleConfirm(id) {
    try {
      const updated = await updateKnowledge(id, { confidence: "confirmed" });
      setEntries(p => p.map(e => e.id === id ? updated : e));
    } catch (e) { console.error(e); }
  }

  async function handleSaveEdit(id, updates) {
    try {
      const updated = await updateKnowledge(id, updates);
      setEntries(p => p.map(e => e.id === id ? updated : e));
    } catch (e) { console.error(e); }
  }

  async function handleDelete(id) {
    try {
      await deleteKnowledge(id);
      setEntries(p => p.filter(e => e.id !== id));
    } catch (e) { console.error(e); }
  }

  // Group by type for display
  const byType = Object.fromEntries(
    Object.keys(KNOWLEDGE_TYPES).map(t => [t, entries.filter(e => e.entry_type === t)])
  );
  const hasEntries = entries.length > 0;

  return (
    <div style={{ ...S.card, marginTop: 14 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
        <div style={S.cardT}>Knowledge Base</div>
        <button onClick={() => setAddOpen(true)} style={S.btn("primary", true)}>+ Add</button>
      </div>

      {addOpen && (
        <AddKnowledgeModal
          datasetId={datasetId}
          onSaved={created => setEntries(p => [created, ...p])}
          onClose={() => setAddOpen(false)}
          modelId={modelId}
        />
      )}
      {editingEntry && (
        <KnowledgeEditModal
          entry={editingEntry}
          onSave={handleSaveEdit}
          onClose={() => setEditingEntry(null)}
        />
      )}

      {loading ? (
        <div style={{ color: C.textMuted, fontSize: 12 }}>Loading…</div>
      ) : !hasEntries ? (
        <div style={{ color: C.textMuted, fontSize: 12, fontStyle: "italic" }}>
          No knowledge entries yet. Ask the AI assistant to explore your data, or use + Add to document it manually.
        </div>
      ) : (
        Object.entries(KNOWLEDGE_TYPES).map(([type, cfg]) => {
          const typeEntries = byType[type] || [];
          if (!typeEntries.length) return null;
          return (
            <div key={type} style={{ marginBottom: 12 }}>
              <div style={{ fontSize: 11, fontWeight: 700, color: cfg.color, marginBottom: 6 }}>
                {cfg.icon} {cfg.label} ({typeEntries.length})
              </div>
              {typeEntries.map(e => (
                <KnowledgeEntryCard
                  key={e.id}
                  entry={e}
                  onConfirm={handleConfirm}
                  onEdit={setEditingEntry}
                  onDelete={handleDelete}
                />
              ))}
            </div>
          );
        })
      )}
    </div>
  );
}

function SchemaView({ schema, setSchema, relationships, setRelationships, onOpenUpload, factDatasetId, knowledgeRefreshKey, modelId = null }) {
  const [addRelOpen, setAddRelOpen] = useState(false);
  const [newRel, setNewRel] = useState({ from: "", to: "", fromCol: "", toCol: "" });
  const [dismissedSuggestions, setDismissedSuggestions] = useState(new Set());
  const tableNames = Object.keys(schema);

  // Collect AI-suggested relationships from all tables' aiNotes (deduped)
  const aiSuggestedRels = useMemo(() => {
    const seen = new Set();
    return Object.values(schema)
      .flatMap(s => s.aiNotes?.relationships ?? [])
      .filter(r => {
        const key = `${r.source_table}|${r.source_column}|${r.target_table}|${r.target_column}`;
        if (seen.has(key) || dismissedSuggestions.has(key)) return false;
        seen.add(key);
        return schema[r.source_table] && schema[r.target_table];
      })
      .filter(r => !relationships.some(
        ex => (ex.from === r.source_table && ex.fromCol === r.source_column && ex.to === r.target_table && ex.toCol === r.target_column) ||
              (ex.from === r.target_table && ex.fromCol === r.target_column && ex.to === r.source_table && ex.toCol === r.source_column)
      ));
  }, [schema, relationships, dismissedSuggestions]);

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
    setRelationships(p => [...p, {
      id: `${newRel.from}-${newRel.to}-${newRel.fromCol}-${Date.now()}`,
      ...newRel,
      coverage: null,
      overlapCount: null,
    }]);
    setNewRel({ from: "", to: "", fromCol: "", toCol: "" });
    setAddRelOpen(false);
  }

  function removeRel(id) { setRelationships(p => p.filter(r => r.id !== id)); }

  function acceptSuggestion(sug) {
    const from = sug.source_table, fromCol = sug.source_column, to = sug.target_table, toCol = sug.target_column;
    setRelationships(p => [...p, {
      id: `${from}-${to}-${fromCol}-${Date.now()}`,
      from, fromCol, to, toCol,
      coverage: null,
      overlapCount: null,
    }]);
  }

  function dismissSuggestion(sug) {
    const key = `${sug.source_table}|${sug.source_column}|${sug.target_table}|${sug.target_column}`;
    setDismissedSuggestions(p => new Set([...p, key]));
  }

  function updateRelCol(id, side, col) {
    setRelationships(p => p.map(r => {
      if (r.id !== id) return r;
      return { ...r, [side]: col, coverage: null, overlapCount: null };
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

        {aiSuggestedRels.length > 0 && (
          <div style={{ marginBottom: 12 }}>
            <div style={{ fontSize: 11, color: C.amber, fontWeight: 600, marginBottom: 6 }}>⚡ AI Suggestions</div>
            <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
              {aiSuggestedRels.map((sug, i) => (
                <div key={i} style={{ background: C.amberBg, borderRadius: 8, padding: "8px 12px", border: `1px solid ${C.amber}30`, display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
                  <span style={{ ...S.badge(C.brand), fontSize: 10 }}>{sug.source_table}</span>
                  <span style={{ ...S.mono, fontSize: 11, color: C.text }}>{sug.source_column}</span>
                  <span style={{ color: C.textMuted }}>→</span>
                  <span style={{ ...S.badge(C.purple), fontSize: 10 }}>{sug.target_table}</span>
                  <span style={{ ...S.mono, fontSize: 11, color: C.text }}>{sug.target_column}</span>
                  <span style={{ ...S.badge(sug.confidence === "high" ? C.green : sug.confidence === "medium" ? C.amber : C.textMuted), fontSize: 9 }}>{sug.confidence}</span>
                  {sug.reasoning && <span title={sug.reasoning} style={{ fontSize: 10, color: C.textMuted, cursor: "help" }}>ⓘ</span>}
                  <div style={{ flex: 1 }} />
                  <button onClick={() => acceptSuggestion(sug)} style={{ ...S.btn("primary", true), fontSize: 10, padding: "2px 10px" }}>Accept</button>
                  <button onClick={() => dismissSuggestion(sug)} style={{ ...S.btn("ghost", true), fontSize: 10, padding: "2px 8px" }}>✕</button>
                </div>
              ))}
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
      <div style={{ display: "flex", gap: 14, overflowX: "auto", paddingBottom: 8, alignItems: "flex-start" }}>
        {Object.entries(schema).map(([name, info]) => (
          <div key={name} style={{
            background: C.white,
            border: `1px solid ${info.isFact ? C.brand + "44" : C.border}`,
            borderRadius: 12,
            flex: "0 0 320px",
            width: 320,
            boxShadow: "0 1px 3px rgba(0,0,0,0.04)",
            display: "flex",
            flexDirection: "column",
            maxHeight: 380,
            overflow: "hidden",
          }}>
            {/* Card header — fixed, doesn't scroll */}
            <div style={{ padding: "14px 16px 10px", borderBottom: `1px solid ${C.borderLight}`, flexShrink: 0 }}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 4 }}>
                <span style={{ fontSize: 15, fontWeight: 700, color: C.text }}>{name}</span>
                <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
                  {info.aiAnalyzing && (
                    <span style={{ ...S.badge(C.amber), fontSize: 9, animation: "pulse 1.5s infinite" }}>⏳ Analyzing…</span>
                  )}
                  <span style={S.badge(
                    info.aiNotes?.is_system ? C.green :
                    info.isFact ? C.brand : C.purple
                  )}>
                    {info.aiNotes?.is_system ? "CALENDAR" : info.isFact ? "FACT" : "DIMENSION"}
                  </span>
                </div>
              </div>
              {info.aiNotes?.description && (
                <div style={{ fontSize: 11, color: C.textSec, fontStyle: "italic", lineHeight: 1.4, display: "-webkit-box", WebkitLineClamp: 2, WebkitBoxOrient: "vertical", overflow: "hidden" }}>
                  {info.aiNotes.description}
                </div>
              )}
              <div style={{ fontSize: 11, color: C.textMuted, marginTop: 4 }}>{info.rowCount} rows</div>
            </div>
            {/* Column list — scrollable */}
            <div style={{ flex: 1, overflowY: "auto", padding: "0 0 8px" }}>
              <table style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead><tr>
                  <th style={{ ...S.th, width: "45%", padding: "8px 16px", position: "sticky", top: 0 }}>Column</th>
                  <th style={{ ...S.th, width: "40%", padding: "8px 8px", position: "sticky", top: 0 }}>Role</th>
                  <th style={{ ...S.th, width: "15%", textAlign: "right", padding: "8px 16px 8px 0", position: "sticky", top: 0 }}>Uniq</th>
                </tr></thead>
                <tbody>{info.columns.map(col => (
                  <tr key={col.name}>
                    <td style={{ ...S.td, ...S.mono, fontSize: 11, wordBreak: "break-word", maxWidth: 130, padding: "6px 16px" }}>{col.name}</td>
                    <td style={{ ...S.td, padding: "6px 8px" }}>
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
                    <td style={{ ...S.td, color: C.textMuted, fontSize: 11, textAlign: "right", padding: "6px 16px 6px 0" }}>{col.uniqueCount}</td>
                  </tr>
                ))}</tbody>
              </table>
            </div>
          </div>
        ))}
      </div>

      {/* Knowledge base panel — shown when a fact dataset is selected */}
      {factDatasetId && (
        <KnowledgePanel datasetId={factDatasetId} knowledgeRefreshKey={knowledgeRefreshKey} modelId={modelId} />
      )}
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════
// ═══════════════════════════════════════════════════════════════
// SAVED VIEWS BAR
// ═══════════════════════════════════════════════════════════════
function SavedViewsBar({ savedViews, onSave, onLoad, onDelete }) {
  const [isNaming, setIsNaming] = useState(false);
  const [newName, setNewName] = useState("");
  const handleSave = () => {
    if (!newName.trim()) return;
    onSave(newName.trim());
    setNewName("");
    setIsNaming(false);
  };
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 8, padding: "6px 0", marginBottom: 8, flexWrap: "wrap", fontSize: 13 }}>
      <span style={{ fontWeight: 600, color: C.textMuted, marginRight: 4 }}>Saved Views:</span>
      {savedViews.map(v => (
        <div key={v.id} style={{ display: "inline-flex", alignItems: "center", gap: 4, padding: "3px 10px", borderRadius: 12, background: "#e8f0fe", color: C.brand, cursor: "pointer", fontSize: 12, fontWeight: 500, border: `1px solid ${C.brand}33` }}>
          <span onClick={() => onLoad(v)} title="Load this view">{v.name}</span>
          <span onClick={e => { e.stopPropagation(); onDelete(v.id); }} style={{ cursor: "pointer", opacity: 0.6, marginLeft: 2, fontSize: 11 }} title="Delete">×</span>
        </div>
      ))}
      {isNaming ? (
        <div style={{ display: "inline-flex", alignItems: "center", gap: 4 }}>
          <input value={newName} onChange={e => setNewName(e.target.value)}
            onKeyDown={e => { if (e.key === "Enter") handleSave(); if (e.key === "Escape") setIsNaming(false); }}
            placeholder="View name..." autoFocus
            style={{ padding: "3px 8px", fontSize: 12, borderRadius: 6, border: `1px solid ${C.border}`, width: 140, outline: "none" }} />
          <button onClick={handleSave} style={{ padding: "3px 8px", fontSize: 11, borderRadius: 6, background: C.brand, color: "#fff", border: "none", cursor: "pointer" }}>Save</button>
          <button onClick={() => setIsNaming(false)} style={{ padding: "3px 8px", fontSize: 11, borderRadius: 6, background: C.surface, color: C.textSec, border: `1px solid ${C.border}`, cursor: "pointer" }}>Cancel</button>
        </div>
      ) : (
        <button onClick={() => setIsNaming(true)} style={{ padding: "3px 10px", fontSize: 12, borderRadius: 12, background: C.surface, color: C.textSec, border: `1px dashed ${C.border}`, cursor: "pointer" }}>
          + Save Current View
        </button>
      )}
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

  const [savedViews, setSavedViews] = useState(() => {
    try { const v = localStorage.getItem("databobiq_saved_views_actuals"); return v ? JSON.parse(v) : []; }
    catch { return []; }
  });
  useEffect(() => {
    localStorage.setItem("databobiq_saved_views_actuals", JSON.stringify(savedViews));
  }, [savedViews]);

  return (
    <div>
      <div style={{ marginBottom: 20 }}>
        <h2 style={{ fontSize: 20, fontWeight: 700, color: C.text, marginBottom: 4 }}>Actuals</h2>
        <p style={{ color: C.textSec, fontSize: 13 }}>{filtered.length} entries after filters</p>
      </div>
      <div style={S.card}>
        <SavedViewsBar
          savedViews={savedViews}
          onSave={name => setSavedViews(prev => [...prev, { id: String(Date.now()), name, rows: [...rowFs], columns: colF ? [colF] : [], values: valF ? [valF] : [], filters: structuredClone(filters), createdAt: Date.now() }])}
          onLoad={view => { setRowFs(view.rows || []); setColF((view.columns || [])[0] || ""); setValF((view.values || [])[0] || ""); setFilters(view.filters || {}); }}
          onDelete={id => setSavedViews(prev => prev.filter(v => v.id !== id))}
        />
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
  const allVals = mergeWithCalendar(getUniq(baseline, dim), dim);
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

const CALENDAR_YEARS = ["2020","2021","2022","2023","2024","2025","2026","2027"];
const CALENDAR_FIELDS = new Set(["year", "month", "quarter", "month_year", "month_name", "_period", "period"]);
const MONTH_NAMES = ["January","February","March","April","May","June","July","August","September","October","November","December"];
function getCalendarValues(field) {
  switch (field) {
    case "year": return CALENDAR_YEARS.map(String);
    case "month": return Array.from({length: 12}, (_, i) => String(i + 1).padStart(2, "0"));
    case "month_name": return [...MONTH_NAMES];
    case "quarter": return CALENDAR_YEARS.flatMap(y => [`Q1 ${y}`, `Q2 ${y}`, `Q3 ${y}`, `Q4 ${y}`]);
    case "month_year": case "_period": case "period":
      return CALENDAR_YEARS.flatMap(y =>
        Array.from({length: 12}, (_, m) => `${y}-${String(m + 1).padStart(2, "0")}`)
      );
    default: return null;
  }
}
function mergeWithCalendar(dataVals, field) {
  const calVals = getCalendarValues(field);
  if (!calVals) return dataVals;
  return [...new Set([...dataVals, ...calVals])].sort();
}

function ScenariosView({ baseline, scenarios, setScenarios, schema, factDatasetId, relIds, modelId = null }) {
  const dims = useMemo(() => getDimFields(baseline), [baseline]);
  const measures = useMemo(() => getMeasureFields(baseline, schema), [baseline, schema]);
  const basePeriods = useMemo(() => getUniq(baseline, "_period"), [baseline]);

  const [active, setActive] = useState(new Set());
  const [editId, setEditId] = useState(null);
  const [rowFs, setRowFs] = useState(() => []);
  const [colF, setColF] = useState("");
  const [valF, setValF] = useState(() => "");
  const [filters, setFilters] = useState({});

  const [savedViews, setSavedViews] = useState(() => {
    try { const v = localStorage.getItem("databobiq_saved_views_scenario"); return v ? JSON.parse(v) : []; }
    catch { return []; }
  });
  useEffect(() => {
    localStorage.setItem("databobiq_saved_views_scenario", JSON.stringify(savedViews));
  }, [savedViews]);

  const [newRule, setNewRule] = useState({ name: "", type: "multiplier", factor: 1.05, offset: 0, filters: {}, periodFrom: "", periodTo: "", distribution: "use_base" });
  const [ruleFilterFields, setRuleFilterFields] = useState([]);
  const [ruleFilterSearch, setRuleFilterSearch] = useState("");
  const [ruleFilterOpen, setRuleFilterOpen] = useState(false);
  const [ruleFilterExpanded, setRuleFilterExpanded] = useState(null);
  const [ruleValSearch, setRuleValSearch] = useState("");
  const ruleFilterRef = useRef(null);
  const [waterfallField, setWaterfallField] = useState("");

  // Auto-init valF to first measure when data loads
  useEffect(() => {
    if (!valF && measures.length > 0) setValF(measures[0]);
  }, [measures]);

  const effectiveValF = valF || measures[0] || "amount";
  // Only use auto-fallback waterfall field when dims exist; don't pick a bad one
  const effectiveWaterfallField = waterfallField || "";

  const toggle = id => setActive(p => { const n = new Set(p); n.has(id) ? n.delete(id) : n.add(id); return n; });
  const filtered = useMemo(() => applyFilters(baseline, filters), [baseline, filters]);

  // Split filters: calendar fields only affect display, not computation baseline
  const displayFilters = useMemo(() => {
    const calFields = new Set([
      "year", "month", "quarter", "month_year", "month_name",
      "_period", "period", "_year", "_month", "_month_name",
    ]);
    const display = {};
    const compute = {};
    for (const [k, v] of Object.entries(filters)) {
      if (calFields.has(k)) display[k] = v;
      else compute[k] = v;
    }
    return { display, compute };
  }, [filters]);

  const scOutputs = useMemo(() => {
    const o = {};
    for (const sc of scenarios) {
      if (!active.has(sc.id)) continue;

      // Use baseline filtered by NON-calendar filters only (calendar filters only affect display)
      let scenarioBaseline = applyFilters(baseline, displayFilters.compute);
      const config = sc.base_config || {};
      const baseYear = config.base_year ? String(config.base_year) : null;

      // Filter baseline to the configured base year (or legacy period_from/period_to)
      if (baseYear) {
        scenarioBaseline = scenarioBaseline.filter(r => {
          const period = r._period || r.period || r.month_year || "";
          return period.startsWith(baseYear);
        });
      } else if (config.period_from || config.period_to) {
        scenarioBaseline = scenarioBaseline.filter(r => {
          const period = r._period || r.period || r.month_year || "";
          if (config.period_from && period < config.period_from) return false;
          if (config.period_to && period > config.period_to) return false;
          return true;
        });
      }

      // If source is another scenario, use that scenario's output as baseline
      if (config.source === "scenario" && config.source_scenario_id) {
        const sourceScenario = scenarios.find(s => s.id === config.source_scenario_id);
        if (sourceScenario && o[sourceScenario.name]) {
          scenarioBaseline = o[sourceScenario.name];
        }
      }

      // Generate rows for future periods targeted by rules
      let expandedBaseline = [...scenarioBaseline];

      // Collect all periods targeted by rules
      const ruleTargetPeriods = new Set();
      for (const rule of sc.rules) {
        if (rule.periodFrom && rule.periodTo) {
          generatePeriodRange(rule.periodFrom, rule.periodTo).forEach(p => ruleTargetPeriods.add(p));
        } else if (rule.periodFrom) {
          ruleTargetPeriods.add(rule.periodFrom);
        }
      }

      const existingPeriods = new Set(
        expandedBaseline.map(r => r._period || r.period || r.month_year || "").filter(Boolean)
      );
      const futurePeriods = [...ruleTargetPeriods].filter(p => !existingPeriods.has(p)).sort();

      if (futurePeriods.length > 0 && expandedBaseline.length > 0) {
        // Build averaged template lazily — used when a base year month has no data.
        const _skipFields = new Set([
          "_period", "period", "month_year", "_data_source", "_row_id",
          "_baseline_period", "_is_comparison_base", "_year", "_month",
          "year", "month", "month_name", "quarter",
          effectiveValF, `baseline_${effectiveValF}`,
        ]);
        const _dimKey = (r) => {
          const parts = [];
          for (const k of Object.keys(r)) {
            if (!_skipFields.has(k)) parts.push(`${k}=${r[k] ?? ""}`);
          }
          return parts.sort().join("|");
        };
        let averagedTemplateRows = null;
        const _getAveragedRows = () => {
          if (averagedTemplateRows !== null) return averagedTemplateRows;
          const groups = {};
          for (const r of expandedBaseline) {
            const dk = _dimKey(r);
            if (!groups[dk]) groups[dk] = { sample: r, values: [] };
            groups[dk].values.push(+(r[effectiveValF]) || 0);
          }
          averagedTemplateRows = [];
          for (const { sample, values } of Object.values(groups)) {
            const avg = values.reduce((s, v) => s + v, 0) / values.length;
            averagedTemplateRows.push({ ...sample, [effectiveValF]: Math.round(avg * 100) / 100 });
          }
          return averagedTemplateRows;
        };

        for (const fp of futurePeriods) {
          const targetMonth = fp.slice(5, 7);
          const newYear = fp.slice(0, 4);
          const matchingBasePeriod = baseYear ? `${baseYear}-${targetMonth}` : null;
          let templateRows;
          let baselinePeriodRef;
          if (matchingBasePeriod && existingPeriods.has(matchingBasePeriod)) {
            templateRows = expandedBaseline.filter(r =>
              (r._period || r.period || r.month_year || "") === matchingBasePeriod
            );
            baselinePeriodRef = matchingBasePeriod;
          } else {
            templateRows = _getAveragedRows();
            baselinePeriodRef = "_averaged";
          }
          for (const tr of templateRows) {
            const projected = {
              ...tr,
              _period: fp, period: fp, month_year: fp,
              year: newYear, _year: newYear,
              month: targetMonth, _month: targetMonth,
              month_name: MONTH_NAMES[parseInt(targetMonth, 10) - 1] || tr.month_name,
              quarter: `Q${Math.ceil(parseInt(targetMonth, 10) / 3)} ${newYear}`,
              _data_source: "projected",
              _baseline_period: baselinePeriodRef,
            };
            // Averaged months have no real actuals — start from 0
            if (baselinePeriodRef === "_averaged") {
              projected[effectiveValF] = 0;
            }
            expandedBaseline.push(projected);
          }
        }
      }

      console.log("[scOutputs] Applying rules for", sc.name, ":", sc.rules.map(r => ({ name: r.name, type: r.type, distribution: r.distribution, offset: r.offset, factor: r.factor })));
      const result = applyRules(expandedBaseline, sc.rules, effectiveValF);
      // Apply ALL filters (including calendar/display) to the final output
      o[sc.name] = applyFilters(result, filters);
    }
    return o;
  }, [scenarios, active, baseline, filters, displayFilters, effectiveValF]);

  const allPeriods = useMemo(() => {
    const extraPeriods = new Set();
    for (const rows of Object.values(scOutputs)) {
      for (const r of rows) {
        const p = r._period || r.period || r.month_year;
        if (p) extraPeriods.add(String(p));
      }
    }
    return [...new Set([...basePeriods, ...extraPeriods])].sort();
  }, [basePeriods, scOutputs]);
  const editSc = scenarios.find(s => s.id === editId);

  async function addScenario() {
    const color = SC_COLORS[scenarios.length % SC_COLORS.length];
    const name = `Scenario ${scenarios.length + 1}`;
    try {
      const created = await createScenario({ name, dataset_id: factDatasetId, rules: [], color }, modelId);
      setScenarios(p => [...p, { id: created.id, name: created.name, rules: created.rules || [], color: created.color || color, base_config: created.base_config || null }]);
      setEditId(created.id); setActive(p => new Set([...p, created.id]));
    } catch {
      const id = Date.now();
      setScenarios(p => [...p, { id, name, rules: [], color, base_config: null }]);
      setEditId(id); setActive(p => new Set([...p, id]));
    }
  }
  function delScenario(id) { setScenarios(p => p.filter(s => s.id !== id)); setActive(p => { const n = new Set(p); n.delete(id); return n; }); if (editId === id) setEditId(null); }
  function renameScenario(id, newName) { if (newName.trim()) setScenarios(p => p.map(s => s.id === id ? { ...s, name: newName.trim() } : s)); }
  function updateBaseConfig(updates) {
    if (!editId) return;
    setScenarios(p => p.map(s => {
      if (s.id !== editId) return s;
      const cur = s.base_config || { source: "actuals", source_scenario_id: null, base_year: null };
      return { ...s, base_config: { ...cur, ...updates } };
    }));
  }

  function addRule() {
    if (!editId || !newRule.name) return;
    setScenarios(p => p.map(s => s.id !== editId ? s : { ...s, rules: [...s.rules, { ...newRule, id: Date.now() }] }));
    setNewRule({ name: "", type: "multiplier", factor: 1.05, offset: 0, filters: {}, periodFrom: "", periodTo: "", distribution: "use_base" });
    setRuleFilterFields([]);
  }
  function rmRule(rid) { setScenarios(p => p.map(s => s.id !== editId ? s : { ...s, rules: s.rules.filter(r => r.id !== rid) })); }
  function updateRule(rid, updates) {
    console.log("[updateRule]", rid, updates);
    setScenarios(p => p.map(s => s.id !== editId ? s : { ...s, rules: s.rules.map(r => r.id === rid ? { ...r, ...updates } : r) }));
  }
  const [editingRuleId, setEditingRuleId] = useState(null);

  // Safe numeric coercion: handles JS numbers AND numeric strings (e.g. from Decimal→JSON)
  const numF = (r, f) => { const n = +(r[f]); return isNaN(n) ? 0 : n; };

  // Per-scenario comparison baselines — isolated so two scenarios with different
  // base years don't corrupt each other.
  const comparisonBaselines = useMemo(() => {
    const result = {};
    for (const sc of scenarios) {
      if (!active.has(sc.id)) continue;
      const config = sc.base_config || {};
      const baseYear = config.base_year ? String(config.base_year) : null;

      // Use NON-calendar-filtered baseline so projected periods are still computed
      // even when a calendar filter (e.g. year=2026) is active
      const computeBase = applyFilters(baseline, displayFilters.compute);
      let scBase = baseYear
        ? computeBase.filter(r => (r._period || r.period || r.month_year || "").startsWith(baseYear))
        : computeBase;

      const scRows = scOutputs[sc.name] || [];
      const rows = [...scBase];
      const addedPeriods = new Set(rows.map(r => r._period || r.period || r.month_year || ""));

      // Add year-shifted rows for projected periods (including averaged ones)
      const _skipF = new Set([
        "_period", "period", "month_year", "_data_source", "_row_id",
        "_baseline_period", "_is_comparison_base", "_year", "_month",
        "year", "month", "month_name", "quarter",
        effectiveValF, `baseline_${effectiveValF}`,
      ]);
      const _dkCB = (r) => {
        const parts = [];
        for (const k of Object.keys(r)) { if (!_skipF.has(k)) parts.push(`${k}=${r[k] ?? ""}`); }
        return parts.sort().join("|");
      };
      // Precompute averaged scBase rows (lazy)
      let _avgBaseRows = null;
      const _getAvgBaseRows = () => {
        if (_avgBaseRows !== null) return _avgBaseRows;
        const groups = {};
        for (const r of scBase) {
          const dk = _dkCB(r);
          if (!groups[dk]) groups[dk] = { sample: r, values: [] };
          groups[dk].values.push(+(r[effectiveValF]) || 0);
        }
        _avgBaseRows = Object.values(groups).map(({ sample }) => {
          // No real actuals for this period — show 0 as baseline
          return { ...sample, [effectiveValF]: 0 };
        });
        return _avgBaseRows;
      };

      for (const row of scRows) {
        if (row._data_source !== "projected" || !row._baseline_period) continue;
        const projPeriod = row._period || row.period || row.month_year || "";
        if (addedPeriods.has(projPeriod)) continue;
        const newYear = projPeriod.slice(0, 4);
        const newMonth = projPeriod.slice(5, 7);
        const baseRows = row._baseline_period === "_averaged"
          ? _getAvgBaseRows()
          : scBase.filter(r => (r._period || r.period || r.month_year || "") === row._baseline_period);
        for (const br of baseRows) {
          rows.push({
            ...br,
            _period: projPeriod, period: projPeriod, month_year: projPeriod,
            year: newYear, _year: newYear,
            month: newMonth, _month: newMonth,
            month_name: MONTH_NAMES[parseInt(newMonth, 10) - 1] || br.month_name,
            quarter: `Q${Math.ceil(parseInt(newMonth, 10) / 3)} ${newYear}`,
            _is_comparison_base: true,
          });
        }
        addedPeriods.add(projPeriod);
      }
      // Apply ALL filters (including calendar) for display
      result[sc.name] = applyFilters(rows, filters);
    }
    return result;
  }, [baseline, scOutputs, scenarios, active, filters, displayFilters]);

  const variance = useMemo(() => {
    if (!active.size || !effectiveValF) return [];
    return scenarios.filter(sc => active.has(sc.id)).map(sc => {
      const scBase = comparisonBaselines[sc.name] || [];
      const scData = scOutputs[sc.name] || [];
      const at = scBase.reduce((s, r) => s + numF(r, effectiveValF), 0);
      const st = scData.reduce((s, r) => s + numF(r, effectiveValF), 0);
      return { name: sc.name, color: sc.color, total: st, variance: st - at,
        pct: at ? ((st - at) / Math.abs(at)) * 100 : 0, baseTotal: at };
    });
  }, [active, scenarios, scOutputs, comparisonBaselines, effectiveValF]);

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

      <div style={S.card}>
        <SavedViewsBar
          savedViews={savedViews}
          onSave={name => setSavedViews(prev => [...prev, { id: String(Date.now()), name, rows: [...rowFs], columns: colF ? [colF] : [], values: valF ? [valF] : [], filters: structuredClone(filters), createdAt: Date.now() }])}
          onLoad={view => { setRowFs(view.rows || []); setColF((view.columns || [])[0] || ""); setValF((view.values || [])[0] || ""); setFilters(view.filters || {}); }}
          onDelete={id => setSavedViews(prev => prev.filter(v => v.id !== id))}
        />
        <div style={{ display: "grid", gridTemplateColumns: "2fr 2fr 1fr", gap: 14, marginBottom: 10 }}>
          <FieldManager label="Row Fields" allFields={dims} selected={rowFs} onChange={setRowFs} color={C.brand} />
          <FieldManager label="Column Field" allFields={dims.filter(f => !rowFs.includes(f))} selected={colF} onChange={setColF} color={C.purple} single />
          <FieldManager label="Value" allFields={measures} selected={valF} onChange={setValF} color={C.green} single />
        </div>
        <FilterManager baseline={baseline} allFields={dims} filters={filters} setFilters={setFilters} />
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
                {sc.base_config && sc.base_config.base_year && (
                  <span style={{ ...S.badge(C.purple), fontSize: 9 }}>
                    📅 {sc.base_config.base_year}
                  </span>
                )}
                {sc.base_config && sc.base_config.source === "scenario" && (
                  <span style={{ ...S.badge(C.amber), fontSize: 9 }}>
                    🔗 chained
                  </span>
                )}
              </button>
              <span onClick={() => setEditId(editId === sc.id ? null : sc.id)} style={{ padding: "6px 8px", cursor: "pointer", color: editId === sc.id ? C.brand : C.textMuted, fontSize: 15 }}>✎</span>
              <span onClick={() => delScenario(sc.id)} style={{ padding: "6px 6px", cursor: "pointer", color: C.textMuted, fontSize: 15 }}>×</span>
            </div>
          ))}
        </div>
      )}

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

          {/* Baseline Configuration */}
          <div style={{ background: C.bg, borderRadius: 8, padding: "10px 12px", marginBottom: 10, border: `1px solid ${C.border}` }}>
            <div style={{ fontSize: 10, fontWeight: 700, color: C.textMuted, textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: 8 }}>Baseline</div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
              <div>
                <label style={{ fontSize: 10, color: C.textMuted, fontWeight: 600 }}>Source</label>
                <select style={{ ...S.select, width: "100%" }}
                  value={(() => {
                    const cfg = editSc.base_config || {};
                    if (cfg.source === "scenario" && cfg.source_scenario_id)
                      return `scenario:${cfg.source_scenario_id}`;
                    return "actuals";
                  })()}
                  onChange={e => {
                    const val = e.target.value;
                    if (val === "actuals") {
                      updateBaseConfig({ source: "actuals", source_scenario_id: null });
                    } else if (val.startsWith("scenario:")) {
                      updateBaseConfig({ source: "scenario", source_scenario_id: val.split(":")[1] });
                    }
                  }}>
                  <option value="actuals">Actuals</option>
                  {scenarios.filter(s => s.id !== editSc.id).map(s => (
                    <option key={s.id} value={`scenario:${s.id}`}>Scenario: {s.name}</option>
                  ))}
                </select>
              </div>
              <div>
                <label style={{ fontSize: 10, color: C.textMuted, fontWeight: 600 }}>Base Year</label>
                {(() => {
                  const cfg = editSc.base_config || {};
                  if (cfg.source === "scenario" && cfg.source_scenario_id) {
                    const parentSc = scenarios.find(s => s.id === cfg.source_scenario_id);
                    const inheritedYear = parentSc?.base_config?.base_year ?? "—";
                    return (
                      <div style={{ ...S.select, width: "100%", background: C.bg, color: C.textMuted, cursor: "not-allowed", display: "flex", alignItems: "center", gap: 6 }}>
                        <span>Inherited: {inheritedYear}</span>
                        <span style={{ fontSize: 9, color: C.amber }}>from {parentSc?.name || "parent"}</span>
                      </div>
                    );
                  }
                  return (
                    <select style={{ ...S.select, width: "100%" }}
                      value={cfg.base_year || ""}
                      onChange={e => updateBaseConfig({ base_year: parseInt(e.target.value) || null })}>
                      <option value="">Select year...</option>
                      {CALENDAR_YEARS.map(y => <option key={y} value={y}>{y}</option>)}
                    </select>
                  );
                })()}
              </div>
            </div>
          </div>
          {!(editSc.base_config || {}).base_year && (
            <div style={{ padding: "8px 12px", borderRadius: 6, background: C.amberBg, border: `1px solid ${C.amber}33`, fontSize: 11, color: C.amber, marginBottom: 8 }}>
              ⚠ Select a base year above before adding rules.
            </div>
          )}
          {(editSc.base_config || {}).base_year ? (<>
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
                    <span style={S.badge(rule.distribution === "equal" ? C.purple : C.textMuted)}>{rule.distribution === "equal" ? "⚖️ equal" : "📊 prop."}</span>
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
                        <input style={S.input} type="text" placeholder="YYYY-MM"
                          list={`periods-from-${rule.id}`}
                          value={rule.periodFrom || ""}
                          onChange={e => updateRule(rule.id, { periodFrom: e.target.value })} />
                        <datalist id={`periods-from-${rule.id}`}>
                          {allPeriods.map(p => <option key={p} value={p} />)}
                        </datalist>
                      </div>
                      <div>
                        <label style={{ fontSize: 10, color: C.textMuted, fontWeight: 600 }}>Period To</label>
                        <input style={S.input} type="text" placeholder="YYYY-MM"
                          list={`periods-to-${rule.id}`}
                          value={rule.periodTo || ""}
                          onChange={e => updateRule(rule.id, { periodTo: e.target.value })} />
                        <datalist id={`periods-to-${rule.id}`}>
                          {allPeriods.map(p => <option key={p} value={p} />)}
                        </datalist>
                      </div>
                    </div>
                    {/* Distribution mode */}
                    <div style={{ marginTop: 8 }}>
                      <label style={{ fontSize: 10, color: C.textMuted, fontWeight: 600 }}>Distribution</label>
                      <div style={{ display: "flex", gap: 8, marginTop: 4 }}>
                        <button onClick={() => updateRule(rule.id, { distribution: "use_base" })}
                          style={{ ...S.btn(rule.distribution !== "equal" ? "primary" : "ghost", true), flex: 1, textAlign: "center" }}>
                          📊 Proportional
                        </button>
                        <button onClick={() => updateRule(rule.id, { distribution: "equal" })}
                          style={{ ...S.btn(rule.distribution === "equal" ? "primary" : "ghost", true), flex: 1, textAlign: "center" }}>
                          ⚖️ Equal split
                        </button>
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
                <input style={S.input} type="text" placeholder="YYYY-MM"
                  list="periods-from-new"
                  value={newRule.periodFrom}
                  onChange={e => setNewRule(p => ({ ...p, periodFrom: e.target.value }))} />
                <datalist id="periods-from-new">
                  {allPeriods.map(p => <option key={p} value={p} />)}
                </datalist>
              </div>
              <div>
                <label style={{ fontSize: 10, color: C.textMuted, fontWeight: 600 }}>Period To</label>
                <input style={S.input} type="text" placeholder="YYYY-MM"
                  list="periods-to-new"
                  value={newRule.periodTo}
                  onChange={e => setNewRule(p => ({ ...p, periodTo: e.target.value }))} />
                <datalist id="periods-to-new">
                  {allPeriods.map(p => <option key={p} value={p} />)}
                </datalist>
              </div>
            </div>

            {/* Distribution mode */}
            <div style={{ marginTop: 10 }}>
              <label style={{ fontSize: 10, color: C.textMuted, fontWeight: 600 }}>Distribution</label>
              <div style={{ display: "flex", gap: 8, marginTop: 4 }}>
                <button onClick={() => setNewRule(p => ({ ...p, distribution: "use_base" }))}
                  style={{ ...S.btn(newRule.distribution !== "equal" ? "primary" : "ghost", true), flex: 1, textAlign: "center" }}>
                  📊 Proportional (follow baseline)
                </button>
                <button onClick={() => setNewRule(p => ({ ...p, distribution: "equal" }))}
                  style={{ ...S.btn(newRule.distribution === "equal" ? "primary" : "ghost", true), flex: 1, textAlign: "center" }}>
                  ⚖️ Equal (flat split)
                </button>
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
                  const dimVals = mergeWithCalendar(getUniq(baseline, dim), dim);
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

          {/* TEMP DEBUG: Distribution calculation panel */}
          {editSc && active.has(editSc.id) && scOutputs[editSc.name] && editSc.rules.some(r => r.type === "offset" || r.type === "multiplier") && (
            <div style={{ background: "#fff8e1", borderRadius: 8, padding: 10, marginTop: 10, border: "1px solid #ffd54f", fontSize: 11 }}>
              <div style={{ fontWeight: 700, marginBottom: 6 }}>🔍 Distribution Debug</div>
              {editSc.rules.map(rule => {
                const dist = rule.distribution || "use_base";
                const output = scOutputs[editSc.name] || [];
                const base = comparisonBaselines[editSc.name] || [];
                const periods = [...new Set(output.map(r => r._period).filter(Boolean))].sort().slice(0, 6);
                return (
                  <div key={rule.id} style={{ marginBottom: 6, paddingBottom: 6, borderBottom: "1px solid #ffe082" }}>
                    <span style={{ fontWeight: 600 }}>{rule.name}</span>
                    <span style={{ color: "#555", marginLeft: 8 }}>({rule.type})</span>
                    <span style={{ color: dist === "equal" ? "#7b1fa2" : "#1565c0", fontWeight: 700, marginLeft: 8 }}>distribution: {dist}</span>
                    {rule.type === "offset" && <span style={{ color: "#555", marginLeft: 8 }}>offset: {rule.offset}</span>}
                    {rule.type === "multiplier" && <span style={{ color: "#555", marginLeft: 8 }}>factor: ×{rule.factor}</span>}
                    <div style={{ marginTop: 4, fontFamily: "monospace", fontSize: 10 }}>
                      {periods.map(p => {
                        const scVal = output.filter(r => r._period === p).reduce((s, r) => s + (+r[effectiveValF] || 0), 0);
                        const bVal = base.filter(r => r._period === p).reduce((s, r) => s + (+r[effectiveValF] || 0), 0);
                        const delta = scVal - bVal;
                        return (
                          <span key={p} style={{ marginRight: 12, color: delta > 0 ? "#2e7d32" : delta < 0 ? "#c62828" : "#555" }}>
                            {p}: Δ={delta >= 0 ? "+" : ""}{Math.round(delta).toLocaleString()}
                          </span>
                        );
                      })}
                    </div>
                  </div>
                );
              })}
            </div>
          )}
          </>) : null}
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
              <tr><td style={S.td}><span style={{ color: C.textSec }}>● Baseline</span></td>
                <td style={{ ...S.td, ...S.mono, textAlign: "right" }}>{fmt(variance[0]?.baseTotal || 0)}</td>
                <td style={{ ...S.td, textAlign: "right" }}>—</td><td style={{ ...S.td, textAlign: "right" }}>—</td></tr>
              {variance.map(v => (
                <tr key={v.name}>
                  <td style={S.td}><span style={{ color: v.color, fontWeight: 600 }}>● {v.name}</span></td>
                  <td style={{ ...S.td, ...S.mono, textAlign: "right" }}>{fmt(v.total)}</td>
                  <td style={{ ...S.td, ...S.mono, textAlign: "right", color: valColor(v.variance) }}>{v.variance > 0 ? "+" : ""}{fmt(v.variance)}</td>
                  <td style={{ ...S.td, textAlign: "right", color: valColor(v.pct), fontWeight: 600 }}>{v.pct > 0 ? "+" : ""}{v.pct.toFixed(1)}%</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {variance.length > 0 && (
        <div style={S.card}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
            <div style={S.cardT}>Waterfall Analysis</div>
            <FieldManager label="" allFields={dims} selected={waterfallField} onChange={setWaterfallField} color={C.purple} single />
          </div>
          {effectiveWaterfallField ? (
            <div style={{ display: "grid", gridTemplateColumns: `repeat(${Math.min(scenarios.filter(sc => active.has(sc.id)).length, 2)}, 1fr)`, gap: 14 }}>
              {scenarios.filter(sc => active.has(sc.id)).map(sc => (
                <div key={sc.id}>
                  <div style={{ fontSize: 12, fontWeight: 600, color: sc.color, marginBottom: 6, textAlign: "center" }}>{sc.name}</div>
                  <WaterfallChart baseline={comparisonBaselines[sc.name] || filtered} scenarioData={scOutputs[sc.name]} scenarioName={sc.name} scenarioColor={sc.color} rowFs={rowFs} valF={effectiveValF} waterfallField={effectiveWaterfallField} />
                </div>
              ))}
            </div>
          ) : (
            <div style={{ textAlign: "center", padding: 24, color: C.textMuted, fontSize: 12 }}>Select a dimension above to break down changes.</div>
          )}
        </div>
      )}

      {active.size > 0 && rowFs.length > 0 && valF && (
        <div style={S.card}>
          <div style={S.cardT}>Comparison Table</div>
          <ComparisonTable baseline={comparisonBaselines[scenarios.find(sc => active.has(sc.id))?.name] || filtered} scenarioOutputs={scOutputs} rowFs={rowFs} colF={colF} valF={valF} scenarios={scenarios.filter(sc => active.has(sc.id))} />
        </div>
      )}

      {active.size > 0 && rowFs.length > 0 && (
        <div style={S.card}>
          <div style={S.cardT}>Comparison Chart</div>
          <PivotChartView data={comparisonBaselines[scenarios.find(sc => active.has(sc.id))?.name] || filtered} rowFs={rowFs} colF={colF} valF={valF} scenarioData={scOutputs} />
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
function ChatPanel({ baseline, scenarios, setScenarios, setActiveTab, activeTab, datasetId, onKnowledgeSaved, pendingOnboardingId, onOnboardingConsumed, modelId = null }) {
  const [dataModelMessages, setDataModelMessages] = useState([
    { role: "assistant", content: "I'm the **Data Understanding Agent**. I can help you document relationships, calculations, definitions, and business rules about your data." }
  ]);
  const [scenarioMessages, setScenarioMessages] = useState([
    { role: "assistant", content: "Data loaded. Ask me anything about your data, or say **\"What if…\"** to build a scenario." }
  ]);
  const isDataModelTab = activeTab === "schema";
  const agentMode = isDataModelTab ? "data_understanding" : "scenario";
  const messages = isDataModelTab ? dataModelMessages : scenarioMessages;
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [knowledgeNotif, setKnowledgeNotif] = useState(null); // {text, id}
  const endRef = useRef(null);
  const abortRef = useRef(null);
  useEffect(() => { endRef.current?.scrollIntoView({ behavior: "smooth" }); }, [messages]);

  // Auto-hide knowledge notifications after 4s
  useEffect(() => {
    if (!knowledgeNotif) return;
    const t = setTimeout(() => setKnowledgeNotif(null), 4000);
    return () => clearTimeout(t);
  }, [knowledgeNotif]);

  // Trigger onboarding when a new dataset finishes profiling
  useEffect(() => {
    if (!pendingOnboardingId || !datasetId || pendingOnboardingId !== datasetId || loading) return;
    onOnboardingConsumed?.();
    sendMessage("__ONBOARDING_START__");
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pendingOnboardingId, datasetId]);

  async function sendMessage(msg, forceDataModel = false) {
    if (!msg || loading || !datasetId) return;
    const isOnboarding = msg === "__ONBOARDING_START__";
    // Onboarding always targets the Data Model agent; otherwise use current tab
    const targetDataModel = forceDataModel || isOnboarding || activeTab === "schema";
    const targetAgentMode = targetDataModel ? "data_understanding" : "scenario";
    const setTargetMessages = targetDataModel ? setDataModelMessages : setScenarioMessages;

    setLoading(true);
    const currentMessages = targetDataModel ? dataModelMessages : scenarioMessages;
    const history = currentMessages
      .filter(m => m.role !== "system" && m.content !== "__ONBOARDING_START__")
      .map(m => ({ role: m.role, content: m.content }));
    // Don't display the synthetic onboarding trigger in the chat
    if (!isOnboarding) {
      setTargetMessages(p => [...p, { role: "user", content: msg }]);
    }
    // Add empty assistant placeholder that text_delta will stream into
    setTargetMessages(p => [...p, { role: "assistant", content: "", agent: targetAgentMode }]);
    const abortCtrl = new AbortController();
    abortRef.current = abortCtrl;
    const pendingRules = [];
    let pendingBaseConfig = null;
    let pendingExistingScenarioId = null;
    let pendingScenarioName = null;

    try {
      for await (const event of streamChat(msg, datasetId, history, abortCtrl.signal, targetAgentMode, modelId)) {
        if (event.type === "text_delta") {
          setTargetMessages(p => {
            const next = [...p];
            next[next.length - 1] = { ...next[next.length - 1], content: next[next.length - 1].content + event.text, agent: targetAgentMode };
            return next;
          });
        } else if (event.type === "knowledge_saved") {
          setKnowledgeNotif({ text: event.plain_text, id: event.id });
          if (onKnowledgeSaved) onKnowledgeSaved();
        } else if (event.type === "scenario_rules") {
          // New plural batch event
          const rules = (event.rules || []).map((r, i) => ({ ...r, id: Date.now() + i }));
          if (event.base_config) pendingBaseConfig = event.base_config;
          if (event.scenario_id) pendingExistingScenarioId = event.scenario_id;
          if (event.scenario_name) pendingScenarioName = event.scenario_name;
          pendingRules.push(...rules);
        } else if (event.type === "scenario_rule") {
          // Backward compat for old singular event
          const { base_config, ...ruleWithoutBaseConfig } = event.rule || {};
          if (base_config) pendingBaseConfig = base_config;
          if (event.scenario_id) pendingExistingScenarioId = event.scenario_id;
          if (event.scenario_name) pendingScenarioName = event.scenario_name;
          pendingRules.push({ ...ruleWithoutBaseConfig, id: Date.now() + pendingRules.length });
        } else if (event.type === "scenario_copied") {
          // Duplicate the source scenario in local state using the new id/name from backend
          setScenarios(prev => {
            const source = prev.find(s => s.id === event.copied_from);
            if (!source) return prev;
            return [...prev, { ...source, id: event.id, name: event.name }];
          });
          setActiveTab("scenarios");
        } else if (event.type === "done") {
          if (pendingRules.length) {
            if (pendingExistingScenarioId) {
              // Add rules to existing scenario
              setScenarios(prev => {
                const updated = prev.map(sc => {
                  if (sc.id !== pendingExistingScenarioId) return sc;
                  const newRules = [...sc.rules, ...pendingRules];
                  const newConfig = pendingBaseConfig
                    ? { ...(sc.base_config || {}), ...pendingBaseConfig }
                    : sc.base_config;
                  updateScenario(sc.id, { rules: newRules, base_config: newConfig }).catch(console.error);
                  return { ...sc, rules: newRules, base_config: newConfig };
                });
                return updated;
              });
            } else {
              // Create new scenario via API then add to state
              const color = SC_COLORS[scenarios.length % SC_COLORS.length];
              const name = pendingScenarioName || `Scenario ${scenarios.length + 1}`;
              const payload = { name, dataset_id: datasetId, rules: pendingRules, color };
              if (pendingBaseConfig) {
                payload.base_config = {
                  source: pendingBaseConfig.source || "actuals",
                  source_scenario_id: pendingBaseConfig.source_scenario_id || null,
                  base_year: pendingBaseConfig.base_year || null,
                };
              }
              try {
                const created = await createScenario(payload, modelId);
                setScenarios(prev => [...prev, {
                  id: created.id, name: created.name,
                  rules: created.rules || pendingRules,
                  color: created.color || color,
                  base_config: created.base_config || null,
                }]);
              } catch (e) {
                console.error("Failed to create scenario via API:", e);
                setScenarios(prev => [...prev, {
                  id: String(Date.now()), name,
                  rules: pendingRules, color,
                  base_config: pendingBaseConfig || null,
                }]);
              }
            }
            setActiveTab("scenarios");
          }
          // Ensure placeholder has content
          setTargetMessages(p => {
            const last = p[p.length - 1];
            if (!last.content) {
              const next = [...p];
              next[next.length - 1] = { ...last, content: pendingRules.length ? "Done! Check the Scenarios tab." : "Could you rephrase?" };
              return next;
            }
            return p;
          });
        } else if (event.type === "error") {
          setTargetMessages(p => { const next = [...p]; next[next.length - 1] = { ...next[next.length - 1], content: `Error: ${event.message}` }; return next; });
        }
      }
    } catch (e) {
      if (e.name !== "AbortError") {
        setTargetMessages(p => { const next = [...p]; next[next.length - 1] = { ...next[next.length - 1], content: "Connection issue. Try again or build scenarios manually." }; return next; });
      }
    }
    setLoading(false);
  }

  function send() {
    if (!input.trim() || loading) return;
    const msg = input.trim();
    setInput("");
    sendMessage(msg);
  }

  const agentLabel = isDataModelTab ? "🔍 Data Agent" : "📊 Scenario Agent";
  const agentColor = isDataModelTab ? C.purple : C.brand;
  const chatPlaceholder = isDataModelTab
    ? "Ask about your data, define relationships, calculations…"
    : "Ask about your data, create scenarios, analyze trends…";

  return (
    <div style={{ width: 340, borderLeft: `1px solid ${C.border}`, display: "flex", flexDirection: "column", background: C.white, flexShrink: 0 }}>
      <div style={{ padding: "14px 16px", borderBottom: `1px solid ${C.border}`, display: "flex", alignItems: "center", justifyContent: "space-between" }}>
        <div style={{ fontSize: 13, fontWeight: 700, color: C.brand, display: "flex", alignItems: "center", gap: 6 }}>
          <svg width="16" height="16" viewBox="0 0 16 16" fill="none"><circle cx="8" cy="8" r="7" stroke={C.brand} strokeWidth="2" /><path d="M5 8h6M8 5v6" stroke={C.brand} strokeWidth="1.5" strokeLinecap="round" /></svg>
          AI Assistant
        </div>
        <span style={{ ...S.badge(agentColor), fontSize: 9 }}>{agentLabel}</span>
      </div>
      {knowledgeNotif && (
        <div style={{ padding: "8px 14px", background: C.purpleBg, borderBottom: `1px solid ${C.purple}22`, borderLeft: `3px solid ${C.purple}`, fontSize: 11, color: C.purple, display: "flex", alignItems: "flex-start", gap: 6 }}>
          <span style={{ flexShrink: 0 }}>💡</span>
          <span style={{ flex: 1, lineHeight: 1.4 }}>Saved: {knowledgeNotif.text}</span>
          <span onClick={() => setKnowledgeNotif(null)} style={{ cursor: "pointer", opacity: 0.5, flexShrink: 0 }}>×</span>
        </div>
      )}
      <div style={{ flex: 1, overflow: "auto", padding: 12, display: "flex", flexDirection: "column", gap: 8 }}>
        {messages.map((m, i) => (
          <div key={i} style={{ padding: "10px 14px", borderRadius: 10, fontSize: 12, lineHeight: 1.6, background: m.role === "user" ? C.brandLight : C.bg, border: `1px solid ${m.role === "user" ? C.brandMid : C.border}`, alignSelf: m.role === "user" ? "flex-end" : "flex-start", maxWidth: "92%" }}>
            {m.role === "assistant" && m.agent && (
              <div style={{ fontSize: 9, color: m.agent === "data_understanding" ? C.purple : C.brand, fontWeight: 600, marginBottom: 4 }}>
                {m.agent === "data_understanding" ? "🔍 Data Agent" : "📊 Scenario Agent"}
              </div>
            )}
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
        <input style={{ ...S.input, flex: 1 }} value={input} onChange={e => setInput(e.target.value)} onKeyDown={e => e.key === "Enter" && send()} placeholder={chatPlaceholder} />
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
    // Derive _period (YYYY-MM) from calendar's month_year if not already present
    if (!obj._period) {
      if (obj.month_year) {
        obj._period = obj.month_year;
      } else {
        // Fall back: scan any value that looks like YYYY-MM-DD or YYYY-MM
        for (const k of Object.keys(obj)) {
          const v = obj[k];
          if (v && typeof v === "string" && /^\d{4}-\d{2}/.test(v)) {
            obj._period = v.slice(0, 7);
            break;
          }
        }
      }
    }
    return obj;
  });
}

// ─── UPLOAD MODAL ────────────────────────────────────────────────
function UploadModal({ isOpen, onClose, onUploaded, schemaList, modelId = null }) {
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
        const result = await uploadFile(next.file, modelId);
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
                  const isSystem = !!(sr.dataset.ai_notes?.is_system);
                  const isDeleting = !isSystem && deletingId === sr.dataset.id;
                  const analyzed = sr.dataset.ai_analyzed;
                  return (
                    <div key={sr.dataset.id} style={{
                      display: "flex", alignItems: "center", gap: 10,
                      padding: "9px 12px", borderRadius: 8,
                      background: isSystem ? C.green + "11" : isDeleting ? "#fff0f0" : C.bg,
                      border: `1px solid ${isSystem ? C.green + "44" : isDeleting ? C.red + "44" : C.border}`,
                      fontSize: 12, opacity: isSystem ? 0.85 : 1,
                    }}>
                      <div style={{ flex: 1, minWidth: 0 }}>
                        <span style={{ fontWeight: 600, color: C.text }}>
                          {isSystem ? "📅 Calendar (2020–2027)" : (sr.dataset.source_filename ?? sr.dataset.name)}
                        </span>
                        <span style={{ color: C.textMuted, marginLeft: 8 }}>{sr.dataset.row_count.toLocaleString()} rows</span>
                      </div>
                      <span style={{ ...S.badge(isSystem ? C.green : analyzed ? C.green : (Date.now() - new Date(sr.dataset.created_at).getTime() < 180_000 ? C.amber : C.border)), fontSize: 9 }}>
                        {isSystem ? "SYSTEM" : analyzed ? "✓ Analyzed" : (Date.now() - new Date(sr.dataset.created_at).getTime() < 180_000 ? "⏳ Analyzing…" : "—")}
                      </span>
                      {!isSystem && (isDeleting ? (
                        <button onClick={() => handleDelete(sr.dataset.id)} style={{ ...S.btn("danger", true), fontSize: 11, padding: "3px 10px" }}>Confirm</button>
                      ) : (
                        <span onClick={() => setDeletingId(sr.dataset.id)} title="Delete dataset" style={{ cursor: "pointer", color: C.textMuted, fontSize: 15, padding: "2px 4px" }}>🗑</span>
                      ))}
                      {!isSystem && isDeleting && (
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
        <img src="/IQLogo.png" alt="dataBobIQ" style={{ height: 40, objectFit: "contain" }} />
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

// ─── FLOW CARD HELPERS ────────────────────────────────────────────
function FlowCard({ step }) {
  return (
    <div style={{
      background: "rgba(255,255,255,0.35)",
      backdropFilter: "blur(6px)",
      WebkitBackdropFilter: "blur(6px)",
      borderRadius: 12,
      padding: "18px 20px",
      width: 185,
      minHeight: 170,
      border: "1px solid rgba(255,255,255,0.4)",
      boxShadow: "0 1px 3px rgba(0,0,0,0.03)",
      display: "flex",
      flexDirection: "column",
    }}>
      <div style={{ fontSize: 22, marginBottom: 10 }}>{step.icon}</div>
      <div style={{ fontSize: 13, fontWeight: 700, color: "#1a1a2e", marginBottom: 6 }}>{step.title}</div>
      <div style={{ fontSize: 11, color: "#6b7280", lineHeight: 1.5, flex: 1 }}>{step.desc}</div>
    </div>
  );
}
function FlowArrow() {
  return (
    <div style={{ color: "#c4c9cf", fontSize: 16, padding: "0 6px", flexShrink: 0 }}>→</div>
  );
}

// ─── MODEL LANDING PAGE ───────────────────────────────────────────
const FLOW_STEPS = [
  { icon: "📤", title: "Upload Data", desc: "Drop your Excel files — GL entries, chart of accounts, invoice lines. dataBobIQ auto-detects the structure." },
  { icon: "🔍", title: "Understand", desc: "The Data Agent analyzes your tables, finds relationships, and asks smart questions to learn your business logic." },
  { icon: "📊", title: "Explore Actuals", desc: "Pivot, filter, and visualize your real data. Drag fields to build any view — save favorites for quick access." },
  { icon: "🔮", title: "Plan Scenarios", desc: "Create what-if models: increase revenue 10%, cut costs 300K, project into 2026. Compare side-by-side with actuals." },
  { icon: "💡", title: "Decide", desc: "Use the comparison tables, waterfall charts, and AI insights to make data-driven decisions with confidence." },
];

function ModelLandingPage({ models, loading, onSelect, onRefresh, onShowHowItWorks }) {
  const [creating, setCreating] = useState(false);
  const [newName, setNewName] = useState("");
  const [newDesc, setNewDesc] = useState("");
  const [saving, setSaving] = useState(false);
  const [editingId, setEditingId] = useState(null);
  const [editName, setEditName] = useState("");
  const [hoveredId, setHoveredId] = useState(null);
  const [menuOpenId, setMenuOpenId] = useState(null);

  const handleCreate = async () => {
    if (!newName.trim() || saving) return;
    setSaving(true);
    try {
      const m = await createModel({ name: newName.trim(), description: newDesc.trim() || null });
      setCreating(false); setNewName(""); setNewDesc("");
      onSelect(m.id, m.name);
    } catch (e) { console.error(e); }
    finally { setSaving(false); }
  };

  const handleRename = async (id) => {
    if (!editName.trim()) { setEditingId(null); return; }
    try {
      await updateModel(id, { name: editName.trim() });
      setEditingId(null); onRefresh();
    } catch (e) { console.error(e); }
  };

  const handleArchive = async (id) => {
    if (!confirm("Archive this model? It will be hidden from the list.")) return;
    try { await updateModel(id, { status: "archived" }); setMenuOpenId(null); onRefresh(); }
    catch (e) { console.error(e); }
  };

  useEffect(() => {
    if (!menuOpenId) return;
    const h = () => setMenuOpenId(null);
    document.addEventListener("click", h);
    return () => document.removeEventListener("click", h);
  }, [menuOpenId]);

  return (
    <div style={{ minHeight: "100vh", background: "#fafbfc", fontFamily: "'Plus Jakarta Sans', -apple-system, sans-serif" }}>
      <link href={FONT_URL} rel="stylesheet" />
      {/* ── TOP BAR ── */}
      <div style={{ background: "#fff", borderBottom: "1px solid #eef0f2", padding: "14px 32px", display: "flex", alignItems: "center", justifyContent: "space-between" }}>
        <span style={{ fontSize: 20, fontWeight: 800, color: "#1a1a2e", letterSpacing: "-0.5px" }}>
          data<span style={{ color: "#6abbd9" }}>Bob</span>IQ
        </span>
        <button onClick={onShowHowItWorks}
          onMouseEnter={e => { e.currentTarget.style.borderColor = C.brand; e.currentTarget.style.color = C.brand; }}
          onMouseLeave={e => { e.currentTarget.style.borderColor = "#e0e3e8"; e.currentTarget.style.color = "#4b5563"; }}
          style={{ background: "none", border: "1.5px solid #e0e3e8", borderRadius: 8, padding: "6px 16px", fontSize: 13, fontWeight: 600, color: "#4b5563", cursor: "pointer", fontFamily: "inherit", display: "flex", alignItems: "center", gap: 6, transition: "all 0.15s" }}>
          <span style={{ fontSize: 15 }}>📖</span> How It Works
        </button>
      </div>

      {/* ── HERO + FLOW ── */}
      <div style={{ background: "linear-gradient(160deg, #f0f9fd 0%, #ffffff 40%, #f8f9fb 100%)", padding: "44px 32px 40px", borderBottom: "1px solid #eef0f2", position: "relative", overflow: "hidden" }}>
        {/* Background logo — large, centered, faded */}
        <div style={{ position: "absolute", inset: 0, display: "flex", alignItems: "center", justifyContent: "center", pointerEvents: "none", zIndex: 0 }}>
          <img
            src="/IQLogo.png"
            alt=""
            style={{ height: "110%", maxWidth: "none", objectFit: "contain", opacity: 0.18, userSelect: "none" }}
          />
        </div>
        <div style={{ position: "relative", zIndex: 1, maxWidth: 960, margin: "0 auto" }}>
          <h1 style={{ fontSize: 28, fontWeight: 800, color: "#1a1a2e", margin: 0, letterSpacing: "-0.5px" }}>Your Models</h1>
          <p style={{ fontSize: 14, color: "#6b7280", marginTop: 6, lineHeight: 1.5, maxWidth: 560 }}>
            Each model is an independent workspace with its own data, scenarios, and AI-learned knowledge. Here's how it works:
          </p>
          {/* ── Flow steps — U-shape around logo ── */}
          <div style={{ marginTop: 32, display: "flex", flexDirection: "column", alignItems: "center", gap: 16 }}>
            {/* Top row: 3 cards */}
            <div style={{ display: "flex", justifyContent: "center", gap: 12, width: "100%" }}>
              {FLOW_STEPS.slice(0, 3).map((step, i) => (
                <div key={i} style={{ display: "flex", alignItems: "center" }}>
                  <FlowCard step={step} />
                  {i < 2 && <FlowArrow />}
                </div>
              ))}
            </div>
            {/* Down arrow */}
            <div style={{ display: "flex", justifyContent: "center", width: "100%" }}>
              <span style={{ color: "#c4c9cf", fontSize: 16, transform: "rotate(90deg)" }}>→</span>
            </div>
            {/* Bottom row: 2 cards, centered */}
            <div style={{ display: "flex", justifyContent: "center", gap: 12 }}>
              {FLOW_STEPS.slice(3).map((step, i) => (
                <div key={i + 3} style={{ display: "flex", alignItems: "center" }}>
                  <FlowCard step={step} />
                  {i < 1 && <FlowArrow />}
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>

      {/* ── MODEL GRID ── */}
      <div style={{ maxWidth: 960, margin: "0 auto", padding: "32px 32px 64px" }}>
        {loading && models.length === 0 && (
          <div style={{ textAlign: "center", padding: 60, color: "#9ca3af", fontSize: 13 }}>Loading models...</div>
        )}
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(280px, 1fr))", gap: 20, justifyItems: "stretch" }}>
          {models.map(m => {
            const isHovered = hoveredId === m.id;
            const isEditing = editingId === m.id;
            const isMenuOpen = menuOpenId === m.id;
            return (
              <div key={m.id}
                onMouseEnter={() => setHoveredId(m.id)}
                onMouseLeave={() => setHoveredId(null)}
                onClick={() => { if (!isEditing && !isMenuOpen) onSelect(m.id, m.name); }}
                style={{ background: "#fff", borderRadius: 14, padding: 24, border: `1.5px solid ${isHovered ? C.brand : "#e8ebee"}`, cursor: isEditing ? "default" : "pointer", transition: "all 0.2s ease", boxShadow: isHovered ? `0 8px 24px ${C.brand}1e` : "0 1px 3px rgba(0,0,0,0.04)", position: "relative", minHeight: 160, display: "flex", flexDirection: "column" }}>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 8 }}>
                  {isEditing ? (
                    <input value={editName} onChange={e => setEditName(e.target.value)}
                      onKeyDown={e => { if (e.key === "Enter") handleRename(m.id); if (e.key === "Escape") setEditingId(null); }}
                      onBlur={() => handleRename(m.id)} autoFocus
                      onClick={e => e.stopPropagation()}
                      style={{ fontSize: 16, fontWeight: 700, color: "#1a1a2e", border: "none", borderBottom: `2px solid ${C.brand}`, outline: "none", background: "transparent", padding: "0 0 2px", width: "80%", fontFamily: "inherit" }} />
                  ) : (
                    <div style={{ fontSize: 16, fontWeight: 700, color: "#1a1a2e", lineHeight: 1.3, flex: 1, paddingRight: 8 }}>{m.name}</div>
                  )}
                  <div style={{ position: "relative" }}>
                    <button onClick={e => { e.stopPropagation(); setMenuOpenId(isMenuOpen ? null : m.id); }}
                      style={{ background: "none", border: "none", cursor: "pointer", padding: "2px 6px", fontSize: 18, color: "#9ca3af", borderRadius: 6, lineHeight: 1, opacity: isHovered || isMenuOpen ? 1 : 0, transition: "opacity 0.15s" }}>⋯</button>
                    {isMenuOpen && (
                      <div onClick={e => e.stopPropagation()} style={{ position: "absolute", right: 0, top: 28, background: "#fff", borderRadius: 10, boxShadow: "0 8px 30px rgba(0,0,0,0.12)", border: "1px solid #e8ebee", overflow: "hidden", zIndex: 10, minWidth: 150 }}>
                        <button onClick={() => { setEditName(m.name); setEditingId(m.id); setMenuOpenId(null); }}
                          onMouseEnter={e => e.currentTarget.style.background = "#f9fafb"}
                          onMouseLeave={e => e.currentTarget.style.background = "none"}
                          style={{ display: "block", width: "100%", padding: "10px 16px", border: "none", background: "none", cursor: "pointer", fontSize: 13, color: "#374151", textAlign: "left" }}>Rename</button>
                        <button onClick={() => handleArchive(m.id)}
                          onMouseEnter={e => e.currentTarget.style.background = "#fef2f2"}
                          onMouseLeave={e => e.currentTarget.style.background = "none"}
                          style={{ display: "block", width: "100%", padding: "10px 16px", border: "none", background: "none", cursor: "pointer", fontSize: 13, color: "#ef4444", textAlign: "left" }}>Archive</button>
                      </div>
                    )}
                  </div>
                </div>
                {m.description && (
                  <div style={{ fontSize: 13, color: "#6b7280", lineHeight: 1.4, marginBottom: 12, display: "-webkit-box", WebkitLineClamp: 2, WebkitBoxOrient: "vertical", overflow: "hidden" }}>{m.description}</div>
                )}
                <div style={{ flex: 1 }} />
                <div style={{ display: "flex", gap: 16, marginTop: 8, paddingTop: 12, borderTop: "1px solid #f3f4f6" }}>
                  <span style={{ fontSize: 12, color: "#6b7280" }}>📊 {m.dataset_count} dataset{m.dataset_count !== 1 ? "s" : ""}</span>
                  <span style={{ fontSize: 12, color: "#6b7280" }}>🔮 {m.scenario_count} scenario{m.scenario_count !== 1 ? "s" : ""}</span>
                </div>
                <div style={{ fontSize: 11, color: "#c4c9cf", marginTop: 8 }}>
                  Created {new Date(m.created_at).toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" })}
                </div>
              </div>
            );
          })}

          {/* ── New Model card ── */}
          {creating ? (
            <div onClick={e => e.stopPropagation()} style={{ background: "#fff", borderRadius: 14, padding: 24, border: `2px solid ${C.brand}`, boxShadow: `0 8px 24px ${C.brand}1e`, minHeight: 160, display: "flex", flexDirection: "column" }}>
              <div style={{ fontSize: 13, fontWeight: 700, color: C.brand, marginBottom: 12, textTransform: "uppercase", letterSpacing: "0.5px" }}>New Model</div>
              <input value={newName} onChange={e => setNewName(e.target.value)} placeholder="Model name" autoFocus
                onKeyDown={e => { if (e.key === "Enter") handleCreate(); if (e.key === "Escape") { setCreating(false); setNewName(""); setNewDesc(""); } }}
                style={{ width: "100%", padding: "8px 10px", fontSize: 14, fontWeight: 600, borderRadius: 8, border: "1.5px solid #e8ebee", outline: "none", marginBottom: 8, fontFamily: "inherit" }} />
              <textarea value={newDesc} onChange={e => setNewDesc(e.target.value)} placeholder="Description (optional)"
                style={{ width: "100%", padding: "8px 10px", fontSize: 12, borderRadius: 8, border: "1.5px solid #e8ebee", outline: "none", marginBottom: 12, minHeight: 44, resize: "vertical", fontFamily: "inherit" }} />
              <div style={{ flex: 1 }} />
              <div style={{ display: "flex", gap: 8 }}>
                <button onClick={handleCreate} disabled={!newName.trim() || saving}
                  style={{ padding: "8px 20px", fontSize: 13, fontWeight: 600, borderRadius: 8, border: "none", cursor: "pointer", fontFamily: "inherit", background: newName.trim() ? C.brand : "#e8ebee", color: newName.trim() ? "#fff" : "#999" }}>
                  {saving ? "Creating..." : "Create Model"}
                </button>
                <button onClick={() => { setCreating(false); setNewName(""); setNewDesc(""); }}
                  style={{ padding: "8px 16px", fontSize: 13, fontWeight: 500, borderRadius: 8, border: "none", cursor: "pointer", background: "#f3f4f6", color: "#6b7280", fontFamily: "inherit" }}>Cancel</button>
              </div>
            </div>
          ) : (
            <div onClick={() => setCreating(true)}
              onMouseEnter={e => {
                e.currentTarget.style.borderColor = C.brand;
                e.currentTarget.querySelector(".plus-icon").style.background = C.brand;
                e.currentTarget.querySelector(".plus-icon").style.color = "#fff";
              }}
              onMouseLeave={e => {
                e.currentTarget.style.borderColor = "#dde0e4";
                e.currentTarget.querySelector(".plus-icon").style.background = "#f0f9fd";
                e.currentTarget.querySelector(".plus-icon").style.color = C.brand;
              }}
              style={{ background: "#fafbfc", borderRadius: 14, border: "2px dashed #dde0e4", cursor: "pointer", minHeight: 160, display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", gap: 8, color: "#9ca3af", transition: "all 0.2s ease" }}>
              <div className="plus-icon" style={{ width: 44, height: 44, borderRadius: 12, background: "#f0f9fd", display: "flex", alignItems: "center", justifyContent: "center", fontSize: 22, color: C.brand, transition: "all 0.2s ease" }}>+</div>
              <span style={{ fontSize: 14, fontWeight: 600 }}>New Model</span>
            </div>
          )}
        </div>

        {/* ── Empty state ── */}
        {!loading && models.length === 0 && (
          <div style={{ textAlign: "center", padding: "60px 0" }}>
            <div style={{ width: 64, height: 64, borderRadius: 16, background: "#f0f9fd", margin: "0 auto 16px", display: "flex", alignItems: "center", justifyContent: "center", fontSize: 28 }}>📦</div>
            <div style={{ fontSize: 16, fontWeight: 700, color: "#1a1a2e", marginBottom: 4 }}>No models yet</div>
            <div style={{ fontSize: 13, color: "#6b7280", marginBottom: 20 }}>Create your first model to start analyzing data.</div>
            <button onClick={() => setCreating(true)} style={{ padding: "10px 24px", fontSize: 14, fontWeight: 600, borderRadius: 10, border: "none", cursor: "pointer", background: C.brand, color: "#fff", fontFamily: "inherit" }}>+ Create Model</button>
          </div>
        )}
      </div>
    </div>
  );
}

// ─── HOW IT WORKS MODAL ───────────────────────────────────────────
function HowItWorksSection({ icon, title, content, details, extraContent, agentBadge, agentNote }) {
  return (
    <div style={{ marginTop: 24 }}>
      <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 8 }}>
        <div style={{ width: 28, height: 28, borderRadius: 8, background: "#f0f9fd", display: "flex", alignItems: "center", justifyContent: "center", fontSize: 15, flexShrink: 0 }}>{icon}</div>
        <div style={{ fontSize: 15, fontWeight: 700, color: "#1a1a2e" }}>{title}</div>
        {agentBadge && (
          <span style={{ fontSize: 9, fontWeight: 700, padding: "2px 8px", borderRadius: 6, textTransform: "uppercase", letterSpacing: "0.5px", background: agentBadge === "data_understanding" ? "#eef2ff" : "#dbeafe", color: agentBadge === "data_understanding" ? "#6366f1" : "#2563eb" }}>
            {agentBadge === "data_understanding" ? "Data Agent" : "Scenario Agent"}
          </span>
        )}
      </div>
      <div style={{ fontSize: 13, color: "#4b5563", lineHeight: 1.6, paddingLeft: 38 }}>
        {content}
        {details && (
          <ul style={{ margin: "8px 0", paddingLeft: 18, listStyleType: "disc" }}>
            {details.map((d, i) => <li key={i} style={{ fontSize: 12, color: "#6b7280", marginBottom: 4, lineHeight: 1.5 }}>{d}</li>)}
          </ul>
        )}
        {extraContent && <div style={{ marginTop: 8 }}>{extraContent}</div>}
        {agentNote && <div style={{ marginTop: 8, fontSize: 12, color: "#6b7280", fontStyle: "italic" }}>💬 {agentNote}</div>}
      </div>
    </div>
  );
}

function HowItWorksModal({ onClose }) {
  return (
    <div style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,0.5)", display: "flex", alignItems: "flex-start", justifyContent: "center", zIndex: 1000, overflowY: "auto", padding: "40px 16px" }}>
      <div style={{ background: "#fff", borderRadius: 16, maxWidth: 680, width: "100%", boxShadow: "0 24px 80px rgba(0,0,0,0.2)", position: "relative" }}>
        <button onClick={onClose} style={{ position: "absolute", top: 16, right: 16, background: "#f3f4f6", border: "none", borderRadius: 8, width: 32, height: 32, cursor: "pointer", fontSize: 16, display: "flex", alignItems: "center", justifyContent: "center", color: "#6b7280" }}>×</button>
        <div style={{ padding: "32px 32px 0" }}>
          <div style={{ fontSize: 11, fontWeight: 700, color: C.brand, textTransform: "uppercase", letterSpacing: "1px", marginBottom: 8 }}>Getting Started</div>
          <h2 style={{ fontSize: 24, fontWeight: 800, color: "#1a1a2e", margin: 0, letterSpacing: "-0.3px" }}>How dataBobIQ Works</h2>
          <p style={{ fontSize: 14, color: "#6b7280", marginTop: 8, lineHeight: 1.6 }}>
            dataBobIQ is your AI-powered CFO companion. Upload your financial data, let the AI understand your business, explore your actuals, and build scenarios for the future — all in one place.
          </p>
        </div>
        <div style={{ padding: "24px 32px 32px" }}>
          <HowItWorksSection number="1" icon="📦" title="Create a Model"
            content="A Model is your workspace — think of it like a project folder. Each model has its own data, scenarios, knowledge, and chat history, completely isolated from other models. You might have one for annual budgeting, another for a reforecast, and another for acquisition analysis." />
          <HowItWorksSection number="2" icon="📤" title="Upload Your Data"
            content="Drop Excel files into the Data Model tab. dataBobIQ automatically detects the structure — tables, columns, data types, and relationships. You can upload multiple files: general ledger entries, chart of accounts, invoice lines, company master data. The system figures out how they connect." />
          <HowItWorksSection number="3" icon="🔍" title="Talk to the Data Agent" agentBadge="data_understanding"
            content="On the Data Model tab, the chat panel connects you to the Data Understanding Agent. This AI specialist analyzes your data and asks smart questions about things it can't figure out automatically — relationships between tables, business calculations, how to interpret codes and values. Your answers become permanent knowledge that improves all future analysis."
            details={["Saves 5 types of knowledge: Relationships, Calculations, Transformations, Definitions, and Notes", "Knowledge appears in the Knowledge panel where you can review, edit, or delete entries", "Everything you teach the Data Agent is used by the Scenario Agent too"]} />
          <HowItWorksSection number="4" icon="📊" title="Explore Your Actuals" agentBadge="scenario" agentNote="The Scenario Agent is available here to help with analysis questions."
            content="The Actuals tab is your interactive pivot table. Drag fields into Rows, Columns, and Values to build any view of your data. Apply filters to focus on specific accounts, periods, or companies. Save your favorite configurations as Views for one-click access later." />
          <HowItWorksSection number="5" icon="🔮" title="Build Scenarios" agentBadge="scenario"
            agentNote="The Scenario Agent can create and modify scenarios through chat: just say 'create a scenario with 5% revenue growth for 2026'."
            content="The Scenarios tab lets you create what-if models. Each scenario starts from a base year of real data and applies rules:"
            details={["Multiplier rules: 'Increase revenue by 10%' → ×1.10", "Offset rules: 'Add 300K to personnel costs' → +300,000", "Equal or proportional distribution across months", "Project into future periods that don't have actuals yet"]}
            extraContent="The comparison table shows Actuals vs Scenario side-by-side with the delta (Δ) for every row. The waterfall chart visualizes what's driving the change." />
          <div style={{ background: "#f0f9fd", borderRadius: 12, padding: "20px 24px", marginTop: 24, border: "1px solid #d4ecf5" }}>
            <div style={{ fontSize: 14, fontWeight: 700, color: "#1a1a2e", marginBottom: 12 }}>🤖 Two AI Agents, One Chat Panel</div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
              <div>
                <div style={{ fontSize: 12, fontWeight: 700, color: "#6366f1", marginBottom: 4 }}>🔍 Data Understanding Agent</div>
                <div style={{ fontSize: 12, color: "#4b5563", lineHeight: 1.5 }}>Active on the <strong>Data Model</strong> tab. Learns about your data structure, saves knowledge entries, answers questions about tables and fields. Proactively asks questions after you upload data.</div>
              </div>
              <div>
                <div style={{ fontSize: 12, fontWeight: 700, color: "#2563eb", marginBottom: 4 }}>📊 Scenario Agent</div>
                <div style={{ fontSize: 12, color: "#4b5563", lineHeight: 1.5 }}>Active on <strong>Actuals</strong> and <strong>Scenarios</strong> tabs. Creates scenarios, applies rules, answers analysis questions. Uses the knowledge saved by the Data Agent.</div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

// ─── ONBOARDING CHECKLIST ─────────────────────────────────────────
const ONBOARDING_STEPS = [
  { key: "upload_data", title: "Upload your data", description: "Go to the Data Model tab and upload an Excel file with your financial data.", tab: "schema" },
  { key: "review_schema", title: "Review the detected schema", description: "Check that column roles (dimension, measure, time) are correct. Adjust if needed.", tab: "schema" },
  { key: "chat_data_agent", title: "Talk to the Data Agent", description: "Answer the Data Agent's questions about your data. This teaches the system your business logic.", tab: "schema" },
  { key: "explore_actuals", title: "Explore your actuals", description: "Go to the Actuals tab. Set up rows, columns, and values to see your data in a pivot table.", tab: "actuals" },
  { key: "save_view", title: "Save a view", description: "Find a pivot configuration you like and save it as a View for quick access later.", tab: "actuals" },
  { key: "create_scenario", title: "Create your first scenario", description: "Go to the Scenarios tab and create a scenario — either manually or by asking the Scenario Agent in chat.", tab: "scenarios" },
];

function getCompletedSteps(modelId, datasets, scenarios, knowledgeEntries, savedViews, onboardingState) {
  const manual = onboardingState[modelId] || {};
  const completed = { ...manual };
  if (datasets.length > 0) completed.upload_data = true;
  if (datasets.some(d => d.columns?.some(c => c.column_role))) completed.review_schema = true;
  if (knowledgeEntries.some(e => e.source === "chat_agent")) completed.chat_data_agent = true;
  if (savedViews.length > 0) completed.save_view = true;
  if (scenarios.length > 0) completed.create_scenario = true;
  return completed;
}

function OnboardingChecklist({ steps, completedSteps, onDismiss, onGoToTab }) {
  const [collapsed, setCollapsed] = useState(false);
  const [dismissed, setDismissed] = useState(false);
  const doneCount = steps.filter(s => completedSteps[s.key]).length;
  const allDone = doneCount === steps.length;
  const progress = doneCount / steps.length;
  const circumference = 2 * Math.PI * 16; // r=16

  if (dismissed) return null;
  return (
    <div style={{ position: "fixed", bottom: 20, right: 20, width: collapsed ? 56 : 300, background: "#fff", borderRadius: 14, boxShadow: "0 8px 40px rgba(0,0,0,0.12)", border: "1px solid #e8ebee", zIndex: 100, transition: "width 0.25s ease", overflow: "hidden", fontFamily: "'Plus Jakarta Sans', -apple-system, sans-serif" }}>
      {collapsed ? (
        <div onClick={() => setCollapsed(false)} style={{ width: 56, height: 56, cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center", position: "relative" }}>
          <svg width="40" height="40" style={{ transform: "rotate(-90deg)" }}>
            <circle cx="20" cy="20" r="16" fill="none" stroke="#eef0f2" strokeWidth="3" />
            <circle cx="20" cy="20" r="16" fill="none" stroke={C.brand} strokeWidth="3" strokeDasharray={`${progress * circumference} ${circumference}`} strokeLinecap="round" />
          </svg>
          <span style={{ position: "absolute", fontSize: 12, fontWeight: 700, color: "#1a1a2e" }}>{doneCount}</span>
        </div>
      ) : (
        <>
          <div style={{ padding: "14px 16px 10px", display: "flex", alignItems: "center", justifyContent: "space-between", borderBottom: "1px solid #f3f4f6" }}>
            <div>
              <div style={{ fontSize: 13, fontWeight: 700, color: "#1a1a2e" }}>Getting Started</div>
              <div style={{ fontSize: 11, color: "#9ca3af", marginTop: 2 }}>{allDone ? "All done! 🎉" : `${doneCount} of ${steps.length} complete`}</div>
            </div>
            <div style={{ display: "flex", gap: 4 }}>
              <button onClick={() => setCollapsed(true)} style={{ background: "none", border: "none", cursor: "pointer", fontSize: 14, color: "#9ca3af", padding: 4 }}>−</button>
              <button onClick={() => { setDismissed(true); onDismiss(); }} style={{ background: "none", border: "none", cursor: "pointer", fontSize: 14, color: "#9ca3af", padding: 4 }}>×</button>
            </div>
          </div>
          <div style={{ padding: "8px 16px 4px" }}>
            <div style={{ height: 4, borderRadius: 2, background: "#eef0f2", overflow: "hidden" }}>
              <div style={{ height: "100%", borderRadius: 2, background: allDone ? "linear-gradient(90deg, #34d399, #10b981)" : `linear-gradient(90deg, ${C.brand}, #38a3c9)`, width: `${progress * 100}%`, transition: "width 0.4s ease" }} />
            </div>
          </div>
          <div style={{ padding: "8px 12px 14px", maxHeight: 340, overflowY: "auto" }}>
            {steps.map(step => {
              const done = !!completedSteps[step.key];
              return (
                <div key={step.key}
                  onClick={() => { if (!done && step.tab) onGoToTab(step.tab); }}
                  onMouseEnter={e => { if (!done) e.currentTarget.style.background = "#f9fafb"; }}
                  onMouseLeave={e => { e.currentTarget.style.background = "transparent"; }}
                  style={{ display: "flex", gap: 10, padding: "8px 6px", borderRadius: 8, cursor: done ? "default" : "pointer", transition: "background 0.1s" }}>
                  <div style={{ width: 20, height: 20, borderRadius: 6, flexShrink: 0, marginTop: 1, border: done ? "none" : "2px solid #d1d5db", background: done ? C.brand : "transparent", display: "flex", alignItems: "center", justifyContent: "center", transition: "all 0.2s" }}>
                    {done && <span style={{ color: "#fff", fontSize: 12, fontWeight: 700 }}>✓</span>}
                  </div>
                  <div>
                    <div style={{ fontSize: 12, fontWeight: 600, color: done ? "#9ca3af" : "#1a1a2e", textDecoration: done ? "line-through" : "none", lineHeight: 1.3 }}>{step.title}</div>
                    {!done && <div style={{ fontSize: 11, color: "#9ca3af", marginTop: 2, lineHeight: 1.4 }}>{step.description}</div>}
                  </div>
                </div>
              );
            })}
          </div>
        </>
      )}
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
  const [knowledgeRefreshKey, setKnowledgeRefreshKey] = useState(0);
  const [currentModelId, setCurrentModelId] = useState(null);
  const [currentModelName, setCurrentModelName] = useState("");
  const [showHowItWorks, setShowHowItWorks] = useState(false);

  // ── Onboarding state (per model, persisted in localStorage) ─────
  const [onboardingState, setOnboardingState] = useState(() => {
    try { return JSON.parse(localStorage.getItem("databobiq_onboarding") || "{}"); }
    catch { return {}; }
  });
  const [onboardingDismissed, setOnboardingDismissed] = useState(() => {
    try { return JSON.parse(localStorage.getItem("databobiq_onboarding_dismissed") || "{}"); }
    catch { return {}; }
  });
  useEffect(() => { localStorage.setItem("databobiq_onboarding", JSON.stringify(onboardingState)); }, [onboardingState]);
  const markOnboardingStep = (modelId, stepKey) => {
    setOnboardingState(prev => ({ ...prev, [modelId]: { ...(prev[modelId] || {}), [stepKey]: true } }));
  };
  const dismissOnboarding = () => {
    const updated = { ...onboardingDismissed, [currentModelId]: true };
    setOnboardingDismissed(updated);
    localStorage.setItem("databobiq_onboarding_dismissed", JSON.stringify(updated));
  };

  // ── Load models to auto-select ──────────────────────────────────
  const { data: models = [], isLoading: modelsLoading } = useQuery({
    queryKey: ["models"],
    queryFn: listModels,
    staleTime: 60_000,
  });

  // Auto-select if there's exactly one model on initial load only
  const hasAutoSelected = useRef(false);
  useEffect(() => {
    if (!hasAutoSelected.current && !currentModelId && models.length === 1) {
      hasAutoSelected.current = true;
      setCurrentModelId(models[0].id);
      setCurrentModelName(models[0].name);
    }
  }, [models]);

  // ── Reset model-scoped state when switching models ──────────────
  const handleBackToModels = () => {
    setCurrentModelId(null);
    setCurrentModelName("");
    setScenarios([]);
    setSchema({});
    setRelationships([]);
  };

  // ── Load datasets from API ──────────────────────────────────────
  const { data: schemaList = [], isLoading } = useQuery({
    queryKey: ["datasets", currentModelId],
    queryFn: () => getDatasets(currentModelId),
    staleTime: 30_000,
    enabled: !!currentModelId,
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

  // ── Onboarding trigger — fire once per dataset when profiling completes ──
  const [pendingOnboardingId, setPendingOnboardingId] = useState(null);
  useEffect(() => {
    if (!factDataset?.dataset.ai_analyzed || !factDataset?.dataset.id) return;
    const dsId = factDataset.dataset.id;
    const key = `databobiq_onboarded_${dsId}`;
    if (!localStorage.getItem(key)) {
      localStorage.setItem(key, "true");
      setPendingOnboardingId(dsId);
    }
  }, [factDataset?.dataset.ai_analyzed, factDataset?.dataset.id]);

  // ── Scenarios from API ──────────────────────────────────────────
  const { data: apiScenarios = [] } = useQuery({
    queryKey: ["scenarios", currentModelId],
    queryFn: () => getScenarios(factDataset.dataset.id, currentModelId),
    enabled: !!factDataset?.dataset.id,
    staleTime: 30_000,
  });
  useEffect(() => {
    if (apiScenarios.length) {
      setScenarios(apiScenarios.map(s => ({
        id: s.id,
        name: s.name,
        rules: s.rules || [],
        color: s.color || SC_COLORS[0],
        base_config: s.base_config || null,
      })));
    }
  }, [apiScenarios]);

  // ── Dataset name → id lookup (for relationship creation) ────────
  const dsNameToId = useMemo(
    () => Object.fromEntries(schemaList.map(sr => [sr.dataset.name, sr.dataset.id])),
    [schemaList]
  );

  // ── Baseline from API ───────────────────────────────────────────
  const relIds = useMemo(() => relationships.map(r => r.id), [relationships]);
  const { data: apiBaseline } = useQuery({
    queryKey: ["baseline", factDataset?.dataset.id, relIds, currentModelId],
    queryFn: () => getBaseline(factDataset.dataset.id, relIds, currentModelId),
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
          apiDeleteRelationship(r.id).then(() => queryClient.invalidateQueries({ queryKey: ["datasets", currentModelId] })).catch(console.error);
        }
      }
      // Added (local temp ID = string like "gl_entries-accounts-…")
      for (const r of next) {
        if (!prevIds.has(r.id)) {
          const srcId = dsNameToId[r.from];
          const tgtId = dsNameToId[r.to];
          if (srcId && tgtId) {
            apiCreateRelationship({ source_dataset_id: srcId, target_dataset_id: tgtId, source_column: r.fromCol, target_column: r.toCol }, currentModelId)
              .then(() => queryClient.invalidateQueries({ queryKey: ["datasets", currentModelId] }))
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
              .then(() => queryClient.invalidateQueries({ queryKey: ["datasets", currentModelId] }))
              .catch(console.error);
          }
        }
      }
      return next;
    });
  }

  // ── Scenario change handler — persists updates to API ──────────
  function handleSetScenarios(updater) {
    setScenarios(prev => {
      const next = typeof updater === "function" ? updater(prev) : updater;
      const prevIds = new Set(prev.map(s => s.id));
      const nextIds = new Set(next.map(s => s.id));
      // Deleted
      for (const s of prev) {
        if (!nextIds.has(s.id)) {
          deleteScenario(s.id).catch(console.error);
        }
      }
      // Modified (addScenario handles creation directly, so only update existing ids)
      for (const s of next) {
        if (prevIds.has(s.id)) {
          const p = prev.find(x => x.id === s.id);
          if (p && JSON.stringify(p) !== JSON.stringify(s)) {
            updateScenario(s.id, { name: s.name, rules: s.rules, color: s.color, base_config: s.base_config || null }).catch(console.error);
          }
        }
      }
      return next;
    });
  }

  // ── Auto-mark explore_actuals when user opens Actuals with data ─
  useEffect(() => {
    if (tab === "actuals" && currentModelId && schemaList.length > 0) {
      markOnboardingStep(currentModelId, "explore_actuals");
    }
  }, [tab, schemaList.length, currentModelId]);

  if (modelsLoading) return <LoadingScreen />;
  if (!currentModelId) {
    return (
      <>
        <ModelLandingPage
          models={models}
          loading={modelsLoading}
          onSelect={(id, name) => { setCurrentModelId(id); setCurrentModelName(name); }}
          onRefresh={() => queryClient.invalidateQueries({ queryKey: ["models"] })}
          onShowHowItWorks={() => setShowHowItWorks(true)}
        />
        {showHowItWorks && <HowItWorksModal onClose={() => setShowHowItWorks(false)} />}
      </>
    );
  }

  if (isLoading) return <LoadingScreen />;
  if (!schemaList.length) return <UploadScreen onUploaded={() => queryClient.invalidateQueries({ queryKey: ["datasets", currentModelId] })} />;

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
          <img src="/IQLogo.png" alt="dataBobIQ" style={{ height: 30, objectFit: "contain" }} />
          <button onClick={handleBackToModels}
            onMouseEnter={e => e.currentTarget.style.background = "#f0f9fd"}
            onMouseLeave={e => e.currentTarget.style.background = "none"}
            style={{ background: "none", border: "none", cursor: "pointer", fontSize: 12, color: C.brand, fontWeight: 600, display: "flex", alignItems: "center", gap: 4, padding: "4px 8px", borderRadius: 6, marginLeft: 4 }}>
            ← Models
          </button>
          <span style={{ color: "#d1d5db", fontSize: 18, fontWeight: 300 }}>/</span>
          <span style={{ fontSize: 14, fontWeight: 600, color: C.text, maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{currentModelName}</span>
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

        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <div style={{ fontSize: 12, color: C.textMuted, fontWeight: 500 }}>{datasetLabel}</div>
          <button onClick={() => setShowHowItWorks(true)}
            onMouseEnter={e => e.currentTarget.style.color = C.brand}
            onMouseLeave={e => e.currentTarget.style.color = C.textMuted}
            style={{ background: "none", border: "none", cursor: "pointer", fontSize: 12, color: C.textMuted, padding: "4px 8px", fontFamily: "inherit", display: "flex", alignItems: "center", gap: 4, borderRadius: 6, transition: "color 0.15s" }}>
            📖 Guide
          </button>
        </div>
      </div>

      <div style={{ display: "flex", flex: 1, overflow: "hidden" }}>
        <div style={{ flex: 1, overflow: "auto", padding: 24 }}>
          {tab === "schema" && <SchemaView schema={schema} setSchema={handleSetSchema} relationships={relationships} setRelationships={handleSetRelationships} onOpenUpload={() => setUploadOpen(true)} factDatasetId={factDataset?.dataset.id} knowledgeRefreshKey={knowledgeRefreshKey} modelId={currentModelId} />}
          {tab === "actuals" && <ActualsView baseline={baseline} schema={schema} />}
          {tab === "scenarios" && <ScenariosView baseline={baseline} scenarios={scenarios} setScenarios={handleSetScenarios} schema={schema} factDatasetId={factDataset?.dataset.id} relIds={relIds} modelId={currentModelId} />}
        </div>
        <ChatPanel baseline={baseline} scenarios={scenarios} setScenarios={handleSetScenarios} setActiveTab={setTab} activeTab={tab} datasetId={factDataset?.dataset.id} onKnowledgeSaved={() => setKnowledgeRefreshKey(k => k + 1)} pendingOnboardingId={pendingOnboardingId} onOnboardingConsumed={() => setPendingOnboardingId(null)} modelId={currentModelId} />
      </div>
      <UploadModal
        isOpen={uploadOpen}
        onClose={() => setUploadOpen(false)}
        onUploaded={() => queryClient.invalidateQueries({ queryKey: ["datasets", currentModelId] })}
        schemaList={schemaList}
        modelId={currentModelId}
      />
      {/* ── Onboarding checklist ── */}
      {currentModelId && !onboardingDismissed[currentModelId] && (
        <OnboardingChecklist
          steps={ONBOARDING_STEPS}
          completedSteps={getCompletedSteps(currentModelId, schemaList, scenarios, [], [], onboardingState)}
          onDismiss={dismissOnboarding}
          onGoToTab={setTab}
        />
      )}
      {/* ── How It Works modal ── */}
      {showHowItWorks && <HowItWorksModal onClose={() => setShowHowItWorks(false)} />}
    </div>
  );
}

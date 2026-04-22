from __future__ import annotations

import html as _html
import re
import tempfile
import webbrowser
from collections import Counter, defaultdict
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy_spy.profiler import Profiler

_CWD = Path.cwd()

_KW_RE = re.compile(
    r"\b(SELECT|FROM|WHERE|JOIN|LEFT|RIGHT|INNER|OUTER|CROSS|FULL|ON|AND|OR|NOT"
    r"|IN|IS|NULL|ORDER\s+BY|GROUP\s+BY|HAVING|LIMIT|OFFSET|INSERT|INTO|VALUES"
    r"|UPDATE|SET|DELETE|CREATE|DROP|ALTER|TABLE|INDEX|RETURNING|DISTINCT"
    r"|AS|COUNT|SUM|MAX|MIN|AVG|COALESCE|CASE|WHEN|THEN|ELSE|END|WITH|UNION|ALL)\b",
    re.IGNORECASE,
)


def _short_path(filename: str) -> str:
    try:
        return str(Path(filename).relative_to(_CWD))
    except ValueError:
        return Path(filename).name


def _hl(sql: str) -> str:
    return _KW_RE.sub(
        lambda m: f'<span class="kw">{m.group()}</span>',
        _html.escape(sql.strip()),
    )


def _dc(ms: float) -> str:
    return "red" if ms >= 100 else "yellow" if ms >= 20 else "green"


def _op_cls(op: str) -> str:
    return f"op-{op}" if op in ("SELECT", "INSERT", "UPDATE", "DELETE") else "op-other"


def _fmt_params(params) -> str:
    """Return an HTML snippet showing the query parameters, or '' if none."""
    if not params:
        return ""

    items: list[tuple[str, str]]

    if isinstance(params, dict):
        if not params:
            return ""
        items = [(f":{k}", repr(v)) for k, v in params.items()]
    elif isinstance(params, (list, tuple)):
        if not params:
            return ""
        # executemany: list/tuple of rows
        if isinstance(params[0], (list, tuple, dict)):
            n = len(params)
            return (
                f'<div class="params-lbl">Parameters</div>'
                f'<div class="params"><div class="param">'
                f'<span class="p-val">executemany \u2014 {n} row{"s" if n != 1 else ""}</span>'
                f"</div></div>"
            )
        items = [(f"${i + 1}", repr(v)) for i, v in enumerate(params)]
    else:
        items = [("", repr(params))]

    rows = "".join(
        f'<div class="param">'
        f'<span class="p-key">{_html.escape(str(k))}</span>'
        f'<span class="p-eq"> = </span>'
        f'<span class="p-val">{_html.escape(str(v))}</span>'
        f"</div>"
        for k, v in items
    )
    return f'<div class="params-lbl">Parameters</div><div class="params">{rows}</div>'


def _classify_plan(plan: list[str]) -> tuple[str, str]:
    """Return (css_key, label) summarising the dominant access type in the plan."""
    has_index = False
    for line in plan:
        upper = line.upper()
        is_index_line = (
            "USING INDEX" in upper
            or "COVERING INDEX" in upper
            or "PRIMARY KEY" in upper
            or "INDEX SCAN" in upper
            or "INDEX ONLY SCAN" in upper
            or "BITMAP INDEX SCAN" in upper
        )
        is_scan_line = (
            "SCAN" in upper
            and "INDEX" not in upper
            and "COVERING" not in upper
            and "USING" not in upper
        ) or "SEQ SCAN" in upper
        if is_index_line:
            paren = line[line.index("(") :] if "(" in line else ""
            if " AND " in paren.upper():
                return "composite-index", "Composite Index"
            has_index = True
        if is_scan_line:
            return "full-scan", "Full Scan"
    if has_index:
        return "index", "Index"
    return "plan", "Plan"


# Numeric sort keys — lower = more problematic, sorts first ascending
_PLAN_SORT: dict[str, str] = {
    "full-scan": "1",
    "index": "2",
    "composite-index": "3",
    "plan": "4",
}

# (prefix_to_match_uppercased, css_class) — first match wins
_NODE_RULES: list[tuple[str, str]] = [
    # PostgreSQL
    ("SEQ SCAN", "np-scan"),
    ("INDEX ONLY SCAN", "np-idxo"),
    ("BITMAP INDEX SCAN", "np-comp"),
    ("BITMAP HEAP SCAN", "np-comp"),
    ("INDEX SCAN", "np-idx"),
    ("NESTED LOOP", "np-join"),
    ("HASH JOIN", "np-join"),
    ("MERGE JOIN", "np-join"),
    ("SORT", "np-sort"),
    ("AGGREGATE", "np-dim"),
    ("HASH", "np-dim"),
    # SQLite
    ("SEARCH", "np-idx"),  # handled separately in _plan_line_cls for composite
    ("SCAN", "np-scan"),
    ("USE TEMP B-TREE", "np-sort"),
    ("CO-ROUTINE", "np-dim"),
    ("SCALAR SUBQUERY", "np-dim"),
    ("CORRELATED SCALAR", "np-dim"),
    ("MATERIALIZE", "np-dim"),
    ("MULTI-INDEX", "np-comp"),
]

_PLAN_COST_RE = re.compile(r"(\([^)]*(?:cost|time)=[^)]+\))", re.IGNORECASE)


def _plan_line_cls(line: str) -> str:
    upper = line.strip().lstrip("-> ").upper()
    if upper.startswith("SEARCH"):
        # Covering index = SQLite's Index Only Scan (no heap fetch needed)
        if "COVERING INDEX" in upper:
            paren = upper[upper.index("(") :] if "(" in upper else ""
            return "np-comp" if " AND " in paren else "np-idxo"
        # Regular index — composite if multiple predicates in parens
        if "USING INDEX" in upper:
            paren = upper[upper.index("(") :] if "(" in upper else ""
            return "np-comp" if " AND " in paren else "np-idx"
        # Primary key lookup
        if "INTEGER PRIMARY KEY" in upper or "PRIMARY KEY" in upper:
            return "np-idxo"
    for prefix, cls in _NODE_RULES:
        if upper.startswith(prefix):
            return cls
    return "np-dim"


def _fmt_plan_line(line: str) -> str:
    cls = _plan_line_cls(line)
    escaped = _PLAN_COST_RE.sub(
        r'<span class="plan-cost">\1</span>',
        _html.escape(line),
    )
    return f'<div class="plan-line {cls}">{escaped}</div>'


def _fmt_explain(plan: list[str] | None, plan_id: str = "x") -> str:
    """Return an HTML snippet showing the EXPLAIN plan badge + collapsible detail."""
    if not plan:
        return ""
    type_key, label = _classify_plan(plan)
    lines_html = "".join(_fmt_plan_line(line) for line in plan)
    eid = f"eplan-{plan_id}"
    return (
        f'<div class="explain-wrap">'
        f'<span class="explain-lbl">Execution plan</span>'
        f'<span class="plan-badge {type_key}">{label}</span>'
        f'<button class="plan-tog" onclick="togglePlan(\'{eid}\',this)">details ▾</button>'
        f'<div class="explain" id="{eid}">'
        f'<div class="plan-tree">{lines_html}</div>'
        f"</div>"
        f"</div>"
    )


_CSS = """\
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{background:#0d1117;color:#c9d1d9;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;font-size:14px;line-height:1.5}
.wrap{max-width:1200px;margin:0 auto;padding:24px}
.hdr{display:flex;align-items:center;justify-content:space-between;gap:16px;padding:18px 24px;background:#161b22;border:1px solid #30363d;border-radius:8px;margin-bottom:20px;flex-wrap:wrap}
.hdr-title{font-size:17px;font-weight:600;color:#58a6ff;letter-spacing:-.3px}
.hdr-stats{display:flex;gap:28px;flex-wrap:wrap}
.stat{text-align:right}
.stat-v{display:block;font-size:20px;font-weight:700;font-variant-numeric:tabular-nums}
.stat-v.green{color:#3fb950}.stat-v.yellow{color:#d29922}.stat-v.red{color:#f85149}
.stat-l{display:block;font-size:10px;color:#8b949e;text-transform:uppercase;letter-spacing:.7px;margin-top:1px}
.filters{display:flex;gap:8px;margin-bottom:12px;flex-wrap:wrap}
.fbtn{padding:4px 14px;border:1px solid #30363d;border-radius:20px;background:transparent;color:#8b949e;font-size:12px;font-family:inherit;cursor:pointer;transition:all .15s;white-space:nowrap}
.fbtn:hover{border-color:#58a6ff;color:#58a6ff}
.fbtn.active{background:#58a6ff;border-color:#58a6ff;color:#0d1117;font-weight:600}
.tbl-wrap{border:1px solid #30363d;border-radius:8px;overflow:hidden;margin-bottom:20px}
table{width:100%;border-collapse:collapse}
thead th{padding:7px 12px;text-align:left;font-size:11px;font-weight:600;color:#8b949e;text-transform:uppercase;letter-spacing:.6px;background:#161b22;border-bottom:1px solid #30363d;white-space:nowrap}
tr.qrow{border-bottom:1px solid #21262d;cursor:pointer;transition:background .1s}
tr.qrow:last-of-type{border-bottom:none}
tr.qrow:hover{background:#161b22}
tr.qrow.open{background:#161b22}
td{padding:9px 12px;vertical-align:middle}
.td-n{color:#8b949e;font-size:12px;font-family:monospace;width:36px}
.dur-cell{white-space:nowrap;width:130px}
.dur-bar-w{display:flex;align-items:center;gap:8px}
.dur-bar{flex-shrink:0;height:4px;border-radius:2px;min-width:2px;opacity:.65}
.dur-bar.green{background:#3fb950}.dur-bar.yellow{background:#d29922}.dur-bar.red{background:#f85149}
.dur{font-family:'SF Mono','Fira Code',ui-monospace,monospace;font-size:12px;font-weight:600}
.dur.green{color:#3fb950}.dur.yellow{color:#d29922}.dur.red{color:#f85149}
.op{display:inline-block;padding:2px 7px;border-radius:4px;font-size:11px;font-weight:700;letter-spacing:.3px;font-family:monospace;min-width:68px;text-align:center}
.op-SELECT{background:rgba(88,166,255,.15);color:#58a6ff}
.op-INSERT{background:rgba(63,185,80,.15);color:#3fb950}
.op-UPDATE{background:rgba(210,153,34,.15);color:#d29922}
.op-DELETE{background:rgba(248,81,73,.15);color:#f85149}
.op-other{background:rgba(139,148,158,.15);color:#8b949e}
.sql-cell{display:flex;align-items:center;gap:4px;min-width:0}
.sql-pre{font-family:'SF Mono','Fira Code',ui-monospace,monospace;font-size:12px;color:#c9d1d9;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;min-width:0}
.sql-pre.err{color:#f85149}
.cs{font-family:'SF Mono','Fira Code',ui-monospace,monospace;font-size:11px;white-space:nowrap}
.cs-file{color:#58a6ff}.cs-fn{color:#d2a8ff}.cs-dim{color:#8b949e}
.chev{display:inline-block;font-size:9px;color:#8b949e;margin-right:6px;transition:transform .15s;user-select:none}
tr.qrow.open .chev{transform:rotate(90deg)}
tr.detail{background:#0d1117}
tr.detail>td{padding:0;border-bottom:1px solid #30363d}
.det-inner{padding:14px 20px 14px 48px}
.det-inner pre{background:#161b22;border:1px solid #30363d;border-radius:6px;padding:11px 14px;overflow-x:auto;font-family:'SF Mono','Fira Code',ui-monospace,monospace;font-size:12px;line-height:1.7;margin-bottom:10px;white-space:pre-wrap;word-break:break-all}
.kw{color:#ff7b72;font-weight:600}
.err-badge{display:inline-block;padding:2px 8px;background:rgba(248,81,73,.2);border:1px solid rgba(248,81,73,.35);color:#f85149;border-radius:4px;font-size:12px;font-family:monospace;margin-bottom:10px;word-break:break-all}
.stk-lbl{font-size:10px;color:#8b949e;text-transform:uppercase;letter-spacing:.7px;margin-bottom:5px}
.stk-frame{display:flex;align-items:baseline;gap:6px;padding:3px 0 3px 10px;border-left:2px solid #30363d;margin-bottom:3px;font-size:12px;font-family:monospace}
.stk-frame.caller{border-left-color:#58a6ff}
.sf-loc{color:#58a6ff}.sf-fn{color:#d2a8ff}
.hp-sec{border:1px solid #30363d;border-radius:8px;overflow:hidden;margin-bottom:16px}
.hp-hdr{display:flex;justify-content:space-between;align-items:center;padding:11px 16px;background:#161b22;cursor:pointer;user-select:none}
.hp-hdr:hover{background:#1c2128}
.hp-title{font-size:13px;font-weight:600}
.hp-tog{color:#8b949e;font-size:11px;transition:transform .2s}
.hp-sec.collapsed .hp-tog{transform:rotate(-90deg)}
.hp-sec.collapsed .hp-body{display:none}
.hp-row{display:flex;align-items:center;gap:12px;padding:9px 16px;border-top:1px solid #21262d;font-size:12px}
.hp-rank{color:#8b949e;width:18px;text-align:right;flex-shrink:0}
.hp-cnt{font-family:monospace;font-weight:700;color:#58a6ff;width:32px;flex-shrink:0}
.hp-dur{font-family:monospace;font-weight:600;width:70px;flex-shrink:0}
.hp-loc{font-family:'SF Mono','Fira Code',ui-monospace,monospace;font-size:11px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.hp-file{color:#58a6ff}.hp-fn{color:#d2a8ff}.hp-dim{color:#8b949e}
th.sortable{cursor:pointer;user-select:none}
th.sortable:hover{color:#c9d1d9}
th.sortable.asc .sort-ind::after{content:' \u25b2'}
th.sortable.desc .sort-ind::after{content:' \u25bc'}
.started{font-family:'SF Mono','Fira Code',ui-monospace,monospace;font-size:12px;color:#8b949e;white-space:nowrap}
.params-lbl{font-size:10px;color:#8b949e;text-transform:uppercase;letter-spacing:.7px;margin-bottom:5px}
.params{margin-bottom:12px;font-family:'SF Mono','Fira Code',ui-monospace,monospace;font-size:12px;display:flex;flex-wrap:wrap;gap:4px 0}
.param{width:100%;display:flex;gap:6px;align-items:baseline;padding:1px 0}
.p-key{color:#d2a8ff;flex-shrink:0}.p-eq{color:#8b949e;flex-shrink:0}.p-val{color:#e6edf3;word-break:break-all}
.explain-wrap{display:flex;align-items:center;flex-wrap:wrap;gap:6px;margin-bottom:12px}
.explain-lbl{font-size:10px;color:#8b949e;text-transform:uppercase;letter-spacing:.7px}
.plan-badge{padding:1px 7px;border-radius:3px;font-size:10px;font-weight:700;letter-spacing:.4px;white-space:nowrap}
.plan-badge.full-scan{background:rgba(210,153,34,.18);border:1px solid rgba(210,153,34,.4);color:#d29922}
.plan-badge.index{background:rgba(63,185,80,.18);border:1px solid rgba(63,185,80,.4);color:#3fb950}
.plan-badge.composite-index{background:rgba(88,166,255,.18);border:1px solid rgba(88,166,255,.4);color:#58a6ff}
.plan-badge.plan{background:rgba(139,148,158,.18);border:1px solid rgba(139,148,158,.35);color:#8b949e}
.plan-tog{background:none;border:none;color:#58a6ff;font-size:11px;cursor:pointer;padding:0;font-family:inherit}
.plan-tog:hover{text-decoration:underline}
.explain{display:none;width:100%;margin-top:6px}
.plan-tree{font-family:'SF Mono','Fira Code',ui-monospace,monospace;font-size:12px;margin-bottom:6px}
.plan-line{padding:1px 0;white-space:pre-wrap;word-break:break-all}
.np-scan{color:#d29922}.np-idx{color:#3fb950}.np-idxo{color:#56d364}
.np-comp{color:#58a6ff}.np-join{color:#d2a8ff}.np-sort{color:#79c0ff}.np-dim{color:#8b949e}
.plan-cost{color:#6e7681;font-size:11px}
.hidden{display:none!important}
"""

_JS = """\
/* ── filter ── */
document.querySelectorAll('.fbtn').forEach(function(btn){
  btn.addEventListener('click',function(){
    document.querySelectorAll('.fbtn').forEach(function(b){b.classList.remove('active')});
    btn.classList.add('active');
    var op=btn.dataset.op;
    document.querySelectorAll('tr.qrow').forEach(function(row){
      var show=!op||row.dataset.op===op;
      row.classList.toggle('hidden',!show);
      var det=document.getElementById('det-'+row.dataset.id);
      if(det) det.classList.toggle('hidden',!show||!row.classList.contains('open'));
    });
  });
});
/* ── expand row ── */
function toggleRow(id){
  var row=document.querySelector('tr.qrow[data-id="'+id+'"]');
  var det=document.getElementById('det-'+id);
  if(!det) return;
  var opening=!row.classList.contains('open');
  row.classList.toggle('open',opening);
  det.classList.toggle('hidden',!opening);
}
/* ── collapse section ── */
function toggleSec(id){
  document.getElementById(id).classList.toggle('collapsed');
}
/* ── explain plan toggle ── */
function togglePlan(id,btn){
  var el=document.getElementById(id);
  if(!el) return;
  var open=el.style.display==='block';
  el.style.display=open?'none':'block';
  btn.textContent=open?'details ▾':'details ▴';
}
/* ── sort ── */
var _sort={col:'started',dir:1};
document.querySelectorAll('th.sortable').forEach(function(th){
  th.addEventListener('click',function(){
    var col=this.dataset.sort;
    _sort.dir=(_sort.col===col)?-_sort.dir:1;
    _sort.col=col;
    _doSort(col,_sort.dir);
    document.querySelectorAll('th.sortable').forEach(function(t){
      t.classList.remove('asc','desc');
    });
    this.classList.add(_sort.dir===1?'asc':'desc');
  });
});
function _doSort(col,dir){
  var tbody=document.querySelector('table tbody');
  var pairs=Array.from(document.querySelectorAll('tr.qrow')).map(function(r){
    return{row:r,det:document.getElementById('det-'+r.dataset.id)};
  });
  pairs.sort(function(a,b){
    var av=a.row.dataset[col]||'',bv=b.row.dataset[col]||'';
    var an=parseFloat(av);
    return isNaN(an)?av.localeCompare(bv)*dir:(an-parseFloat(bv))*dir;
  });
  pairs.forEach(function(p){
    tbody.appendChild(p.row);
    if(p.det)tbody.appendChild(p.det);
  });
}
"""

_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>SQLAlchemy Profiler \u2014 {query_count} {query_label}</title>
<style>{css}</style>
</head>
<body>
<div class="wrap">
{header}
{filters}
<div class="tbl-wrap"><table>
<thead><tr>
<th>#</th>
<th class="sortable asc" data-sort="started">Started<span class="sort-ind"></span></th>
<th class="sortable" data-sort="dur">Duration<span class="sort-ind"></span></th>
<th class="sortable" data-sort="op">Op<span class="sort-ind"></span></th>
{plan_hdr}
<th>SQL</th>
<th class="sortable" data-sort="cs">Call site<span class="sort-ind"></span></th>
</tr></thead>
<tbody>{rows}</tbody>
</table></div>
{hot_paths}
</div>
<script>{js}</script>
</body>
</html>"""


class HtmlRenderer:
    """Renders profiling results as a self-contained interactive HTML page."""

    def render(self, profiler: "Profiler") -> str:
        queries = profiler.queries
        n = len(queries)
        total = profiler.total_time_ms
        max_ms = max((q.duration_ms for q in queries), default=0.0)
        first_started = queries[0].started_at if queries else 0.0
        has_plans = any(q.explain_plan is not None for q in queries)
        colspan = 7 if has_plans else 6
        plan_hdr = (
            '<th class="sortable" data-sort="plan">Plan<span class="sort-ind"></span></th>'
            if has_plans
            else ""
        )

        return _TEMPLATE.format(
            query_count=n,
            query_label="query" if n == 1 else "queries",
            css=_CSS,
            js=_JS,
            header=self._header(n, total, max_ms),
            filters=self._filters(queries),
            rows=self._rows(queries, max_ms, first_started, has_plans, colspan),
            hot_paths=self._hot_paths(queries),
            plan_hdr=plan_hdr,
        )

    def save(self, profiler: "Profiler", path: "str | Path") -> Path:
        """Write the HTML report to *path* and return the resolved Path."""
        p = Path(path)
        p.write_text(self.render(profiler), encoding="utf-8")
        return p

    def open(self, profiler: "Profiler") -> None:
        """Save to a temp file and open it in the default browser."""
        with tempfile.NamedTemporaryFile(
            suffix=".html", delete=False, mode="w", encoding="utf-8"
        ) as f:
            f.write(self.render(profiler))
            webbrowser.open(f"file://{f.name}")

    def _header(self, n: int, total: float, max_ms: float) -> str:
        avg = total / n if n else 0.0
        return (
            f'<div class="hdr">'
            f'<span class="hdr-title">SQLAlchemy Profiler</span>'
            f'<div class="hdr-stats">'
            f'<div class="stat"><span class="stat-v">{n}</span><span class="stat-l">queries</span></div>'
            f'<div class="stat"><span class="stat-v">{total:.2f}ms</span><span class="stat-l">total</span></div>'
            f'<div class="stat"><span class="stat-v {_dc(avg)}">{avg:.2f}ms</span><span class="stat-l">avg</span></div>'
            f'<div class="stat"><span class="stat-v {_dc(max_ms)}">{max_ms:.2f}ms</span><span class="stat-l">slowest</span></div>'
            f"</div></div>"
        )

    def _filters(self, queries: list) -> str:
        counts = Counter(q.operation for q in queries)
        n = len(queries)
        btns = [f'<button class="fbtn active" data-op="">All ({n})</button>']
        for op in ("SELECT", "INSERT", "UPDATE", "DELETE"):
            if op in counts:
                btns.append(
                    f'<button class="fbtn" data-op="{op}">{op} ({counts[op]})</button>'
                )
        other = sum(
            v
            for k, v in counts.items()
            if k not in ("SELECT", "INSERT", "UPDATE", "DELETE")
        )
        if other:
            btns.append(
                f'<button class="fbtn" data-op="other">Other ({other})</button>'
            )
        return f'<div class="filters">{"".join(btns)}</div>'

    def _rows(
        self,
        queries: list,
        max_ms: float,
        first_started: float,
        has_plans: bool,
        colspan: int,
    ) -> str:
        return "".join(
            self._row(q, i, max_ms, first_started, has_plans, colspan)
            for i, q in enumerate(queries, 1)
        )

    def _row(
        self,
        q,
        idx: int,
        max_ms: float,
        first_started: float,
        has_plans: bool,
        colspan: int,
    ) -> str:
        dc = _dc(q.duration_ms)
        op = q.operation
        filter_op = op if op in ("SELECT", "INSERT", "UPDATE", "DELETE") else "other"
        pct = (q.duration_ms / max_ms * 100) if max_ms > 0 else 0.0
        bar_px = round(min(pct * 0.5, 50))
        offset_ms = (q.started_at - first_started) * 1000

        sql_short = _html.escape(q.statement.strip().replace("\n", " ")[:80])
        if len(q.statement.strip()) > 80:
            sql_short += "\u2026"
        sql_cls = "err" if q.error else ""

        cs_sort = ""
        cs_html = ""
        if q.stack:
            frame = q.stack[-1]
            cs_sort = f"{_short_path(frame.filename)}:{frame.lineno}"
            cs_html = (
                f'<span class="cs">'
                f'<span class="cs-file">{_html.escape(_short_path(frame.filename))}:{frame.lineno}</span>'
                f'<span class="cs-dim"> in </span>'
                f'<span class="cs-fn">{_html.escape(frame.name)}()</span>'
                f"</span>"
            )

        plan_sort = ""
        plan_cell = ""
        if has_plans:
            if q.explain_plan is not None:
                type_key, label = _classify_plan(q.explain_plan)
                plan_sort = _PLAN_SORT.get(type_key, "4")
                plan_cell = (
                    f'<td><span class="plan-badge {type_key}">{label}</span></td>'
                )
            else:
                plan_cell = "<td></td>"

        main_row = (
            f'<tr class="qrow" data-id="{idx}" data-op="{filter_op}"'
            f' data-started="{offset_ms:.4f}" data-dur="{q.duration_ms:.4f}"'
            f' data-cs="{_html.escape(cs_sort)}" data-plan="{plan_sort}"'
            f' onclick="toggleRow({idx})">'
            f'<td class="td-n">{idx}</td>'
            f'<td><span class="started">+{offset_ms:.2f}ms</span></td>'
            f'<td class="dur-cell"><div class="dur-bar-w">'
            f'<span class="dur-bar {dc}" style="width:{bar_px}px"></span>'
            f'<span class="dur {dc}">{q.duration_ms:.2f}ms</span>'
            f"</div></td>"
            f'<td><span class="op {_op_cls(op)}">{op}</span></td>'
            f"{plan_cell}"
            f'<td><div class="sql-cell">'
            f'<span class="chev">&#9658;</span>'
            f'<span class="sql-pre {sql_cls}">{sql_short}</span>'
            f"</div></td>"
            f"<td>{cs_html}</td>"
            f"</tr>"
        )

        return main_row + self._detail(q, idx, colspan)

    def _detail(self, q, idx: int, colspan: int = 6) -> str:
        err_html = ""
        if q.error:
            err_html = f'<div class="err-badge">{_html.escape(q.error)}</div>'

        stk_html = ""
        if q.stack:
            stk_html = '<div class="stk-lbl">Call stack</div>'
            for i, frame in enumerate(q.stack):
                cls = " caller" if i == len(q.stack) - 1 else ""
                stk_html += (
                    f'<div class="stk-frame{cls}">'
                    f'<span class="sf-loc">{_html.escape(_short_path(frame.filename))}:{frame.lineno}</span>'
                    f'<span class="sf-fn">{_html.escape(frame.name)}()</span>'
                    f"</div>"
                )

        params_html = _fmt_params(q.params)
        explain_html = _fmt_explain(q.explain_plan, str(idx))

        return (
            f'<tr class="detail hidden" id="det-{idx}">'
            f'<td colspan="{colspan}"><div class="det-inner">'
            f"{err_html}<pre>{_hl(q.statement)}</pre>{params_html}{explain_html}{stk_html}"
            f"</div></td></tr>"
        )

    def _hot_paths(self, queries: list) -> str:
        groups: dict = defaultdict(list)
        for q in queries:
            if q.stack:
                frame = q.stack[-1]
                groups[(frame.filename, frame.lineno, frame.name)].append(q)

        if not groups:
            return ""

        def hp_row(rank: int, key: tuple, qs: list) -> str:
            filename, lineno, fn = key
            total = sum(q.duration_ms for q in qs)
            return (
                f'<div class="hp-row">'
                f'<span class="hp-rank">{rank}.</span>'
                f'<span class="hp-cnt">{len(qs)}\u00d7</span>'
                f'<span class="hp-dur {_dc(total)}">{total:.2f}ms</span>'
                f'<span class="hp-loc">'
                f'<span class="hp-file">{_html.escape(_short_path(filename))}:{lineno}</span>'
                f'<span class="hp-dim"> in </span>'
                f'<span class="hp-fn">{_html.escape(fn)}()</span>'
                f"</span></div>"
            )

        def section(sec_id: str, title: str, ranked: list) -> str:
            body = "".join(hp_row(i, k, v) for i, (k, v) in enumerate(ranked, 1))
            return (
                f'<div class="hp-sec" id="{sec_id}">'
                f'<div class="hp-hdr" onclick="toggleSec(\'{sec_id}\')">'
                f'<span class="hp-title">{title}</span>'
                f'<span class="hp-tog">&#9660;</span></div>'
                f'<div class="hp-body">{body}</div></div>'
            )

        by_count = sorted(groups.items(), key=lambda x: len(x[1]), reverse=True)[:10]
        by_time = sorted(
            groups.items(), key=lambda x: sum(q.duration_ms for q in x[1]), reverse=True
        )[:10]

        return "\n".join(
            [
                section("hp-count", "Hot paths \u2014 most queries", by_count),
                section("hp-time", "Hot paths \u2014 most time", by_time),
            ]
        )

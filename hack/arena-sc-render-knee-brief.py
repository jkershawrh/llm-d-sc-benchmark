#!/usr/bin/env python3
"""Render a self-contained HTML knee/bottleneck brief from an audited campaign set."""

from __future__ import annotations

import argparse
import html
import json
from pathlib import Path


COLORS = {"clusterip": "#166534", "direct": "#1d4ed8"}


def fmt(value: float, digits: int = 1) -> str:
    return f"{value:,.{digits}f}"


def chart(rows: list[dict], metric: str, title: str, unit: str) -> str:
    width, height = 820, 265
    left, right, top, bottom = 62, 24, 28, 48
    plot_w, plot_h = width - left - right, height - top - bottom
    concurrencies = [row["concurrency"] for row in rows]
    series = {
        treatment: [row["treatments"][treatment][metric] for row in rows]
        for treatment in ("clusterip", "direct")
    }
    maximum = max(value for values in series.values() for value in values) * 1.1
    x = lambda index: left + index * plot_w / max(1, len(rows) - 1)
    y = lambda value: top + plot_h - value / maximum * plot_h
    parts = [
        f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="{html.escape(title)}">',
        f'<text x="{left}" y="18" class="chart-title">{html.escape(title)}</text>',
    ]
    for tick in range(5):
        value = maximum * tick / 4
        py = y(value)
        parts.append(f'<line x1="{left}" y1="{py:.1f}" x2="{width-right}" y2="{py:.1f}" class="grid"/>')
        parts.append(f'<text x="{left-9}" y="{py+4:.1f}" text-anchor="end" class="axis">{fmt(value, 0)}</text>')
    for index, concurrency in enumerate(concurrencies):
        parts.append(f'<text x="{x(index):.1f}" y="{height-24}" text-anchor="middle" class="axis">{concurrency}</text>')
    for treatment, values in series.items():
        points = " ".join(f"{x(i):.1f},{y(value):.1f}" for i, value in enumerate(values))
        parts.append(f'<polyline points="{points}" fill="none" stroke="{COLORS[treatment]}" stroke-width="3"/>')
        for index, value in enumerate(values):
            parts.append(f'<circle cx="{x(index):.1f}" cy="{y(value):.1f}" r="4" fill="{COLORS[treatment]}"/>')
    parts.extend([
        f'<text x="{left + plot_w/2:.1f}" y="{height-5}" text-anchor="middle" class="axis-label">Aggregate concurrency</text>',
        f'<text transform="translate(15 {top + plot_h/2:.1f}) rotate(-90)" text-anchor="middle" class="axis-label">{html.escape(unit)}</text>',
        f'<rect x="{width-205}" y="8" width="10" height="10" fill="{COLORS["clusterip"]}"/><text x="{width-190}" y="17" class="legend">ClusterIP</text>',
        f'<rect x="{width-110}" y="8" width="10" height="10" fill="{COLORS["direct"]}"/><text x="{width-95}" y="17" class="legend">Direct Pod</text>',
        '</svg>',
    ])
    return "".join(parts)


def render(audit: dict) -> str:
    rows = audit["by_concurrency"]
    accounting = audit["accounting"]
    row250 = next(row for row in rows if row["concurrency"] == 250)
    row500 = next(row for row in rows if row["concurrency"] == 500)
    loaded_cells = sum(t["cells"] for row in rows for t in row["treatments"].values())
    peak_rps = max(t["max_useful_rps"] for row in rows for t in row["treatments"].values())
    external = [
        t for row in rows for t in row["treatments"].values()
        if t["external_telemetry_cells"]
    ]
    max_target_cpu = max(t["max_otel_target_cpu_sum_of_pod_max_cores"] for t in external)
    max_target_memory = max(t["max_otel_target_memory_sum_of_pod_max_bytes"] for t in external)
    max_conntrack = max(t["max_node_conntrack_fraction"] for t in external)
    max_retransmits = max(t["max_node_tcp_retransmit_delta"] for t in external)
    max_driver_cpu = max(
        cell["driver_cpu_average_cores"]
        for run in audit["runs"] for cell in run["cells"]
        if cell.get("driver_cpu_average_cores") is not None
    )
    max_throttle = max(
        cell["target_throttle_ratio_max"]
        for run in audit["runs"] for cell in run["cells"]
        if cell.get("target_throttle_ratio_max") is not None
    )
    rows_html = []
    for row in rows:
        c, d = row["treatments"]["clusterip"], row["treatments"]["direct"]
        rows_html.append(
            "<tr>"
            f"<td>{row['concurrency']}</td><td>{c['cells']} / {d['cells']}</td>"
            f"<td>{fmt(c['median_useful_rps']/1000)}k / {fmt(d['median_useful_rps']/1000)}k</td>"
            f"<td>{fmt(c['median_p99_ms'], 2)} / {fmt(d['median_p99_ms'], 2)}</td>"
            f"<td>{c['error_requests']:,} / {d['error_requests']:,}</td>"
            f"<td>{c['health_break_cells']}/{c['cells']} / {d['health_break_cells']}/{d['cells']}</td>"
            "</tr>"
        )
    c_gain = (row500["treatments"]["clusterip"]["median_useful_rps"] /
              row250["treatments"]["clusterip"]["median_useful_rps"] - 1) * 100
    d_gain = (row500["treatments"]["direct"]["median_useful_rps"] /
              row250["treatments"]["direct"]["median_useful_rps"] - 1) * 100
    c_p99 = (row500["treatments"]["clusterip"]["median_p99_ms"] /
             row250["treatments"]["clusterip"]["median_p99_ms"] - 1) * 100
    d_p99 = (row500["treatments"]["direct"]["median_p99_ms"] /
             row250["treatments"]["direct"]["median_p99_ms"] - 1) * 100
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>llm-d-sc five-replica knee and bottleneck brief</title>
<style>
:root{{--ink:#172033;--muted:#5d6878;--line:#d8dee8;--soft:#f5f7fa;--green:#166534;--blue:#1d4ed8;--amber:#92400e;--red:#991b1b}}
*{{box-sizing:border-box}} body{{margin:0;background:#edf1f5;color:var(--ink);font:15px/1.5 system-ui,-apple-system,Segoe UI,sans-serif}}
main{{max-width:1060px;margin:0 auto;background:white;min-height:100vh;padding:48px 58px 72px}} h1{{font-size:30px;line-height:1.15;margin:0 0 8px}} h2{{font-size:20px;margin:36px 0 12px}} h3{{font-size:15px;margin:18px 0 6px}} p{{margin:7px 0}} .subtitle{{color:var(--muted);font-size:16px}}
.verdict{{margin:28px 0;padding:22px 24px;background:#f0fdf4;border-left:5px solid var(--green)}} .verdict strong{{font-size:19px}}
.kpis{{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:22px 0}} .kpi{{padding:15px;border:1px solid var(--line);background:var(--soft)}} .kpi b{{display:block;font-size:23px}} .kpi span{{color:var(--muted);font-size:12px}}
.two{{display:grid;grid-template-columns:1fr 1fr;gap:20px}} .panel{{border:1px solid var(--line);padding:17px}} .pass{{color:var(--green);font-weight:700}} .fail{{color:var(--red);font-weight:700}} .caution{{color:var(--amber);font-weight:700}}
table{{width:100%;border-collapse:collapse;font-size:13px}} th,td{{padding:9px 8px;border-bottom:1px solid var(--line);text-align:right}} th:first-child,td:first-child{{text-align:left}} th{{background:var(--soft)}}
svg{{width:100%;height:auto;border:1px solid var(--line);margin:8px 0}} .grid{{stroke:#e4e8ef;stroke-width:1}} .axis,.legend{{font-size:10px;fill:#5d6878}} .axis-label{{font-size:11px;fill:#394457}} .chart-title{{font-size:13px;font-weight:700;fill:#172033}}
ol,ul{{padding-left:21px}} li{{margin:6px 0}} code{{font-size:12px;background:var(--soft);padding:2px 4px}} .small{{font-size:12px;color:var(--muted)}}
@media(max-width:760px){{main{{padding:28px 20px}}.kpis,.two{{grid-template-columns:1fr 1fr}}}} @media print{{body{{background:white}}main{{padding:20px;max-width:none}}}}
</style></head><body><main>
<h1>llm-d-sc: five-replica knee and bottleneck</h1>
<p class="subtitle">Corrected, independently audited cache-hit result · Arena · 1 September 2026</p>
<section class="verdict"><strong>The operational knee is between aggregate concurrency 250 and 500.</strong>
<p>At 500, both ClusterIP and direct-Pod paths begin returning application <code>RESOURCE_EXHAUSTED</code>; median p99 rises {fmt(c_p99,0)}% / {fmt(d_p99,0)}% while useful throughput gains only {fmt(c_gain)}% / {fmt(d_gain)}%. The same break on both transports rules out ClusterIP as the dominant limiter.</p></section>
<div class="kpis"><div class="kpi"><b>{accounting['selected_requests']:,}</b><span>audited requests</span></div><div class="kpi"><b>{accounting['statuses']['GRPC_RESOURCEEXHAUSTED']:,}</b><span>explicit overload responses</span></div><div class="kpi"><b>{accounting['warning_event_delta']:,}</b><span>probe warning deltas</span></div><div class="kpi"><b>{accounting['restart_delta']}</b><span>target restarts</span></div></div>
<p><b>Correction:</b> 48.8k RPS is not a stable five-replica ceiling. The highest observed useful rate was {fmt(peak_rps/1000)}k RPS, but that point was unhealthy and returned overload responses; it is not promotable capacity.</p>

<h2>Where the curve bends</h2>
{chart(rows, 'median_useful_rps', 'Median useful throughput by transport', 'Useful requests per second')}
{chart(rows, 'median_p99_ms', 'Median successful-response p99 by transport', 'Latency (milliseconds)')}
<table><thead><tr><th>Concurrency</th><th>Cells C / D</th><th>Median RPS C / D</th><th>p99 ms C / D</th><th>Errors C / D</th><th>Health breaks C / D</th></tr></thead><tbody>{''.join(rows_html)}</tbody></table>
<p class="small">C = ClusterIP; D = direct Pod IP. Every loaded cell sent five million exact-key cache-hit requests over 125 persistent HTTP/2 connections.</p>

<h2>Bottleneck attribution</h2>
<div class="two"><div class="panel"><h3 class="fail">Application admission / serve path: strongest evidence</h3><p>The first response failure is explicit gRPC <code>RESOURCE_EXHAUSTED</code> on both transports. Repository source maps this status to bounded admission/queue overload. The unchanged runtime image does not export the internal queue/stage counters, so the exact primitive remains source-corroborated rather than directly observed.</p></div>
<div class="panel"><h3 class="pass">ClusterIP: not dominant</h3><p>Direct Pod IP does not remove the knee. At c250 the transport medians differ by less than 1%; at c500 both paths error and have nearly identical p99.</p></div>
<div class="panel"><h3 class="pass">CPU / memory: headroom remained</h3><p>External OTel saw at most {fmt(max_target_cpu,2)} aggregate target cores using the conservative sum of per-Pod maxima, versus 20 cores of limits. Driver CPU peaked at {fmt(max_driver_cpu,2)} of 8 cores; target throttling was {fmt(max_throttle,1)}. Target memory stayed below {fmt(max_target_memory/1024/1024)} MiB aggregate.</p></div>
<div class="panel"><h3 class="pass">Node transport: no exhaustion signature</h3><p>Conntrack peaked at {fmt(max_conntrack*100)}% of limit. Across instrumented boundary cells: zero target network errors, zero Pod packet drops, and zero softnet drops. The largest node-wide retransmit delta was {fmt(max_retransmits,0)} over a five-million-request cell.</p></div></div>

<h2>How we determined it</h2><ol>
<li>Pinned the unchanged classifier image digest and signal-emulator digest.</li>
<li>Placed all five target replicas on <code>gnr2.fm2aihpcsed.com</code> and the driver on <code>rhgnr1</code>, then verified identity before and after every cell.</li>
<li>Compared Kubernetes ClusterIP with direct Pod IP using the same five Pods, workload, connection count, and request count; treatment order was counterbalanced.</li>
<li>Ran three repetitions across c50–c1500, then a fourth OTel-instrumented repetition at c50/c250/c500/c1000.</li>
<li>Captured OTel kubelet metrics, node-exporter TCP/conntrack/softnet metrics, cAdvisor CPU/throttling, per-Pod network counters, Kubernetes probe events, and exact driver status accounting.</li>
<li>Recomputed all {accounting['selected_requests']:,} outcomes from raw cell files and hashed the result, health, resource, and external telemetry summaries.</li></ol>

<h2>What this means for staging</h2><ul>
<li><b>Do not use peak RPS as capacity.</b> Zero response errors end at c250, while health is already unreliable there and intermittent at c50.</li>
<li><b>Instrument the exact runtime revision.</b> Export admitted depth, queue capacity, admission rejections, queue wait, cache hit, and total-stage histograms. OTel collection is already working.</li>
<li><b>Tune policy, then repeat.</b> Exercise queue bound, inference-worker count, and a health endpoint isolated from the saturated gRPC listener. A larger queue alone may only trade rejection for tail latency.</li>
<li><b>Find the healthy floor.</b> Repeat c10/c25/c50 after health-path changes; staging needs zero probe failures, zero overload responses, and recovery evidence.</li>
<li><b>Do not extrapolate this to 40–50 replicas.</b> This proves a five-replica cache-hit boundary. Multi-node model distribution and 10/20/40/50-replica scaling remain a separate campaign.</li></ul>

<h2>Claim boundaries</h2><p>This is a closed-loop, exact-key cache-hit result for one pinned classifier/model shape, five replicas, two nodes, and one cluster. It does not measure cache misses, representative token distributions, open-loop arrival bursts, multi-zone routing, or 40–50 replica deployability.</p>
<p class="small">Evidence: schema v{audit['schema_version']} independent audit; {loaded_cells} loaded cells; classifier digest <code>sha256:04323612…d5aa</code>; driver digest <code>sha256:5c7420b2…e452</code>.</p>
</main></body></html>"""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("audit_json", type=Path)
    parser.add_argument("output_html", type=Path)
    args = parser.parse_args()
    audit = json.loads(args.audit_json.read_text())
    args.output_html.parent.mkdir(parents=True, exist_ok=True)
    args.output_html.write_text(render(audit))


if __name__ == "__main__":
    main()

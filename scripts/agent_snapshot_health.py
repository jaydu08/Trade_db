"""Compact quality audit for one frozen Agent daily snapshot."""

from __future__ import annotations

import json
import statistics
from datetime import date, datetime
from pathlib import Path


def audit(snapshot, output, now):
    from generate_agent_snapshot import atomically_write, immutably_create, number
    problems = []
    required = ('schema_version', 'snapshot_id', 'generated_at', 'overview',
                'market_anomalies', 'source_health')
    problems.extend(f'missing top-level {key}' for key in required if key not in snapshot)
    try:
        json.dumps(snapshot, allow_nan=False)
    except ValueError:
        problems.append('nonfinite JSON number')
    coverage = snapshot.get('market_anomalies', {}).get('coverage') or []
    comparisons = {}
    archive = output / 'history'
    previous = sorted(archive.glob('*.json'), reverse=True) if archive.exists() else []
    for market in ('CN', 'HK', 'US'):
        current = next((row for row in coverage if row.get('market') == market), {})
        if current.get('negative_coverage_status') == 'market_closed':
            comparisons[market] = dict(eligible_universe_count=0, comparable_days=0,
                                       baseline_median=None, drop_pct=None, significant_drop=False,
                                       comparison_status='market_closed',
                                       explanation='exchange-verified full-day closure; no current-session scan')
            continue
        scope, version = current.get('coverage_scope'), current.get('selection_version')
        counts = []
        for path in previous:
            if path.stem >= now.date().isoformat():
                continue
            try:
                old = json.loads(path.read_text(encoding='utf-8'))
                row = next((r for r in old.get('market_anomalies', {}).get('coverage', [])
                            if r.get('market') == market), {})
                if row.get('coverage_scope') == scope and row.get('selection_version') == version:
                    count = row.get('eligible_universe_count')
                    if isinstance(count, int):
                        counts.append(count)
            except (OSError, ValueError):
                continue
            if len(counts) >= 5:
                break
        baseline = statistics.median(counts) if counts else None
        eligible = current.get('eligible_universe_count')
        drop_pct = round((1 - eligible / baseline) * 100, 1) if baseline and eligible is not None else None
        significant = drop_pct is not None and drop_pct > 30
        comparisons[market] = dict(eligible_universe_count=eligible,
                                   comparable_days=len(counts), baseline_median=baseline,
                                   drop_pct=drop_pct, significant_drop=significant,
                                   comparison_status='comparable' if len(counts) >= 3 else 'insufficient_comparable_history',
                                   explanation='coverage change is a review flag, not proof of collection failure' if significant else None)
        if current.get('negative_coverage_status') == 'insufficient':
            problems.append(f'{market} independent negative scan coverage insufficient')
        for item in (snapshot.get('market_anomalies', {}).get('stocks_up') or []) + (
                    snapshot.get('market_anomalies', {}).get('stocks_down') or []):
            if item.get('market') != market:
                continue
            if (number(item.get('price')) or 0) <= 0 or ((number(item.get('amount')) or 0) < 0):
                problems.append(f'{market} invalid price or amount')
            for field in ('observed_at', 'source_checked_at'):
                value = item.get(field)
                if value and datetime.fromisoformat(value.replace('Z', '+00:00')) > now:
                    problems.append(f'{market} future {field}')
    stale_macro = [row['id'] for row in snapshot.get('overview', {}).get('macro', []) if row.get('stale')]
    quality = 'error' if problems else 'partial' if stale_macro or any(v['significant_drop'] for v in comparisons.values()) else 'ok'
    result = dict(schema_version='1.0', snapshot_id=snapshot.get('snapshot_id'),
                  snapshot_generated_at=snapshot.get('generated_at'),
                  audited_at=now.isoformat(timespec='seconds'), quality=quality,
                  suitable_for_automated_research=not problems,
                  use_caveat='Exclude stale macro indicators; coverage is a candidate pool, not full market' if stale_macro else None,
                  failures=problems, coverage_change=comparisons,
                  negative_coverage={r['market']: r.get('negative_coverage_status') for r in coverage},
                  source_health=snapshot.get('source_health', []),
                  stale_macro_indicators=stale_macro,
                  stale_indicator_count=len(stale_macro),
                  missing_fields=['market_index_breadth', 'verified_stock_adjustment'])
    return result


def write_health(snapshot, output, now):
    from generate_agent_snapshot import immutably_create
    path = output / 'health' / f'{now.date().isoformat()}.json'
    payload = audit(snapshot, output, now)
    data = (json.dumps(payload, ensure_ascii=False, separators=(',', ':'), allow_nan=False) + '\n').encode()
    if len(data) > 30_000:
        raise ValueError('health report exceeds 30 KB')
    immutably_create(path, data)
    return json.loads(path.read_bytes())

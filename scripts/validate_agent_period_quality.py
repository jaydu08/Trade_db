"""JSON Schema and P0 semantic validation for 7D/30D Agent exports."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

SCHEMA = {
    'type': 'object',
    'required': ['schema_version', 'snapshot_id', 'generated_at', 'period', 'markets',
                 'source_health', 'stale', 'stale_item_count', 'stale_item_ids'],
    'properties': {
        'schema_version': {'const': '1.2'},
        'period': {'enum': ['weekly', 'monthly']},
        'stale': {'type': 'boolean'},
        'stale_item_count': {'type': 'integer', 'minimum': 0},
        'stale_item_ids': {'type': 'array', 'items': {'type': 'string'}},
        'markets': {'type': 'object', 'required': ['equities_and_commodities', 'crypto',
                                                   'global_assets', 'macro_rates'],
                    'properties': {key: {'type': 'array'} for key in
                                   ('equities_and_commodities', 'crypto', 'global_assets', 'macro_rates')}},
        'source_health': {'type': 'array'},
    },
}


def validate(snapshot):
    errors = []
    try:
        import jsonschema
        jsonschema.validate(snapshot, SCHEMA)
    except ImportError:
        for key in SCHEMA['required']:
            if key not in snapshot:
                errors.append(f'missing top-level {key}')
    except Exception as exc:
        errors.append(f'JSON Schema: {exc}')
    if errors:
        return errors
    stocks = snapshot['markets']['equities_and_commodities']
    for row in stocks:
        symbol = row.get('symbol')
        if not row.get('is_latest_eligible') or row.get('eligibility_status') != 'eligible':
            errors.append(f'{symbol}: noneligible row in formal stock/CF ranking')
        if row.get('market') != 'CF' and row.get('asset_session_date') != row.get('market_reference_session'):
            errors.append(f'{symbol}: asset session differs from market reference')
        if row.get('amount_latest') == 0 and row.get('amount_status') != 'verified_zero':
            errors.append(f'{symbol}: unverified zero amount')
        if row.get('amount_latest') is None and row.get('amount_change_pct') is not None:
            errors.append(f'{symbol}: amount change despite missing latest amount')
        if symbol in ('07709', '07747', '09747') and row.get('asset_type') != 'LEVERAGED_ETF':
            errors.append(f'{symbol}: verified leveraged product classified incorrectly')
        if row.get('baseline_quality') != 'aligned':
            errors.append(f'{symbol}: unacceptable baseline')
    all_items = stocks + snapshot['markets']['crypto'] + snapshot['markets']['global_assets'] + snapshot['markets']['macro_rates']
    stale_count = sum(bool(row.get('stale')) for row in all_items)
    if snapshot['stale_item_count'] != stale_count:
        errors.append(f'stale item count mismatch: {snapshot["stale_item_count"]} != {stale_count}')
    if snapshot['stale'] != (stale_count > 0 or any(h.get('stale') for h in snapshot['source_health'])):
        errors.append('top-level stale rollup mismatch')
    return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('path', type=Path)
    args = parser.parse_args()
    snapshot = json.loads(args.path.read_text(encoding='utf-8'),
                          parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))
    errors = validate(snapshot)
    if errors:
        raise SystemExit('\n'.join(errors))
    print('schema+semantics OK', args.path)


if __name__ == '__main__':
    main()

"""Isolated P0 replay: recalculate stocks from SQLite, freeze unavailable historical feeds.

Never writes to the immutable weekly archive. Crypto and macro observations are
copied byte-for-byte at item level from the specified archived snapshot because
their current caches cannot reconstruct a historical point-in-time view.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import generate_agent_period_snapshot as period
from generate_agent_snapshot import immutably_create


def replay(root: Path, archive: Path, output: Path):
    if '/replay/' not in str(output) or output.resolve() == archive.resolve():
        raise ValueError('output must be a distinct agent/v1/replay path')
    original = json.loads(archive.read_text(encoding='utf-8'))
    if original.get('period') != 'weekly' or original.get('schema_version') != '1.2':
        raise ValueError('expected an immutable schema 1.2 weekly snapshot')
    now = datetime.fromisoformat(original['generated_at'])
    old_health = original['source_health']
    crypto_health = [h for h in old_health if str(h.get('domain', '')).startswith('crypto_')]
    macro_health = next(h for h in old_health if h.get('domain') == 'macro')
    macro_checked = datetime.fromisoformat(macro_health['data_asof']).timestamp()
    archived_crypto = original['markets']['crypto']
    archived_global = original['markets']['global_assets']
    archived_rates = original['markets']['macro_rates']
    scores = {(r['market'], r['symbol']): r.get('trend_score') for r in
              original['markets']['equities_and_commodities']}
    with patch.object(period, 'crypto_period', return_value=(archived_crypto, crypto_health)), \
         patch.object(period, 'macro_period', return_value=(archived_global, archived_rates)), \
         patch.object(period, 'read_json', return_value={'refreshed_at': macro_checked}):
        result = period.build(root, Path('/nonexistent/historical_crypto.db'), 7, now, scores=scores)
    result['snapshot_id'] = original['snapshot_id'] + '-quality-v2-replay'
    result['run_kind'] = 'historical_replay'
    result['replay_provenance'] = {
        'source_archive': str(archive), 'asof_cutoff': original['generated_at'],
        'recomputed_domains': ['stocks_CN', 'stocks_HK', 'stocks_US', 'commodities'],
        'frozen_from_original_domains': ['crypto_spot', 'crypto_futures', 'crypto_onchain', 'macro'],
        'limitation': 'Crypto/macro historical input caches were not versioned; their original archived items are retained, not re-sampled from future caches.'}
    encoded = (json.dumps(result, ensure_ascii=False, separators=(',', ':'), allow_nan=False) + '\n').encode()
    if len(encoded) > period.MAX_BYTES:
        raise ValueError(f'replay exceeds size limit: {len(encoded)} > {period.MAX_BYTES}')
    immutably_create(output, encoded)
    return result, len(encoded)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--archive', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result, size = replay(args.root, args.archive, args.output)
    print(result['snapshot_id'], size)


if __name__ == '__main__':
    main()

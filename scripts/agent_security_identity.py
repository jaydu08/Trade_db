"""Conservative provider aliases, not company-name or underlying-asset merges."""
import json
import re


def canonical_symbol(market, symbol):
    raw = str(symbol).strip()
    # These are Eastmoney's US exchange prefixes already used by our quote adapter.
    if market == 'US':
        match = re.fullmatch(r'(105|106|107)\.([A-Za-z][A-Za-z0-9.\-]*)', raw)
        return (match.group(2) if match else raw).upper()
    return raw


def catalog_type(market, symbol, catalog):
    direct = catalog.get(symbol)
    if direct and str(direct).upper() != 'UNKNOWN':
        return direct
    if market != 'US':
        return direct
    ticker = canonical_symbol(market, symbol)
    types = {catalog.get(alias) for alias in [ticker] + [prefix + '.' + ticker for prefix in ('105', '106', '107')]}
    types -= {None, '', 'UNKNOWN'}
    return next(iter(types)) if len(types) == 1 else direct


def deduplicate(items):
    groups = {}
    for item in items:
        canonical = canonical_symbol(item['market'], item['symbol'])
        item = dict(item, canonical_symbol=canonical,
                    asset_id=f"{item['market']}:{canonical}:{item.get('instrument_type') or item.get('asset_type')}")
        key = (item['market'], canonical, item.get('session_date'),
               item.get('instrument_type') or item.get('asset_type'))
        groups.setdefault(key, []).append(item)
    result = []
    for key in sorted(groups, key=lambda k: tuple(str(v or '') for v in k)):
        rows = groups[key]
        # Latest verified observation first; stable lexical tie-breaks independent of DB row order.
        rows.sort(key=lambda r: (str(r.get('data_asof') or ''),
                                r['symbol'] == r['canonical_symbol'],
                                json.dumps(r, sort_keys=True, ensure_ascii=False)), reverse=True)
        selected = dict(rows[0])
        selected['raw_symbols'] = sorted({r.get('raw_symbol', r['symbol']) for r in rows})
        selected['identity_basis'] = 'same-market exact ticker; verified Eastmoney 105/106/107 prefix aliases only'
        selected['source_records'] = [dict(raw_symbol=r.get('raw_symbol', r['symbol']), source=r.get('source'),
                                          data_asof=r.get('data_asof'), price=r.get('price'),
                                          change_pct=r.get('change_pct')) for r in rows]
        selected['alias_conflict'] = len({(r.get('price'), r.get('change_pct')) for r in rows}) > 1
        result.append(selected)
    return result

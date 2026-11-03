"""Read-only, allowlisted List/active-Holds export for one explicitly selected user."""

from __future__ import annotations

import json
import os
import re
import sqlite3
from contextlib import closing
from datetime import datetime


RESEARCH_INSTRUCTIONS = [
    '覆盖清单中的美股、港股、A股；按市场与代码核实公司身份，避免同名公司混淆。',
    '检索最新重大公告和新闻：增发/配售/ATM、可转债、减持、限售解禁、业绩及指引、'
    '董事会与管理层变动、并购、监管诉讼、退市及指数纳入/剔除风险。',
    '优先监管机构、交易所、公司IR和指数编制方原文；统一中文，保留链接、披露时间、'
    '事件生效时间，区分拟议、获批、生效、取消和未经证实的报道。',
    '仅报告重大新事件或实质进展；对照本任务可访问的近期日报，同一事件转载合并。'
    '无法读取历史时明确去重范围，不能声称已完成跨日报或TG去重。',
    '简述事实、对公司业务/融资/股本的影响和后续关键日期；未查到重大新信息无需逐股凑数。',
    '公司简介是历史背景，不是近期新闻；清单数据不代表真实持仓或交易建议。',
]


def _read_json(path):
    return json.loads(path.read_text(encoding='utf-8'))


def _text(value, limit=420):
    return re.sub(r'\s+', ' ', str(value or '')).strip()[:limit] or None


def canonical_symbol(market, symbol):
    value = str(symbol or '').strip().upper()
    if market == 'US':
        value = re.sub(r'^10[567]\.', '', value)
    elif market == 'HK' and value.isdigit():
        value = value.zfill(5)
    elif market == 'CN':
        value = re.sub(r'^(SH|SZ|BJ)', '', value)
    return value if re.fullmatch(r'[A-Z0-9.\-]{1,24}', value) else None


def _owner_chat_id(root, username):
    # Read only the auth settings needed to reproduce the UI's owner mapping.
    config = {}
    if (root / '.env').exists():
        from dotenv import dotenv_values
        config = dotenv_values(root / '.env')
    for key in ('WEB_USERNAME', 'WEB_USERS_JSON', 'ALLOWED_USER_IDS'):
        if key in os.environ:
            config[key] = os.environ[key]
    primary = str(config.get('WEB_USERNAME') or 'game2du').strip()
    users = json.loads(config.get('WEB_USERS_JSON') or '{}')
    user = users.get(username) if isinstance(users, dict) else None
    value = user.get('chat_id', user.get('chatId')) if isinstance(user, dict) else user
    if value is not None and str(value).strip():
        try:
            return int(value)
        except (ValueError, TypeError):
            pass
    if username == primary:
        try:
            return int(str(config.get('ALLOWED_USER_IDS') or '0').split(',')[0].strip())
        except ValueError:
            return 0
    if isinstance(users, dict) and username not in users:
        raise ValueError('focus owner is not configured')
    if isinstance(users, list) and username not in users:
        raise ValueError('focus owner is not configured')
    import hashlib
    return -int(hashlib.md5(username.encode()).hexdigest()[:8], 16)


def _profiles(root, identities):
    path = root / 'data/meta.db'
    if not path.exists():
        return {}
    result = {}
    with closing(sqlite3.connect(f'{path.resolve().as_uri()}?mode=ro', uri=True)) as db:
        db.row_factory = sqlite3.Row
        for row in db.execute('SELECT symbol,name,market,asset_type FROM asset'):
            key = (row['market'], canonical_symbol(row['market'], row['symbol']))
            if key not in identities:
                continue
            # Never choose an arbitrary issuer when alias resolution is ambiguous.
            result.setdefault(key, []).append(dict(row))
        profiles = {}
        for key, matches in result.items():
            if len(matches) != 1:
                continue
            asset = matches[0]
            profile = db.execute('SELECT main_business,products,company_profile,updated_at '
                                 'FROM asset_profile WHERE symbol=?', (asset['symbol'],)).fetchone()
            sectors = db.execute('SELECT i.name FROM industry i JOIN asset_industry_link l '
                                 'ON l.industry_code=i.code WHERE l.symbol=? '
                                 'ORDER BY l.is_primary DESC,i.level DESC', (asset['symbol'],)).fetchall()
            business = _text(profile['main_business']) if profile else None
            intro = _text(profile['company_profile']) if profile else None
            profiles[key] = dict(name=_text(asset['name'], 120), asset_type=_text(asset['asset_type'], 30),
                                 industry=_text(sectors[0]['name'], 120) if sectors else None,
                                 main_business=business or intro,
                                 products=_text(profile['products'], 240) if profile else None,
                                 profile_asof=_text(profile['updated_at'], 40) if profile else None)
        return profiles


def build_focus(root, now):
    """No source writes, HTTP, LLM calls, or application imports. Disabled by default."""
    config_path = root / 'data/agent_focus_config.json'
    if not config_path.exists():
        return None
    config = _read_json(config_path)
    if not config.get('enabled'):
        return None
    username = str(config.get('username') or '').strip()
    if not username:
        raise ValueError('enabled research focus requires an explicit username')
    primary = 'game2du'
    if (root / '.env').exists():
        from dotenv import dotenv_values
        primary = dotenv_values(root / '.env').get('WEB_USERNAME') or primary
    primary = os.getenv('WEB_USERNAME', primary)
    slug = re.sub(r'[^A-Za-z0-9_.-]+', '_', username)[:80] or 'default'
    watch_path = root / 'data' / ('watchlist.json' if username == primary else f'watchlists/{slug}.json')
    issues, merged = [], {}

    def add(item):
        market = str(item.get('market') or '').strip().upper()
        symbol = canonical_symbol(market, item.get('symbol'))
        if market not in ('CN', 'HK', 'US') or not symbol:
            return
        key = (market, symbol)
        # Strict allowlist: no tags, entry reasons, costs, sizes, dates, IDs or account fields.
        record = merged.setdefault(key, dict(market=market, symbol=symbol, name=None))
        record['name'] = record['name'] or _text(item.get('name'), 120)

    try:
        watch = _read_json(watch_path)
        if not isinstance(watch, dict):
            raise ValueError('invalid list format')
        for item in watch.values():
            if isinstance(item, dict) and item.get('is_active', True):
                add(item)
    except (OSError, ValueError):
        issues.append('list source unavailable')
    try:
        owner = _owner_chat_id(root, username)
        ledger = root / 'data/ledger.db'
        with closing(sqlite3.connect(f'{ledger.resolve().as_uri()}?mode=ro', uri=True)) as db:
            db.row_factory = sqlite3.Row
            for row in db.execute('SELECT symbol,name,market FROM papertrade '
                                  'WHERE status=? AND chat_id=?', ('ACTIVE', owner)):
                add(dict(row))
    except (OSError, ValueError, sqlite3.Error):
        issues.append('active research source unavailable for selected owner')
    try:
        profiles = _profiles(root, set(merged)) if merged else {}
    except (OSError, sqlite3.Error):
        profiles = {}
        issues.append('company profile source unavailable')
    items = []
    for key, item in sorted(merged.items()):
        profile = profiles.get(key, {})
        item['name'] = profile.get('name') or item['name']
        for field in ('asset_type', 'industry', 'main_business', 'products', 'profile_asof'):
            item[field] = profile.get(field)
        asof = item['profile_asof']
        try:
            stale = (now.date() - datetime.fromisoformat(asof).date()).days > 180 if asof else None
        except ValueError:
            stale = None
        item.update(profile_source='Trade_db local company metadata' if profile else None,
                    profile_stale=stale, quality='partial' if profile else 'missing',
                    missing_reason=None if item['main_business'] else 'company business description unavailable')
        items.append(item)
    return dict(schema_version='1.0', generated_at=now.isoformat(timespec='seconds'),
                scope='CN/HK/US combined research universe; membership does not identify holdings',
                count=len(items), items=items, quality='partial' if issues else 'ok',
                missing_reason='; '.join(issues) if issues else None,
                empty_reason='selected account has no active research symbols' if not items and not issues else None,
                research_instructions=RESEARCH_INSTRUCTIONS,
                recent_news_dedup=dict(status='not_available',
                    missing_reason='No verified cross-report/TG event delivery ledger is exported; '
                                   'the research task must use accessible past reports and disclose gaps.'))


def write_focus(root, output, now):
    """Stable standalone entry point, including when today's daily archive is already frozen."""
    from generate_agent_snapshot import atomically_write
    focus = build_focus(root, now)
    if focus is None:
        return None
    path = output / 'focus.json'
    if path.exists():
        old = _read_json(path)
        if {k: v for k, v in old.items() if k != 'generated_at'} == {
                k: v for k, v in focus.items() if k != 'generated_at'}:
            return old
    data = (json.dumps(focus, ensure_ascii=False, separators=(',', ':'), allow_nan=False) + '\n').encode()
    if len(data) > 100_000:
        raise ValueError('research focus exceeds 100 KB; refusing silent truncation')
    atomically_write(path, data)
    return focus

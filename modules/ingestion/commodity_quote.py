"""Dated Tencent foreign-futures quotes; no HTML/name/FX scraping dependency."""
import math
import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import requests

PRODUCTS = {
    'CL': ('WTI 原油','美元/桶','能源'),
    'OIL': ('Brent 原油','美元/桶','能源'),
    'GC': ('COMEX 黄金','美元/盎司','贵金属'),
    'SI': ('COMEX 白银','美元/盎司','贵金属'),
    'HG': ('COMEX 铜','美分/磅','工业金属'),
}
SH = ZoneInfo('Asia/Shanghai')


def parse_quotes(text, checked_at):
    result = []
    for code, raw in re.findall(r'v_hf_([A-Z]+)="([^"\n]*)"', text):
        if code not in PRODUCTS:
            continue
        fields = raw.split(',')
        try:
            value, settlement = float(fields[0]), float(fields[7])
            observed = datetime.fromisoformat(fields[12]+'T'+fields[6]).replace(tzinfo=SH)
            if (not all(math.isfinite(v) and v>0 for v in (value,settlement))
                    or observed > checked_at + timedelta(minutes=5)):
                continue
        except (IndexError,ValueError):
            continue
        name,unit,group = PRODUCTS[code]
        result.append(dict(id=code,name=name,unit=unit,group=group,value=value,
                           change_pct=(value/settlement-1)*100,previous_settlement=settlement,
                           change_basis='provider previous settlement; not previous tick or calendar midnight',
                           asof=observed.isoformat(),source_checked_at=checked_at.isoformat(),
                           source='Tencent foreign futures quote',source_symbol='hf_'+code,
                           instrument_type='COMMODITY_FUTURE',
                           contract_identity='provider rolling quote; expiry not supplied',
                           timestamp_timezone='Asia/Shanghai',
                           url='https://qt.gtimg.cn/q=hf_'+code))
    return result


def fetch_quotes():
    response = requests.get('https://qt.gtimg.cn/q='+','.join('hf_'+s for s in PRODUCTS),
                            headers={'User-Agent':'Mozilla/5.0'},timeout=12)
    response.raise_for_status()
    response.encoding = 'gbk'
    checked = datetime.now(SH)
    rows = parse_quotes(response.text,checked)
    if {r['id'] for r in rows} != set(PRODUCTS):
        raise ValueError('foreign futures quote incomplete or malformed; preserving previous cache')
    return rows

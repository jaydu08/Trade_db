"""Best-effort commodity refresh; stale quotes are exported without daily returns."""
def collect(root,now):
    if not (root / 'data/ledger.db').exists():
        return None  # No live source root in isolated publisher unit fixtures.
    from modules.monitor.macro_dashboard import refresh_futures_snapshot
    return refresh_futures_snapshot(root / 'data/macro_dashboard_snapshot.json')

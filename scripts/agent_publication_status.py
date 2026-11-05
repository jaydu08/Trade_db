"""Private, atomic publication receipts and bounded operational alerts."""
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from generate_agent_snapshot import atomically_write


def classify_failure(exc, stage):
    diagnostic = str(getattr(exc, 'stderr', '') or str(exc))
    if re.search(r'permission denied|publickey|authentication failed|write access|403|repository not found', diagnostic, re.I):
        return 'permission_error'
    if stage in ('sync', 'upload'):
        return 'upload_failed'
    if stage == 'verify':
        return 'verification_failed'
    if re.search(r'not ready|unavailable|insufficient|coverage|source|completed session', diagnostic, re.I):
        return 'data_source_unavailable'
    return 'generation_failed'


def safe_error(exc):
    # Never serialize command arguments, remotes or provider responses into receipts.
    raw = getattr(exc, 'stderr', '') or str(exc)
    message = re.sub(r'(?:https?|ssh)://\S+|\b(?:sk-|ghp_|github_pat_)\S+', '[redacted]', raw)
    message = re.sub(r'(?i)(authorization|token|password)\s*[:=]\s*\S+', r'\1=[redacted]', message)
    return {'error_type': type(exc).__name__, 'exit_code': getattr(exc, 'returncode', None),
            'diagnostic': message[:500]}


def receipt(root, mode, started, status, stage, **extra):
    now = datetime.now(ZoneInfo('Asia/Shanghai'))
    result = dict(schema_version='1.0', archive_date=started.date().isoformat(),
                  branch='agent-data', mode=mode, started_at=started.isoformat(),
                  finished_at=now.isoformat(), status=status, stage=stage, **extra)
    result['invocation_id'] = os.getenv('INVOCATION_ID')
    directory = Path(root) / 'data/agent_publication_status'
    data = (json.dumps(result, ensure_ascii=False, allow_nan=False) + '\n').encode()
    attempt = started.strftime('%Y-%m-%d/%H%M%S-%f') + '-' + mode + '.json'
    atomically_write(directory / attempt, data)
    atomically_write(directory / (mode + '-latest.json'), data)
    print(json.dumps(result, ensure_ascii=False), file=sys.stderr, flush=True)
    return result


def alert(root, result):
    """Operational failure only, separate from all market-signal templates."""
    try:
        from dotenv import dotenv_values
        import requests
        config = dotenv_values(Path(root) / '.env')
        token = config.get('TELEGRAM_BOT_TOKEN')
        chat = config.get('TELEGRAM_ADMIN_ID') or (config.get('ALLOWED_USER_IDS') or '').split(',')[0]
        if not token or not chat:
            return False
        text = ('⚠️ Agent快照发布失败（运维告警，非行情信号）\n'
                f"日期: {result['archive_date']}\n阶段: {result['stage']}\n"
                f"分类: {result['category']}\n后续独立定时任务将继续重试。")
        response = requests.post('https://api.telegram.org/bot' + token + '/sendMessage',
                                 json={'chat_id': chat, 'text': text}, timeout=10)
        return response.status_code == 200 and bool(response.json().get('ok'))
    except Exception:
        return False


def systemd_finalize(root):
    """Catch startup errors, SIGKILL and timeouts that bypass Python's exception path."""
    service_result = os.getenv('SERVICE_RESULT', 'success')
    if service_result == 'success':
        return None
    path = Path(root) / 'data/agent_publication_status/daily-latest.json'
    try:
        previous = json.loads(path.read_text())
    except (OSError, ValueError):
        previous = {}
    if previous.get('invocation_id') == os.getenv('INVOCATION_ID') and previous.get('status') == 'failed':
        return previous  # Main already recorded and alerted this invocation.
    now = datetime.now(ZoneInfo('Asia/Shanghai'))
    result = receipt(root, 'daily', now, 'failed', 'service_exit', category='generation_failed',
                     service_result=service_result, exit_code=os.getenv('EXIT_STATUS'),
                     diagnostic='service terminated before successful verified completion')
    alert(root, result)
    return result


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--systemd-finalize', action='store_true')
    parser.add_argument('--root', type=Path, required=True)
    args = parser.parse_args()
    if args.systemd_finalize:
        systemd_finalize(args.root)

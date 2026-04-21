#!/usr/bin/env python3
"""Auto-retry provisioning NanoDemo account after rate limit clears."""
import urllib.request, json, time, subprocess, sys
from datetime import datetime, timezone

TOKEN = open('/Users/karunaditya/shiva-dday/.env').read()
TOKEN = next(l.split('=',1)[1].strip() for l in TOKEN.splitlines() if l.startswith('METAAPI_TOKEN='))

LOGIN = '13327381'
PASSWORD = '&$8YJze6*Hf'

SERVERS_TO_TRY = [
    ('AxioryAsia-02Demo', 'mt4'),
    ('AxioryAsia-02Demo', 'mt5'),
    ('AxioryAsia-01Demo', 'mt4'),
    ('AxioryAsia-01Demo', 'mt5'),
    ('AxioryUS-01Demo',   'mt4'),
    ('AxioryEU-01Demo',   'mt4'),
    ('Axiory-Demo',       'mt4'),
    ('Axiory-Demo',       'mt5'),
]

def try_provision(server, platform):
    payload = {
        'login': LOGIN,
        'password': PASSWORD,
        'name': f'NanoDemo-{LOGIN}',
        'server': server,
        'type': 'cloud-g2',
        'platform': platform,
        'magic': 123456
    }
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        'https://mt-provisioning-api-v1.agiliumtrade.agiliumtrade.ai/users/current/accounts',
        data=data,
        headers={'auth-token': TOKEN, 'Content-Type': 'application/json'},
        method='POST'
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        return json.loads(e.read())

def update_env_and_restart(account_id):
    with open('/Users/karunaditya/shiva-dday/.env', 'r') as f:
        content = f.read()
    lines = []
    for line in content.splitlines():
        if line.startswith('METAAPI_ACCOUNT_ID='):
            lines.append(f'METAAPI_ACCOUNT_ID={account_id}')
        else:
            lines.append(line)
    with open('/Users/karunaditya/shiva-dday/.env', 'w') as f:
        f.write('\n'.join(lines) + '\n')
    print(f'✅ .env updated: METAAPI_ACCOUNT_ID={account_id}')
    subprocess.run(['pm2', 'restart', 'shiva-bot'], check=True)
    print('✅ Bot restarted!')

print(f'🕐 Waiting until 06:45 UTC for rate limit to clear...')
print(f'   Current time: {datetime.now(timezone.utc).strftime("%H:%M:%S UTC")}')

# Wait until 06:45 UTC
target = datetime.now(timezone.utc).replace(hour=6, minute=45, second=0, microsecond=0)
now = datetime.now(timezone.utc)
if now < target:
    wait_secs = (target - now).total_seconds()
    print(f'   Sleeping {int(wait_secs//60)}m {int(wait_secs%60)}s...')
    time.sleep(wait_secs)

print(f'\n🚀 Rate limit should be clear. Trying provisioning...')

for server, platform in SERVERS_TO_TRY:
    print(f'   Trying {server} ({platform})...', end=' ', flush=True)
    result = try_provision(server, platform)

    # Check for rate limit
    if result.get('error') == 'TooManyRequestsError':
        retry_time = result.get('metadata', {}).get('recommendedRetryTime', '')
        print(f'⏳ Rate limited until {retry_time}')
        print('   Sleeping 70 minutes and retrying...')
        time.sleep(70 * 60)
        result = try_provision(server, platform)

    if '_id' in result:
        account_id = result['_id']
        print(f'✅ SUCCESS! Account ID: {account_id}')
        update_env_and_restart(account_id)
        sys.exit(0)
    elif result.get('details') == 'E_AUTH':
        print('❌ Wrong credentials for this server')
    else:
        print(f'❌ {result.get("error","?")}')

    time.sleep(5)

print('\n❌ All servers failed. Check credentials manually.')
print('Run: ! pm2 logs shiva-bot --lines 20')

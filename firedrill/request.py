"""Call the actual owned ASGI webhook with local-only action configuration."""
import argparse
import asyncio
import json
from pathlib import Path
from uuid import uuid4
import httpx
from support_escalation.managed_api import create_managed_app

async def execute(args):
    token = str(uuid4())
    app = create_managed_app(database=args.db, event_token=token, model=args.model,
                             allow_writes=args.allow_writes, output_root=args.output,
                             action_mode='local', allow_pipedream_actions=False)
    report = {'actionMode': 'local', 'event': json.loads(args.event)}
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app, raise_app_exceptions=False), base_url='http://application.local') as client:
            before = await client.get('/health')
            report['healthBefore'] = {'status': before.status_code, 'body': before.json()}
            response = await client.post('/events', headers={'x-event-token': token}, json=report['event'])
            try: body = response.json()
            except ValueError: body = {'text': response.text}
            report['http'] = {'status': response.status_code, 'body': body}
            after = await client.get('/health')
            report['healthAfter'] = {'status': after.status_code, 'body': after.json()}
    owner = app.state.support_admission
    report['admissionClosing'] = owner.closing
    report['activeAfterClose'] = len(owner.active)
    print(json.dumps(report, allow_nan=False))
    return 0 if report['http']['status'] == 200 and owner.closing and not owner.active else 1

parser = argparse.ArgumentParser(allow_abbrev=False)
parser.add_argument('--db', required=True)
parser.add_argument('--event', required=True)
parser.add_argument('--output', required=True)
parser.add_argument('--model')
parser.add_argument('--allow-writes', action='store_true')
raise SystemExit(asyncio.run(execute(parser.parse_args())))

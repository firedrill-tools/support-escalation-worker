"""Real private case API, complete inspection file and bounded actual summary."""
import hashlib
import json
import os
from pathlib import Path
import sys
from support_escalation.cases import initialize, inspect_case

action, workspace, *selection = sys.argv[1:]
workspace = Path(workspace)
result = initialize(workspace, selection[0]) if action == 'init' else inspect_case(workspace)
raw = json.dumps(result, ensure_ascii=True, allow_nan=False).encode()
if len(raw) > 64 * 1024 * 1024: raise ValueError('Private inspection exceeds its 64 MiB retained-file bound')
path = workspace.parent / ('initial-inspection.json' if action == 'init' else 'inspection.json')
with path.open('xb') as file:
    os.chmod(path, 0o600)
    file.write(raw)
    file.flush()
    os.fsync(file.fileno())
keys = ['root', 'database', 'instance_id', 'case_id', 'profile', 'title', 'generation', 'reset_of', 'seed_sha256', 'source_sha256', 'metrics', 'authority']
summary = {key: result[key] for key in keys if key in result}
print(json.dumps({**summary, 'fullInspection': {'path': str(path), 'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest()}}))

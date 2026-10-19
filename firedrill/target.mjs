import { fileURLToPath } from 'node:url';
import { resolve, join } from 'node:path';
import { ApplicationError, commandMain, invokeCLI, privateDirectory } from './adapter-runtime.mjs';
const root = fileURLToPath(new URL('../', import.meta.url));
const cases = new Set(["starter-due", "starter-recent", "starter-repeat", "busy-due", "busy-repeat", "busy-answered", "ambiguous-reply", "ambiguous-resolution", "ambiguous-action-reply", "boundary-two-hours", "boundary-calendar", "boundary-revision"]);
export async function runApplication(input, parameters, signal, invocation) {
  if (Object.keys(input).some(key => !['caseId', 'action', 'event', 'allowWrites'].includes(key)) || !cases.has(input.caseId) || !['event', 'inspect'].includes(input.action) || typeof input.allowWrites !== 'boolean' ||
      (input.action === 'event' ? !input.event || typeof input.event !== 'object' || Array.isArray(input.event) || Object.keys(input.event).sort().join(',') !== 'event_id,kind,occurred_at,ticket_id' || !Object.values(input.event).every(value => typeof value === 'string' && value.trim() && value.length <= 200 && !/[\x00-\x1f\x7f]/.test(value)) : input.event !== null || input.allowWrites)) throw new Error('Select a reviewed case and exact explicit event/write grant or model-free inspection.');
  const directory = await privateDirectory(root, invocation), workspace = join(directory, 'workspace');
  const cli = ['uv', 'run', '--locked', '--no-sync'];
  try {
    const initialized = await invokeCLI([...cli, 'python', 'firedrill/case-driver.py', 'init', workspace, input.caseId], { cwd: root, signal });
    const db = initialized.database;
    if (typeof db !== 'string' || !db.startsWith(workspace + '/')) throw new Error('Initialized database is outside its owned workspace.');
    const argv = input.action === 'inspect' ? [...cli, 'python', 'firedrill/case-driver.py', 'inspect', workspace] : [...cli, 'python', 'firedrill/request.py', '--db', db, '--event', JSON.stringify(input.event), '--output', join(directory, 'receipts')];
    if (input.action === 'event') {
      if (input.allowWrites) argv.push('--allow-writes');
      if (parameters.model) argv.push('--model', parameters.model);
    }
    return { executionKind: input.action === 'inspect' ? 'application_state_inspection' : 'actual_owned_webhook_request', application: await invokeCLI(argv, { cwd: root, signal }), localState: { directory, workspaceRoot: workspace, databaseFile: db } };
  } catch (error) { if (error instanceof ApplicationError) error.evidence.runDirectory = directory; throw error; }
}
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) await commandMain(runApplication);

import { AsyncLocalStorage } from 'node:async_hooks';

// JSON-lines logger. Context (trace_id, job_id, page, lane, attempt) comes from AsyncLocalStorage, so every
// line under a task, including in the fetch and DB layers, is correlated without passing context around.

export type LogContext = Record<string, string | number>;

const als = new AsyncLocalStorage<LogContext>();
// Runs fn with extra log context merged onto the current one.
export function withLogContext<T>(ctx: LogContext, fn: () => T): T {
  return als.run({ ...als.getStore(), ...ctx }, fn);
}

// Writes one JSON line: time, level, message, the current context, then the given fields.
function write(level: string, msg: string, fields?: Record<string, unknown>): void {
  if (process.env.LOG_LEVEL === 'silent') return;
  const line = JSON.stringify({ t: new Date().toISOString(), level, msg, ...als.getStore(), ...fields });
  if (level === 'error') process.stderr.write(line + '\n');
  else process.stdout.write(line + '\n');
}

export const log = {
  // Routine lifecycle events.
  info: (msg: string, fields?: Record<string, unknown>) => write('info', msg, fields),
  // Recoverable problems (retries, fallbacks).
  warn: (msg: string, fields?: Record<string, unknown>) => write('warn', msg, fields),
  // Bugs and unexpected failures.
  error: (msg: string, fields?: Record<string, unknown>) => write('error', msg, fields),
};

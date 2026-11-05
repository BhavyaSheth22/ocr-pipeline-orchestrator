import { buildApp } from './app.js';

const port = Number(process.env.PORT ?? 8080);

const { app } = buildApp();
const server = app.listen(port, '0.0.0.0', () => {
  console.log(JSON.stringify({ level: 'info', msg: 'mock-models listening', port }));
});

// On SIGTERM or SIGINT, stop accepting connections, drop idle ones, and exit once in-flight requests finish.
for (const sig of ['SIGTERM', 'SIGINT'] as const) {
  process.once(sig, () => {
    console.log(JSON.stringify({ level: 'info', msg: 'shutting down', sig }));
    server.close(() => process.exit(0));
    server.closeIdleConnections();
    // Slow VLM calls can run 3 s; force-close anything still open after 5 s.
    setTimeout(() => server.closeAllConnections(), 5000).unref();
  });
}

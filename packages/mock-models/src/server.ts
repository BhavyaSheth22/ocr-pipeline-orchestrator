import { buildApp } from './app.js';

const port = Number(process.env.PORT ?? 8080);
if (!process.env.ADMIN_TOKEN) {
  console.warn(JSON.stringify({ level: 'warn', msg: 'ADMIN_TOKEN not set; /admin/* endpoints are disabled' }));
}

const { app } = buildApp({ logger: true });

for (const sig of ['SIGTERM', 'SIGINT'] as const) {
  process.once(sig, () => {
    app.log.info({ sig }, 'shutting down');
    app.close().then(() => process.exit(0), () => process.exit(1));
  });
}

await app.listen({ port, host: '0.0.0.0' });

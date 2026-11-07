import { createServer, type RequestListener, type Server } from 'node:http';
import { createServer as createNetServer, type AddressInfo } from 'node:net';

// libuv accepts one connection per listening socket per event-loop iteration (uv__server_io, libuv 1.51).
// During a 50-upload burst one iteration runs 5-45 ms finishing a batch of uploads, so connections already
// established in the kernel waited one such iteration each: traced, the last uploads were accepted 150-450 ms
// late. Several SO_REUSEPORT sockets on one port give several accepts per iteration (the kernel spreads new
// connections across them by hash, so 32 keeps any one socket's queue short). Linux only; one socket elsewhere.
export const LISTENERS = process.platform === 'linux' ? 32 : 1;

// Binds `port` once without SO_REUSEPORT and releases it. A port held by another process (say a second
// orchestrator) still fails with EADDRINUSE instead of being silently shared; port 0 resolves to a free port.
export async function claimPort(port: number): Promise<number> {
  const probe = createNetServer();
  await new Promise<void>((resolve, reject) => {
    probe.once('error', reject);
    probe.listen({ port, host: '0.0.0.0' }, resolve);
  });
  const bound = (probe.address() as AddressInfo).port;
  await new Promise((r) => probe.close(r));
  return bound;
}

// Opens `n` HTTP servers for `handler` on one (already claimed) port, with SO_REUSEPORT when n > 1 or when
// other processes share the port (`shared`).
export async function listenAll(handler: RequestListener, port: number, n: number, shared = false): Promise<Server[]> {
  // Opens one server; a bind error rejects instead of crashing.
  const listenOn = () =>
    new Promise<Server>((resolve, reject) => {
      const srv = createServer(handler);
      srv.once('error', reject);
      srv.listen({ port, host: '0.0.0.0', reusePort: shared || n > 1 }, () => {
        srv.off('error', reject);
        resolve(srv);
      });
    });
  return Promise.all(Array.from({ length: n }, listenOn));
}

// Graceful close for a set of servers: stop accepting, drop idle keep-alive connections, and return a
// function that force-closes whatever is still open (SSE streams) and waits for every server to close.
export function closeAll(servers: Server[]): () => Promise<void> {
  const closed = Promise.all(servers.map((srv) => new Promise((r) => srv.close(r))));
  for (const srv of servers) srv.closeIdleConnections();
  return async () => {
    for (const srv of servers) srv.closeAllConnections();
    await closed;
  };
}

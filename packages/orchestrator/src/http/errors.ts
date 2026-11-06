/** An error that maps straight to an HTTP response. */
export class HttpError extends Error {
  // Carries the status, a machine-readable code, optional response headers and extra body fields.
  constructor(
    readonly status: number,
    readonly code: string,
    message?: string,
    readonly headers: Record<string, string> = {},
    readonly details: Record<string, unknown> = {},
  ) {
    super(message ?? code);
  }
}

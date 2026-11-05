// Hand-written request validation (Express has none built in), kept tiny and dependency-free.

export type Rule = { required?: boolean } & (
  | { type: 'string'; min?: number; max?: number }
  | { type: 'integer' | 'number'; min?: number; max?: number }
);

// Checks a JSON body against per-field rules; returns an error message, or null when it is valid.
export function validate(body: unknown, rules: Record<string, Rule>): string | null {
  if (typeof body !== 'object' || body === null || Array.isArray(body)) return 'body must be a JSON object';
  const obj = body as Record<string, unknown>;
  for (const key of Object.keys(obj)) {
    if (!(key in rules)) return `unknown property "${key}"`;
  }
  for (const [key, rule] of Object.entries(rules)) {
    const v = obj[key];
    if (v === undefined) {
      if (rule.required) return `missing required property "${key}"`;
      continue;
    }
    if (rule.type === 'string') {
      if (typeof v !== 'string') return `"${key}" must be a string`;
      if (rule.min !== undefined && v.length < rule.min) return `"${key}" must be at least ${rule.min} characters`;
      if (rule.max !== undefined && v.length > rule.max) return `"${key}" must be at most ${rule.max} characters`;
    } else {
      if (typeof v !== 'number' || !Number.isFinite(v)) return `"${key}" must be a number`;
      if (rule.type === 'integer' && !Number.isInteger(v)) return `"${key}" must be an integer`;
      if (rule.min !== undefined && v < rule.min) return `"${key}" must be >= ${rule.min}`;
      if (rule.max !== undefined && v > rule.max) return `"${key}" must be <= ${rule.max}`;
    }
  }
  return null;
}

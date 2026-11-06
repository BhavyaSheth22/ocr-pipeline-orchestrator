import type { FinalPage } from '../storage/db.js';
import type { TreeNode } from '../pipeline/pipeline.js';

// Request and response bodies of every HTTP endpoint, in one place. Server code and tests import from here.

/** Error body returned by every non-2xx JSON response. `details` fields (e.g. steps, max_steps) are spread in. */
export interface ErrorResponse {
  error: string;
  message?: string;
  [detail: string]: unknown;
}

// ---- POST /jobs ------------------------------------------------------------------------------------------

/** Request: the raw PDF as the body, optionally with an `Idempotency-Key` header (1-255 chars). */
export interface CreateJobHeaders {
  'idempotency-key'?: string;
}

/** 202 body (also for an Idempotency-Key replay). */
export interface JobAccepted {
  job_id: string;
  events_url: string;
  total_pages: number;
  backlog_pages: number;
  eta_s: number;
}

// ---- GET /jobs/:id/events (SSE) --------------------------------------------------------------------------

/** First event of every stream: `event: job`. */
export interface JobHeaderEventData {
  job_id: string;
  total_pages: number;
  window: number;
  backlog_pages: number;
  eta_s: number;
}

/** `event: page` with `stage: 'layout'`: a provisional result from the layout model. */
export interface PageLayoutEventData {
  page: number;
  stage: 'layout';
  watermark: number;
  result: TreeNode;
}

/** `event: page` with `stage: 'final'`: the page's final result. */
export interface PageFinalEventData extends FinalPage {
  page: number;
  stage: 'final';
  watermark: number;
  result: TreeNode;
}

/** Last event of a stream: `event: job.completed`. */
export interface JobCompletedEventData {
  job_id: string;
  total_pages: number;
  failed_pages: number;
  low_confidence_pages: number;
}

/** One SSE frame, discriminated by the `event:` name (and `stage` for pages). `id` is the per-job seq. */
export type JobEvent =
  | { id: number; event: 'job'; data: JobHeaderEventData }
  | { id: number; event: 'page'; data: PageLayoutEventData | PageFinalEventData }
  | { id: number; event: 'job.completed'; data: JobCompletedEventData };

// ---- GET /jobs/:id/result --------------------------------------------------------------------------------

/** One page of the result: the page tree plus how it was produced. */
export type ResultPage = TreeNode & Pick<FinalPage, 'status' | 'source' | 'low_confidence'> & { page: number };

/** 200 body, once every page is final (409 `not_ready` before that). */
export interface JobResultResponse {
  type: 'document';
  job_id: string;
  children: ResultPage[];
}

// ---- POST /evaluate --------------------------------------------------------------------------------------

/** Request body: two document trees; pages are matched by index. */
export interface EvaluateRequest {
  prediction: TreeNode;
  ground_truth: TreeNode;
}

export interface EvaluateResponse {
  /** Character error rate: edits / reference chars. */
  cer: { value: number; edits: number; ref_chars: number };
  /** Word error rate: edits / reference words. */
  wer: { value: number; edits: number; ref_words: number };
  iou: {
    mean_iou: number;
    precision: number;
    recall: number;
    f1: number;
    matched: number;
    pred: number;
    gt: number;
    threshold: number;
  };
  /** Zhang-Shasha tree edit distance, `normalized` by the sum of both tree sizes. */
  ted: { distance: number; normalized: number; nodes_pred: number; nodes_gt: number; steps: number };
  timing_ms: { parse: number; text: number; iou: number; ted: number; total: number };
}

// ---- GET /healthz ----------------------------------------------------------------------------------------

export interface HealthResponse {
  ok: true;
}

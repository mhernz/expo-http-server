import { EventEmitter } from "expo-modules-core";

import ExpoHttpServerModule from "./ExpoHttpServerModule";

export type HttpMethod = "GET" | "POST" | "PUT" | "DELETE" | "OPTIONS";

/** PAUSED/RESUMED fire on both platforms around app-lifecycle transitions. */
export type Status = "STARTED" | "PAUSED" | "RESUMED" | "STOPPED" | "ERROR";

export interface StatusEvent {
  status: Status;
  message: string;
}

export interface RequestEvent {
  /** Per-request uuid. Use this to respond to a specific connection. */
  uuid: string;
  method: string;
  path: string;
  body: string;
  headersJson: string;
  paramsJson: string;
  cookiesJson: string;
  /**
   * Aborts when the client TCP connection closes before the response is
   * written (e.g. MapLibre cancels a tile that scrolled out of viewport).
   * Long-running handlers should check `signal.aborted` between expensive
   * steps and pass `signal` into `fetch()` to free in-flight network work.
   */
  signal: AbortSignal;
}

interface NativeRequestEvent {
  uuid: string;
  method: string;
  path: string;
  body: string;
  headersJson: string;
  paramsJson: string;
  cookiesJson: string;
}

interface RequestCancelEvent {
  uuid: string;
}

export interface Response {
  statusCode?: number;
  statusDescription?: string;
  contentType?: string;
  headers?: Record<string, string>;
  /**
   * Response body. Pass a `string` for text payloads (JSON, HTML, plain text);
   * pass a `Uint8Array` for binary payloads (images, PNG/WebP, gzipped data,
   * protobuf, etc.). Binary bodies are handed to native via the typed-array
   * fast path and written with the correct byte-count `Content-Length`.
   */
  body?: string | Uint8Array;
}

export type RouteHandler = (request: RequestEvent) => Promise<Response>;

export interface EnsureStartedOptions {
  port: number;
  onStatus?: (event: StatusEvent) => void;
}

// ---------------------------------------------------------------------------
// Module-import-time state. Lives for the JS context's lifetime.
//
// Layering: the route table and the emitter subscriptions are JS-context-
// scoped — fresh on every JS reload, exactly when we want them fresh. The
// native side keeps process-scoped state (server, catch-all route, lifecycle
// observers) wired in its OnCreate, so it survives JS reloads without
// stacking.
// ---------------------------------------------------------------------------

const emitter = new EventEmitter(ExpoHttpServerModule);

const routes = new Map<string, RouteHandler>();
const inFlightControllers = new Map<string, AbortController>();

let statusSubscription: { remove: () => void } | null = null;

const routeKey = (method: string, path: string) => `${method.toUpperCase()} ${path}`;

emitter.addListener<NativeRequestEvent>("onRequest", async (event) => {
  const handler = routes.get(routeKey(event.method, event.path));
  if (!handler) {
    ExpoHttpServerModule.respond(
      event.uuid,
      404,
      "Not Found",
      "application/json",
      {},
      JSON.stringify({ error: "Handler not found" }),
    );
    return;
  }

  const controller = new AbortController();
  inFlightControllers.set(event.uuid, controller);
  const enriched: RequestEvent = { ...event, signal: controller.signal };

  try {
    const response = await handler(enriched);
    const statusCode = response.statusCode ?? 200;
    const statusDescription = response.statusDescription ?? "OK";
    const contentType = response.contentType ?? "application/json";
    const headers = response.headers ?? {};
    const body = response.body;
    if (body instanceof Uint8Array) {
      ExpoHttpServerModule.respondBinary(
        event.uuid,
        statusCode,
        statusDescription,
        contentType,
        headers,
        body,
      );
    } else {
      ExpoHttpServerModule.respond(
        event.uuid,
        statusCode,
        statusDescription,
        contentType,
        headers,
        body ?? "{}",
      );
    }
  } catch (err) {
    // Native is still waiting for a respond/respondBinary against event.uuid —
    // without one, the busy-wait loop pins a Netty worker (Android) or the
    // CRResponse stays in the responses map forever (iOS). Reply with a 500
    // so the slot drains and the client sees a real error.
    console.warn(
      `[expo-http-server] handler threw for ${event.method} ${event.path}:`,
      err,
    );
    try {
      ExpoHttpServerModule.respond(
        event.uuid,
        500,
        "Internal Server Error",
        "application/json",
        {},
        JSON.stringify({ error: "Handler threw" }),
      );
    } catch {
      // Native module unloading or already-responded — nothing more to do.
    }
  } finally {
    inFlightControllers.delete(event.uuid);
  }
});

emitter.addListener<RequestCancelEvent>("onRequestCancel", (event) => {
  const controller = inFlightControllers.get(event.uuid);
  if (controller) {
    controller.abort(new Error("Client disconnected"));
  }
});

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Register (or replace) a JS handler for a path+method. Pure JS — no native
 * call, idempotent across registrations of the same key, and safe to call
 * before or after the server is listening.
 */
export const route = (path: string, method: HttpMethod, callback: RouteHandler): void => {
  routes.set(routeKey(method, path), callback);
};

/**
 * Ensure the native server is listening on `opts.port`. Idempotent:
 *   - already listening on this port → resolves immediately
 *   - listening on a different port → rebinds, resolves on STARTED
 *   - not listening → binds, resolves on STARTED, rejects on ERROR
 *
 * `onStatus` replaces any prior status subscription (the native side fires
 * STARTED/PAUSED/RESUMED/STOPPED/ERROR for lifecycle transitions).
 */
export const ensureStarted = (opts: EnsureStartedOptions): Promise<void> => {
  if (statusSubscription) {
    statusSubscription.remove();
    statusSubscription = null;
  }
  if (opts.onStatus) {
    statusSubscription = emitter.addListener<StatusEvent>("onStatusUpdate", opts.onStatus);
  }
  return ExpoHttpServerModule.ensureListening(opts.port);
};

/**
 * Subscribe to status transitions for the process's lifetime. Unlike
 * `ensureStarted`'s `onStatus`, this listener is never replaced by a later
 * `ensureStarted` — use it to watch for a bind the native side performs on its
 * own (foreground resume) or loses without JS asking.
 */
export const addStatusListener = (
  listener: (event: StatusEvent) => void,
): { remove: () => void } => emitter.addListener<StatusEvent>("onStatusUpdate", listener);

/**
 * Stop the native server. Safe to call when not started. Does not clear the
 * JS-side route table — a subsequent `ensureStarted` will reuse it.
 */
export const stop = (): Promise<void> => ExpoHttpServerModule.stop();

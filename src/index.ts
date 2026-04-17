import { EventEmitter } from "expo-modules-core";

import ExpoHttpServerModule from "./ExpoHttpServerModule";

const emitter = new EventEmitter(ExpoHttpServerModule);
const requestCallbacks: Callback[] = [];

export type HttpMethod = "GET" | "POST" | "PUT" | "DELETE" | "OPTIONS";
/**
 * PAUSED AND RESUMED are iOS only
 */
export type Status = "STARTED" | "PAUSED" | "RESUMED" | "STOPPED" | "ERROR";

export interface StatusEvent {
  status: Status;
  message: string;
}

export interface RequestEvent {
  /** Per-request uuid. Use this to respond to a specific connection. */
  uuid: string;
  /** Route-level uuid, shared by every request to the same path+method pair. */
  routeUuid: string;
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

/** Payload for the native `onRequestCancel` event — matches `RequestEvent.uuid`. */
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

export interface Callback {
  method: string;
  path: string;
  uuid: string;
  callback: (request: RequestEvent) => Promise<Response>;
}

// Per-request AbortControllers, keyed by the native request uuid. Populated
// when the route callback is dispatched and cleared once the response is
// written. The native side fires `onRequestCancel` when the client closes
// the TCP connection before we respond.
const inFlightControllers = new Map<string, AbortController>();

export const start = () => {
  emitter.addListener<RequestCancelEvent>("onRequestCancel", (event) => {
    const controller = inFlightControllers.get(event.uuid);
    if (controller) {
      controller.abort(new Error("Client disconnected"));
    }
  });

  emitter.addListener<RequestEvent>("onRequest", async (event) => {
    // Look up the handler by routeUuid (stable across requests); reply with
    // event.uuid (unique per request) so concurrent connections don't collide.
    const responseHandler = requestCallbacks.find((c) => c.uuid === event.routeUuid);
    if (!responseHandler) {
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
    const enrichedEvent: RequestEvent = { ...event, signal: controller.signal };

    try {
      const response = await responseHandler.callback(enrichedEvent);
      const statusCode = response.statusCode || 200;
      const statusDescription = response.statusDescription || "OK";
      const contentType = response.contentType || "application/json";
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
    } finally {
      inFlightControllers.delete(event.uuid);
    }
  });
  ExpoHttpServerModule.start();
};

export const route = (
  path: string,
  method: HttpMethod,
  callback: (request: RequestEvent) => Promise<Response>,
) => {
  const uuid = Math.random().toString(16).slice(2);
  requestCallbacks.push({
    method,
    path,
    uuid,
    callback,
  });
  ExpoHttpServerModule.route(path, method, uuid);
};

export const setup = (
  port: number,
  onStatusUpdate?: (event: StatusEvent) => void,
) => {
  if (onStatusUpdate) {
    emitter.addListener<StatusEvent>("onStatusUpdate", async (event) => {
      onStatusUpdate(event);
    });
  }
  ExpoHttpServerModule.setup(port);
};

export const stop = () => ExpoHttpServerModule.stop();

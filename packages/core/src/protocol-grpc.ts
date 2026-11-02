/**
 * gRPC (binary protobuf) protocol for proto-sourced SDKs.
 *
 * Proto SDKs are generated as REST-shaped operations: each unary RPC is
 * `POST /<package>.<Service>/<Method>` with a proto3 JSON body. Servers that
 * accept JSON (Connect, grpc-gateway) use {@link makeRestProtocol} directly.
 * Servers that only speak gRPC use this protocol, which wraps the REST one:
 *
 * - **encode** builds the REST request, then transcodes its JSON body to a
 *   protobuf message ({@link encodeMessage}) in one gRPC frame, sent as
 *   `application/grpc`. Plain HTTP/1.1 `fetch` works against gRPC servers
 *   fronted by a bridging proxy (envoy), so no HTTP/2 client is needed.
 * - **decode** reads the frames, transcodes the protobuf reply back to proto3
 *   JSON, and hands a JSON response to the REST decoder, so schemas,
 *   sensitive values and response validation behave exactly as for REST.
 *   A non-zero `grpc-status` becomes a JSON error with the HTTP status
 *   grpc-gateway maps it to (`NOT_FOUND` -> 404, `ALREADY_EXISTS` -> 409,
 *   `UNAUTHENTICATED` -> 401, ...), whose body is
 *   `{ code: 5, status: "NOT_FOUND", message }`. Typed-error patches match
 *   the numeric gRPC `code` (which tells `FAILED_PRECONDITION` from
 *   `INVALID_ARGUMENT`, both HTTP 400), the HTTP `status`, or the `message`.
 *
 * Server-streaming methods (flagged in the descriptors) are collected: the
 * response's frames decode to `{ items: [message, ...] }`. Calls must bound
 * the stream (the server-side timeouts these RPCs take).
 *
 * gRPC status in HTTP/2 trailers that a proxy drops for HTTP/1.1 clients is
 * not observable through `fetch`; a reply with message frames is treated as
 * OK, and a reply with neither frames nor `grpc-status` fails as unknown.
 */
import * as Effect from "effect/Effect";
import type * as HttpBody from "effect/http/HttpBody";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import * as Layer from "effect/Layer";
import * as API from "./api.ts";
import {
  decodeMessage,
  encodeMessage,
  frame,
  type ProtobufEncodeError,
  unframe,
  type ProtoDescriptors,
} from "./protobuf.ts";
import { InvalidInput } from "./protocol-http.ts";
import { makeRestProtocol, type RestProtocolOptions } from "./protocol-rest.ts";

/** gRPC status code -> name. */
export const GRPC_STATUS_NAMES: Readonly<Record<number, string>> = {
  0: "OK",
  1: "CANCELLED",
  2: "UNKNOWN",
  3: "INVALID_ARGUMENT",
  4: "DEADLINE_EXCEEDED",
  5: "NOT_FOUND",
  6: "ALREADY_EXISTS",
  7: "PERMISSION_DENIED",
  8: "RESOURCE_EXHAUSTED",
  9: "FAILED_PRECONDITION",
  10: "ABORTED",
  11: "OUT_OF_RANGE",
  12: "UNIMPLEMENTED",
  13: "INTERNAL",
  14: "UNAVAILABLE",
  15: "DATA_LOSS",
  16: "UNAUTHENTICATED",
};

/** gRPC status code -> HTTP status (grpc-gateway's mapping). */
export const GRPC_HTTP_STATUS: Readonly<Record<number, number>> = {
  0: 200,
  1: 499,
  2: 500,
  3: 400,
  4: 504,
  5: 404,
  6: 409,
  7: 403,
  8: 429,
  9: 400,
  10: 409,
  11: 400,
  12: 501,
  13: 500,
  14: 503,
  15: 500,
  16: 401,
};

export interface GrpcProtocolOptions<C> extends RestProtocolOptions<C> {
  /** The SDK's generated descriptors (`src/descriptors.ts`). */
  readonly descriptors: ProtoDescriptors;
}

const textDecoder = /*@__PURE__*/ new TextDecoder();

const isEncodeError = (cause: unknown): cause is ProtobufEncodeError =>
  typeof cause === "object" &&
  cause !== null &&
  "_tag" in cause &&
  cause._tag === "ProtobufEncodeError";

const methodPath = (url: string): string => {
  try {
    return new URL(url).pathname;
  } catch {
    return url;
  }
};

/** The JSON a REST request carries, or `undefined` for an empty body. */
const jsonBody = (body: HttpBody.HttpBody): unknown => {
  switch (body._tag) {
    case "Empty":
      return undefined;
    case "Uint8Array": {
      const text = textDecoder.decode((body as HttpBody.Uint8Array).body);
      return text ? JSON.parse(text) : undefined;
    }
    case "Raw": {
      const raw = (body as HttpBody.Raw).body;
      return typeof raw === "string" && raw ? JSON.parse(raw) : raw;
    }
    default:
      throw new Error(`gRPC requests need a JSON body, got ${body._tag}`);
  }
};

const jsonResponse = (
  request: HttpClientRequest.HttpClientRequest,
  status: number,
  headers: Record<string, string>,
  value: unknown,
): HttpClientResponse.HttpClientResponse =>
  HttpClientResponse.fromWeb(
    request,
    new Response(JSON.stringify(value), {
      status,
      headers: { ...headers, "content-type": "application/json" },
    }),
  );

const plainHeaders = (headers: Readonly<Record<string, string | undefined>>) => {
  const out: Record<string, string> = {};
  for (const key in headers) {
    const v = headers[key];
    if (v !== undefined && key !== "content-length" && key !== "content-type") out[key] = v;
  }
  return out;
};

/**
 * A gRPC protocol over the given REST protocol options. See the module
 * documentation for the wire behavior.
 */
export const makeGrpcProtocol = <C>(options: GrpcProtocolOptions<C>): Layer.Layer<API.Protocol> =>
  Layer.effect(
    API.Protocol,
    Effect.map(Effect.service(API.Protocol), (rest) => {
      const d = options.descriptors;
      return API.Protocol.of({
        encode: (args) =>
          Effect.flatMap(rest.encode(args), (request) => {
            const method = d.methods[methodPath(request.url)];
            if (!method) return Effect.die(new Error(`no gRPC descriptor for ${request.url}`));
            let payload: Uint8Array;
            try {
              payload = encodeMessage(d, method[0], jsonBody(request.body) ?? {});
            } catch (cause) {
              if (isEncodeError(cause)) {
                // The encode channel is typed `never`; InvalidInput is part of
                // every operation's declared error channel.
                return Effect.fail(
                  new InvalidInput({ member: method[0], message: cause.message }),
                ) as unknown as Effect.Effect<HttpClientRequest.HttpClientRequest>;
              }
              return Effect.die(cause);
            }
            return Effect.succeed(
              request.pipe(
                HttpClientRequest.bodyUint8Array(frame(payload), "application/grpc"),
                HttpClientRequest.setHeader("te", "trailers"),
              ),
            );
          }),
        decode: (args) => {
          const { response } = args;
          const headers = response.headers as Readonly<Record<string, string | undefined>>;
          const contentType = headers["content-type"] ?? "";
          // A proxy's own failure (415, 503 text/plain) is not gRPC: let the
          // REST decoder classify it by HTTP status.
          if (!contentType.startsWith("application/grpc")) return rest.decode(args);
          return Effect.flatMap(response.arrayBuffer, (buffer) => {
            const request = response.request;
            const statusHeader = headers["grpc-status"];
            const status = statusHeader === undefined ? undefined : Number(statusHeader);
            const base = plainHeaders(headers);
            if (status !== undefined && status !== 0) {
              const raw = headers["grpc-message"] ?? "";
              let message = raw;
              try {
                message = decodeURIComponent(raw);
              } catch {}
              return rest.decode({
                ...args,
                response: jsonResponse(request, GRPC_HTTP_STATUS[status] ?? 500, base, {
                  code: status,
                  message: message || GRPC_STATUS_NAMES[status] || `gRPC status ${status}`,
                  status: GRPC_STATUS_NAMES[status] ?? String(status),
                }),
              });
            }
            const method = d.methods[methodPath(request.url)];
            if (!method) return Effect.die(new Error(`no gRPC descriptor for ${request.url}`));
            let frames: Uint8Array[];
            try {
              frames = unframe(new Uint8Array(buffer));
            } catch (cause) {
              return Effect.fail(options.parseError({ body: buffer, cause }));
            }
            if (frames.length === 0 && status === undefined) {
              return rest.decode({
                ...args,
                response: jsonResponse(request, 500, base, {
                  code: 2,
                  status: "UNKNOWN",
                  message: "gRPC response carried no message and no grpc-status",
                }),
              });
            }
            const [, responseType, stream] = method;
            let value: unknown;
            try {
              value = stream
                ? { items: frames.map((f) => decodeMessage(d, responseType, f)) }
                : frames.length === 0
                  ? {}
                  : decodeMessage(d, responseType, frames[0]!);
            } catch (cause) {
              return Effect.fail(options.parseError({ body: buffer, cause }));
            }
            return rest.decode({ ...args, response: jsonResponse(request, 200, base, value) });
          });
        },
      });
    }),
  ).pipe(Layer.provide(makeRestProtocol(options)));

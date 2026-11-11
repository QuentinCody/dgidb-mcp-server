import type {GraphqlFetchFn} from "@bio-mcp/shared/codemode/graphql-introspection";
import {commercialDgidbFetch} from "./commercial-profile";

async function readEnvelope(response: Response): Promise<Record<string, unknown>> {
  if ((response.status >= 300 && response.status < 400) || !response.headers.get("content-type")?.includes("json") || !response.body) {
    void response.body?.cancel().catch(() => {}); // Cleanup must not delay a failed request.
    throw new Error(`DGIdb invalid JSON response (HTTP ${response.status})`);
  }
  const reader = response.body.getReader();
  const decoder = new TextDecoder("utf-8", {fatal: true});
  let text = "", size = 0;
  try {
    while (true) {
      const next = await reader.read();
      if (next.done) break;
      size += next.value.byteLength;
      if (size > 8 * 1024 * 1024) throw new Error("DGIdb response exceeds 8 MiB; narrow the query");
      text += decoder.decode(next.value, {stream: true});
    }
    text += decoder.decode();
  } finally { void reader.cancel().catch(() => {}); } // Cancellation is best effort.
  const value: unknown = JSON.parse(text);
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("Invalid DGIdb envelope");
  // SAFETY: narrowed to a JSON object; individual GraphQL fields are checked below.
  const envelope = value as Record<string, unknown>;
  if (!response.ok && !(Array.isArray(envelope.errors) && envelope.errors.length)) throw new Error(`DGIdb HTTP ${response.status}`);
  const data = envelope.data;
  if ((!data || typeof data !== "object" || Array.isArray(data)) && !(Array.isArray(envelope.errors) && envelope.errors.length)) throw new Error("DGIdb returned neither data nor errors");
  return envelope;
}

/**
 * The DGIdb fetch of the shared Code Mode tools and of dgidb_graphql_query.
 * With `licenseProfile: "commercial"` it applies the source-license rule of
 * ./commercial-profile; without it the request and the envelope do not change.
 */
export const fetchDgidb: GraphqlFetchFn = async (query, variables, request) =>
  request?.licenseProfile === "commercial" ? commercialDgidbFetch(query, variables, fetchUnrestricted) : fetchUnrestricted(query, variables);

async function fetchUnrestricted(query: string, variables?: Record<string, unknown>): Promise<Record<string, unknown>> {
  const controller = new AbortController();
  let timer: ReturnType<typeof setTimeout> | undefined;
  const deadline = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {controller.abort(); reject(new Error("DGIdb request exceeded 30 seconds"));}, 30000);
  });
  try {
    const receive = fetch("https://dgidb.org/api/graphql", {
      method: "POST", redirect: "manual", signal: controller.signal,
      headers: {"Content-Type": "application/json", Accept: "application/json", "User-Agent": "MCPDGIdbServer/0.1.0"},
      body: JSON.stringify({query, variables}),
    }).then(readEnvelope);
    return await Promise.race([receive, deadline]);
  } finally {clearTimeout(timer); controller.abort();}
}

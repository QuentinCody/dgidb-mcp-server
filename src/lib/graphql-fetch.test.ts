import {afterEach, expect, it, vi} from "vitest";
import {fetchDgidb} from "./graphql-fetch";
afterEach(() => {vi.unstubAllGlobals(); vi.useRealTimers();});
const json = (value: unknown) => new Response(JSON.stringify(value), {headers: {"content-type": "application/json"}});
it("preserves HTTP 400 GraphQL validation details and rejects JSON transport failures", async () => {
  const errors = {errors: [{message: "Unsupported field", extensions: {code: "FIELD_ERROR"}}]};
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify(errors), {status: 400, headers: {"content-type": "application/json"}})));
  expect(await fetchDgidb("{missing}")).toEqual(errors);
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response('{"message":"unavailable"}', {status: 503, headers: {"content-type": "application/json"}})));
  await expect(fetchDgidb("{} ")).rejects.toThrow("DGIdb HTTP 503");
});
it.each([{data: {interactions: {nodes: []}}}, {data: {interactions: null}, errors: [{message: "partial", path: ["interactions"]}]}, {errors: [{message: "rejected"}]}])("preserves native GraphQL success, partial and rejection bodies %#", async body => {
  const fetch = vi.fn().mockResolvedValue(json(body)); vi.stubGlobal("fetch", fetch);
  expect(await fetchDgidb("query Evidence { __typename }", {first: 1})).toEqual(body);
  expect(fetch).toHaveBeenCalledWith("https://dgidb.org/api/graphql", expect.objectContaining({method: "POST", redirect: "manual", signal: expect.any(AbortSignal), body: JSON.stringify({query: "query Evidence { __typename }", variables: {first: 1}})}));
});
it.each([null, [], {}, {data: []}, {errors: []}])("rejects invalid envelope %#", async body => {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(json(body)));
  await expect(fetchDgidb("{}" )).rejects.toThrow();
});
it.each([new Response("error", {status: 429}), new Response("<html>"), new Response("bad", {headers: {"content-type": "application/json"}}), new Response(null, {headers: {"content-type": "application/json"}})])("rejects HTTP and format failures %#", async response => {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(response));
  await expect(fetchDgidb("{}" )).rejects.toThrow();
});
it("enforces an 8 MiB byte cap", async () => {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(json({data: {text: "α".repeat(4 * 1024 * 1024)}})));
  await expect(fetchDgidb("{}" )).rejects.toThrow("8 MiB");
});
it.each(["fetch", "body"])("applies a deadline to a stalled %s", async stalled => {
  vi.useFakeTimers();
  const response = stalled === "fetch" ? new Promise<Response>(() => {}) : Promise.resolve(new Response(new ReadableStream({pull() {return new Promise(() => {});}}), {headers: {"content-type": "application/json"}}));
  const fetch = vi.fn().mockReturnValue(response); vi.stubGlobal("fetch", fetch);
  const result = expect(fetchDgidb("{}" )).rejects.toThrow("30 seconds");
  await vi.advanceTimersByTimeAsync(30000); await result;
  expect(fetch.mock.calls[0][1].signal.aborted).toBe(true);
});

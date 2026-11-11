import {expect, it} from "vitest";
import {McpServer} from "@bio-mcp/shared/mcp";
import {registerCodeMode} from "./code-mode";

it("exposes both Code Mode aliases with evidence guidance and bounded error envelopes", async () => {
  const server = new McpServer({name: "fixture", version: "1"});
  // SAFETY: the loader is constructed but no isolate is invoked for empty code.
  registerCodeMode(server, {CODE_MODE_LOADER: {get: () => ({})}} as unknown as Parameters<typeof registerCodeMode>[1]);
  // SAFETY: SDK registry access is test-only to inspect real registrations and handlers.
  const tools = (server as unknown as {_registeredTools: Record<string, {description: string; handler: (args: unknown, extra: unknown) => Promise<unknown>}>})._registeredTools;
  expect(tools.dgidb_search).toBeDefined(); expect(tools.mcp_dgidb_search).toBeDefined();
  for (const name of ["dgidb_execute", "mcp_dgidb_execute"]) {
    expect(tools[name].description).toContain("dgidb.evidence");
    expect(await tools[name].handler({code: ""}, {})).toMatchObject({content: expect.any(Array), structuredContent: expect.any(Object), isError: true});
  }
});

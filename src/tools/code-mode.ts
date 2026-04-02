/**
 * DGIdb Code Mode — registers the dgidb_execute tool for full GraphQL API access.
 *
 * The V8 isolate gets gql.query() for GraphQL execution and schema.* helpers
 * for introspection-based discovery. No hand-written catalog needed.
 */

import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { createGraphqlExecuteTool } from "@bio-mcp/shared/codemode/graphql-execute-tool";
import type { GraphqlFetchFn } from "@bio-mcp/shared/codemode/graphql-introspection";

const DGIDB_ENDPOINT = "https://dgidb.org/api/graphql";

// DGIdb-specific quirks and helpers injected into the V8 isolate
const DGIDB_PREAMBLE = `
// --- DGIdb quirks ---
// Gene.interactions is a plain list (NOT a Relay connection).
// Do NOT use pagination args (first/after) or .nodes on interactions.
//   Correct: { genes(names: ["EGFR"]) { nodes { interactions { interactionScore drug { name approved } } } } }
//   WRONG:   { genes(names: ["EGFR"]) { nodes { interactions(first: 10) { nodes { ... } } } } }
//
// Top-level queries (genes, drugs) use Relay connections with nodes/edges:
//   gql.query('{ genes(names: ["BRAF"]) { nodes { name longName interactions { drug { name } interactionScore } } } }')
//   gql.query('{ drugs(names: ["IMATINIB"]) { nodes { name approved interactions { gene { name } interactionScore } } } }')
//
// Nested fields like interactions and interactionTypes are plain arrays — do NOT use .nodes on them.
//
// Gene search by name: gql.query('{ genes(names: ["EGFR"]) { nodes { name longName geneCategories { name } } } }')
// Drug search by name: gql.query('{ drugs(names: ["IMATINIB"]) { nodes { name approved conceptId } } }')
`;

function createDgidbGqlFetch(): GraphqlFetchFn {
	return async (query: string, variables?: Record<string, unknown>) => {
		const response = await fetch(DGIDB_ENDPOINT, {
			method: "POST",
			headers: {
				"Content-Type": "application/json",
				"User-Agent": "MCPDGIdbServer/0.1.0",
			},
			body: JSON.stringify({ query, ...(variables && { variables }) }),
		});
		return await response.json();
	};
}

interface DgidbCodeModeEnv {
	JSON_TO_SQL_DO: DurableObjectNamespace;
	CODE_MODE_LOADER: { get: (...args: unknown[]) => unknown };
}

/**
 * Register dgidb_execute tool on the MCP server.
 */
export function registerCodeMode(
	server: McpServer,
	env: DgidbCodeModeEnv,
): void {
	const gqlFetch = createDgidbGqlFetch();

	const executeTool = createGraphqlExecuteTool({
		prefix: "dgidb",
		apiName: "DGIdb",
		gqlFetch,
		doNamespace: env.JSON_TO_SQL_DO,
		loader: env.CODE_MODE_LOADER,
		preamble: DGIDB_PREAMBLE,
	});

	executeTool.register(server as unknown as { tool: (...args: unknown[]) => void });
}

/**
 * DGIdb Code Mode — registers the dgidb_execute tool for full GraphQL API access.
 *
 * The V8 isolate gets gql.query() for GraphQL execution and schema.* helpers
 * for introspection-based discovery. No hand-written catalog needed.
 */

import type { McpServer } from "@bio-mcp/shared/mcp";
import { createGraphqlExecuteTool } from "@bio-mcp/shared/codemode/graphql-execute-tool";
import { fetchDgidb } from "../lib/graphql-fetch";
import { DGIDB_EVIDENCE_PREAMBLE } from "../lib/evidence-preamble";
import { DGIDB_SOURCE } from "../lib/tool-response";

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

interface DgidbCodeModeEnv {
	DGIDB_DATA_DO?: DurableObjectNamespace;
	CODE_MODE_LOADER: { get: (...args: unknown[]) => unknown };
	/** Cross-script binding to the shared workspace data plane (ADR-006). Optional so
	 * the worker's hand-written Env (which omits it) still satisfies this interface;
	 * present at runtime via wrangler.jsonc. */
	WORKSPACE_DO?: DurableObjectNamespace;
}

/**
 * Register dgidb_execute tool on the MCP server.
 */
export function registerCodeMode(
	server: McpServer,
	env: DgidbCodeModeEnv,
): void {
	const gqlFetch = fetchDgidb;

	const executeTool = createGraphqlExecuteTool({
		prefix: "dgidb",
		// Verifiable provenance: dgidb_execute results carry a _meta.citation.
		source: DGIDB_SOURCE,
		apiName: "DGIdb",
		gqlFetch,
		doNamespace: env.DGIDB_DATA_DO,
		loader: env.CODE_MODE_LOADER,
		preamble: DGIDB_PREAMBLE + DGIDB_EVIDENCE_PREAMBLE,
		stagingThreshold: 30 * 1024,
		workspaceNamespace: env.WORKSPACE_DO,
	});

	executeTool.register({tool: (...args: unknown[]) => {
        const [name, ...rest] = args;
        // SAFETY: shared factory supplies SDK arguments; only the alias changes.
        const register = server.tool.bind(server) as (...values: unknown[]) => void;
        register(name, ...rest); register(`mcp_${String(name)}`, ...rest);
    }});
}

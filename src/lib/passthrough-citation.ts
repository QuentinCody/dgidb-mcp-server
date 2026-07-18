// Verifiable provenance for the dgidb_graphql_query passthrough tool.
// Builds the `_meta.citation` envelope so passthrough queries appear in the chat
// Sources strip, matching what dgidb_execute attaches via Code Mode.
import { buildPassthroughCitation, type SourceDescriptor } from "@bio-mcp/shared";

// DGIdb source descriptor. Keep identical to the one in src/tools/code-mode.ts.
export const DGIDB_SOURCE: SourceDescriptor = {
	id: "dgidb",
	name: "DGIdb",
	url: "https://www.dgidb.org",
};

const TOOL_NAME = "dgidb_graphql_query";

/** What was asked — hashed into the citation's query_hash. */
export interface DgidbQuery {
	query: string;
	variables?: Record<string, unknown>;
}

// The MCP SDK types `structuredContent` as an open record, so both result
// shapes carry an index signature alongside their named fields.
type CitationMeta = Awaited<ReturnType<typeof buildPassthroughCitation>>;

/** structuredContent for a small, inline (non-staged) GraphQL result. */
export interface InlineStructuredContent {
	[key: string]: unknown;
	success: true;
	/** Omitted when the serialized result could exceed the ~100KB transport cap. */
	data?: unknown;
	/** GraphQL errors returned ALONGSIDE data — a partial result, never a clean one. */
	partial_errors?: string[];
	_meta: CitationMeta;
}

/** structuredContent for a staged GraphQL result. */
export interface StagedStructuredContent {
	[key: string]: unknown;
	success: true;
	data_access_id: string;
	processing_details: { table_count?: number; total_rows?: number };
	/** GraphQL errors returned ALONGSIDE data — a partial result, never a clean one. */
	partial_errors?: string[];
	_meta: CitationMeta;
}

/**
 * Build structuredContent (with `_meta.citation`) for an inline result. The full
 * `result` is always cited; bulky inline data is omitted past the ~100KB cap, but
 * the complete payload still travels in the tool's `content` text.
 *
 * `partialErrors` carries GraphQL errors that came back ALONGSIDE data. The data is
 * real, so the result stays `success: true` — but the errors are surfaced so a
 * partial result can never read as clean. Errors WITHOUT data never reach here:
 * the tool rejects them as a GRAPHQL_ERROR failure, uncited.
 */
export async function buildInlineStructuredContent(
	query: DgidbQuery,
	result: unknown,
	serializedLength: number,
	partialErrors?: string[],
): Promise<InlineStructuredContent> {
	const cite = await buildPassthroughCitation({
		source: DGIDB_SOURCE,
		server: DGIDB_SOURCE.id,
		tool: TOOL_NAME,
		query,
		result,
	});
	return {
		success: true,
		data: serializedLength > 100_000 ? undefined : result,
		...(partialErrors?.length ? { partial_errors: partialErrors } : {}),
		_meta: { ...cite },
	};
}

/**
 * Build structuredContent (with `_meta.citation`) for a staged result. The full
 * GraphQL `result` is cited; the staged dataset is referenced by dataAccessId +
 * recordCount (total_rows).
 */
export async function buildStagedStructuredContent(
	query: DgidbQuery,
	result: unknown,
	staging: { data_access_id: string; processing_details: { table_count?: number; total_rows?: number } },
	partialErrors?: string[],
): Promise<StagedStructuredContent> {
	const cite = await buildPassthroughCitation({
		source: DGIDB_SOURCE,
		server: DGIDB_SOURCE.id,
		tool: TOOL_NAME,
		query,
		result,
		dataAccessId: staging.data_access_id,
		recordCount: staging.processing_details.total_rows,
	});
	return {
		success: true,
		data_access_id: staging.data_access_id,
		processing_details: staging.processing_details,
		...(partialErrors?.length ? { partial_errors: partialErrors } : {}),
		_meta: { ...cite },
	};
}

/** structuredContent for an upstream GraphQL rejection. */
export interface GraphqlErrorStructuredContent {
	[key: string]: unknown;
	success: false;
	error: { code: "GRAPHQL_ERROR"; message: string };
}

/** An MCP tool error result for an upstream GraphQL rejection. */
export interface GraphqlErrorResult {
	[key: string]: unknown;
	content: Array<{ type: "text"; text: string }>;
	structuredContent: GraphqlErrorStructuredContent;
	isError: true;
}

/**
 * Build the failure response for a GraphQL rejection — HTTP 200 + `{errors:[…]}`
 * with NO data. Deliberately carries **no citation**: stamping provenance on an
 * error payload is the citation-forgery path. Satisfies the fleet contract
 * (content + structuredContent{success:false,error} + isError) so the tool can
 * actually fail instead of going green against a dead API.
 */
export function buildGraphqlErrorResult(messages: string[]): GraphqlErrorResult {
	const message = messages.join("; ");
	console.error(`DGIdb GraphQL query failed: ${message}`);
	return {
		content: [{ type: "text", text: `Error: DGIdb GraphQL query failed: ${message}` }],
		structuredContent: { success: false, error: { code: "GRAPHQL_ERROR", message } },
		isError: true,
	};
}

/**
 * Build the failure response for a thrown execution error (HTTP failure, DO staging
 * failure, bad SQL, …). Carries no citation — errors are never cited.
 *
 * The `structuredContent` here is load-bearing: without it this tool's error path
 * fails the fleet contract by construction (it returned `content` + `isError` only),
 * so the error could not be read as one by a structuredContent-based checker.
 */
export function buildErrorResult(message: string, error: unknown) {
	const errorDetails = error instanceof Error ? error.message : String(error);
	const timestamp = new Date().toISOString();

	return {
		content: [{
			type: "text" as const,
			text: JSON.stringify({
				success: false,
				error: message,
				details: errorDetails,
				timestamp,
				help: "Check the DGIdb API documentation for valid query formats and parameters"
			})
		}],
		structuredContent: {
			success: false,
			error: { code: "EXECUTION_FAILED", message: `${message}: ${errorDetails}` },
		},
		isError: true,
		_meta: {
			progress: 0.0,
			statusMessage: message,
			error_code: "EXECUTION_FAILED",
			error_category: error instanceof Error && error.name ? error.name : "UnknownError",
			timestamp,
			recovery_suggestions: [
				"Verify your GraphQL query syntax",
				"Check that all required variables are provided",
				"Ensure the DGIdb API is accessible"
			]
		}
	};
}

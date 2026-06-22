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
	_meta: CitationMeta;
}

/** structuredContent for a staged GraphQL result. */
export interface StagedStructuredContent {
	[key: string]: unknown;
	success: true;
	data_access_id: string;
	processing_details: { table_count?: number; total_rows?: number };
	_meta: CitationMeta;
}

/**
 * Build structuredContent (with `_meta.citation`) for an inline result. The full
 * `result` is always cited; bulky inline data is omitted past the ~100KB cap, but
 * the complete payload still travels in the tool's `content` text.
 */
export async function buildInlineStructuredContent(
	query: DgidbQuery,
	result: unknown,
	serializedLength: number,
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
		_meta: { ...cite },
	};
}

// interlinked-tdd: exempt -- re-export barrel, no testable surface
/** Monitoring primitive — public surface (engine + provenance ledger). */

export type {
	SavedQuery,
	TableSpec,
	MonitorProfile,
	ChangeKind,
	Materiality,
	FieldDelta,
	RowChange,
	TableSummary,
	SnapshotDiff,
	SourceModule,
} from "./types";
export {
	type RowSet,
	resolvePath,
	cleanResult,
	extractRowSets,
	rowKey,
	selectValueFields,
	reparse,
	canonicalValue,
	keyedValueMap,
	snapshotHash,
	KEY_SEP,
} from "./canonicalize";
export { diffTable, diffSnapshots } from "./diff";
export { defaultMateriality, classifyChanges } from "./materiality";
export { type KeyColumnStat, autoDetectKey } from "./key-detect";
export {
	GENESIS_HASH,
	type SqlRunner,
	type SnapshotInput,
	type SnapshotRow,
	type ChainVerifyResult,
	computeEntryHash,
	buildSnapshotRow,
	verifyChainRows,
	appendSnapshot,
	verifySnapshotChain,
} from "./snapshot-chain";
export { type QueryRunner, type RunOnceInput, type RunOnceResult, runOnce } from "./run-once";
export {
	type McpRpcStub,
	type McpRpcResponse,
	buildToolCall,
	parseToolResult,
	callTool,
} from "./internal-call";
export { SOURCES, fdaOrangeBook } from "./sources/index";

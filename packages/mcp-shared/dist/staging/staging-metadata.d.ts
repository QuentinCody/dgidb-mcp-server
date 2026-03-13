/**
 * Canonical staging metadata — a machine-readable signal in every staged response.
 *
 * Every tool that stages data (specific tools, Code Mode auto-staging, etc.)
 * includes `_staging: StagingMetadata` in structuredContent so clients and
 * models can reliably detect and use staged data without regex parsing.
 */
/** Describes a parent→child table relationship created by child table extraction */
export interface TableRelationship {
    child_table: string;
    parent_table: string;
    /** Column in child table referencing parent (always "parent_id") */
    fk_column: string;
    /** Column in parent that contained the source array */
    source_column: string;
}
export interface StagingMetadata {
    /** Always true — discriminant for detection */
    staged: true;
    /** Unique ID to pass to query_data / get_schema tools */
    data_access_id: string;
    /** Tables created in SQLite */
    tables: string[];
    /** Primary table (usually the first / most important) */
    primary_table?: string;
    /** Total rows inserted across all tables */
    total_rows?: number;
    /** Approximate payload size in bytes before staging */
    payload_size_bytes?: number;
    /** Tool name for querying staged data (e.g. "ctgov_query_data") */
    query_tool: string;
    /** Tool name for inspecting schema (e.g. "ctgov_get_schema") */
    schema_tool: string;
    /** Parent→child table relationships (from child table extraction) */
    relationships?: TableRelationship[];
}
/**
 * Build a StagingMetadata object from staging results.
 */
export declare function buildStagingMetadata(opts: {
    dataAccessId: string;
    tables: string[];
    primaryTable?: string;
    totalRows?: number;
    payloadSizeBytes?: number;
    toolPrefix: string;
    relationships?: TableRelationship[];
}): StagingMetadata;
//# sourceMappingURL=staging-metadata.d.ts.map
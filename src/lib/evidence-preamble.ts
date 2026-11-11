import {DGIDB_EVIDENCE_QUERY, DGIDB_METADATA_QUERY} from "./evidence-query";

export const DGIDB_EVIDENCE_PREAMBLE = `
// dgidb.evidence({genes:["BRAF"],first:10,after:null}) reads one native interaction page with source/publication evidence attached to each claim.
// Alternatively use drugs:["IMATINIB"]. Supplying both filters restricts both sides; names are not variant-specific identifiers.
// first is 1–25; genes/drugs each accept 1–10 names. after is the provider's opaque endCursor. Preserve pageInfo and check __errors before continuing.
// interactionScore is a research ranking, not efficacy, clinical actionability, causation or drug–drug interaction severity. evidenceScore and specificity components stay separate.
// Aggregate sources/publications are not a claim-level cross-product: use interactionClaims to retain the source–publication–type linkage, including conflicts and missing evidence.
// dgidb.metadata() returns native serviceInfo. Evidence includes the same fields alongside the interaction page. version is software; dataVersion is the provider's data label. Source versions describe individual imports, not an atomic DGIdb release. No-data/empty results do not prove lack of drug effect.
// Large results stage; return __staged handles or inspect them with dgidb_get_schema/query_data. Never treat a handle as zero interactions. Nested claims are unpaged and may require a narrower query.
var __dgidbEvidenceQuery = ${JSON.stringify(DGIDB_EVIDENCE_QUERY)};
var __dgidbMetadataQuery = ${JSON.stringify(DGIDB_METADATA_QUERY)};
function __dgidbNames(value) {
  if (value === undefined) return undefined;
  if (!Array.isArray(value) || !value.length || value.length > 10 || value.some(function(name) { return typeof name !== "string" || !name.trim() || name.length > 200; })) throw new Error("Provide 1–10 nonempty names of at most 200 characters");
  return value;
}
function __dgidbVariables(options) {
  if (!options || typeof options !== "object" || Array.isArray(options) || Object.keys(options).some(function(key) { return ["genes","drugs","first","after"].indexOf(key) < 0; })) throw new Error("Unsupported DGIdb evidence options");
  var genes = __dgidbNames(options.genes), drugs = __dgidbNames(options.drugs);
  if (!genes && !drugs) throw new Error("Provide genes or drugs to bound the interaction search");
  var first = options.first === undefined ? 10 : options.first;
  if (!Number.isInteger(first) || first < 1 || first > 25) throw new Error("first must be 1–25");
  var after = options.after === undefined ? null : options.after;
  if (after !== null && (typeof after !== "string" || !after.length || after.length > 4096)) throw new Error("after must be an opaque cursor or null");
  return {genes:genes,drugs:drugs,first:first,after:after};
}
var dgidb = { metadata: async function() { return await gql.query(__dgidbMetadataQuery); }, evidence: async function(options) {
  var variables = __dgidbVariables(options);
  var native = await gql.query(__dgidbEvidenceQuery, variables);
  if (native && (native.__staged || native.__gql_error || native.__stage_error)) return native;
  var result = {native:native,_dgidb:{variables:variables,observed_at:new Date().toISOString(),data_release:native && native.serviceInfo ? native.serviceInfo.dataVersion : null,software_version:native && native.serviceInfo ? native.serviceInfo.version : null,release_pinned:false,complete:false,interpretation:"Drug–gene research evidence, not a clinical drug–drug interaction or efficacy verdict. Keep claim-level evidence and ranking separate."}};
  if (new TextEncoder().encode(JSON.stringify(result)).byteLength > 30 * 1024) return Object.assign({__staged:true}, await db.stage(result));
  return result;
}};
`;

/**
 * The source-license rule of the DGIdb server for a reselling caller
 * (`licenseProfile: "commercial"`).
 *
 * License audit 2026-11-10: "Filter interactions by source license in the
 * DGIdb server. Per the DGIdb API on 2026-11-10, serve only CIViC, DoCM, Drug
 * Repurposing Hub, FDA, NCI (open) and ChEMBL, ClinPGx, GuideToPharmacology,
 * MOAlmanac (share-alike)."
 *
 * DGIdb imports each claim from one source, and each source has its own
 * license (`sources { sourceDbName license licenseLink }`, read on 2026-11-10:
 * 48 sources). The allowlist is the nine sources of the audit. Each other
 * source is not permitted: the non-commercial and unclear sources of the audit,
 * PRISM (an interaction source that the audit does not name), the gene and drug
 * sources (for example Drugs@FDA, HemOnc, RxNorm), and each new source.
 *
 * The rule:
 * - A query that selects a source by name in an argument (`sources`,
 *   `sourceName`, `sourceNames`, `sourceDbName`, `sourceDbNames`) that is not
 *   permitted is refused with no upstream request.
 * - The adapter adds `dgidbLicenseFilterTypename: __typename` to each selection
 *   set, so that the rule knows the type of each record also when the query
 *   uses aliases. The rule removes this field from the result.
 * - A source record is kept when its `sourceDbName` is permitted. A claim
 *   (`InteractionClaim`, `DrugClaim`, `GeneClaim`, `DrugApprovalRating`) is kept
 *   when its `source` is permitted. An interaction, attribute, alias or gene
 *   category is kept when a permitted source or claim remains in it. Records
 *   with no source of their own (publications, interaction types, claim
 *   attributes) are kept only inside a kept claim or source.
 * - A record whose source the query did not select is removed and counted as
 *   `unverified` (fail closed). `_license_filter.fields_to_add` names the
 *   fields that keep it.
 * - Scores, counts and flags are upstream values over all sources. The rule
 *   does not recompute them.
 */
import { commercialUseRefusal } from "@bio-mcp/shared/codemode/catalog";

type Envelope = Record<string, unknown>;
export type UnrestrictedFetch = (query: string, variables?: Record<string, unknown>) => Promise<Envelope>;

/** The live `license` value of each permitted source (https://dgidb.org/api/graphql, 2026-11-10). */
export const COMMERCIAL_SOURCE_LICENSES: Readonly<Record<string, string>> = {
	CIViC: "Creative Commons CC0 1.0 Universal (CC0 1.0) Public Domain Dedication",
	DoCM: "Creative Commons Attribution 4.0 International (CC BY 4.0)",
	"Drug Repurposing Hub": "CC-BY 4.0",
	FDA: "Public domain",
	NCI: "Public domain",
	ChEMBL: "Creative Commons Attribution-ShareAlike 3.0 Unported (CC BY-SA 3.0)",
	ClinPGx: "Create Commons Attribution-ShareAlike 4.0 International License",
	GuideToPharmacology: "Create Commons Attribution-ShareAlike 4.0 International License",
	MOAlmanac: "ODbL",
	// Gene and drug claim sources (categories, attributes, approval ratings,
	// aliases). The audit names only interaction sources; these are added from the
	// license values of the live DGIdb `sources` root (2026-11-10), each an open
	// license that permits commercial use.
	Wikidata: "Creative Commons CC0 1.0 Universal (CC0 1.0) Public Domain Dedication",
	"Drugs@FDA": "Creative Commons CC0 1.0 Universal (CC0 1.0) Public Domain Dedication",
	NCIt: "Creative Commons Attribution 4.0 International (CC BY 4.0)",
	GO: "Creative Commons Attribution 4.0 International",
	HemOnc: "Creative Commons Attribution 3.0 Unported (CC BY 3.0)",
	HumanProteinAtlas: "Creative Commons Attribution-ShareAlike 3.0 Unported (CC BY-SA 3.0)",
	IDG: "Create Commons Attribution-ShareAlike 4.0 International License",
	Pharos: "Create Commons Attribution-ShareAlike 4.0 International License",
	// DGIdb labels these three "Custom". The providers place no restriction on
	// use: NCBI "places no restrictions on the use or distribution of the data"
	// (ncbi.nlm.nih.gov/home/about/policies), Ensembl "imposes no restrictions on
	// access to, or use of, the data" (ensembl.org/info/about/legal), and HGNC
	// data is CC0 (genenames.org/about/license).
	NCBI: "Custom (NCBI: no restrictions on use or distribution)",
	Ensembl: "Custom (Ensembl: no restrictions on use)",
	HGNC: "Custom (HGNC: CC0)",
};

/** The DGIdb `sourceDbName` values that a commercial product may serve. */
export const COMMERCIAL_SOURCE_ALLOWLIST: readonly string[] = Object.keys(COMMERCIAL_SOURCE_LICENSES);

const sourceKey = (name: string) => name.trim().toLowerCase();
const PERMITTED = new Set(COMMERCIAL_SOURCE_ALLOWLIST.map(sourceKey));

/**
 * True when a source name is in the allowlist. The comparison ignores case and
 * outer spaces, as the DGIdb arguments do. A value that is not a string is not
 * permitted.
 */
export function isPermittedSource(name: unknown): boolean {
	return typeof name === "string" && PERMITTED.has(sourceKey(name));
}

/** The response key that the adapter adds to each selection set. The caller may not use it. */
export const TYPENAME_KEY = "dgidbLicenseFilterTypename";
/** Arguments whose value names a DGIdb source. */
export const SOURCE_ARGUMENTS: ReadonlySet<string> = new Set(["sources", "sourceName", "sourceNames", "sourceDbName", "sourceDbNames"]);
/** Response keys that the rule reads. An alias with one of these names must select the same field. */
const GUARDED_KEYS = new Set(["source", "sources", "sourceDbName", "sourceNames", "sourceDbNames", "__typename"]);

const isRecord = (value: unknown): value is Record<string, unknown> =>
	value !== null && typeof value === "object" && !Array.isArray(value);

// ---------------------------------------------------------------------------
// Query reading
// ---------------------------------------------------------------------------

interface Token {
	kind: "punct" | "name" | "string" | "block" | "number";
	text: string;
	end: number;
}

const PUNCTUATORS = new Set(["!", "$", "&", "(", ")", ":", "=", "@", "[", "]", "{", "|", "}"]);
const IGNORED = /[\t \n\r,﻿]/;
const PATTERNS: Array<[Token["kind"], RegExp]> = [
	["block", /"""(?:\\"""|[\s\S])*?"""/y],
	["string", /"(?:\\.|[^"\\\n\r])*"/y],
	["name", /[_A-Za-z][_0-9A-Za-z]*/y],
	["number", /-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?/y],
];

function skipComment(query: string, start: number): number {
	let index = start;
	while (index < query.length && query[index] !== "\n" && query[index] !== "\r") index++;
	return index;
}

function readToken(query: string, index: number): Token {
	if (query.startsWith("...", index)) return { kind: "punct", text: "...", end: index + 3 };
	if (PUNCTUATORS.has(query[index])) return { kind: "punct", text: query[index], end: index + 1 };
	for (const [kind, pattern] of PATTERNS) {
		pattern.lastIndex = index;
		const match = pattern.exec(query);
		if (match) return { kind, text: match[0], end: index + match[0].length };
	}
	throw new Error(`unexpected character ${JSON.stringify(query[index])} at offset ${index}`);
}

/** The GraphQL tokens of a query (comments, commas and white space removed). */
function tokenize(query: string): Token[] {
	const tokens: Token[] = [];
	let index = 0;
	while (index < query.length) {
		if (query[index] === "#") index = skipComment(query, index);
		else if (IGNORED.test(query[index])) index++;
		else {
			const token = readToken(query, index);
			tokens.push(token);
			index = token.end;
		}
	}
	return tokens;
}

function stringValue(token: Token): string {
	if (token.kind === "block") return token.text.slice(3, -3).replace(/\\"""/g, '"""').trim();
	if (token.kind === "string") return JSON.parse(token.text) as string;
	return token.text;
}

interface SourceSelection {
	names: string[];
	unreadable: string[];
}

function variableNames(name: string, variables: Record<string, unknown> | undefined, out: SourceSelection): void {
	const value = variables?.[name];
	if (value === null) return;
	const values = Array.isArray(value) ? value : [value];
	if (value === undefined || values.some((item) => item !== null && typeof item !== "string")) {
		out.unreadable.push(`$${name}`);
		return;
	}
	for (const item of values) if (typeof item === "string") out.names.push(item);
}

/** Read one argument value at `index`; return the index after it. */
function readValue(tokens: Token[], index: number, variables: Record<string, unknown> | undefined, out: SourceSelection): number {
	const token = tokens[index];
	if (!token) return index;
	if (token.text === "$" && tokens[index + 1]?.kind === "name") {
		variableNames(tokens[index + 1].text, variables, out);
		return index + 2;
	}
	if (token.text === "[") {
		let next = index + 1;
		while (next < tokens.length && tokens[next].text !== "]") next = readValue(tokens, next, variables, out);
		return next + 1;
	}
	if (token.kind === "punct") {
		out.unreadable.push(token.text === "{" ? "an input object" : token.text);
		return index + 1;
	}
	if (token.text !== "null") {
		try {
			out.names.push(stringValue(token));
		} catch {
			out.unreadable.push(token.text.slice(0, 80));
		}
	}
	return index + 1;
}

/** The query with a `dgidbLicenseFilterTypename: __typename` field in each selection set, and its source selection. */
interface QueryPlan {
	query: string;
	selection: SourceSelection;
	aliasProblems: string[];
}

function readAlias(tokens: Token[], index: number, plan: QueryPlan): void {
	const alias = tokens[index].text;
	const field = tokens[index + 2]?.text;
	if (tokens[index + 1]?.text === ":" && tokens[index + 2]?.kind === "name" && GUARDED_KEYS.has(alias) && alias !== field) {
		plan.aliasProblems.push(`${alias}: ${field}`);
	}
}

function readArgument(tokens: Token[], index: number, variables: Record<string, unknown> | undefined, plan: QueryPlan): void {
	if (tokens[index - 1]?.text === "$" || tokens[index + 1]?.text !== ":") return;
	if (SOURCE_ARGUMENTS.has(tokens[index].text)) readValue(tokens, index + 2, variables, plan.selection);
}

/**
 * Read a query for the commercial profile. A `{` outside parentheses opens a
 * selection set (an input object can only be inside the parentheses of
 * arguments), and a `:` outside parentheses follows an alias.
 */
export function planCommercialQuery(query: string, variables?: Record<string, unknown>): QueryPlan {
	const tokens = tokenize(query);
	const plan: QueryPlan = { query: "", selection: { names: [], unreadable: [] }, aliasProblems: [] };
	const insertAt: number[] = [];
	let depth = 0;
	tokens.forEach((token, index) => {
		if (token.kind === "name" && token.text === TYPENAME_KEY) plan.aliasProblems.push(TYPENAME_KEY);
		if (token.text === "(") depth++;
		else if (token.text === ")") depth--;
		else if (depth === 0 && token.text === "{") insertAt.push(token.end);
		else if (depth === 0 && token.kind === "name") readAlias(tokens, index, plan);
		else if (depth > 0 && token.kind === "name") readArgument(tokens, index, variables, plan);
		if (depth < 0) throw new Error("unbalanced parentheses");
	});
	let last = 0;
	for (const offset of insertAt) {
		plan.query += `${query.slice(last, offset)} ${TYPENAME_KEY}: __typename `;
		last = offset;
	}
	plan.query += query.slice(last);
	return plan;
}

/** A GraphQL error envelope with the refusal text of the shared catalog. No upstream request is made. */
export function commercialRefusalEnvelope(what: string, terms: string): Envelope {
	const refusal = commercialUseRefusal(what, terms).data;
	const code = String(refusal.error);
	return {
		errors: [{
			message: `${code}: ${String(refusal.message)}`,
			extensions: { code, permitted_sources: [...COMMERCIAL_SOURCE_ALLOWLIST] },
		}],
	};
}

const ALLOWED_TEXT = `Permitted sources: ${COMMERCIAL_SOURCE_ALLOWLIST.join(", ")}`;

/** The refusal for a query that the rule cannot serve, or undefined. */
export function commercialQueryRefusal(plan: QueryPlan): Envelope | undefined {
	const withheld = plan.selection.names.filter((name) => !isPermittedSource(name));
	if (withheld.length > 0) {
		return commercialRefusalEnvelope(
			`The DGIdb source selection ${withheld.map((name) => JSON.stringify(name.slice(0, 80))).join(", ")}`,
			`the license of the source does not permit commercial use, or the source is not in the allowlist of the license audit of 2026-11-10. ${ALLOWED_TEXT}`,
		);
	}
	if (plan.selection.unreadable.length > 0) {
		return commercialRefusalEnvelope(
			`A DGIdb source selection with ${plan.selection.unreadable.join(", ")}`,
			`the rule cannot read which sources it names. Give the source names as string literals, or set each variable in variables. ${ALLOWED_TEXT}`,
		);
	}
	if (plan.aliasProblems.length > 0) {
		return commercialRefusalEnvelope(
			`A DGIdb query with the response key ${plan.aliasProblems.map((alias) => JSON.stringify(alias)).join(", ")}`,
			`the rule reads the source of each record from these keys. Do not use ${TYPENAME_KEY}, and do not give the names ${[...GUARDED_KEYS].join(", ")} to another field`,
		);
	}
	return undefined;
}

// ---------------------------------------------------------------------------
// Response filter
// ---------------------------------------------------------------------------

type Role = "source" | "claim" | "aggregate" | "dependent" | "entity" | "container" | "neutral";

const roles = (role: Role, names: string[]) => names.map((name): [string, Role] => [name, role]);
/** The role of each DGIdb type (live schema 2026-11-10). A type that is not here is removed as unverified. */
export const TYPE_ROLES: ReadonlyMap<string, Role> = new Map([
	["Source", "source"],
	...roles("claim", ["InteractionClaim", "DrugClaim", "GeneClaim", "DrugApprovalRating"]),
	...roles("aggregate", ["Interaction", "InteractionAttribute", "DrugAttribute", "DrugAlias", "GeneAttribute", "GeneAlias",
		"GeneClaimCategory", "CategoryWithSources", "GeneCategoryResult"]),
	...roles("dependent", ["Publication", "InteractionClaimType", "InteractionClaimAttribute", "InteractionClaimLink",
		"DrugClaimAlias", "DrugClaimAttribute", "DrugClaimApprovalRating", "GeneClaimAlias", "GeneClaimAttribute",
		"DrugApplication", "CategoryWithCount"]),
	...roles("entity", ["Drug", "Gene", "DrugSuggestion", "GeneSuggestion"]),
	...roles("container", ["DrugConnection", "DrugEdge", "GeneConnection", "GeneEdge", "InteractionConnection", "InteractionEdge",
		"SourceConnection", "SourceEdge", "GeneClaimCategoryConnection", "GeneClaimCategoryEdge", "GeneCategoryResultConnection",
		"GeneCategoryResultEdge", "InteractionClaimTypeConnection", "InteractionClaimTypeEdge"]),
	...roles("neutral", ["Query", "Mutation", "PageInfo", "Meta", "Organization", "Service", "SourceType", "SourceTrustLevel",
		"DrugMatch", "GeneMatch", "DrugSearchResult", "GeneSearchResult"]),
]);

/** Fields with a list of source names in a record. */
const SOURCE_NAME_LISTS: Readonly<Record<string, string>> = { CategoryWithSources: "sourceNames", GeneCategoryResult: "sourceDbNames" };

const INSIDE_CLAIM = "Select it inside a claim with the claim's source, for example interactionClaims { source { sourceDbName } publications { pmid } interactionClaimTypes { type } }. The list on an interaction is the union over all sources.";
const FIELD_HINTS: Readonly<Record<string, string>> = {
	Source: "Select sourceDbName in the source.",
	InteractionClaim: "Select source { sourceDbName } in the claim.",
	DrugClaim: "Select source { sourceDbName } in the claim.",
	GeneClaim: "Select source { sourceDbName } in the claim.",
	DrugApprovalRating: "Select source { sourceDbName } in the rating.",
	Interaction: "Select sources { sourceDbName } or interactionClaims { source { sourceDbName } } in the interaction.",
	CategoryWithSources: "Select sourceNames in geneCategoriesWithSources.",
	GeneCategoryResult: "Select sourceDbNames in the gene of the category.",
	GeneClaimCategory: "Use geneCategoriesWithSources { name sourceNames } on a gene, or geneClaims { source { sourceDbName } geneClaimCategories { name } }.",
	Publication: INSIDE_CLAIM,
	InteractionClaimType: INSIDE_CLAIM,
	DrugApplication: "DGIdb gives no source for a drug application, so a commercial product does not get it.",
	CategoryWithCount: "Select it inside a source together with sourceDbName.",
};
const SOURCES_HINT = "Select sources { sourceDbName } in the record.";
const CLAIM_PART_HINT = "Select it inside its claim, together with source { sourceDbName } of the claim.";
const UNKNOWN_HINT = "The rule has no role for this type, so a commercial product does not get it.";

function fieldHint(type: string): string {
	if (FIELD_HINTS[type]) return FIELD_HINTS[type];
	if (TYPE_ROLES.get(type) === "aggregate") return SOURCES_HINT;
	return TYPE_ROLES.get(type) === "dependent" ? CLAIM_PART_HINT : UNKNOWN_HINT;
}

interface Tally {
	removed: Record<string, number>;
	unverified: Record<string, number>;
}

const newTally = (): Tally => ({ removed: {}, unverified: {} });
const count = (counts: Record<string, number>, type: string, by = 1) => {
	counts[type] = (counts[type] ?? 0) + by;
};
function merge(into: Tally, from: Tally): void {
	for (const [type, by] of Object.entries(from.removed)) count(into.removed, type, by);
	for (const [type, by] of Object.entries(from.unverified)) count(into.unverified, type, by);
}

const DROP = Symbol("drop");
interface Walked {
	value: unknown;
	/** A kept source, claim or aggregate is in this value. */
	evidence: boolean;
	/** A source, claim or aggregate was in this value, kept or removed for its source. */
	seen: boolean;
}
type Verdict = "keep" | "removed" | "unverified";

const typeOf = (record: Record<string, unknown>) => (typeof record[TYPENAME_KEY] === "string" ? (record[TYPENAME_KEY] as string) : "(no type)");
const roleOf = (type: string): Role | undefined => (type.startsWith("__") ? "neutral" : TYPE_ROLES.get(type));
const EVIDENCE_ROLES = new Set<Role | undefined>(["source", "claim", "aggregate"]);

function drop(type: string, verdict: Exclude<Verdict, "keep">, tally: Tally): Walked {
	count(verdict === "removed" ? tally.removed : tally.unverified, type);
	return { value: DROP, evidence: false, seen: verdict === "removed" };
}

function sourceVerdict(source: Record<string, unknown>): Verdict {
	if (typeof source.sourceDbName !== "string") return "unverified";
	return isPermittedSource(source.sourceDbName) ? "keep" : "removed";
}

function claimVerdict(claim: Record<string, unknown>, context: boolean): Verdict {
	const sources = Object.values(claim).filter((child) => isRecord(child) && child[TYPENAME_KEY] === "Source");
	if (sources.length === 0) return context ? "keep" : "unverified";
	const verdicts = sources.map((source) => sourceVerdict(source as Record<string, unknown>));
	if (verdicts.includes("removed")) return "removed";
	return verdicts.includes("unverified") ? "unverified" : "keep";
}

/** Walk the fields of a record. The added type field is removed; a removed child field is left out. */
function walkFields(record: Record<string, unknown>, context: boolean, tally: Tally): Walked {
	const value: Record<string, unknown> = {};
	let evidence = false;
	let seen = false;
	for (const [key, child] of Object.entries(record)) {
		if (key === TYPENAME_KEY) continue;
		const walked = walk(child, context, tally);
		evidence ||= walked.evidence;
		seen ||= walked.seen;
		if (walked.value !== DROP) value[key] = walked.value;
	}
	return { value, evidence, seen };
}

/** Remove the source names that are not permitted from the name list of a record. */
function filterNameList(type: string, record: Record<string, unknown>, tally: Tally): { seen: boolean; evidence: boolean } {
	const field = SOURCE_NAME_LISTS[type];
	const names = field ? record[field] : undefined;
	if (!Array.isArray(names) || names.length === 0) return { seen: false, evidence: false };
	const kept = names.filter(isPermittedSource);
	if (kept.length < names.length) count(tally.removed, "(source name)", names.length - kept.length);
	record[field] = kept;
	return { seen: true, evidence: kept.length > 0 };
}

function walkAggregate(type: string, record: Record<string, unknown>, context: boolean, tally: Tally): Walked {
	const inner = newTally();
	const walked = walkFields(record, false, inner);
	const names = filterNameList(type, walked.value as Record<string, unknown>, inner);
	const evidence = walked.evidence || names.evidence;
	const seen = walked.seen || names.seen;
	if (evidence || (!seen && context)) {
		merge(tally, inner);
		return { value: walked.value, evidence: true, seen: true };
	}
	return drop(type, seen ? "removed" : "unverified", tally);
}

function walkRecord(record: Record<string, unknown>, context: boolean, tally: Tally): Walked {
	const type = typeOf(record);
	const role = roleOf(type);
	if (role === "source" || role === "claim") {
		const verdict = role === "source" ? sourceVerdict(record) : claimVerdict(record, context);
		if (verdict !== "keep") return drop(type, verdict, tally);
		return { value: walkFields(record, true, tally).value, evidence: true, seen: true };
	}
	if (role === "aggregate") return walkAggregate(type, record, context, tally);
	if (role === "dependent") return context ? { ...walkFields(record, false, tally), evidence: false, seen: false } : drop(type, "unverified", tally);
	if (role === "entity") return { value: walkFields(record, false, tally).value, evidence: false, seen: false };
	if (role === "container") return walkFields(record, context, tally);
	if (role === "neutral") return { value: walkFields(record, context, tally).value, evidence: false, seen: false };
	return drop(type, "unverified", tally);
}

function walk(value: unknown, context: boolean, tally: Tally): Walked {
	if (Array.isArray(value)) {
		const items = value.map((item) => walk(item, context, tally));
		return {
			value: items.filter((item) => item.value !== DROP).map((item) => item.value),
			evidence: items.some((item) => item.evidence),
			seen: items.some((item) => item.seen),
		};
	}
	if (isRecord(value)) return walkRecord(value, context, tally);
	return { value, evidence: false, seen: false };
}

const sum = (counts: Record<string, number>) => Object.values(counts).reduce((total, by) => total + by, 0);

const REASON = "DGIdb imports each claim from a source with its own license. A commercial product gets only the sources, claims "
	+ "and records of the permitted sources (license audit 2026-11-10). A record of another source, and a record whose source the "
	+ "query did not select (unverified), is not in this response. removed counts the outermost removed records, unverified included; "
	+ "a record inside a removed record is not counted again.";
const UPSTREAM_VALUES = "interactionScore, evidenceScore, geneSpecificity, drugSpecificity, totalCount, pageCount, geneCount, the claim "
	+ "counts of a source, the approved, immunotherapy and antiNeoplastic flags, and the cursors are upstream values computed over all "
	+ "sources, the removed sources included. The rule recomputes none of them. A short or empty list does not mean that DGIdb has no more data.";

/**
 * Apply the rule to the `data` of a response to a query from
 * `planCommercialQuery`. The result starts with `_license_filter`.
 */
export function filterCommercialData(data: Record<string, unknown>): Record<string, unknown> {
	const tally = newTally();
	const walked = walk(data, false, tally);
	const kept = walked.value === DROP ? {} : (walked.value as Record<string, unknown>);
	const unverifiedTypes = Object.keys(tally.unverified).sort();
	const byType = { ...tally.removed };
	for (const [type, by] of Object.entries(tally.unverified)) count(byType, type, by);
	const marker = {
		profile: "commercial",
		removed: sum(tally.removed) + sum(tally.unverified),
		unverified: sum(tally.unverified),
		removed_by_type: byType,
		...(unverifiedTypes.length > 0 ? { fields_to_add: Object.fromEntries(unverifiedTypes.map((type) => [type, fieldHint(type)])) } : {}),
		permitted_sources: [...COMMERCIAL_SOURCE_ALLOWLIST],
		reason: REASON,
		upstream_values: UPSTREAM_VALUES,
	};
	return { _license_filter: marker, ...kept };
}

/**
 * The DGIdb GraphQL request of a reselling caller. `unrestricted` is the fetch
 * of each other caller; this function refuses a query, adds the type field,
 * and removes records from the result. GraphQL `errors` stay unchanged.
 */
export async function commercialDgidbFetch(
	query: string,
	variables: Record<string, unknown> | undefined,
	unrestricted: UnrestrictedFetch,
): Promise<Envelope> {
	let plan: QueryPlan;
	try {
		plan = planCommercialQuery(query, variables);
	} catch (error) {
		const detail = error instanceof Error ? error.message : String(error);
		return commercialRefusalEnvelope("This DGIdb query", `the rule cannot read it (${detail}), so the source of its records cannot be checked`);
	}
	const refusal = commercialQueryRefusal(plan);
	if (refusal) return refusal;
	const envelope = await unrestricted(plan.query, variables);
	return isRecord(envelope.data) ? { ...envelope, data: filterCommercialData(envelope.data) } : envelope;
}

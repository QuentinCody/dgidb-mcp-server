import { afterEach, describe, expect, it, vi } from "vitest";

// The real createGraphqlExecuteTool runs user code in a Worker Loader isolate.
// The mock captures the host functions that the isolate gets, so that a test
// can call __graphql_proxy as the isolate does.
const captured = vi.hoisted(() => ({ fns: {} as Record<string, (args: unknown) => Promise<unknown>> }));
vi.mock("@bio-mcp/shared/codemode/execute-tool", async (importOriginal) => {
	const actual = await importOriginal<typeof import("@bio-mcp/shared/codemode/execute-tool")>();
	return {
		...actual,
		DynamicWorkerExecutor: class {
			// biome-ignore lint/complexity/noUselessConstructor: the mock takes the constructor argument of the real executor
			constructor(_options: unknown) {}
			async execute(_code: string, fns: Record<string, (args: unknown) => Promise<unknown>>) {
				captured.fns = fns;
				return { result: { ok: true } };
			}
		},
	};
});

import { LICENSE_PROFILE_META_KEY } from "@bio-mcp/shared";
import { McpServer } from "@bio-mcp/shared/mcp";
import { registerCodeMode } from "../tools/code-mode";
import {
	COMMERCIAL_SOURCE_ALLOWLIST,
	COMMERCIAL_SOURCE_LICENSES,
	filterCommercialData,
	isPermittedSource,
	planCommercialQuery,
	TYPENAME_KEY,
} from "./commercial-profile";
import { fetchDgidb } from "./graphql-fetch";
import { graphqlHandler } from "./legacy-handlers";

// License audit 2026-11-10: "Filter interactions by source license in the DGIdb
// server. Per the DGIdb API on 2026-11-10, serve only CIViC, DoCM, Drug
// Repurposing Hub, FDA, NCI (open) and ChEMBL, ClinPGx, GuideToPharmacology,
// MOAlmanac (share-alike)."
//
// The fixtures are real DGIdb records (https://dgidb.org/api/graphql,
// 2026-11-10), cut to the fields that matter here. Each object carries the type
// field that the adapter adds to each selection set, as the upstream answers.
const T = TYPENAME_KEY;
const COMMERCIAL = { licenseProfile: "commercial" as const };

const source = (name: string) => ({ [T]: "Source", sourceDbName: name });
const publication = (pmid: number) => ({ [T]: "Publication", pmid });
const interaction = (drug: string, score: number, claims: Array<[string, number[]]>, publications: number[]) => ({
	[T]: "Interaction",
	drug: { [T]: "Drug", name: drug },
	interactionScore: score,
	interactionClaims: claims.map(([name, pmids]) => ({ [T]: "InteractionClaim", source: source(name), publications: pmids.map(publication) })),
	sources: claims.map(([name]) => source(name)),
	publications: publications.map(publication),
});
// EGFR: three permitted sources. Kept as it is.
const atezolizumab = interaction("ATEZOLIZUMAB", 0.01707820104760644, [["MOAlmanac", []], ["ClinPGx", []], ["FDA", []]], []);
// EGFR: the DTC claim (CC BY-NC-SA 3.0) goes; GuideToPharmacology and ChEMBL stay.
// The interaction-level publication 19821562 is the publication of the DTC claim.
const bms599626 = interaction("BMS-599626", 0.1897577894178493, [["DTC", [19821562]], ["GuideToPharmacology", []], ["ChEMBL", []]], [19821562]);
// EGFR: PRISM only (not in the audit, so not permitted). Removed.
const dasatinib = interaction("DASATINIB ANHYDROUS", 0.001735589537358378, [["PRISM", []]], []);
// EGFR: TALC only (an Elsevier publication table: unclear). Removed.
const seribantumab = interaction("SERIBANTUMAB", 0.07115917103169352, [["TALC", []]], []);

const geneInteractions = (interactions: unknown[]) => ({
	data: {
		[T]: "Query",
		genes: { [T]: "GeneConnection", nodes: [{ [T]: "Gene", name: "EGFR", conceptId: "hgnc:3236", interactions }] },
	},
});
const INTERACTION_QUERY = `{ genes(names:["EGFR"]) { nodes { name conceptId interactions { drug { name } interactionScore
  interactionClaims { source { sourceDbName } publications { pmid } } sources { sourceDbName } publications { pmid } } } } }`;

/** The value without the type field that the adapter adds. */
function untyped(value: unknown): unknown {
	if (Array.isArray(value)) return value.map(untyped);
	if (value === null || typeof value !== "object") return value;
	return Object.fromEntries(Object.entries(value).filter(([key]) => key !== T).map(([key, child]) => [key, untyped(child)]));
}

const json = (value: unknown) => new Response(JSON.stringify(value), { headers: { "content-type": "application/json" } });
function stubUpstream(answer: (query: string) => unknown) {
	const sent: Array<{ query: string; variables?: Record<string, unknown> }> = [];
	const fetch = vi.fn(async (_url: string, init: RequestInit) => {
		// SAFETY: fetchDgidb sends a JSON string body.
		const body = JSON.parse(String(init.body)) as { query: string; variables?: Record<string, unknown> };
		sent.push(body);
		return json(answer(body.query));
	});
	vi.stubGlobal("fetch", fetch);
	return { fetch, sent };
}
afterEach(() => {
	vi.unstubAllGlobals();
	captured.fns = {};
});

type Data = Record<string, unknown> & { _license_filter: Record<string, unknown> };
async function commercialData(query: string, answer: unknown, variables?: Record<string, unknown>): Promise<Data> {
	stubUpstream(() => answer);
	const result = await fetchDgidb(query, variables, COMMERCIAL);
	return result.data as Data;
}

describe("the allowlist", () => {
	it("is the nine interaction sources of the audit and eleven open gene and drug sources, with their license values", () => {
		expect([...COMMERCIAL_SOURCE_ALLOWLIST].sort()).toEqual(
			[
				"ChEMBL", "CIViC", "ClinPGx", "DoCM", "Drug Repurposing Hub", "FDA", "GuideToPharmacology", "MOAlmanac", "NCI",
				"Wikidata", "Drugs@FDA", "NCIt", "GO", "HemOnc", "HumanProteinAtlas", "IDG", "Pharos", "NCBI", "Ensembl", "HGNC",
			].sort(),
		);
		expect(COMMERCIAL_SOURCE_LICENSES).toMatchObject({ "Drugs@FDA": expect.stringContaining("CC0"), HemOnc: expect.stringContaining("CC BY 3.0") });
		expect(COMMERCIAL_SOURCE_LICENSES).toMatchObject({
			CIViC: "Creative Commons CC0 1.0 Universal (CC0 1.0) Public Domain Dedication",
			ChEMBL: "Creative Commons Attribution-ShareAlike 3.0 Unported (CC BY-SA 3.0)",
			MOAlmanac: "ODbL",
			NCI: "Public domain",
		});
	});

	it("permits only those names, without regard to case, and fails closed for every other value", () => {
		for (const name of COMMERCIAL_SOURCE_ALLOWLIST) expect(isPermittedSource(name)).toBe(true);
		expect(isPermittedSource(" civic ")).toBe(true);
		const notPermitted = ["CancerCommons", "CGI", "CKB-CORE", "COSMIC", "DrugBank", "DTC", "MyCancerGenome", "MyCancerGenomeClinicalTrial",
			"OncoKB", "ClearityFoundationBiomarkers", "ClearityFoundationClinicalTrial", "TALC", "TdgClinicalTrial", "TEND", "TTD",
			"PRISM", "dGene", "BaderLab", "Tempus", "ChemIDplus", "RxNorm", "Oncomine", "", "Unknown"];
		for (const name of notPermitted) expect(isPermittedSource(name)).toBe(false);
		for (const value of [undefined, null, 1, ["CIViC"], { sourceDbName: "CIViC" }]) expect(isPermittedSource(value)).toBe(false);
	});
});

describe("interactions under the commercial profile", () => {
	it("removes the claims, sources and interactions of sources that are not permitted, and keeps permitted records untouched", async () => {
		const data = await commercialData(INTERACTION_QUERY, geneInteractions([atezolizumab, bms599626, dasatinib, seribantumab]));
		const gene = (data.genes as { nodes: Array<{ interactions: Array<Record<string, unknown>> }> }).nodes[0];
		expect(gene.interactions).toHaveLength(2);
		expect(gene.interactions[0]).toEqual(untyped(atezolizumab));
		expect(gene.interactions[1]).toEqual({
			drug: { name: "BMS-599626" },
			interactionScore: 0.1897577894178493,
			interactionClaims: [
				{ source: { sourceDbName: "GuideToPharmacology" }, publications: [] },
				{ source: { sourceDbName: "ChEMBL" }, publications: [] },
			],
			sources: [{ sourceDbName: "GuideToPharmacology" }, { sourceDbName: "ChEMBL" }],
			publications: [],
		});
		expect(JSON.stringify(data)).not.toContain(T);
	});

	it("puts _license_filter first in data, with the counts, the allowlist and the upstream-value note", async () => {
		const data = await commercialData(INTERACTION_QUERY, geneInteractions([atezolizumab, bms599626, dasatinib, seribantumab]));
		expect(Object.keys(data)[0]).toBe("_license_filter");
		expect(data._license_filter).toEqual({
			profile: "commercial",
			removed: 5,
			unverified: 1,
			removed_by_type: { InteractionClaim: 1, Source: 1, Interaction: 2, Publication: 1 },
			fields_to_add: { Publication: expect.stringContaining("interactionClaims { source { sourceDbName } publications { pmid }") },
			permitted_sources: [...COMMERCIAL_SOURCE_ALLOWLIST],
			reason: expect.stringContaining("license audit 2026-11-10"),
			upstream_values: expect.stringContaining("interactionScore"),
		});
		expect(data._license_filter.upstream_values).toContain("computed over all sources");
	});

	it("adds the type field to each selection set of the request, and only there", async () => {
		const { sent } = stubUpstream(() => geneInteractions([atezolizumab]));
		await fetchDgidb(INTERACTION_QUERY, undefined, COMMERCIAL);
		expect(sent).toHaveLength(1);
		const sets = INTERACTION_QUERY.split("{").length - 1;
		expect(sent[0].query.split(`${T}: __typename`).length - 1).toBe(sets);
		expect(planCommercialQuery('query Q($x: String = "{") { drugs(names: ["{"]) { nodes { name } } } # {').query).toBe(
			`query Q($x: String = "{") { ${T}: __typename  drugs(names: ["{"]) { ${T}: __typename  nodes { ${T}: __typename  name } } } # {`,
		);
	});

	it("removes as unverified an interaction or claim whose source the query did not select, and names the fields to add", async () => {
		const noSource = { [T]: "Interaction", drug: { [T]: "Drug", name: "GEFITINIB" }, interactionScore: 0.121987150340046,
			interactionClaims: [{ [T]: "InteractionClaim", id: "claim-1" }] };
		const aliasedName = { [T]: "Interaction", drug: { [T]: "Drug", name: "OSIMERTINIB" },
			sources: [{ [T]: "Source", n: "CIViC" }] };
		const data = await commercialData(INTERACTION_QUERY, geneInteractions([noSource, aliasedName]));
		expect((data.genes as { nodes: Array<{ interactions: unknown[] }> }).nodes[0].interactions).toEqual([]);
		expect(data._license_filter).toMatchObject({
			removed: 2,
			unverified: 2,
			removed_by_type: { Interaction: 2 },
			fields_to_add: { Interaction: "Select sources { sourceDbName } or interactionClaims { source { sourceDbName } } in the interaction." },
		});
	});

	it("reads the source by type, so aliases do not hide a source that is not permitted", async () => {
		const aliased = { [T]: "Interaction", d: { [T]: "Drug", name: "BMS-599626" },
			c: [{ [T]: "InteractionClaim", s: source("DTC") }, { [T]: "InteractionClaim", s: source("ChEMBL") }] };
		const data = await commercialData(INTERACTION_QUERY, geneInteractions([aliased]));
		expect((data.genes as { nodes: Array<{ interactions: unknown[] }> }).nodes[0].interactions).toEqual([
			{ d: { name: "BMS-599626" }, c: [{ s: { sourceDbName: "ChEMBL" } }] },
		]);
	});

	it("keeps GraphQL errors of a partial answer and passes an answer with no data unchanged", async () => {
		const errors = [{ message: "partial", path: ["genes"] }];
		stubUpstream(() => ({ ...geneInteractions([dasatinib]), errors }));
		const partial = await fetchDgidb(INTERACTION_QUERY, undefined, COMMERCIAL);
		expect(partial.errors).toEqual(errors);
		expect((partial.data as Data)._license_filter).toMatchObject({ removed: 1 });
		stubUpstream(() => ({ errors: [{ message: "Field 'x' doesn't exist" }] }));
		expect(await fetchDgidb("{ x }", undefined, COMMERCIAL)).toEqual({ errors: [{ message: "Field 'x' doesn't exist" }] });
	});
});

describe("drug and gene records under the commercial profile", () => {
	const attribute = (name: string, value: string, sources: string[]) => ({ [T]: "DrugAttribute", name, value, sources: sources.map(source) });
	const rating = (value: string, name: string) => ({ [T]: "DrugApprovalRating", rating: value, source: source(name) });
	const imatinib = {
		data: {
			[T]: "Query",
			drugs: {
				[T]: "DrugConnection",
				nodes: [{
					[T]: "Drug", name: "IMATINIB", conceptId: "rxcui:282388", approved: true,
					drugAttributes: [attribute("Year of Approval", "2001", ["HemOnc"]), attribute("therapy_type", "Targeted therapy", ["MOAlmanac"]),
						attribute("Drug Class", "MEK inhibitor", ["CGI"])],
					drugApprovalRatings: [rating("Discontinued", "Drugs@FDA"), rating("Approved", "MOAlmanac"), rating("Launched", "Drug Repurposing Hub")],
					drugApplications: [{ [T]: "DrugApplication", appNo: "drugsatfda.anda:078340" }],
					drugClaims: [
						{ [T]: "DrugClaim", name: "Imatinib", source: source("CGI"),
							drugClaimAttributes: [{ [T]: "DrugClaimAttribute", name: "Drug Class", value: "BCR-ABL inhibitor 1st gen" }] },
						{ [T]: "DrugClaim", name: "imatinib", source: source("ClinPGx"),
							drugClaimAliases: [{ [T]: "DrugClaimAlias", alias: "CLINPGX.DRUG:PA10804" }] },
					],
				}],
			},
		},
	};

	it("filters drug attributes, approval ratings and claims; keeps the parts of a permitted claim; removes drug applications", async () => {
		const data = await commercialData("{ drugs(names:[\"IMATINIB\"]) { nodes { name } } }", imatinib);
		expect(data.drugs).toEqual({
			nodes: [{
				name: "IMATINIB", conceptId: "rxcui:282388", approved: true,
				drugAttributes: [
					{ name: "Year of Approval", value: "2001", sources: [{ sourceDbName: "HemOnc" }] },
					{ name: "therapy_type", value: "Targeted therapy", sources: [{ sourceDbName: "MOAlmanac" }] },
				],
				drugApprovalRatings: [
					{ rating: "Discontinued", source: { sourceDbName: "Drugs@FDA" } },
					{ rating: "Approved", source: { sourceDbName: "MOAlmanac" } },
					{ rating: "Launched", source: { sourceDbName: "Drug Repurposing Hub" } },
				],
				drugApplications: [],
				drugClaims: [{ name: "imatinib", source: { sourceDbName: "ClinPGx" }, drugClaimAliases: [{ alias: "CLINPGX.DRUG:PA10804" }] }],
			}],
		});
		expect(data._license_filter).toMatchObject({
			removed: 3,
			unverified: 1,
			removed_by_type: { DrugAttribute: 1, DrugClaim: 1, DrugApplication: 1 },
			fields_to_add: { DrugApplication: expect.stringContaining("no source") },
		});
	});

	it("filters the source names of gene categories, removes unsourced categories, and filters the sources root", async () => {
		const category = (name: string, sourceNames: string[]) => ({ [T]: "CategoryWithSources", name, sourceNames });
		const braf = {
			data: {
				[T]: "Query",
				genes: { [T]: "GeneConnection", nodes: [{
					[T]: "Gene", name: "BRAF",
					geneCategories: [{ [T]: "GeneClaimCategory", name: "KINASE" }],
					geneCategoriesWithSources: [
						category("CLINICALLY ACTIONABLE", ["CIViC", "CarisMolecularIntelligence", "FoundationOneGenes", "MskImpact", "Oncomine", "Tempus"]),
						category("DRUGGABLE GENOME", ["HingoraniCasas", "HopkinsGroom", "RussLampel"]),
						category("ENZYME", ["GuideToPharmacology"]),
					],
					geneClaims: [
						{ [T]: "GeneClaim", name: "BRAF", source: source("dGene"), geneClaimCategories: [{ [T]: "GeneClaimCategory", name: "KINASE" }] },
						{ [T]: "GeneClaim", name: "BRAF", source: source("ChEMBL"), geneClaimCategories: [] },
					],
				}] },
				sources: { [T]: "SourceConnection", totalCount: 48, nodes: [
					{ [T]: "Source", sourceDbName: "CIViC", license: COMMERCIAL_SOURCE_LICENSES.CIViC },
					{ [T]: "Source", sourceDbName: "DrugBank", license: "Custom Non-Commercial" },
					{ [T]: "Source", sourceDbName: "PRISM", license: "Supplementary data from Nature Publishing Group copyright publication" },
				] },
			},
		};
		const data = await commercialData("{ genes(names:[\"BRAF\"]) { nodes { name } } sources { nodes { sourceDbName } } }", braf);
		expect(data.genes).toEqual({ nodes: [{
			name: "BRAF",
			geneCategories: [],
			geneCategoriesWithSources: [{ name: "CLINICALLY ACTIONABLE", sourceNames: ["CIViC"] }, { name: "ENZYME", sourceNames: ["GuideToPharmacology"] }],
			geneClaims: [{ name: "BRAF", source: { sourceDbName: "ChEMBL" }, geneClaimCategories: [] }],
		}] });
		expect(data.sources).toEqual({ totalCount: 48, nodes: [{ sourceDbName: "CIViC", license: COMMERCIAL_SOURCE_LICENSES.CIViC }] });
		expect(data._license_filter).toMatchObject({
			unverified: 1,
			removed_by_type: { GeneClaimCategory: 1, "(source name)": 5, CategoryWithSources: 1, GeneClaim: 1, Source: 2 },
		});
	});

	it("keeps schema records and removes a type that the rule does not know", () => {
		const data = filterCommercialData({ [T]: "Query", __schema: { [T]: "__Schema", queryType: { [T]: "__Type", name: "Query" } },
			future: { [T]: "FutureType", value: 1 }, plain: { value: 2 } });
		expect(data).toMatchObject({ __schema: { queryType: { name: "Query" } } });
		expect(data).not.toHaveProperty("future");
		expect(data).not.toHaveProperty("plain");
		expect(data._license_filter).toMatchObject({ unverified: 2, removed_by_type: { FutureType: 1, "(no type)": 1 } });
	});
});

describe("refusals under the commercial profile", () => {
	const refusals: Array<[string, string, Record<string, unknown>?]> = [
		["a literal", '{ drugs(sourceDbName: "DrugBank") { nodes { name } } }'],
		["a list", '{ interactions(geneNames: ["BRAF"], sources: ["CIViC", "OncoKB"]) { nodes { id } } }'],
		["a block string", '{ genes(sourceName: """COSMIC""") { nodes { name } } }'],
		["a nested argument", '{ categories(first: 1) { nodes { name geneCount(sourceDbNames: ["CGI"]) } } }'],
		["a variable", "query Q($s: [String]) { interactions(sources: $s) { nodes { id } } }", { s: ["DrugBank"] }],
		["a variable that is not set", "query Q($s: [String] = [\"DrugBank\"]) { interactions(sources: $s) { nodes { id } } }"],
		["an alias that hides a field", "{ sources { nodes { sourceDbName: fullName } } }"],
		["an alias with the type key", `{ sources { nodes { ${T}: sourceDbName } } }`],
		["a query that the rule cannot read", "{ genes ^ }"],
	];

	it.each(refusals)("refuses %s with NOT_LICENSED_FOR_COMMERCIAL_USE and no upstream request", async (_label, query, variables) => {
		const { fetch } = stubUpstream(() => ({ data: {} }));
		const result = await fetchDgidb(query, variables, COMMERCIAL);
		expect(fetch).not.toHaveBeenCalled();
		expect(result.data).toBeUndefined();
		expect(result.errors).toHaveLength(1);
		expect(result.errors?.[0].message).toMatch(/^NOT_LICENSED_FOR_COMMERCIAL_USE: /);
	});

	it("names the source that is not permitted", async () => {
		stubUpstream(() => ({ data: {} }));
		const result = await fetchDgidb('{ interactions(sources: ["CIViC", "OncoKB"]) { nodes { id } } }', undefined, COMMERCIAL);
		expect(result.errors?.[0].message).toContain('"OncoKB"');
		expect(result.errors?.[0].message).not.toContain('"CIViC"');
	});

	it("sends a query that selects only permitted sources, and an alias that does not hide a field", async () => {
		const { fetch } = stubUpstream(() => ({ data: { [T]: "Query" } }));
		await fetchDgidb('{ interactions(sources: ["civic", "ChEMBL"]) { nodes { s: sources { sourceDbName } } } }', undefined, COMMERCIAL);
		await fetchDgidb("query Q($s: [String]) { interactions(sources: $s) { nodes { id } } }", { s: ["FDA"] }, COMMERCIAL);
		await fetchDgidb("{ sources { nodes { sourceDbName: sourceDbName } } }", undefined, COMMERCIAL);
		expect(fetch).toHaveBeenCalledTimes(3);
	});
});

describe("without the profile", () => {
	it("sends the query unchanged and returns the upstream envelope unchanged", async () => {
		const upstream = untyped(geneInteractions([bms599626, dasatinib]));
		const { sent } = stubUpstream(() => upstream);
		expect(await fetchDgidb(INTERACTION_QUERY, { first: 1 })).toEqual(upstream);
		expect(await fetchDgidb(INTERACTION_QUERY, { first: 1 }, {})).toEqual(upstream);
		expect(sent).toEqual([{ query: INTERACTION_QUERY, variables: { first: 1 } }, { query: INTERACTION_QUERY, variables: { first: 1 } }]);
		stubUpstream(() => ({ data: { drugs: { nodes: [] } } }));
		expect(await fetchDgidb('{ drugs(sourceDbName: "DrugBank") { nodes { name } } }')).toEqual({ data: { drugs: { nodes: [] } } });
	});
});

describe("the tools carry the profile to the rule", () => {
	const meta = { _meta: { [LICENSE_PROFILE_META_KEY]: "commercial" } };

	it("dgidb_graphql_query filters under the profile and refuses with zero upstream calls", async () => {
		stubUpstream(() => geneInteractions([atezolizumab, dasatinib]));
		const filtered = await graphqlHandler({ query: INTERACTION_QUERY }, {}, meta);
		const envelope = filtered.structuredContent.data as { data: Data };
		expect(filtered.isError).toBe(false);
		expect(envelope.data._license_filter).toMatchObject({ removed: 1, removed_by_type: { Interaction: 1 } });

		const { fetch } = stubUpstream(() => ({ data: {} }));
		const refused = await graphqlHandler({ query: '{ drugs(sourceDbName: "DrugBank") { nodes { name } } }' }, {}, meta);
		expect(fetch).not.toHaveBeenCalled();
		expect(refused).toMatchObject({ isError: true, structuredContent: { success: false, error: { code: "GRAPHQL_ERROR" } } });
		expect(refused.content[0].text).toContain("NOT_LICENSED_FOR_COMMERCIAL_USE: ");

		const plain = untyped(geneInteractions([dasatinib]));
		stubUpstream(() => plain);
		expect((await graphqlHandler({ query: INTERACTION_QUERY }, {}, {})).structuredContent.data).toEqual(plain);
	});

	async function executeProxy(extra: unknown): Promise<{ result: unknown; sent: string[] }> {
		// The stub refuses introspection, so the tool skips its pre-flight check. The stub
		// answers with the type field only when the query asks for it, as DGIdb does.
		const answer = geneInteractions([atezolizumab, dasatinib]);
		const { sent } = stubUpstream((query) => {
			if (query.includes("__schema")) return { errors: [{ message: "introspection disabled" }] };
			return query.includes(T) ? answer : untyped(answer);
		});
		const server = new McpServer({ name: "fixture", version: "1" });
		// SAFETY: the mocked executor never uses the loader.
		registerCodeMode(server, { CODE_MODE_LOADER: { get: () => ({}) } } as unknown as Parameters<typeof registerCodeMode>[1]);
		// SAFETY: test-only access to the real registered handler.
		const tools = (server as unknown as { _registeredTools: Record<string, { handler: (args: unknown, extra: unknown) => Promise<unknown> }> })._registeredTools;
		await tools.dgidb_execute.handler({ code: "return 1" }, extra);
		const result = await captured.fns.__graphql_proxy({ query: INTERACTION_QUERY });
		return { result, sent: sent.map((body) => body.query).filter((query) => !query.includes("__schema")) };
	}

	it("dgidb_execute: the profile reaches fetchDgidb through the real createGraphqlExecuteTool", async () => {
		const { result, sent } = await executeProxy(meta);
		expect(sent).toHaveLength(1);
		expect(sent[0]).toContain(`${T}: __typename`);
		expect(result).toMatchObject({ _license_filter: { removed: 1 }, genes: { nodes: [{ interactions: [untyped(atezolizumab)] }] } });
	});

	it("dgidb_execute without the profile sends the query unchanged", async () => {
		const { result, sent } = await executeProxy({});
		expect(sent).toEqual([INTERACTION_QUERY]);
		expect(result).toEqual(untyped(geneInteractions([atezolizumab, dasatinib]).data));
	});
});

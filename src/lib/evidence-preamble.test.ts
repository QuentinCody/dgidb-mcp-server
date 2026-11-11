import {expect, it, vi} from "vitest";
import {DGIDB_EVIDENCE_PREAMBLE} from "./evidence-preamble";
import {DGIDB_EVIDENCE_QUERY, DGIDB_METADATA_QUERY} from "./evidence-query";

function run(options: unknown, native: unknown = {interactions: {nodes: [], pageInfo: {hasNextPage: false, endCursor: null}}}) {
  const query = vi.fn(async () => native);
  const stage = vi.fn(async () => ({data_access_id: "fixture", _staging: {fixture: true}}));
  const result = new Function("gql", "db", "options", `${DGIDB_EVIDENCE_PREAMBLE}; return dgidb.evidence(options);`)({query}, {stage}, options);
  return {query, stage, result};
}
it("preserves distinct observed data/software versions without asserting a pinned snapshot", async () => {
  const native = { serviceInfo: {version: "v.5.0.12", dataVersion: "Dec-2023", updatedAt: "2026-07-14T18:00:15+00:00"}, interactions: {nodes: [], pageInfo: {hasNextPage: false, endCursor: null}} };
  expect(await run({genes: ["BRAF"]}, native).result).toMatchObject({native, _dgidb: {data_release: "Dec-2023", software_version: "v.5.0.12", release_pinned: false}});
  const metadata = {serviceInfo: native.serviceInfo};
  const query = vi.fn(async () => metadata);
  const result = await new Function("gql", `${DGIDB_EVIDENCE_PREAMBLE}; return dgidb.metadata();`)({query});
  expect(query).toHaveBeenCalledExactlyOnceWith(DGIDB_METADATA_QUERY);
  expect(result).toEqual(metadata);
});
it("uses native opaque paging and variables without interpolating names into GraphQL", async () => {
  const {result, query} = run({genes: ['BRAF"{x}'], drugs: ["IMATINIB"], first: 2, after: "opaque+/="});
  await result;
  expect(query).toHaveBeenCalledExactlyOnceWith(DGIDB_EVIDENCE_QUERY, {genes: ['BRAF"{x}'], drugs: ["IMATINIB"], first: 2, after: "opaque+/="});
});
it("keeps conflicting source claims, repeated publications, ranking and partial errors distinct", async () => {
  const native = {interactions: {pageInfo: {hasNextPage: true, endCursor: "next"}, nodes: [{id: "edge", interactionScore: 0.1, evidenceScore: 4, interactionClaims: [
    {id: "claim1", source: {sourceDbName: "A", sourceDbVersion: "release-A"}, publications: [{pmid: 123}], interactionClaimTypes: [{type: "inhibitor"}]},
    {id: "claim2", source: null, publications: [{pmid: 123}], interactionClaimTypes: [{type: "activator"}]},
  ]}]}, __errors: [{message: "one field unavailable"}]};
  expect(await run({genes: ["BRAF"]}, native).result).toMatchObject({native, _dgidb: {complete: false, data_release: null, variables: {first: 10, after: null}}});
});
it.each([null, {}, [], {genes: []}, {genes: [""]}, {genes: [2]}, {genes: Array(11).fill("x")}, {genes: ["x".repeat(201)]}, {genes: ["x"], first: 0}, {genes: ["x"], first: 26}, {genes: ["x"], first: 1.5}, {genes: ["x"], after: ""}, {genes: ["x"], after: 2}, {genes: ["x"], after: "x".repeat(4097)}, {genes: ["x"], score: 2}])("rejects unbounded or unsupported input %# before fetch", async options => {
  const {query, result} = run(options);
  await expect(result).rejects.toThrow(); expect(query).not.toHaveBeenCalled();
});
it.each([{__staged: true, data_access_id: "already"}, {__gql_error: true, incomplete: true}, {__stage_error: true}])("preserves transport handles/errors %#", async native => {
  const {result, stage} = run({drugs: ["IMATINIB"]}, native);
  expect(await result).toBe(native); expect(stage).not.toHaveBeenCalled();
});
it("stages all evidence with multibyte text rather than dropping claim context", async () => {
  const native = {interactions: {nodes: [{interactionClaims: [{publications: [{pmid: 1, citation: "α".repeat(16000)}]}]}], pageInfo: {hasNextPage: true, endCursor: "next"}}};
  const {result, stage} = run({genes: ["BRAF"]}, native);
  expect(await result).toMatchObject({__staged: true, data_access_id: "fixture", _staging: expect.any(Object)});
  expect(stage).toHaveBeenCalledExactlyOnceWith(expect.objectContaining({native, _dgidb: expect.objectContaining({complete: false})}));
});

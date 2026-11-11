import {expect, it} from "vitest";
import {validateGraphqlQuery} from "@bio-mcp/shared/codemode/graphql-validate";
import type {TrimmedIntrospection} from "@bio-mcp/shared/codemode/graphql-introspection";
import {DGIDB_EVIDENCE_QUERY} from "./evidence-query";

// Independently transcribed provider Ruby definitions, reviewed 2026-09-29:
// https://github.com/dgidb/dgidb-v5/tree/main/server/app/graphql/types
// https://github.com/dgidb/dgidb-v5/blob/main/server/app/graphql/resolvers/interactions.rb
// This subset tests published names/relationships, not live deployment compatibility.
const definitions: Record<string, string> = {
  Meta: "id:String! name:String! version:String! dataVersion:String! createdAt:ISO8601DateTime! updatedAt:ISO8601DateTime! environment:String! documentationUrl:String!",
  InteractionConnection: "nodes:[Interaction!] pageInfo:PageInfo!",
  PageInfo: "hasNextPage:Boolean! endCursor:String",
  Gene: "id:ID! name:String longName:String conceptId:String interactions:[Interaction!]!",
  Drug: "id:ID! name:String! conceptId:String! approved:Boolean interactions:[Interaction!]!",
  Interaction: "id:ID! gene:Gene! drug:Drug! interactionClaims:[InteractionClaim!]! interactionTypes:[InteractionClaimType!]! interactionAttributes:[InteractionAttribute!]! publications:[Publication!]! sources:[Source!]! interactionScore:Float! drugSpecificity:Float! geneSpecificity:Float! evidenceScore:Int!",
  InteractionClaim: "id:ID! drugClaimId:ID! geneClaimId:ID! source:Source publications:[Publication!]! interactionClaimTypes:[InteractionClaimType!]! interactionClaimAttributes:[InteractionClaimAttribute!]! interactionClaimLinks:[InteractionClaimLink!]!",
  InteractionClaimType: "id:ID! type:String definition:String reference:String",
  InteractionClaimAttribute: "id:ID! name:String! value:String!",
  InteractionClaimLink: "id:ID! linkText:String! linkUrl:String!",
  Publication: "id:ID! pmid:Int! citation:String createdAt:DateTime updatedAt:DateTime",
  Source: "id:String! sourceDbName:String! sourceDbVersion:String! fullName:String license:String licenseLink:String pmid:String citation:String",
};
const schema: TrimmedIntrospection = {queryType: {name: "Query"}, types: [
  {name: "Query", kind: "OBJECT", fields: [{name: "serviceInfo", type: "Meta!"}, {name: "interactions", type: "InteractionConnection", args: [{name: "geneNames", type: "[String!]"}, {name: "drugNames", type: "[String!]"}, {name: "first", type: "Int"}, {name: "after", type: "String"}]}]},
  ...Object.entries(definitions).map(([name, fields]) => ({name, kind: "OBJECT", fields: fields.split(" ").map(item => {const [name, type] = item.split(":"); return {name, type};})})),
]};

it("validates native claim-level evidence and paged interaction fields against published provider definitions", () => {
  expect(validateGraphqlQuery(DGIDB_EVIDENCE_QUERY, schema)).toEqual({checked: true, errors: []});
});
it.each([
  DGIDB_EVIDENCE_QUERY.replace("geneNames:", "genes:"),
  DGIDB_EVIDENCE_QUERY.replaceAll("sourceDbVersion", "releaseVersion"),
  DGIDB_EVIDENCE_QUERY.replace("interactionClaims {", "interactionClaims(first:10) {"),
])("rejects unsupported filters, release labels and nested pagination", query => {
  expect(validateGraphqlQuery(query, schema).errors.length).toBeGreaterThan(0);
});

// Provider contract: dgidb/dgidb-v5 server/app/graphql/{resolvers/interactions,types/interaction_type,types/interaction_claim_type}.rb.
// Reviewed 2026-09-29; these published definitions are not a live-schema snapshot.
export const DGIDB_EVIDENCE_QUERY = `query InteractionEvidence($genes:[String!],$drugs:[String!],$first:Int!,$after:String) {
  serviceInfo { id name version dataVersion createdAt updatedAt environment documentationUrl }
  interactions(geneNames:$genes,drugNames:$drugs,first:$first,after:$after) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id gene { name conceptId } drug { name conceptId approved }
      interactionScore evidenceScore geneSpecificity drugSpecificity
      interactionTypes { type definition reference }
      publications { pmid citation }
      sources { id sourceDbName sourceDbVersion fullName license licenseLink }
      interactionClaims {
        id geneClaimId drugClaimId
        source { id sourceDbName sourceDbVersion fullName license licenseLink }
        publications { pmid citation }
        interactionClaimTypes { type definition reference }
        interactionClaimAttributes { name value }
        interactionClaimLinks { linkText linkUrl }
      }
    }
  }
}`;

// Live Meta fields verified September 30, 2026. Software and data versions differ.
export const DGIDB_METADATA_QUERY = `{ serviceInfo { id name version dataVersion createdAt updatedAt environment documentationUrl } }`;

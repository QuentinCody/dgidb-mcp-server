# Interacting with the Drug Gene Interaction Database (DGIdb) via Claude Desktop

This guide helps scientists and researchers use the Drug Gene Interaction Database (DGIdb) through an application called Claude Desktop. This setup allows you to ask questions about drug-gene interactions in plain language and get results directly within Claude.

## What is this?

*   **DGIdb:** A comprehensive database containing information about how drugs interact with genes. It gathers data from many different sources.
*   **MCP Server:** A piece of software that acts as a bridge. This specific server connects to the DGIdb API. "MCP" stands for Model Context Protocol, a way for AI models (like the one in Claude) to use tools.
*   **Claude Desktop:** An application where you can interact with an AI assistant. By configuring it correctly, Claude can use this MCP Server to fetch data from DGIdb for you.

Essentially, this server gives Claude the "tool" it needs to look up information in the DGIdb.

## How to Use This with Claude Desktop

If this DGIdb MCP Server has been set up and deployed (e.g., by a technical colleague), you can connect your Claude Desktop application to it. This allows you to ask Claude questions like:

*   "What are the interactions for the drug Imatinib?"
*   "Find genes that interact with Dovitinib."
*   "Show me drug attributes for Trametinib."

Claude will then use this server to find the answers in the DGIdb and present them to you.

### Configuration for Claude Desktop

Your Claude Desktop application needs to be told where to find this DGIdb server. This is usually done by editing a configuration file for Claude Desktop.

A technical user or administrator would typically handle the deployment of the server and provide you with a specific URL (web address). For example, the server might be available at an address like: `https://dgidb-mcp-server.your-organization.workers.dev/mcp`.

The configuration in Claude Desktop would then look something like this. You would add the "dgidb" section within the `mcpServers` part of your Claude Desktop configuration file:

```json
{
  "mcpServers": {
    // ... (other server configurations might be here) ...

    "dgidb": {
      "command": "npx",
      "args": [
        "mcp-remote",
        "https://dgidb-mcp-server.quentincody.workers.dev/mcp" // <-- This URL needs to be the actual address of YOUR deployed DGIdb MCP server
      ]
    }

    // ... (other server configurations might be here) ...
  }
}
```

**Important:**
*   The URL `https://dgidb-mcp-server.quentincody.workers.dev/mcp` in the example above is illustrative. You will need to replace it with the actual URL where *your* instance of the DGIdb MCP server is running.
*   After updating the configuration, you usually need to restart Claude Desktop for the changes to take effect.

Once configured, the DGIdb tool should become available within Claude, allowing you to query the database easily.

## For Technical Users: Deploying the Server

If you are a technical user and need to deploy this server (e.g., on Cloudflare Workers):

1.  **Get Started:**
    You can deploy this server to Cloudflare Workers. Refer to the original, more technical README for deployment buttons and command-line instructions (often involving `npm create cloudflare@latest`).

2.  **Customization:**
    The server code is in `src/index.ts`. This file defines how the server connects to DGIdb and what "tools" it provides to MCP clients.

3.  **Connecting to Other Clients (like AI Playground):**
    Once deployed, you can also connect to your MCP server from other clients like the Cloudflare AI Playground by providing its URL (e.g., `https://your-dgidb-server-name.your-account.workers.dev/mcp`).

This DGIdb MCP Server uses the publicly available DGIdb GraphQL API at `https://dgidb.org/api/graphql`.

## Interaction evidence workflow — September 29, 2026

The [5.0.12 release](https://github.com/dgidb/dgidb-v5/releases/tag/v.5.0.12)
(July 14, 2026) includes importer fixes and GitHub-based data-release discovery.
The software release, individual source import versions and the live database
version are separate facts. Do not label current API results as a pinned release.

Code Mode now provides `dgidb.metadata()` and `dgidb.evidence(options)` through `dgidb_execute` and
`mcp_dgidb_execute`. The paired schema-search tools also have both aliases.

<!-- dgidb-evidence-example -->
```js
return await dgidb.evidence({genes: ["BRAF"], first: 10, after: null});
```
<!-- /dgidb-evidence-example -->

Use `drugs: ["IMATINIB"]` for the drug side, or both filters to restrict both
sides. Each accepts 1–10 names; `first` is 1–25. The helper queries the native
top-level `interactions` connection, preserving `pageInfo.endCursor` and
`hasNextPage`. Pass the returned cursor unchanged with the same filters to read
another page. Nested `Gene.interactions` and claim lists are unpaged; no `first`
argument is invented for them. The transport caps reads at 8 MiB and 30 seconds;
large nested evidence may require narrowing the gene/drug combination.

Inline results contain `native` and `_dgidb`; native partial errors remain under
`native.__errors`. The helper does not sort, deduplicate or cross-join evidence.
Each evidence query includes native `serviceInfo` alongside its interaction page.
It retains aggregate `publications`/`sources` plus the original claim-level
source, source version/license, publications, interaction types, attributes and
links. Claims without sources, repeated PMIDs and conflicting interaction types
stay distinguishable. A source-level citation is not automatically a publication
supporting every claim associated with that source.

`interactionScore`, `evidenceScore`, `geneSpecificity` and `drugSpecificity` remain
separate provider values. The [interaction-score documentation](https://dgidb.org/about/overview/interaction-score)
describes a research ranking that changes with source/publication coverage and
interaction partners. It is not an efficacy probability, treatment recommendation
or drug–drug interaction severity. Gene-level associations do not establish
variant-specific, indication-specific or patient-specific actionability.

Responses over 30 KiB use shared staging. Return any `__staged` handle directly,
then inspect it with `dgidb_get_schema` and `dgidb_query_data`; the lossless root
retains the native evidence and partial errors. A pre-staged GraphQL response is
returned as its handle rather than wrapped under `native`. Retain the query,
filters and observation metadata with every page. `_dgidb.complete` remains false,
and `_dgidb.release_pinned` remains false. `_dgidb.data_release` reflects the
provider's observed `serviceInfo.dataVersion` when available; `software_version`
is separate. A cursor, data-version label or set of import versions does not
prove an atomic database snapshot. Empty results do not prove absence of a drug
effect. `dgidb.metadata()` exposes the native service metadata for comparisons.
On September 30, software was `v.5.0.12` but the data label was `Dec-2023`.
Staged native payloads retain `serviceInfo`; recover and inspect it before joining
pages when a consistent observed version is required.

Provider field names and relationships were checked against the published
[interaction resolver](https://github.com/dgidb/dgidb-v5/blob/main/server/app/graphql/resolvers/interactions.rb),
[interaction type](https://github.com/dgidb/dgidb-v5/blob/main/server/app/graphql/types/interaction_type.rb)
and [claim type](https://github.com/dgidb/dgidb-v5/blob/main/server/app/graphql/types/interaction_claim_type.rb).
Local tests cover query contracts, bounded inputs, deadlines, partial failures,
claim preservation and shared staging. September 30 local MCP acceptance matched
seven identical native queries: metadata, two BRAF pages, drug-only and combined
gene/drug filters, an empty result and a 25-interaction page. All 74 claims in that
321,710-byte page were recovered exactly. Both SQL tool families recovered
60,000-character values; both schema aliases, raw introspection, dataset listing/
deletion, invalid inputs and the README recipe passed. Workspace routing retains
fixture coverage; this run did not start the separate workspace Worker.
Deployment remains pending.

## Passthrough and SQL response contracts

All six tools expose both `dgidb_` and `mcp_dgidb_` names, including
`graphql_query`, `query_sql`, `query_data` and `get_schema`. Both response fields
are returned on success and failure. The GraphQL passthrough uses the same
bounded HTTP reader as Code Mode, and all response types use full MCP-envelope UTF-8 size checks.
Large introspection responses and responses containing GraphQL errors follow
the shared staging path instead of bypassing it. `_staging` identifies the
lossless root payload, which retains the complete native envelope and errors.

GraphQL errors now set `isError: true` and `success: false`, including partial
results. `partial: true` indicates that some data accompanies those errors;
`error_count` reports the number of native errors. Inspect `data.errors` for
inline responses or the preserved staged envelope for large responses. A partial
result with data retains its citation; a pure error does not receive a data
citation. Do not treat a staging handle or an error as an empty evidence set.

`dgidb_query_sql` retains enhanced chunk-content retrieval and the optional
quality-analysis note, and now returns `structuredContent` as well as `content`.
Large SQL results receive a new staged handle. Existing dataset IDs continue to
work. The `/datasets` listing/deletion routes are admin-only: they require
`Authorization: Bearer <ADMIN_TOKEN>` (403 otherwise) and return 503 when the
`ADMIN_TOKEN` secret is not set. Nonempty
`params` are now rejected explicitly: the underlying enhanced-query endpoint
does not support parameter binding, and the old tool silently ignored them.
The separate workspace-aware `query_data` and `get_schema` tools retain their
workspace routing and per-session discovery behavior.
Large standard `query_data`/`get_schema` responses stage the complete original MCP
result in the server's local data namespace. Retrieve that new `data_access_id`
without a `workspace` argument. Raw GraphQL and legacy SQL staging retain their
original native-envelope/root-result shapes.

The data binding now follows the fleet convention: `DgidbDataDO` / `DGIDB_DATA_DO`.
The appended class-rename migration retains the existing namespace and leaves
historical migration entries intact. The old `JsonToSqlDO` export remains as an
alias for rollout compatibility. Deployment must account for Cloudflare's
[class-rename rollout sequence](https://developers.cloudflare.com/durable-objects/reference/durable-objects-migrations/#avoid-downtime-during-a-rename);
the local validation does not apply or verify the production migration.
For the first rollout, export both class names, bind `DGIDB_DATA_DO` to
`JsonToSqlDO`, and omit the appended rename step. Once that version is active,
deploy the checked-in binding and rename migration. Keep the old export until
the rename rollout completes.

<!-- license-and-data-terms:start -->
## License and data terms

The code in this repository is licensed under the Apache License 2.0 (see `LICENSE`).
If you use it in your work, please cite it (see `CITATION.cff`). Citation is requested, not required.

The license covers the code only. Data that this server returns comes from DGIdb v5 and stays under the terms of that source: Mixed per source; DrugBank-derived records non-commercial.
You are responsible for your use of the data under those terms.
<!-- license-and-data-terms:end -->

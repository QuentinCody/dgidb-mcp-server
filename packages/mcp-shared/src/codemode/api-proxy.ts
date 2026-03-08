/**
 * API Proxy source — pure JS injected into V8 isolates.
 *
 * Provides an `api` object with .get() and .post() methods that route
 * through the hidden __api_proxy tool back to the server's HTTP layer.
 * API keys never enter the isolate.
 *
 * Large responses (>30KB) are auto-staged into SQLite. When this happens,
 * the result has `__staged: true` with a `data_access_id` and `schema`.
 * Common data-access patterns (.results, .data, .entries, iteration) throw
 * a clear error directing the LLM to return the staging info and use query_data.
 */

/**
 * Returns the JS source string to inject into V8 isolates.
 * Relies on `codemode` proxy being available (from evaluator prefix).
 */
export function buildApiProxySource(): string {
	return `
// --- API proxy helpers (injected) ---

/** Wrap a staged response in a Proxy that gives clear errors on data access. */
function __wrapStaged(raw) {
  var msg = raw.message || "Response was auto-staged.";
  var hint = " Return this object and use the query_data tool with data_access_id=\\"" +
    raw.data_access_id + "\\" to query it with SQL.";
  var TRAP_KEYS = ["results", "data", "entries", "items", "records", "rows", "hits", "nodes", "edges"];
  return new Proxy(raw, {
    get: function(target, prop) {
      if (typeof prop === "string" && TRAP_KEYS.indexOf(prop) !== -1 && !(prop in target)) {
        throw new Error(msg + hint);
      }
      return target[prop];
    }
  });
}

var api = {
  /**
   * GET request. Path params are interpolated: api.get("/lookup/id/{id}", { id: "ENSG..." })
   * becomes GET /lookup/id/ENSG...
   * Extra params become query string parameters.
   *
   * If the response is large (>30KB), it is auto-staged into SQLite.
   * In that case the return value has __staged=true, data_access_id, and schema.
   * Return this object directly — the caller can use query_data to explore it.
   */
  get: async function(path, params) {
    var result = await codemode.__api_proxy({
      method: "GET",
      path: path,
      params: params || {},
    });
    if (result && result.__api_error) {
      var err = new Error("API error " + result.status + ": " + (result.message || "Unknown error"));
      err.status = result.status;
      err.data = result.data;
      throw err;
    }
    if (result && result.__staged) {
      return __wrapStaged(result);
    }
    return result;
  },

  /**
   * POST request with JSON body.
   * Same staging behavior as api.get() for large responses.
   */
  post: async function(path, body, params) {
    var result = await codemode.__api_proxy({
      method: "POST",
      path: path,
      params: params || {},
      body: body,
    });
    if (result && result.__api_error) {
      var err = new Error("API error " + result.status + ": " + (result.message || "Unknown error"));
      err.status = result.status;
      err.data = result.data;
      throw err;
    }
    if (result && result.__staged) {
      return __wrapStaged(result);
    }
    return result;
  },
};
// --- End API proxy helpers ---
`;
}

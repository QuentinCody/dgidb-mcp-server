/**
 * OpenAPI spec resolver — resolves $ref references and converts Swagger 2.0
 * to OpenAPI 3.0 format.
 *
 * Used to produce a self-contained, reference-free JSON spec that can be
 * injected into V8 isolates for Code Mode search tools.
 */

export interface ResolveOptions {
	/** Remove x-* extension fields from the output */
	stripExtensions?: boolean;
	/** Remove example/examples fields from the output */
	stripExamples?: boolean;
}

export interface ResolvedSpec {
	openapi: string;
	info: { title: string; version: string; [k: string]: unknown };
	servers?: Array<{ url: string; [k: string]: unknown }>;
	paths: Record<string, Record<string, unknown>>;
}

/**
 * Follow a JSON pointer path (e.g., "#/components/schemas/Study") through
 * the root document and return the referenced value.
 */
function followRef(root: unknown, refPath: string): unknown {
	if (!refPath.startsWith("#/")) {
		throw new Error(`Cannot resolve external $ref: ${refPath}`);
	}

	const segments = refPath.slice(2).split("/");
	let current: unknown = root;

	for (const segment of segments) {
		// JSON Pointer decoding: ~1 → /, ~0 → ~
		const decoded = segment.replace(/~1/g, "/").replace(/~0/g, "~");
		if (current === null || current === undefined || typeof current !== "object") {
			throw new Error(`Cannot resolve $ref "${refPath}": path segment "${decoded}" not found`);
		}
		current = (current as Record<string, unknown>)[decoded];
	}

	if (current === undefined) {
		throw new Error(`Cannot resolve $ref "${refPath}": target not found`);
	}

	return current;
}

/**
 * Recursively resolve all $ref references in an object tree.
 *
 * Handles circular references by tracking visited ref paths and stopping
 * recursion when a cycle is detected.
 */
function resolveRefs(
	node: unknown,
	root: unknown,
	options: ResolveOptions,
	visited: Set<string>,
): unknown {
	if (node === null || node === undefined) return node;
	if (typeof node !== "object") return node;

	// Handle arrays
	if (Array.isArray(node)) {
		return node.map((item) => resolveRefs(item, root, options, visited));
	}

	// Handle objects
	const obj = node as Record<string, unknown>;

	// If this object is a $ref, resolve it
	if (typeof obj["$ref"] === "string") {
		const refPath = obj["$ref"] as string;

		// Guard against circular references
		if (visited.has(refPath)) {
			// Return a placeholder for circular refs
			return { _circular_ref: refPath };
		}

		const newVisited = new Set(visited);
		newVisited.add(refPath);

		const referenced = followRef(root, refPath);
		return resolveRefs(referenced, root, options, newVisited);
	}

	// Regular object — recurse into all properties
	const result: Record<string, unknown> = {};

	for (const [key, value] of Object.entries(obj)) {
		// Strip x-* extensions if configured
		if (options.stripExtensions && key.startsWith("x-")) {
			continue;
		}

		// Strip examples if configured
		if (options.stripExamples && (key === "example" || key === "examples")) {
			continue;
		}

		result[key] = resolveRefs(value, root, options, visited);
	}

	return result;
}

/**
 * Convert a Swagger 2.0 spec to OpenAPI 3.0 format.
 *
 * Only handles the top-level structural conversion (host/basePath/schemes → servers).
 * Parameter format differences are minimal enough to leave as-is for search purposes.
 */
function convertSwagger20(spec: Record<string, unknown>): Record<string, unknown> {
	const result: Record<string, unknown> = { ...spec };

	// Set OpenAPI version
	result.openapi = "3.0.0";
	delete result.swagger;

	// Convert host + basePath + schemes → servers
	const host = spec.host as string | undefined;
	const basePath = (spec.basePath as string) || "";
	const schemes = (spec.schemes as string[]) || ["https"];

	if (host) {
		const scheme = schemes[0] || "https";
		result.servers = [{ url: `${scheme}://${host}${basePath}` }];
	}

	// Remove Swagger 2.0-specific fields
	delete result.host;
	delete result.basePath;
	delete result.schemes;
	delete result.produces;
	delete result.consumes;

	return result;
}

/**
 * Resolve an OpenAPI/Swagger spec by inlining all $ref references.
 *
 * Supports:
 * - OpenAPI 3.0.x specs with $ref in parameters, schemas, responses
 * - Swagger 2.0 specs (auto-converted to OpenAPI 3.0 format)
 * - Nested and chained $ref resolution
 * - Circular reference detection
 * - Optional stripping of x-* extensions and examples
 *
 * @throws Error if a $ref cannot be resolved
 */
export function resolveOpenApiSpec(raw: unknown, options?: ResolveOptions): ResolvedSpec {
	const opts: ResolveOptions = options || {};

	if (raw === null || raw === undefined || typeof raw !== "object") {
		throw new Error("Invalid OpenAPI spec: must be an object");
	}

	let spec = raw as Record<string, unknown>;

	// Detect Swagger 2.0 and convert
	if (spec.swagger && typeof spec.swagger === "string" && spec.swagger.startsWith("2.")) {
		spec = convertSwagger20(spec);
	}

	// Ensure we have paths
	if (!spec.paths || typeof spec.paths !== "object") {
		spec = { ...spec, paths: {} };
	}

	// Resolve all $ref references (use original raw as the lookup root
	// since $refs point into the original document structure)
	const resolved = resolveRefs(spec, raw, opts, new Set()) as Record<string, unknown>;

	// Build the ResolvedSpec
	const info = (resolved.info || { title: "Unknown", version: "0.0" }) as ResolvedSpec["info"];
	const servers = resolved.servers as ResolvedSpec["servers"];
	const paths = (resolved.paths || {}) as ResolvedSpec["paths"];
	const openapi = (resolved.openapi || "3.0.0") as string;

	return {
		openapi,
		info,
		...(servers ? { servers } : {}),
		paths,
	};
}

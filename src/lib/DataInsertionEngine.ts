import { TableSchema } from "./types";
import { ChunkingEngine } from "./ChunkingEngine";

export class DataInsertionEngine {
	private chunkingEngine = new ChunkingEngine();
	private processedEntities: Map<string, Map<any, number | string>> = new Map();
	private relationshipData: Map<string, Set<string>> = new Map(); // Track actual relationships found in data

	async insertData(data: any, schemas: Record<string, TableSchema>, sql: any): Promise<void> {
		// Reset state for new insertion
		this.processedEntities.clear();
		this.relationshipData.clear();

		const schemaNames = Object.keys(schemas);

		// Check if this is one of the simple fallback schemas
		if (schemaNames.length === 1 && (schemaNames[0] === 'scalar_data' || schemaNames[0] === 'array_data' || schemaNames[0] === 'root_object')) {
			const tableName = schemaNames[0];
			const schema = schemas[tableName];
			if (tableName === 'scalar_data' || tableName === 'root_object') {
				await this.insertSimpleRow(data, tableName, schema, sql);
			} else { // array_data
				if (Array.isArray(data)) {
					for (const item of data) {
						await this.insertSimpleRow(item, tableName, schema, sql);
					}
				} else {
					await this.insertSimpleRow(data, tableName, schema, sql); 
				}
			}
			return;
		}

		// Phase 1: Insert all entities first (to establish primary keys)
		await this.insertAllEntities(data, schemas, sql);
		
		// Phase 2: Handle relationships via junction tables (only for tables with data)
		await this.insertJunctionTableRecords(data, schemas, sql);
	}

	private async insertAllEntities(obj: any, schemas: Record<string, TableSchema>, sql: any, path: string[] = []): Promise<void> {
		if (!obj || typeof obj !== 'object') return;

		// Handle arrays
		if (Array.isArray(obj)) {
			for (const item of obj) {
				await this.insertAllEntities(item, schemas, sql, path);
			}
			return;
		}

		// Handle GraphQL edges pattern
		if (obj.edges && Array.isArray(obj.edges)) {
			const nodes = obj.edges.map((edge: any) => edge.node).filter(Boolean);
			for (const node of nodes) {
				await this.insertAllEntities(node, schemas, sql, path);
			}
			return;
		}

		// CHILDREN FIRST: Recursively process all nested values before this entity.
		// This ensures child entities (drug, interactionType) are in processedEntities
		// before their parent (interaction) tries to resolve foreign keys.
		for (const [key, value] of Object.entries(obj)) {
			if (value && typeof value === 'object') {
				await this.insertAllEntities(value, schemas, sql, [...path, key]);
			}
		}

		// THEN insert this entity (all nested entities are now available for FK resolution)
		if (this.isEntity(obj)) {
			const entityType = this.inferEntityType(obj, path);
			if (schemas[entityType]) {
				const entityId = await this.insertEntityRecord(obj, entityType, schemas[entityType], sql);

				// Track relationships for junction tables
				if (entityId !== null) {
					this.trackEntityRelationships(obj, entityType, entityId, schemas);
				}
			}
		}
	}

	private trackEntityRelationships(entity: any, entityType: string, entityId: number | string, schemas: Record<string, TableSchema>): void {
		for (const [key, value] of Object.entries(entity)) {
			// Unwrap {nodes: [...]} or {edges: [{node: ...}]} wrappers, or use array directly
			let items: any[] | null = null;
			if (Array.isArray(value) && value.length > 0) {
				items = value;
			} else if (value && typeof value === 'object' && !Array.isArray(value)) {
				const wrapper = value as Record<string, any>;
				if (wrapper.nodes && Array.isArray(wrapper.nodes)) {
					items = wrapper.nodes;
				} else if (wrapper.edges && Array.isArray(wrapper.edges)) {
					items = wrapper.edges.map((e: any) => e.node).filter(Boolean);
				}
			}
			// 1:1 nested entities (single objects) use direct FK columns — no junction table needed.
			if (!items || items.length === 0) continue;

			const firstItem = items.find(item => this.isEntity(item));
			if (!firstItem) continue;

			const relatedType = this.inferEntityType(firstItem, [key]);
			const junctionName = [entityType, relatedType].sort().join('_');
			if (!schemas[junctionName]) continue;

			const pairs = this.relationshipData.get(junctionName) || new Set<string>();
			for (const item of items) {
				if (!this.isEntity(item)) continue;
				const relatedId = this.getEntityId(item, relatedType);
				if (this.isValidId(entityId) && this.isValidId(relatedId)) {
					const [sortedType1] = [entityType, relatedType].sort();
					const id1 = sortedType1 === entityType ? entityId : relatedId;
					const id2 = sortedType1 === entityType ? relatedId : entityId;
					pairs.add(`${id1}::${id2}`);
				}
			}
			this.relationshipData.set(junctionName, pairs);
		}
	}
	
	private async insertEntityRecord(entity: any, tableName: string, schema: TableSchema, sql: any): Promise<number | string | null> {
		// Check if this entity was already processed
		const entityMap = this.processedEntities.get(tableName) || new Map();
		if (entityMap.has(entity)) {
			return entityMap.get(entity)!;
		}
		
		const rowData = await this.mapEntityToSchema(entity, schema, sql);
		if (Object.keys(rowData).length === 0) return null;
		
		const columns = Object.keys(rowData);
		const placeholders = columns.map(() => '?').join(', ');
		const values = Object.values(rowData);

		let insertedId: number | string | null = null;

		if (entity.id && (typeof entity.id === 'string' || typeof entity.id === 'number')) {
			// Entity has its own ID — use it directly
			insertedId = entity.id;
			const insertSQL = `INSERT OR REPLACE INTO ${tableName} (${columns.join(', ')}) VALUES (${placeholders})`;
			sql.exec(insertSQL, ...values);
		} else {
			// Auto-increment ID — insert then get last_insert_rowid()
			const insertSQL = `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${placeholders})`;
			sql.exec(insertSQL, ...values);
			try {
				const idRow = sql.exec(`SELECT last_insert_rowid() as lid`).toArray();
				insertedId = idRow[0]?.lid ?? null;
			} catch {
				insertedId = null;
			}
		}

		// Track this entity by object reference for FK resolution
		if (insertedId !== null) {
			entityMap.set(entity, insertedId);
			this.processedEntities.set(tableName, entityMap);
		}

		return insertedId;
	}
	
	private async insertJunctionTableRecords(data: any, schemas: Record<string, TableSchema>, sql: any): Promise<void> {
		for (const [junctionName, pairs] of this.relationshipData.entries()) {
			if (!schemas[junctionName]) continue;
			// Junction table name is sorted: "drug_interaction" → columns are drug_id, interaction_id
			const columns = Object.keys(schemas[junctionName].columns).filter(c => c.endsWith('_id'));
			if (columns.length < 2) continue;

			for (const pairKey of pairs) {
				const [id1Str, id2Str] = pairKey.split('::');
				const id1 = isNaN(Number(id1Str)) ? id1Str : Number(id1Str);
				const id2 = isNaN(Number(id2Str)) ? id2Str : Number(id2Str);
				const insertSQL = `INSERT OR IGNORE INTO ${junctionName} (${columns[0]}, ${columns[1]}) VALUES (?, ?)`;
				sql.exec(insertSQL, id1, id2);
			}
		}
	}
	
	private getEntityId(entity: any, entityType: string): number | string | null {
		const entityMap = this.processedEntities.get(entityType);
		return entityMap?.get(entity) ?? null;
	}
	
	private isValidId(id: number | string | null): boolean {
		// Consider an ID valid if it's a non-null number or non-empty string
		// Exclude placeholder values like 'null' strings
		return id !== null && id !== undefined && id !== '' && id !== 'null';
	}
	
	private async mapEntityToSchema(obj: any, schema: TableSchema, sql: any): Promise<any> {
		const rowData: any = {};

		if (!obj || typeof obj !== 'object') {
			if (schema.columns.value) rowData.value = obj;
			return rowData;
		}

		for (const columnName of Object.keys(schema.columns)) {
			if (columnName === 'id' && schema.columns[columnName].includes('AUTOINCREMENT')) {
				continue;
			}

			let value = null;
			let found = false;

			// 1. Handle JSON columns with chunking
			if (columnName.endsWith('_json')) {
				const baseKey = columnName.slice(0, -5);
				const originalKey = this.findOriginalKey(obj, baseKey);
				if (originalKey && obj[originalKey] && typeof obj[originalKey] === 'object') {
					value = await this.chunkingEngine.smartJsonStringify(obj[originalKey], sql);
					found = true;
				}
			}

			// 2. Try direct column match FIRST (handles camelCase→snake_case like interactionScore→interaction_score)
			if (!found) {
				const originalKey = this.findOriginalKey(obj, columnName);
				if (originalKey && obj[originalKey] !== undefined) {
					value = obj[originalKey];
					if (typeof value === 'boolean') value = value ? 1 : 0;
					// Skip arrays of entities (handled via junction tables)
					if (Array.isArray(value) && value.length > 0 && this.isEntity(value[0])) {
						continue;
					}
					// Skip nested entity objects (handled via foreign keys)
					if (value && typeof value === 'object' && !Array.isArray(value) && this.isEntity(value)) {
						value = null;
					} else {
						found = true;
					}
				}
			}

			// 3. Handle foreign key columns — look up the related entity's database-assigned ID
			if (!found && columnName.endsWith('_id')) {
				const baseKey = columnName.slice(0, -3);
				const originalKey = this.findOriginalKey(obj, baseKey);
				if (originalKey && obj[originalKey] && typeof obj[originalKey] === 'object') {
					const nestedEntity = obj[originalKey];
					// First check if we already inserted this entity and have its DB id
					for (const [tableName, entityMap] of this.processedEntities.entries()) {
						if (entityMap.has(nestedEntity)) {
							value = entityMap.get(nestedEntity)!;
							found = true;
							break;
						}
					}
					// Fall back to the entity's own id field
					if (!found && nestedEntity.id !== undefined) {
						value = nestedEntity.id;
						found = true;
					}
				}
			}

			// 4. Try nested field extraction (prefix_subfield from prefix.subfield)
			if (!found && columnName.includes('_') && !columnName.endsWith('_json') && !columnName.endsWith('_id')) {
				const parts = columnName.split('_');
				for (let splitPoint = 1; splitPoint < parts.length; splitPoint++) {
					const baseKey = parts.slice(0, splitPoint).join('_');
					const subKey = parts.slice(splitPoint).join('_');
					const originalKey = this.findOriginalKey(obj, baseKey);
					if (originalKey && obj[originalKey] && typeof obj[originalKey] === 'object' && !Array.isArray(obj[originalKey])) {
						const nestedObj = obj[originalKey];
						const originalSubKey = this.findOriginalKey(nestedObj, subKey);
						if (originalSubKey && nestedObj[originalSubKey] !== undefined) {
							value = nestedObj[originalSubKey];
							if (typeof value === 'boolean') value = value ? 1 : 0;
							found = true;
							break;
						}
					}
				}
			}

			if (found && value !== null && value !== undefined) {
				rowData[columnName] = value;
			}
		}

		return rowData;
	}
	
	// Entity detection and type inference (reuse logic from SchemaInferenceEngine)
	private isEntity(obj: any): boolean {
		if (!obj || typeof obj !== 'object' || Array.isArray(obj)) return false;

		const keys = Object.keys(obj);
		const fieldCount = keys.length;
		if (fieldCount < 2) return false;

		if (obj.id !== undefined || obj._id !== undefined) return true;

		if (obj.name !== undefined || obj.title !== undefined ||
			obj.description !== undefined || obj.type !== undefined) return true;

		// Any object with 2+ fields where at least one is a scalar value
		const hasScalarField = keys.some(key => {
			const value = obj[key];
			return value !== null && value !== undefined && typeof value !== 'object';
		});
		return hasScalarField;
	}
	
	private inferEntityType(obj: any, path: string[]): string {
		// Must match SchemaInferenceEngine.inferEntityType exactly for junction table consistency
		if (obj.__typename) return this.sanitizeTableName(obj.__typename);

		if (path.length > 0) {
			let lastName = path[path.length - 1];

			// Handle GraphQL patterns (matches SchemaInferenceEngine)
			if ((lastName === 'node' || lastName === 'nodes') && path.length > 1) {
				lastName = path[path.length - 2];
				if (lastName === 'edges' && path.length > 2) {
					lastName = path[path.length - 3];
				}
			} else if (lastName === 'edges' && path.length > 1) {
				lastName = path[path.length - 2];
			}

			// Sanitize THEN singularize (matches SchemaInferenceEngine order)
			const sanitized = this.sanitizeTableName(lastName);
			if (sanitized.endsWith('ies')) {
				return sanitized.slice(0, -3) + 'y';
			} else if (sanitized.endsWith('s') && !sanitized.endsWith('ss') && sanitized.length > 1) {
				const potentialSingular = sanitized.slice(0, -1);
				if (potentialSingular.length > 1) return potentialSingular;
			}
			return sanitized;
		}

		return 'entity_' + Math.random().toString(36).substr(2, 9);
	}
	
	private sanitizeTableName(name: string): string {
		if (!name || typeof name !== 'string') {
			return 'table_' + Math.random().toString(36).substr(2, 9);
		}
		
		let sanitized = name
			.replace(/[^a-zA-Z0-9_]/g, '_')
			.replace(/_{2,}/g, '_')  // Replace multiple underscores with single
			.replace(/^_|_$/g, '')  // Remove leading/trailing underscores
			.toLowerCase();
		
		// Ensure it doesn't start with a number
		if (/^[0-9]/.test(sanitized)) {
			sanitized = 'table_' + sanitized;
		}
		
		// Ensure it's not empty and not a SQL keyword
		if (!sanitized || sanitized.length === 0) {
			sanitized = 'table_' + Math.random().toString(36).substr(2, 9);
		}
		
		// Handle SQL reserved words
		const reservedWords = ['table', 'index', 'view', 'column', 'primary', 'key', 'foreign', 'constraint'];
		if (reservedWords.includes(sanitized)) {
			sanitized = sanitized + '_table';
		}
		
		return sanitized;
	}
	
	private findOriginalKey(obj: any, sanitizedKey: string): string | null {
		const keys = Object.keys(obj);
		
		// Direct match
		if (keys.includes(sanitizedKey)) return sanitizedKey;
		
		// Find key that sanitizes to the same value
		return keys.find(key => 
			this.sanitizeColumnName(key) === sanitizedKey
		) || null;
	}
	
	private sanitizeColumnName(name: string): string {
		if (!name || typeof name !== 'string') {
			return 'column_' + Math.random().toString(36).substr(2, 9);
		}
		
		// Convert camelCase to snake_case
		let snakeCase = name
			.replace(/([A-Z])/g, '_$1')
			.toLowerCase()
			.replace(/[^a-zA-Z0-9_]/g, '_')
			.replace(/_{2,}/g, '_')  // Replace multiple underscores with single
			.replace(/^_|_$/g, ''); // Remove leading/trailing underscores
		
		// Ensure it doesn't start with a number
		if (/^[0-9]/.test(snakeCase)) {
			snakeCase = 'col_' + snakeCase;
		}
		
		// Ensure it's not empty
		if (!snakeCase || snakeCase.length === 0) {
			snakeCase = 'column_' + Math.random().toString(36).substr(2, 9);
		}
		
		// Handle common DGIdb abbreviations properly
		const dgidbTerms: Record<string, string> = {
			'entrezid': 'entrez_id',
			'displayname': 'display_name',
			'conceptid': 'concept_id',
			'drugname': 'drug_name',
			'genename': 'gene_name',
			'interactiontype': 'interaction_type',
			'sourcetype': 'source_type',
			'sourcedb': 'source_db',
			'pmids': 'pmids'
		};
		
		const result = dgidbTerms[snakeCase] || snakeCase;
		
		// Handle SQL reserved words
		const reservedWords = ['table', 'index', 'view', 'column', 'primary', 'key', 'foreign', 'constraint', 'order', 'group', 'select', 'from', 'where'];
		if (reservedWords.includes(result)) {
			return result + '_col';
		}
		
		return result;
	}

	private async insertSimpleRow(obj: any, tableName: string, schema: TableSchema, sql: any): Promise<void> {
		const rowData = await this.mapObjectToSimpleSchema(obj, schema, sql);
		if (Object.keys(rowData).length === 0 && !(tableName === 'scalar_data' && obj === null)) return; // Allow inserting null for scalar_data

		const columns = Object.keys(rowData);
		const placeholders = columns.map(() => '?').join(', ');
		const values = Object.values(rowData);

		const insertSQL = `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${placeholders})`;
		sql.exec(insertSQL, ...values);
	}

	private async mapObjectToSimpleSchema(obj: any, schema: TableSchema, sql: any): Promise<any> {
		const rowData: any = {};

		if (obj === null || typeof obj !== 'object') {
			if (schema.columns.value) { // For scalar_data or array_data of primitives
				rowData.value = obj;
			} else if (Object.keys(schema.columns).length > 0) {
				// This case should ideally not be hit if schema generation is right for primitives
				// but as a fallback, if there's a column, try to put it there.
				const firstCol = Object.keys(schema.columns)[0];
				rowData[firstCol] = obj;
			}
			return rowData;
		}

		for (const columnName of Object.keys(schema.columns)) {
			let valueToInsert = undefined;
			let originalKeyFound = false;

			if (columnName.endsWith('_json')) {
				const baseKey = columnName.slice(0, -5);
				const originalKey = this.findOriginalKey(obj, baseKey);
				if (originalKey && obj[originalKey] !== undefined) {
					valueToInsert = await this.chunkingEngine.smartJsonStringify(obj[originalKey], sql);
					originalKeyFound = true;
				}
			} else {
				const originalKey = this.findOriginalKey(obj, columnName);
				if (originalKey && obj[originalKey] !== undefined) {
					const val = obj[originalKey];
					if (typeof val === 'boolean') {
						valueToInsert = val ? 1 : 0;
					} else if (typeof val === 'object' && val !== null) {
						// This should not happen if schema is from extractSimpleFields, which JSONifies nested objects.
						// If it does, it implies a mismatch. For safety, try to JSON stringify.
						valueToInsert = await this.chunkingEngine.smartJsonStringify(val, sql);
					} else {
						valueToInsert = val;
					}
					originalKeyFound = true;
				}
			}

			if (originalKeyFound && valueToInsert !== undefined) {
				rowData[columnName] = valueToInsert;
			} else if (obj.hasOwnProperty(columnName) && obj[columnName] !== undefined){ // Direct match as last resort
				// This handles cases where sanitized names might not be used or `findOriginalKey` fails but direct prop exists
				const val = obj[columnName];
				if (typeof val === 'boolean') valueToInsert = val ? 1:0;
				else if (typeof val === 'object' && val !== null) valueToInsert = await this.chunkingEngine.smartJsonStringify(val, sql);
				else valueToInsert = val;
				rowData[columnName] = valueToInsert;
			}
		}
		return rowData;
	}
} 
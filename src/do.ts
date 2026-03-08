import { RestStagingDO } from "@bio-mcp/shared/staging/rest-staging-do";
import { DGIDB_CONFIG } from "@bio-mcp/shared/staging/domain-config";

export class JsonToSqlDO extends RestStagingDO {
	protected useConsolidatedEngine() {
		return true;
	}
	protected getDomainConfig() {
		return DGIDB_CONFIG;
	}
}

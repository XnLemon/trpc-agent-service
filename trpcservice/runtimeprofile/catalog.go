package runtimeprofile

// CatalogEntry describes one platform-maintained runtime kind and schema.
type CatalogEntry struct { Kind string; Version string; ExecutionMode string; Capabilities []string; GovernanceMode string }

// BuiltinCatalog returns the immutable platform catalog. Configuration for
// composition is interpreted by the selected implementation; arbitrary code
// references are never accepted as a kind.
func BuiltinCatalog() []CatalogEntry { return []CatalogEntry{
	{Kind:"builtin-llm",Version:"v1",ExecutionMode:"builtin",Capabilities:[]string{"text","tool"},GovernanceMode:"full"},
	{Kind:"builtin-chain",Version:"v1",ExecutionMode:"builtin",Capabilities:[]string{"text","composition"},GovernanceMode:"full"},
	{Kind:"builtin-parallel",Version:"v1",ExecutionMode:"builtin",Capabilities:[]string{"text","composition"},GovernanceMode:"full"},
	{Kind:"builtin-cycle",Version:"v1",ExecutionMode:"builtin",Capabilities:[]string{"text","composition"},GovernanceMode:"full"},
	{Kind:"builtin-graph",Version:"v1",ExecutionMode:"builtin",Capabilities:[]string{"text","composition"},GovernanceMode:"full"},
} }

package schedulerapi

// Cluster is the gateway-facing view returned by scheduler cluster APIs.
type Cluster struct {
	ClusterID         string `json:"cluster_id"`
	ClusterGatewayURL string `json:"cluster_gateway_url"`
	Enabled           bool   `json:"enabled"`
}

// ListClustersResponse is the scheduler response body for cluster listing.
type ListClustersResponse struct {
	Clusters []Cluster `json:"clusters"`
	Count    int       `json:"count"`
}

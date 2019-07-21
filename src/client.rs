use elastic::prelude::*;

///
/// Configs to setup Elasticsearch client.
pub struct EsClient {
    pub client: SyncClient,
}

impl EsClient {
    ///
    /// Returns EsClient with inner client connected to specified es_host.
    /// 
    /// # Arguments
    ///
    /// * `es_host` - Elasticsearch host:port pair to setup sync client.
    pub fn new(es_host: &str) -> EsClient {
        let client = SyncClientBuilder::new()
            .static_node(es_host)
            .params_fluent(|p| p.url_param("pretty", true))
            .build()
            .unwrap();

        EsClient { client }
    }
}



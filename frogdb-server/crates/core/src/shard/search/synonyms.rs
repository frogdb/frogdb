use bytes::Bytes;
use frogdb_protocol::Response;

use super::super::worker::ShardWorker;

impl ShardWorker {
    pub(crate) async fn execute_ft_synupdate(
        &mut self,
        index_name: &Bytes,
        group_id: &Bytes,
        terms: &[Bytes],
    ) -> Vec<(Bytes, Response)> {
        let name = std::str::from_utf8(index_name).unwrap_or("");
        let name = self.resolve_index_name(name).to_string();
        let gid = std::str::from_utf8(group_id).unwrap_or("");

        let idx = match self.search.indexes.get_mut(&name) {
            Some(idx) => idx,
            None => {
                return vec![(
                    Bytes::from_static(b"__ft_synupdate__"),
                    Response::error("Unknown index name"),
                )];
            }
        };

        let term_strings: Vec<String> = terms
            .iter()
            .map(|t| std::str::from_utf8(t).unwrap_or("").to_string())
            .collect();

        idx.def.synonym_groups.insert(gid.to_string(), term_strings);

        // Persist updated definition to RocksDB
        if let Some(ref rocks) = self.persistence.rocks_store
            && let Ok(json) = serde_json::to_vec(&idx.def)
            && let Err(e) = rocks.put_search_meta(self.identity.shard_id, name.as_bytes(), &json)
        {
            tracing::error!(error = %e, "Failed to persist synonym update");
        }

        vec![(Bytes::from_static(b"__ft_synupdate__"), Response::ok())]
    }

    pub(crate) fn execute_ft_syndump(&self, index_name: &Bytes) -> Vec<(Bytes, Response)> {
        let name = std::str::from_utf8(index_name).unwrap_or("");
        let name = self.resolve_index_name(name);
        let idx = match self.search.indexes.get(name) {
            Some(idx) => idx,
            None => {
                return vec![(
                    Bytes::from_static(b"__ft_syndump__"),
                    Response::error("Unknown index name"),
                )];
            }
        };

        // RediSearch SYNDUMP format: alternating term, [group_id, ...] pairs
        // For each term in each group, emit the term followed by the group_id(s)
        let mut entries = Vec::new();
        for (group_id, terms) in &idx.def.synonym_groups {
            for term in terms {
                entries.push(Response::bulk(Bytes::from(term.clone())));
                entries.push(Response::Array(vec![Response::bulk(Bytes::from(
                    group_id.clone(),
                ))]));
            }
        }

        vec![(
            Bytes::from_static(b"__ft_syndump__"),
            Response::Array(entries),
        )]
    }
}

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
        let tag = Bytes::from_static(b"__ft_synupdate__");
        let name = std::str::from_utf8(index_name).unwrap_or("");
        let gid = std::str::from_utf8(group_id).unwrap_or("");
        let term_strings: Vec<String> = terms
            .iter()
            .map(|t| std::str::from_utf8(t).unwrap_or("").to_string())
            .collect();

        let resp = match self.search.synupdate(name, gid, term_strings) {
            Ok(()) => Response::ok(),
            Err(_) => Response::error("Unknown index name"),
        };
        vec![(tag, resp)]
    }

    pub(crate) fn execute_ft_syndump(&self, index_name: &Bytes) -> Vec<(Bytes, Response)> {
        let name = std::str::from_utf8(index_name).unwrap_or("");
        let name = self.search.resolve_index_name(name);
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

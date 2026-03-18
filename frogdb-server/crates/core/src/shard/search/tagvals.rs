use bytes::Bytes;
use frogdb_protocol::Response;

use super::super::worker::ShardWorker;

impl ShardWorker {
    pub(crate) fn execute_ft_tagvals(
        &self,
        index_name: &Bytes,
        field_name: &Bytes,
    ) -> Vec<(Bytes, Response)> {
        let name = std::str::from_utf8(index_name).unwrap_or("");
        let resolved = self.resolve_index_name(name);
        let field = std::str::from_utf8(field_name).unwrap_or("");

        let idx = match self.search.indexes.get(resolved) {
            Some(idx) => idx,
            None => {
                return vec![(
                    Bytes::from_static(b"__ft_tagvals__"),
                    Response::error(format!("{}: no such index", name)),
                )];
            }
        };

        match idx.tag_values(field) {
            Ok(values) => {
                let items: Vec<Response> = values
                    .into_iter()
                    .map(|v| Response::bulk(Bytes::from(v)))
                    .collect();
                vec![(
                    Bytes::from_static(b"__ft_tagvals__"),
                    Response::Array(items),
                )]
            }
            Err(e) => vec![(
                Bytes::from_static(b"__ft_tagvals__"),
                Response::error(format!("{}", e)),
            )],
        }
    }
}

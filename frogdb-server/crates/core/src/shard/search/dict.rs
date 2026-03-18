use bytes::Bytes;
use frogdb_protocol::Response;

use super::super::worker::ShardWorker;

impl ShardWorker {
    pub(crate) fn execute_ft_dictadd(
        &mut self,
        dict_name: &Bytes,
        terms: &[Bytes],
    ) -> Vec<(Bytes, Response)> {
        let name = std::str::from_utf8(dict_name).unwrap_or("");
        let dict = self
            .search
            .dictionaries
            .entry(name.to_string())
            .or_default();
        let mut added = 0i64;
        for term in terms {
            let t = std::str::from_utf8(term).unwrap_or("").to_string();
            if dict.insert(t) {
                added += 1;
            }
        }
        self.persist_dict(name);
        vec![(
            Bytes::from_static(b"__ft_dictadd__"),
            Response::Integer(added),
        )]
    }

    pub(crate) fn execute_ft_dictdel(
        &mut self,
        dict_name: &Bytes,
        terms: &[Bytes],
    ) -> Vec<(Bytes, Response)> {
        let name = std::str::from_utf8(dict_name).unwrap_or("");
        let dict = match self.search.dictionaries.get_mut(name) {
            Some(d) => d,
            None => {
                return vec![(Bytes::from_static(b"__ft_dictdel__"), Response::Integer(0))];
            }
        };
        let mut removed = 0i64;
        for term in terms {
            let t = std::str::from_utf8(term).unwrap_or("");
            if dict.remove(t) {
                removed += 1;
            }
        }
        self.persist_dict(name);
        vec![(
            Bytes::from_static(b"__ft_dictdel__"),
            Response::Integer(removed),
        )]
    }

    pub(crate) fn execute_ft_dictdump(&self, dict_name: &Bytes) -> Vec<(Bytes, Response)> {
        let name = std::str::from_utf8(dict_name).unwrap_or("");
        let items = match self.search.dictionaries.get(name) {
            Some(dict) => {
                let mut terms: Vec<&String> = dict.iter().collect();
                terms.sort();
                terms
                    .into_iter()
                    .map(|t| Response::bulk(Bytes::from(t.clone())))
                    .collect()
            }
            None => vec![],
        };
        vec![(
            Bytes::from_static(b"__ft_dictdump__"),
            Response::Array(items),
        )]
    }
}

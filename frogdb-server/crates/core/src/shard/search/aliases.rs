use bytes::Bytes;
use frogdb_protocol::Response;

use super::super::worker::ShardWorker;

impl ShardWorker {
    pub(crate) fn execute_ft_aliasadd(
        &mut self,
        alias_name: &Bytes,
        index_name: &Bytes,
    ) -> Vec<(Bytes, Response)> {
        let alias = std::str::from_utf8(alias_name).unwrap_or("");
        let index = std::str::from_utf8(index_name).unwrap_or("");

        // Check that the target index exists
        if !self.search.indexes.contains_key(index) {
            return vec![(
                Bytes::from_static(b"__ft_aliasadd__"),
                Response::error(format!("{}: no such index", index)),
            )];
        }

        // Check alias doesn't already exist
        if self.search.aliases.contains_key(alias) {
            return vec![(
                Bytes::from_static(b"__ft_aliasadd__"),
                Response::error("Alias already exists"),
            )];
        }

        self.search
            .aliases
            .insert(alias.to_string(), index.to_string());
        self.persist_aliases();

        vec![(Bytes::from_static(b"__ft_aliasadd__"), Response::ok())]
    }

    pub(crate) fn execute_ft_aliasdel(&mut self, alias_name: &Bytes) -> Vec<(Bytes, Response)> {
        let alias = std::str::from_utf8(alias_name).unwrap_or("");

        if self.search.aliases.remove(alias).is_none() {
            return vec![(
                Bytes::from_static(b"__ft_aliasdel__"),
                Response::error("Alias does not exist"),
            )];
        }
        self.persist_aliases();

        vec![(Bytes::from_static(b"__ft_aliasdel__"), Response::ok())]
    }

    pub(crate) fn execute_ft_aliasupdate(
        &mut self,
        alias_name: &Bytes,
        index_name: &Bytes,
    ) -> Vec<(Bytes, Response)> {
        let alias = std::str::from_utf8(alias_name).unwrap_or("");
        let index = std::str::from_utf8(index_name).unwrap_or("");

        // Check that the target index exists
        if !self.search.indexes.contains_key(index) {
            return vec![(
                Bytes::from_static(b"__ft_aliasupdate__"),
                Response::error(format!("{}: no such index", index)),
            )];
        }

        self.search
            .aliases
            .insert(alias.to_string(), index.to_string());
        self.persist_aliases();

        vec![(Bytes::from_static(b"__ft_aliasupdate__"), Response::ok())]
    }
}

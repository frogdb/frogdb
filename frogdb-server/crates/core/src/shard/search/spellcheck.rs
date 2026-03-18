use bytes::Bytes;
use frogdb_protocol::Response;

use super::super::worker::ShardWorker;

impl ShardWorker {
    pub(crate) fn execute_ft_spellcheck(
        &self,
        index_name: &Bytes,
        query_args: &[Bytes],
    ) -> Vec<(Bytes, Response)> {
        let name = std::str::from_utf8(index_name).unwrap_or("");
        let resolved = self.resolve_index_name(name);

        let idx = match self.search.indexes.get(resolved) {
            Some(idx) => idx,
            None => {
                return vec![(
                    Bytes::from_static(b"__ft_spellcheck__"),
                    Response::error(format!("{}: no such index", name)),
                )];
            }
        };

        // Parse query and options
        if query_args.is_empty() {
            return vec![(
                Bytes::from_static(b"__ft_spellcheck__"),
                Response::error("ERR wrong number of arguments"),
            )];
        }

        let query_str = std::str::from_utf8(&query_args[0]).unwrap_or("");
        let mut distance: usize = 1;
        let mut include_dicts: Vec<String> = Vec::new();
        let mut exclude_dicts: Vec<String> = Vec::new();

        // Parse optional args: DISTANCE n, TERMS INCLUDE dict, TERMS EXCLUDE dict
        let mut i = 1;
        while i < query_args.len() {
            let arg = std::str::from_utf8(&query_args[i])
                .unwrap_or("")
                .to_ascii_uppercase();
            match arg.as_str() {
                "DISTANCE" => {
                    i += 1;
                    if i < query_args.len()
                        && let Ok(d) = std::str::from_utf8(&query_args[i])
                            .unwrap_or("1")
                            .parse::<usize>()
                    {
                        distance = d;
                    }
                }
                "TERMS" => {
                    i += 1;
                    if i < query_args.len() {
                        let mode = std::str::from_utf8(&query_args[i])
                            .unwrap_or("")
                            .to_ascii_uppercase();
                        i += 1;
                        if i < query_args.len() {
                            let dict_name = std::str::from_utf8(&query_args[i])
                                .unwrap_or("")
                                .to_string();
                            match mode.as_str() {
                                "INCLUDE" => include_dicts.push(dict_name),
                                "EXCLUDE" => exclude_dicts.push(dict_name),
                                _ => {}
                            }
                        }
                    }
                }
                _ => {}
            }
            i += 1;
        }

        // Collect include/exclude dict references
        let include_sets: Vec<&std::collections::HashSet<String>> = include_dicts
            .iter()
            .filter_map(|d| self.search.dictionaries.get(d))
            .collect();
        let exclude_sets: Vec<&std::collections::HashSet<String>> = exclude_dicts
            .iter()
            .filter_map(|d| self.search.dictionaries.get(d))
            .collect();

        // Run spellcheck
        let results = frogdb_search::spellcheck::spellcheck_query(
            idx,
            query_str,
            distance,
            &include_sets,
            &exclude_sets,
        );

        // Build Redis-compatible response
        let term_responses: Vec<Response> = results
            .into_iter()
            .map(|(term, suggestions)| {
                let suggestion_items: Vec<Response> = suggestions
                    .into_iter()
                    .map(|(score, suggestion)| {
                        Response::Array(vec![
                            Response::bulk(Bytes::from(format!("{}", score))),
                            Response::bulk(Bytes::from(suggestion)),
                        ])
                    })
                    .collect();
                Response::Array(vec![
                    Response::bulk(Bytes::from_static(b"TERM")),
                    Response::bulk(Bytes::from(term)),
                    Response::Array(suggestion_items),
                ])
            })
            .collect();

        vec![(
            Bytes::from_static(b"__ft_spellcheck__"),
            Response::Array(term_responses),
        )]
    }
}

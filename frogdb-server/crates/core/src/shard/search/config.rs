use bytes::Bytes;
use frogdb_protocol::Response;

use super::super::worker::ShardWorker;
use super::glob_match_simple;

impl ShardWorker {
    pub(crate) fn execute_ft_config(&mut self, args: &[Bytes]) -> Vec<(Bytes, Response)> {
        if args.is_empty() {
            return vec![(
                Bytes::from_static(b"__ft_config__"),
                Response::error("ERR wrong number of arguments for 'ft.config' command"),
            )];
        }

        let subcommand = std::str::from_utf8(&args[0])
            .unwrap_or("")
            .to_ascii_uppercase();

        match subcommand.as_str() {
            "GET" => {
                if args.len() < 2 {
                    return vec![(
                        Bytes::from_static(b"__ft_config__"),
                        Response::error(
                            "ERR wrong number of arguments for 'ft.config get' command",
                        ),
                    )];
                }
                let pattern = std::str::from_utf8(&args[1]).unwrap_or("*");

                // Default config values
                let defaults: Vec<(&str, &str)> = vec![
                    ("MINPREFIX", "2"),
                    ("MAXEXPANSIONS", "200"),
                    ("TIMEOUT", "500"),
                    ("DEFAULT_DIALECT", "2"),
                ];

                let mut results: Vec<Response> = Vec::new();
                for (key, default_val) in &defaults {
                    if glob_match_simple(pattern, key) {
                        let val = self
                            .search
                            .config
                            .get(*key)
                            .map(|s| s.as_str())
                            .unwrap_or(default_val);
                        results.push(Response::Array(vec![
                            Response::bulk(Bytes::from(key.to_string())),
                            Response::bulk(Bytes::from(val.to_string())),
                        ]));
                    }
                }
                vec![(
                    Bytes::from_static(b"__ft_config__"),
                    Response::Array(results),
                )]
            }
            "SET" => {
                if args.len() < 3 {
                    return vec![(
                        Bytes::from_static(b"__ft_config__"),
                        Response::error(
                            "ERR wrong number of arguments for 'ft.config set' command",
                        ),
                    )];
                }
                let param = std::str::from_utf8(&args[1]).unwrap_or("");
                let value = std::str::from_utf8(&args[2]).unwrap_or("");
                self.execute_ft_config_set(param, value)
            }
            _ => vec![(
                Bytes::from_static(b"__ft_config__"),
                Response::error(
                    "ERR Unknown subcommand or wrong number of arguments for 'ft.config' command",
                ),
            )],
        }
    }

    pub(crate) fn execute_ft_config_set(
        &mut self,
        param: &str,
        value: &str,
    ) -> Vec<(Bytes, Response)> {
        let key = param.to_ascii_uppercase();
        let known = ["MINPREFIX", "MAXEXPANSIONS", "TIMEOUT", "DEFAULT_DIALECT"];
        if !known.contains(&key.as_str()) {
            return vec![(
                Bytes::from_static(b"__ft_config__"),
                Response::error("ERR Invalid option"),
            )];
        }
        self.search.config.insert(key, value.to_string());
        self.persist_search_config();
        vec![(Bytes::from_static(b"__ft_config__"), Response::ok())]
    }
}

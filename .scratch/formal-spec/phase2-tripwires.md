# Phase 2 tripwires

Short list of gotchas the phase-1 scaffolding review surfaced, so phase-2 (the first area
migrations) doesn't rediscover them the hard way.

1. **New area = new `AREAS` entry.** Adding a `specs/<area>.md` without a matching entry in
   `website/scripts/spec-gen.py`'s `AREAS` list fails the whole docs build, loudly and
   self-describing — but only at generation time, not at `lint-spec` time. Add the entry in the
   same commit that adds the spec file.

2. **Quint globs are non-recursive, in two places.** Both `scripts/spec-lint.py` (the header
   citation scan) and the `quint-check` Justfile recipe glob `specs/quint/*.qnt` — flat, one
   level. Before nesting Quint models into subdirectories (e.g. `specs/quint/cluster/*.qnt`),
   make both globs recursive (`**/*.qnt`) or the lint/typecheck will silently skip the nested
   models. Do this before the first subdirectory lands, not after.

3. **No escape hatch for non-claiming helper `.qnt` models.** Every `.qnt` file under
   `specs/quint/` must cite at least one spec id in its header comment (`test_quint_model_without_citations_is_an_error`
   pins this). If phase 2 wants shared helper/library modules that no single row "models", the
   lint has no exemption for that shape yet — decide the convention (a citation-free allowlist,
   or requiring even helpers to cite the rows they support) before the second model lands.

4. **Citation scanning and link rewriting are fence-blind.** Both `spec-lint.py`'s citation scan
   (`SPEC_REF_RE`, `INV_REF_RE`) and `spec-gen.py`'s link-rewriting walk spec prose without
   skipping fenced code blocks. No spec today has a fenced block containing an id-shaped token, so
   this is latent, not yet a bug — but a Quint snippet or example table pasted into a spec's prose
   could produce a false citation match or a mangled rewritten link. Make both fence-aware before
   embedding code examples in spec prose.

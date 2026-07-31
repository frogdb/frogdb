# spec.upgrade.autoFinalize is dead config

Status: done

`UpgradeSpec.auto_finalize` (`frogdb-operator/src/crd.rs:271-273`) is documented ("auto-finalize
after all pods are upgraded") but no reconcile path reads `spec.upgrade` at all — upgrade
detection is inferred from image-tag diffing.

Decide: implement the finalize step (frogctl has `upgrade finalize`, so a server-side finalize
exists to call), or remove the field until the operator actually drives upgrades.

Needs-info: is operator-driven upgrade finalization on the roadmap, or is `frogctl upgrade`
the intended driver?

## Comments

Resolved: **`frogctl upgrade` is the finalization driver; the dead config was removed.**

Investigation confirmed the controller reads *nothing* from `spec.upgrade`:
`min_upgrade_delay_secs` was referenced only inside `crd.rs` itself (its own default fn +
tests), and `auto_finalize` was referenced nowhere in `controller.rs`. Upgrade detection is
inferred purely from image-tag diffing. So per the issue's guidance, the **entire `UpgradeSpec`
block** was removed, not just `auto_finalize`:

- Deleted `UpgradeSpec` (both fields), `default_min_upgrade_delay`, its `Default` impl, and the
  `upgrade: Option<UpgradeSpec>` field on `FrogDBSpec` in `crd.rs`.
- Removed the `upgrade: None` initializer from `crd.rs` tests + `testing.rs::default_spec`, and
  deleted the now-obsolete `test_upgrade_spec_defaults` / `test_spec_with_upgrade` unit tests.
- Regenerated `deploy/crd.json` (the block was already absent from the stale artifact; the
  regen also refreshed other drifted fields). The `deploy/examples/*.yaml` never referenced
  `spec.upgrade`, so no example changes were needed.
- Note: `status.upgradeInProgress` / `currentVersion` / `targetVersion` are unaffected — those
  are *status* fields the controller writes from tag-diffing, unrelated to the removed *spec*.

Verification: `just operator-test` — 69/69 pass; `cargo clippy --all-targets` clean.

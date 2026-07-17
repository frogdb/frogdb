# spec.upgrade.autoFinalize is dead config

Status: needs-info

`UpgradeSpec.auto_finalize` (`frogdb-operator/src/crd.rs:271-273`) is documented ("auto-finalize
after all pods are upgraded") but no reconcile path reads `spec.upgrade` at all — upgrade
detection is inferred from image-tag diffing.

Decide: implement the finalize step (frogctl has `upgrade finalize`, so a server-side finalize
exists to call), or remove the field until the operator actually drives upgrades.

Needs-info: is operator-driven upgrade finalization on the roadmap, or is `frogctl upgrade`
the intended driver?

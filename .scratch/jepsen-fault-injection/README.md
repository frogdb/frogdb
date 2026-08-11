# Jepsen fault injection via DEBUG commands

State: active

Initiative to expand FrogDB's jepsen fault surface beyond process signals and iptables:
wire the existing DEBUG fault commands into nemeses, add the missing injection commands
(clock offset, replica apply delay, replication-id churn, backlog shrink, link kill, WAL
error injection), and give jepsen a cross-node view checker the node-local invariant
catalogs cannot express.

Filed 2026-08-11 from the DEBUG-surface investigation run alongside the
replication-correctness campaign (jepsen invariant sweep, issue 13 there, was the trigger).

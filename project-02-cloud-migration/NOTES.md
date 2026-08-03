# Project 2 — Build Notes

Dated build log, same convention as project-04/project-05/project-06: what
was built, why that approach, the key concept worth remembering, and
gotchas actually hit.

---

## 2026-08-02 — Scoping: real Cloud SQL, not simulated CSVs

**What I decided:**
Project-02 needs a PostgreSQL source to migrate from -- the one project in
this portfolio where the "source system" is a live OLTP database, not a
CSV. Decided to stand up a real (smallest-tier) Cloud SQL PostgreSQL
instance rather than simulate Postgres-shaped CSVs, since the JDBC-reads-
from-a-live-database step is the actual "migration" skill worth
demonstrating -- same create/verify/teardown discipline as every other
billable resource in this portfolio, just with Cloud SQL's different
(hourly, not on-demand) billing model made explicit up front.

Data reused from `shared/data-generators` (the same customers/products/
suppliers/orders/order_items/inventory_snapshots/product_price_history CSVs
already used by projects 1 and 3) rather than a new bespoke dataset --
frames the whole story as "PeakCart's legacy on-prem OLTP source, migrated
to BigQuery," ties the portfolio together as one company's data.

---

## 2026-08-02 — Phase 1: Cloud SQL PostgreSQL + legacy data load

**What I built:**
Terraform-managed Cloud SQL PostgreSQL instance (`db-f1-micro`, smallest
available tier), database, and user. Connected via the **Cloud SQL Auth
Proxy** rather than whitelisting a raw IP -- IAM-based auth, works
regardless of a changing local/dynamic IP (my own current IP was IPv6,
which would have been an extra complication for authorized-networks
whitelisting anyway). Loaded all 7 tables via a Python/psycopg2 script.

**Key concept to remember (intentional duplicate orders, not a bug):**
The `orders` table load initially failed with `UniqueViolation: duplicate
key value ... order_id`. Before assuming this was a generator bug,
confirmed directly in `generate_peakcart_data.py`: an `intentional_duplicate()`
helper (dupe_rate=0.01) is deliberately applied to orders -- 50 of 5050 raw
rows are exact duplicates, by design, so that dbt's staging-layer dedup
pattern (`ROW_NUMBER() OVER (PARTITION BY ... ORDER BY _loaded_at DESC)`,
used in project-01) has something real to demonstrate. This had simply
never surfaced before, since `bq load` (used everywhere else) never
enforces uniqueness the way a real OLTP database's primary key does --
migrating to a system with actual constraints exposed a data characteristic
that had been silently present the whole time.

**Fix:** loaded via a **stage-then-merge** pattern (industry-standard for
handling dirty source data during migration): land the raw CSV in an
unconstrained staging table first, then `INSERT ... SELECT DISTINCT ON
(pk) ... ORDER BY pk` into the constrained final table. A real legacy OLTP
source wouldn't have duplicate primary keys, so this dedup step represents
what an actual live database would already enforce -- resolved at load
time, not by relaxing the target schema's constraints.

---

## 2026-08-02 — Phase 2: Dataproc/Spark bulk migration -- three real problems, in order

**What I built:**
A single-node Dataproc cluster (`dataproc:dataproc.allow.zero.workers`,
smallest reasonable machine type) running a PySpark job that reads all 7
tables via JDBC and writes them to a new `peakcart_migration_bronze`
BigQuery dataset via the Spark BigQuery connector. Chose a cluster over
Dataproc Serverless for this build specifically for the hands-on cluster-
management learning value (Serverless is mentioned as a viable alternative
in the README, since it avoids the init-action/network complexity below,
but a traditional cluster is still the more common real-world Dataproc
experience). DB password fetched at runtime via Secret Manager
(`gcloud secrets versions access`) rather than passed as a plaintext job
argument, so it never appears in job history/logs.

**Problem 1 -- JDBC SSL negotiation hangs against the Auth Proxy.**
First job attempt hung indefinitely inside `enableSSL()` during the JDBC
handshake, eventually timing out. The Cloud SQL Auth Proxy's local listener
is already the trusted, encrypted channel to Cloud SQL -- the JDBC driver's
own SSL negotiation on top of that loopback connection isn't needed and
(with this proxy version/setup) isn't answered, so it just hangs. Fixed
with `?sslmode=disable` on the JDBC URL for the proxy-based connection
(this fix became moot once Problem 2 forced a different connection path
entirely, but it's worth remembering as a standalone Cloud-SQL-Auth-Proxy-
plus-JDBC gotcha).

**Problem 2 -- the real one: the Dataproc node has no general internet
egress.** After fixing the SSL hang, the *next* failure was a plain
connection reset during authentication. Traced via the Auth Proxy's own
log on the master node (SSH'd in directly rather than guessing): `failed to
dial ... dial tcp 34.133.174.42:3307: i/o timeout` -- the proxy itself,
running on the Dataproc node, couldn't reach Cloud SQL's public IP at all.
Cross-checked against an earlier, seemingly unrelated observation: `apt-get
install postgresql-client` on the same node had failed with "Network is
unreachable" reaching `deb.debian.org`. Both point to the same root cause:
an org policy blocks external IPs on VMs in this project. `curl` downloads
from `storage.googleapis.com` during the init action had worked fine
because that's a Google API endpoint reachable via Private Google Access
without an external IP -- but Cloud SQL's public IP is a normal internet
address, unreachable the same way.

Confirmed with `terraform plan` too: after adding a private-IP config
change, Terraform's own drift detection showed `internal_ip_only` had
silently been forced to `true` on the real cluster (GCP enforcing the org
policy regardless of what was requested), which was quietly about to force
an unwanted cluster replacement (`internal_ip_only` and
`service_account_scopes` are both ForceNew fields) until the Terraform
config was updated to declare `internal_ip_only = true` explicitly, matching
reality instead of fighting it.

**Fix:** gave Cloud SQL a **private IP** via Private Service Access (VPC
peering) on the same `default` network as the Dataproc cluster, and
connected the Spark JDBC read directly to that private IP -- no proxy
needed at all for VPC-internal traffic (the Auth Proxy is for
external/local-dev access; direct private IP is the more idiomatic pattern
for VPC-internal service-to-service traffic anyway). This is also the exact
groundwork Dataproc Serverless would have needed, since Serverless batches
run in a VPC subnet too.

**Problem 3 -- a second, conflicting peering connection.** The first
attempt at adding Private Service Access failed: `Cannot modify allocated
ranges in CreateConnection ... Existing allocated IP ranges:
[google-managed-services-default]`. A servicenetworking peering connection
already existed on the `default` network (from something else in this GCP
project, unrelated to project-02) -- Terraform was trying to *create* a
second, competing connection rather than *update* the existing one.
Checked directly with `gcloud services vpc-peerings list` before assuming
anything more exotic. Fixed by dropping the custom
`google_compute_global_address` / `google_service_networking_connection`
resources entirely and just setting `private_network` on the Cloud SQL
instance -- it uses whatever peering already exists on the network, no
explicit connection resource needed when one's already there. Also cleaned
up the orphaned reserved IP range Terraform had created before the
connection step failed.

**Result once fixed:** a clean end-to-end run -- all 7 tables read via JDBC
from Cloud SQL's private IP and written to
`peakcart_migration_bronze.<table>` in BigQuery, with row counts confirmed
matching source exactly (20/1000/200/5000/15000/3000/359) via a direct
`bq query`, and spot-checked actual row values (not just counts) matching
the source CSV byte-for-byte.

**Why this diagnostic approach:** every step traced to a specific piece of
independent evidence (the proxy's own log, `apt-get`'s unrelated but
corroborating failure, `terraform plan`'s drift detection, `gcloud services
vpc-peerings list`) rather than guessing at fixes -- the same discipline
used throughout this portfolio (the hidden tenant training SA in
project-05, the Cloud Run instance-wedging investigation in project-06).

---

## 2026-08-03 -- Phase 3: Datastream incremental sync -- three more real problems

**Decision: Datastream over Dataflow.** For the incremental/CDC half of
this project, chose Datastream (Google's purpose-built Postgres CDC
service, reading the write-ahead log directly via logical replication)
over a Dataflow polling pipeline. Real CDC (captures inserts/updates/
deletes precisely) beats a polling-based approach (misses deletes, needs
a tracked `updated_at` column), and it's a service nothing else in this
portfolio has used yet. The bulk historical load stays with Dataproc
(Phase 2); Datastream is configured with `backfill_none` and only picks
up changes going forward -- the realistic reason both mechanisms coexist
in a real migration (one-time cutover vs. ongoing sync).

**Problem 1 -- VPC peering is non-transitive, even with custom routes
attempted.** First approach: private connectivity via a Datastream
`private_connection` (its own VPC peering into the `default` network,
alongside Cloud SQL's existing Private Service Access peering).
Connection profile validation timed out repeatedly. Traced to: Datastream
lives in one peering, Cloud SQL's private IP lives in a different peering,
and traffic from one peered network to another peered network through a
shared hub VPC is exactly the "transitive peering" pattern GCP peering
doesn't support by default. Enabled custom route export/import on both
peerings (`gcloud compute networks peerings update --export-custom-routes`
/ `--import-custom-routes`) -- routes became visible (confirmed via
`gcloud compute routes list`), but the connection still timed out after
several retries and a 3-minute propagation wait. Rather than keep
debugging an increasingly deep networking rabbit hole, stepped back and
reconsidered the whole approach.

**Fix -- pivoted to IP-allowlist connectivity.** Datastream isn't a VM in
our project (unlike the Dataproc cluster) -- it's an external Google-
managed service, so it's not subject to the org policy blocking external
IPs that caused Phase 2's Auth Proxy problem, and it doesn't need private
connectivity at all. Cloud SQL already has a public IP. Fetched
Datastream's published static IPs for us-central1 via the
`fetchStaticIps` API and added them as Cloud SQL authorized networks
instead. Connection validated immediately. Much simpler than the private-
connectivity path, and a good reminder to check for the simpler option
before deep-diving a hard one.

**Problem 2 -- Cloud SQL's `postgres` user isn't a true superuser.**
Stream creation then failed `POSTGRES_REPLICATION_SLOT_DOES_NOT_EXIST` /
`POSTGRES_PUBLICATION_DOES_NOT_EXIST` -- hadn't yet run the setup SQL.
Running it as the `postgres` admin user failed with
`must be superuser or replication role to use replication slots`: Cloud
SQL's `postgres` is a managed admin role (member of `cloudsqlsuperuser`),
not a real Postgres superuser, and doesn't have the REPLICATION login
attribute by default. Fixed with `ALTER ROLE postgres WITH REPLICATION;`
(Cloud SQL's admin role is specifically permitted to grant this to
itself) before creating the publication and slot.

**Problem 3 -- table ownership ACLs.** Stream creation still failed
`POSTGRES_TABLES_MISSING_PERMISSIONS` even with the slot/publication in
place and `postgres` granted REPLICATION. The tables were created by
`peakcart_app` (the loader script's user) in Phase 1, and `postgres`'s
`cloudsqlsuperuser` membership doesn't bypass standard table ACLs the way
a real superuser would -- it still needs an explicit grant. Fixed with
`GRANT SELECT ON ALL TABLES IN SCHEMA public TO postgres;` run as the
owning user (`peakcart_app`).

**Result once fixed:** stream reached `RUNNING` with no errors. Ran
`simulate_incremental_writes.py` (insert, update, delete against `orders`)
and confirmed all three captured correctly in
`peakcart_migration_cdc.public_orders` (note the Postgres-schema-prefixed
table name Datastream auto-creates): the new row present, the updated
row's status changed in place, and the deleted row's absence from the
table (merge-mode destination gives a true current-state replica, not an
append-only change log).

**Also worth remembering:** multi-statement SQL passed to `psycopg2` in
one `cur.execute()` call runs as a single implicit transaction --
`pg_create_logical_replication_slot()` failed with `cannot create logical
replication slot in transaction that has performed writes` when combined
with `CREATE PUBLICATION` in the same call. Split into separate
`cur.execute()` calls.

---

## 2026-08-03 -- Phase 4: Composer orchestration -- ephemeral Dataproc cluster lifecycle

**What I built:** `dags/project02_migration_dag.py` -- a DAG managing the
Dataproc cluster's full lifecycle per run (`DataprocCreateClusterOperator`
-> `DataprocSubmitJobOperator` -> `DataprocDeleteClusterOperator`), rather
than reusing the persistent cluster from Phase 2's interactive
development. This is the standard real-world Composer+Dataproc pattern --
a cluster that only exists for one job's duration costs nothing the rest
of the time. A `BigQueryCheckOperator` validates all 7 bronze table row
counts in one combined query (`MIN(actual = expected) = 1`) as a single
quality gate, same style as project-04's rollup DAG. Datastream is
deliberately not part of this DAG -- it's an always-on managed stream, not
a batch job to trigger periodically.

**Environment setup:** the `peakcart-composer-verify` SA and Composer
environment from project-04's verification had both already been torn
down (as they should be) -- recreated the same one-time IAM setup
(composer.worker, bigquery roles, the one-time `composer.ServiceAgentV2Ext`
grant on the Composer service agent), plus a new grant this DAG needed
that project-04's didn't: `roles/dataproc.editor` (to create/delete
clusters and submit jobs) and `roles/iam.serviceAccountUser` on the
Dataproc cluster's own service account (Composer's SA needs explicit
permission to launch a cluster running as a *different* SA).

**Gotcha -- BigQuery multi-region vs. specific-region location
mismatch.** First DAG run: `create_dataproc_cluster`,
`submit_bulk_migration_job`, and `delete_dataproc_cluster` all succeeded
correctly (confirmed against the real Dataproc job status directly, since
Airflow's own status polling lagged a few minutes behind reality --
worth checking the actual GCP resource state, not just Airflow's cached
view, when task status seems stale). `validate_migration_counts` failed:
`Dataset ... was not found in location US`. The `BigQueryCheckOperator`
was configured with `location="US"` (multi-region), but
`peakcart_migration_bronze` lives in `us-central1` (a specific region) --
these are different BigQuery locations, not interchangeable. Fixed by
using the same `REGION` constant as everything else in the DAG. Cleared
and retried just the failed task (`airflow tasks clear`) rather than
re-running the whole DAG, since the actual migration work had already
succeeded -- re-triggering it would have wastefully recreated a cluster
and rerun a job that didn't need rerunning.

**Verified live, with real evidence:** all 4 tasks green in the Airflow
UI, and independently re-ran the validation SQL directly via `bq query`
to confirm `all_counts_match: true` across all 7 tables -- not just
trusting the task's SUCCESS status.

---

## 2026-08-03 -- Full teardown

Deleted, in dependency order: Datastream stream -> Datastream connection
profiles -> Dataproc cluster (Phase 2's persistent one) -> Cloud SQL
instance -> the local Cloud SQL Auth Proxy process. The Cloud SQL instance
deletion via Terraform initially failed
(`role "peakcart_app" cannot be dropped because some objects depend on it`)
-- Terraform's destroy graph tries to cleanly drop the SQL user resource
before the parent instance, which fails on real Postgres ownership
semantics (the user still owns tables). Since the whole instance was
being deleted anyway, this ordering constraint was moot -- deleted the
instance directly via `gcloud sql instances delete` (which wipes
everything atomically, bypassing the per-object ownership check a live
`DROP ROLE` would hit) and reconciled Terraform state with `terraform
state rm` afterward. Composer environment deleted via `gcloud composer
environments delete`.

Kept (free, no ongoing cost, the actual reusable deliverable): both
BigQuery datasets (`peakcart_migration_bronze`, `peakcart_migration_cdc`),
service accounts, the Secret Manager secret, IAM bindings, the staging
bucket and its uploaded scripts/DAG -- the whole pipeline is
re-deployable from Terraform plus `gcloud dataproc jobs submit`/DAG
trigger on demand. Confirmed clean afterward: zero Cloud SQL instances,
Dataproc clusters, Datastream streams, or Composer environments listed,
and a `terraform plan` showing only the expected 9-resources-to-recreate
drift with no unexpected changes.

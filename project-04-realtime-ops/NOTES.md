# Project 4 Study Log

Running notes on what was built, why, and what's worth remembering. Not documentation — see the project README for that.

---

## 2026-07-03 - Repo-wide CLAUDE.md added

**What I built/changed:**
Added a root-level `CLAUDE.md` documenting commands and architecture across all six projects in the repo, including project-04's Pub/Sub → Beam streaming pipeline.

**Why this approach:**
Future Claude Code sessions need enough context to be productive without re-deriving the shared dbt project layout, the stage-and-replace Bronze pattern, and project-04's simulator/pipeline structure from scratch each time.

**Key concept to remember:**
Project-04's Terraform computes the Pub/Sub service agent email directly from `data.google_project.current.number` instead of relying on `google_project_service_identity`'s output, because the latter is unknown until apply and was forcing IAM bindings to be destroyed/recreated on every run.

**Gotchas/issues hit:**
None.

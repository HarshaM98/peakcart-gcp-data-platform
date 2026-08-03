# Contributing

This is a personal portfolio repository built for learning, so it isn't
seeking feature contributions the way a library would. That said, **issues
and corrections are genuinely welcome** — if something here is wrong,
outdated, or a bad practice, I'd rather know.

## What's most useful

- **Corrections.** If an IAM pattern, Terraform resource, dbt model, or Beam
  transform here is wrong or has since been superseded by a better GCP
  approach, please open an issue. Include a pointer to the file and, if you
  can, the documentation that supersedes it.
- **Reproducibility problems.** If you cloned this and something in a
  project's "How to Run" section didn't work, that's a real bug in the docs.
- **Security findings.** See [SECURITY.md](SECURITY.md).

## Running things locally

Each project is documented in its own README, and projects 2 and 4 have a
`COMMANDS.md` with the exact commands used. Start there.

Broadly you'll need:

- A GCP project with billing enabled (most resources here cost money while
  they exist — see the cost note in the root README)
- `gcloud`, `terraform`, and Python 3.11
- Generated sample data: `python3.11 shared/data-generators/generate_peakcart_data.py`

**Note on portability:** several values are currently hardcoded to my own GCP
project (project IDs, bucket names, a service account email). Running this
against your own project requires substituting them. This is a known gap
rather than a design choice.

## Conventions used here

If you do open a PR, matching the existing conventions helps:

- **Commits** follow [Conventional Commits](https://www.conventionalcommits.org/):
  `feat(project-04): ...`, `fix(...)`, `docs(...)`, `test(...)`, `ci(...)`,
  `chore(...)`. 78 of 79 commits in the history follow this.
- **Terraform** must pass `terraform fmt -check -recursive` and
  `terraform validate` (enforced in CI for projects 4 and 5).
- **Python** targets 3.11. There is no linter configured yet, so match the
  style of the surrounding file.
- **Comments explain *why*, not *what*.** The most valuable comments in this
  repo record a real failure and the reasoning that resolved it — see
  `project-04-realtime-ops/pipeline/step10_avg_pick_time.py` or any
  `NOTES.md`. Please keep that bar.
- **Each project maintains a `NOTES.md`** with a dated entry per meaningful
  change, and projects with recurring workflows maintain a `COMMANDS.md`.

## Data files

Never commit generated data. `shared/data-generators/output/` is gitignored
and should stay that way. The generators are deterministic (`SEED = 42`), so
anyone can reproduce the same dataset.

The one exception is `shared/data-generators/fixtures/`, which holds committed
CSVs that are **not** generated and are copied into `output/` on each run.
Currently that is `product_price_history.csv`, which has to stay byte-stable
because it drives the SCD Type 2 date ranges and its row count is asserted as
a quality gate. See the [fixtures README](shared/data-generators/fixtures/README.md)
for the reasoning.

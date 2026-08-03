---
name: Reproducibility problem
about: You tried to run something here and it didn't work
title: "[repro] "
labels: reproducibility
---

## What you were trying to run

<!-- Which project, and which section of its README / COMMANDS.md -->

## Command you ran

```bash

```

## What happened

<!-- Full error output if you have it -->

## What you expected

## Environment

- OS:
- Python version:
- Terraform version:
- Did you run `shared/data-generators/generate_peakcart_data.py` first? yes / no
- Are you using your own GCP project (rather than the hardcoded one)? yes / no

---

**Known gaps before you file:** several values are hardcoded to the author's
GCP project (project IDs, bucket names, a service account email), so running
against your own project currently requires substitution. If that's the
problem you hit, it's a known issue — but please still file it so it's
tracked and prioritized.

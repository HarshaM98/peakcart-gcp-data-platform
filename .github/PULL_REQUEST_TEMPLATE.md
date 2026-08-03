## What this changes

<!-- Brief description. Which project(s) does this touch? -->

## Why

<!-- The reasoning. If this fixes something that was wrong, what was wrong? -->

## Verification

<!--
How do you know it works? This repo's convention is evidence over assertion --
row counts, query output, a screenshot, a passing test. "It should work" isn't
enough; "SUCCEEDED" alone isn't either.
-->

## Checklist

- [ ] Commit messages follow Conventional Commits (`feat(project-0N): ...`)
- [ ] `terraform fmt -check -recursive` and `terraform validate` pass (if Terraform changed)
- [ ] Existing tests still pass (if Python changed)
- [ ] No credentials, keys, or real data added
- [ ] No generated data files committed (`shared/data-generators/output/` stays ignored)
- [ ] Relevant `NOTES.md` updated with a dated entry (if this was a meaningful change)
- [ ] Relevant `COMMANDS.md` updated (if a new command/workflow was used)
- [ ] Any GCP resources created for verification have been torn down

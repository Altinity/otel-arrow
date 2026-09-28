# CI strategy

This directory contains the repository's main CI workflows:

- [`rust-ci.yml`](rust-ci.yml): Rust validation.
- [`go-ci.yml`](go-ci.yml): Go validation and CodeQL.
- [`repo-lint.yaml`](repo-lint.yaml): Repository lint and sanity checks.
- [`changelog.yml`](changelog.yml): Changelog validation.
- [`post-merge-actions.yml`](post-merge-actions.yml): Shared Rust cache warming.

## Event model

| Event | Rust | Go | Repository |
| --- | --- | --- | --- |
| Pull request | Required and non-required jobs | Required jobs | Lint and changelog |
| Merge queue | Required jobs and coverage | Required jobs | Lint and changelog |
| Merge to `main` | Shared-cache maintenance | CodeQL | - |

Pull requests provide broad feedback. Merge-queue runs validate what is
required for merging and upload complete Rust and Go coverage for the commit
that will reach `main`. Post-merge workflows avoid repeating validation that
already passed in the merge queue.

## Required checks

The aggregate Rust and Go status jobs define required validation through their
`needs` lists. Treat those lists as the source of truth when adding or removing
required jobs.

## Non-required workflows

These run on pull requests that touch their paths and by hand
(`workflow_dispatch`), never in the merge queue or on `main`, and no required
check depends on them:

- [`series-lake-golden.yml`](series-lake-golden.yml): regenerates the
  series-lake golden vectors with the independent Python generator.
- [`series-parquet-e2e.yml`](series-parquet-e2e.yml): builds `df_engine` and
  runs the series_parquet end-to-end suite against local files, MinIO and
  Alloy in Docker.

## Caching and artifacts

- Pull-request and merge-queue jobs restore shared Rust caches without writing
  them.
- Post-merge jobs on trusted `main` own required Linux and Windows cache
  updates.
- Exact cache hits skip compilation; misses and fallback restores warm a new
  cache.
- Caches provide reusable inputs across runs.
- Workflow artifacts distribute nextest archives to test partitions within one
  run.

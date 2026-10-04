# Design decisions and limits

## Raw ingestion

Schema contracts compare the complete ordered header list and reject changed headers or malformed record lengths. Business values remain strings with source path, source record index, ingestion time, run ID and an original-header mapping.

For these modest historical exports, Spark distributes binary files and a strict CSV parser preserves physical record order inside each file. Each file is capped at 64 MiB. This approach is deliberate for DST-pair ordering; multi-GB inputs would require a different ingestion strategy.

Reruns rebuild snapshots rather than append duplicates. The known metadata shape may be overwritten explicitly; source headers still must match the reviewed contract. Do not regenerate contracts automatically on each run: that would conceal schema drift.

## Cleaning and quality

The supplied exports use English numeric separators. Missing markers and malformed values receive distinct reasons. Numeric null/conversion checks apply to configured generation/consumption measures and the Germany/Luxembourg target; foreign-market price values remain in raw.

Nuclear nulls are an explicit, audited exception. Nonnegative constraints are measure-specific: negative prices and residual load are valid.

Delivery-start times are normalized from Europe/Berlin to UTC. Exactly two ambiguous autumn occurrences in one source file are assigned earlier/later UTC instants by source order. Full source chronology is checked for hourly progression. Other ambiguity or nonexistent local times fail. The interval end is derived as start plus one elapsed hour from the hourly export resolution; original end-label semantics are not independently validated.

The gate checks nulls, conversion errors, duplicate UTC times, off-hour timestamps, missing intervals between observed bounds and source chronology. Missing intervals outside observed bounds require an externally specified expected date range, which this historical project does not impose.

Staging and audit tables retain evidence before promotion. All three datasets must pass. Delta commits are per table rather than one cross-table transaction; workflow dependencies, run markers and config hashes guard downstream consumption.

## Joins and features

Uniqueness is checked before a full outer join. Presence flags expose unmatched sources, and any unmatched hour blocks promotion.

Calendar features use German local delivery time. Lags use exact elapsed UTC hours, so a 24-hour lag may refer to a different local clock hour across DST. Initial missing lag history is not filled from future prices.

## Modeling

The 60/20/20 chronological split applies after the 48-row lag warm-up. Twenty-four rows are purged from the end of training and validation so labels precede the first next-partition forecast origin. Median imputation and both XGBoost candidates fit only training. Validation selects parameters; test evaluates once.

This rolling 24-hour historical protocol assumes lag prices are available at origin `t−24h`. Publication timestamps are absent, so market-release availability cannot be established from the delivery-time CSVs alone.

Actual generation/consumption are excluded as predictors. Future forecast vintages could be added only with issue times and point-in-time joins. Targets are not scaled or globally capped.

Spark prepares the data; at most 200,000 rows are collected for driver-side scikit-learn/XGBoost. Larger data needs distributed training or a revised bounded modeling strategy.

## Prediction and operations

The selected evaluation model stays frozen and is reused for the 24-hour demonstration after the historical input ends. These output hours have no supplied target observations for scoring. A production model might be separately refitted after evaluation under versioning and monitoring controls.

There is no live data ingestion, production schedule, release-time feed, alerting system or production service-level objective. The workflow is an verified historical batch pipeline. Credentials and personal workspace config stay outside the public repository.

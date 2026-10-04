# Measured verification results

Recorded on **4 October 2026** from a successful Databricks Free Edition serverless run. Public JSON evidence is in [verified_run.json](../reports/verified_run.json). Workspace identifiers are omitted; measured results are unchanged.

| Check | Actual result |
|---|---|
| Raw/cleaned datasets | 53,256 rows per dataset |
| Duplicate UTC timestamps | 0 |
| Missing hourly intervals | 0 within observed bounds |
| Malformed modeled numeric values | 0 |
| Nuclear missing values | 8,724; retained under explicit nullable contract |
| Negative price observations | 1,489; preserved |
| Negative residual-load observations | 130; preserved |
| Joined observations | 53,256 |
| Unmatched source timestamps | 0 |
| Exact 24/48-hour lag mismatches | 0 |
| Lag warm-up exclusions | 48 rows |
| Next-24-hour prediction output | 24 finite historical predictions |
| Databricks cloud test cases | 10 passed |
| Original local unit tests | 18 passed |

## Held-out evaluation

| Model | Test MAE (EUR/MWh) | Test RMSE (EUR/MWh) |
|---|---:|---:|
| XGBoost | 26.871534111925683 | 39.694418350467046 |
| Previous-day price | 28.26717628265364 | 44.59733790565309 |

Selected model: 200 estimators, depth 4, learning rate 0.05, random seed 42. Validation MAE: 35.77988134429995. The other candidate (100 estimators, depth 3) achieved validation MAE 35.9188637559682.

| Partition | Rows | Start UTC | End UTC |
|---|---:|---|---|
| Training | 31,900 | 2019-01-02 23:00 | 2022-08-24 02:00 |
| Validation | 10,618 | 2022-08-25 03:00 | 2023-11-10 12:00 |
| Test | 10,642 | 2023-11-11 13:00 | 2025-01-27 22:00 |

The evaluation model used training-only preprocessing, validation-only candidate selection and a 24-hour purge at each boundary. The held-out test was not used to tune parameters.

Verified model environment: Python 3.11.10; XGBoost 2.1.4; scikit-learn 1.5.2; pandas 2.2.3; numpy 1.26.4; joblib 1.4.2.

## Forecast scope

The last source observation was 2025-01-27 22:00 UTC. The output spans 2025-01-27 23:00 through 2025-01-28 22:00 UTC: **28 January 2025, 00:00–23:00 Europe/Berlin**. It is a historical demonstration, not a current forecast. Target observations for that output period are absent, so it has no claimed prediction-period accuracy score.

## Test scope

Local pytest verification covered strict parsing, DST behavior, chronological splits, boundary purges, training-only imputer statistics, warm-up handling and model reload. Databricks additionally executed schema-drift, corrupted CSV, unmatched join, exact lags over a gap, DST pairs/lone ambiguity and explicit null-policy tests with Spark.

The diagrams and evidence visuals are derived from these actual outputs, not screenshots of the Databricks console. GitHub Actions status is independently visible in the repository and should not be inferred from this recorded cloud run.

Local Windows Spark/JVM and path-based Delta execution, distributed XGBoost, live forecasting and production operations are not claimed. Release-time availability is an assumption because the source contains delivery timestamps rather than publication timestamps.

## Public repository verification

The [GitHub Actions verification run](https://github.com/abdulwahabshah236/german-electricity-forecasting-pipeline/actions/workflows/tests.yml) passed on 4 October 2026: **18 unit tests**, **6 Linux Spark integration tests**, and the generated-notebook consistency check. The public package wheel also built successfully, and its 18 unit tests passed locally on Windows.

The configurable public bundle was then deployed into a **separate validation schema** and executed end to end. All seven tasks succeeded, including 10 cloud test cases; the evaluation metrics exactly reproduced the earlier run. Sanitized task outputs are available in [portfolio_run.json](../reports/portfolio_run.json). This verifies the public notebooks and deployment configuration in Databricks, rather than assuming portability from local tests.


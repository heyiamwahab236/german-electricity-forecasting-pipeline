# Pipeline stages

## 1. Preserve and inspect the source

Git records the original project. We cloned it without changing its notebook. The three CSVs are the original generation, consumption and Germany/Luxembourg day-ahead-price history. File checksums prove the Databricks copies match the originals.

## 2. Create raw Delta tables

A DataFrame is Spark's representation of a table. Spark usually plans transformations first and executes them when an action such as a write or count is requested. PySpark lets us describe that work in Python.

The raw layer stores the original strings, plus their source file and record identifiers. Delta is a table storage format with a transaction log: a successful write becomes a committed table version. Unity Catalog registers those tables so they are discoverable and governed.

## 3. Parse and validate the cleaned layer

`4,815.25` is text in the CSV. We convert it to the numeric value `4815.25`. If a value is malformed, we retain an explicit failure reason rather than creating an unexplained null. Negative electricity prices are real observations.

German delivery timestamps need careful treatment: autumn has two local 02:00 hours, while spring skips an hour. We preserve both real autumn records, use source order to distinguish them, and store unique UTC timestamps. UTC makes interval arithmetic consistent.

The quality gate checks duplicates, gaps, nulls, numeric failures and schema changes before publishing cleaned data. The nuclear null exception is visible in the contract and report; it is not silently filled.

## 4. Join datasets without hiding missing records

A join aligns records using a shared key. Here the key is the UTC delivery-start timestamp. An inner join could discard an hour missing from one source. A full join keeps it so we can count unmatched sources, investigate them, and fail safely.

## 5. Prepare forecast-time features

Calendar features tell the model when an hour occurs. Sine/cosine encode hour around a circle: midnight should be near 23:00, rather than far away numerically. Lag features provide prices exactly 24 and 48 elapsed hours earlier.

We do not simply shift rows. If an hour were missing, shifting by 24 rows could mean 25 elapsed hours. Exact timestamp joins preserve meaning. Missing initial history remains missing; future prices never fill it.

## 6. Evaluate XGBoost honestly

XGBoost combines decision trees to learn relationships between the features and price. Older data trains it; later data selects between candidate settings; the newest test period measures held-out accuracy. A purge gap prevents labels near a boundary from falling after the next forecast origin.

Preprocessing learns only from training. We compare against a simple prediction: use the price from 24 hours earlier. MAE is the average absolute prediction error; RMSE penalizes large errors more strongly. Both are measured in EUR/MWh. The model is not assumed to outperform the baseline.

Actual generation and consumption are retained for analysis but excluded from future-price predictors. We cannot know future actual values when making a forecast.

## 7. Reuse the saved model for prediction

The saved model includes its fitted preprocessing and feature list. Prediction uses the same definitions. The demo creates 24 hours after the last source timestamp, using historical prices and known calendar information.

This is a January 2025 historical demonstration, not a current forecast. We do not have actual target prices for that output period in the supplied history, so we do not claim an accuracy score for it.

## 8. Connect the stages and test failure cases

A Databricks workflow is the orchestrator: it starts tasks in order and stops dependent tasks when an earlier task fails. Run IDs and the config manifest connect each result to its inputs.

Good tests deliberately break data. The project tests corrupted numbers, missing and duplicate hours, changed headers, unmatched joins and ambiguous DST records. It also checks that preprocessing does not learn from validation/test values and that the saved model reloads.

## What this project proves, and its limits

The verification report lists what actually ran locally and in Databricks. The design is a bounded historical batch project with driver-side XGBoost, rather than a live streaming service. Original release timestamps are absent, cross-table writes are not a single transaction, and larger data would require a different ingestion/modeling strategy. 

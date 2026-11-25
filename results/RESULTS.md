# Results

Synthetic data only (generator in `src/mlplatform/synthetic.py`). Rebuild with `make bench`;
`tests/test_bench.py` reruns it and compares with these files.

Market: 40 entities, 2021-01-01 to 2024-12-31, seed 7. Test block: decisions from 2024-01-01.

## 1. Pipeline run (Dagster asset graph, in-process)

| | signal | control |
|---|---|---|
| raw price rows | 41,720 | 41,720 |
| raw fundamental rows (restatements) | 890 (210) | 890 (210) |
| training rows / test rows | 30,240 / 10,280 | 30,240 / 10,280 |
| rows dropped by purge + embargo | 200 | 200 |
| held-out AUC | 0.5975 | 0.5030 |
| AUC 95% CI (10-day block bootstrap) | 0.5763 to 0.6159 | 0.4876 to 0.5170 |
| log loss vs constant prior | 0.6789 vs 0.6931 | 0.6934 vs 0.6931 |
| accuracy | 0.5708 | 0.5058 |
| gate passed | True | False |
| model_artifact materialized | True | False |
| promotion sensor run requests | 1 | 0 |
| models/CURRENT set | True | False |
| asset checks passed | 5 / 5 | 5 / 5 |

Gate checks (signal): `auc_ci95_low_above_0.5`=True, `beats_constant_prior_log_loss`=True, `enough_test_rows`=True

Gate checks (control): `auc_ci95_low_above_0.5`=False, `beats_constant_prior_log_loss`=False, `enough_test_rows`=True

Signal-mode coefficients (standardised): `fundamental_score` +0.2022, `ret_20d` +0.1560, `ret_5d` -0.0181, `vol_20d` +0.0261, `volume_z_20d` -0.0003

## 2. Look-ahead join, for contrast (not a result to trust)

| mode | join | train rows | test rows | AUC | AUC 95% CI | fundamental coef |
|---|---|---|---|---|---|---|
| signal | point in time | 30,240 | 7,840 | 0.5980 | 0.5724 to 0.6233 | +0.2022 |
| signal | by quarter, ignoring publication | 30,240 | 7,840 | 0.6314 | 0.6127 to 0.6528 | +0.3459 |
| control | point in time | 30,240 | 7,840 | 0.5093 | 0.4925 to 0.5210 | +0.0189 |
| control | by quarter, ignoring publication | 30,240 | 7,840 | 0.5152 | 0.4979 to 0.5309 | +0.0235 |

## 3. Point-in-time audit of the feature snapshot

| check | value |
|---|---|
| queries (half at a publication time or 1 µs before it) | 300 |
| records in snapshot | 205,490 |
| SQL lookup vs reference rule, mismatches | 0 |
| vectorised join vs reference rule, mismatches | 0 |
| responses containing a value published after as_of | 0 |
| queries between a first report and its restatement | 49 |
| ... that returned the original report, as they should | 49 |

## 4. Cache replay across a snapshot switch

3,000 requests through the feature service with fakeredis; 60% ask for the latest values, the rest repeat 300 historical (entity, as_of) pairs. At request 1,500 snapshot v2 is published with 60 late-arriving corrections. Every response is compared with an uncached lookup on the snapshot it was served from. The last column replays the same requests through a cache keyed only on (entity, as_of).

| phase | requests | cache hits | hit rate | snapshot queries | stale responses (versioned key) | stale responses (naive key) |
|---|---|---|---|---|---|---|
| before switch | 1,500 | 1,198 | 0.799 | 302 | 0 | 0 |
| after switch | 1,500 | 1,209 | 0.806 | 291 | 0 | 215 |

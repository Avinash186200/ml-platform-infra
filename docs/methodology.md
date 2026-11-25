# Methodology: synthetic data, training and the gate

## The generator (`src/mlplatform/synthetic.py`)

- 40 entities (`SYN000`..`SYN039`), business days 2021-01-01 to 2024-12-31 in the reported run.
- A hidden quarterly signal `s` per entity follows an AR(1) across quarters (rho = 0.8).
- Daily log return = beta x `s` of the quarter the day is in + a common market shock (sd 1%) + idiosyncratic noise (sd 1.5%). beta = 0.002 in **signal** mode, 0 in **control** mode. Everything else, including the random draws, is identical between the modes.
- Closes are events at 21:00 UTC, published at 21:15 UTC.
- The quarterly score is `s` + noise (sd 0.5), reported 25 to 45 days after the quarter ends. About 30% of reports are restated 10 to 60 days later with less noise (sd 0.2).
- Everything is generated on a fixed calendar and cut at the run date by publication time, so a later run date reproduces all earlier rows exactly (tested).

Because the effect is on the *current* quarter, which is only reported after it ends, a point-in-time model can only use last quarter's report (correlated through the AR(1)) and price momentum. A join that ignores publication dates sees the current quarter early; section 2 of the results measures that.

## Training

- Decision rows: one per entity and business day, at 22:00 UTC.
- Label: 1 if the entity's log return over the next 5 business days beats that day's cross-sectional median (balanced per day by construction).
- Features (all via the as-of rule): `ret_5d`, `ret_20d`, `vol_20d`, `volume_z_20d`, `fundamental_score`. `ret_1d` is stored and served but not used by the model.
- Split: test = decisions on or after 2024-01-01. Training rows need `label_end < 2024-01-01` (purge) and a decision date more than 5 business days before it (embargo). In this run both conditions drop the same 200 rows.
- Model: standardised logistic regression (scikit-learn, C = 1), exported to JSON. Evaluation scores the exported JSON model, the same object the model API loads.

## Evaluation and the gate

- AUC, log loss, Brier and accuracy on the test block, and a constant-prior log loss (the training base rate) as the baseline.
- 95% interval for the AUC from a moving-block bootstrap over test days (blocks of 10 consecutive days, 200 resamples, fixed seed). Neighbouring days share 4 of 5 label days and the features are persistent, so resampling single days would understate the uncertainty. I first resampled single days, saw that the intervals were too narrow for overlapping labels, and switched to blocks before freezing the results.
- Gate (all must hold): at least 1,000 test rows; AUC interval lower bound above 0.5; log loss below the constant-prior log loss.
- A warning-only asset check flags any AUC above 0.70 as probable leakage.

## What was chosen on which data

There is no separate held-out run here: the generator parameters (beta, rho, noise levels), the gate rules and the block length were set while looking at these seed-7 results. The claims are therefore about the machinery (the gate separates a real signal from none, the point-in-time join is consistent across implementations, the cache never serves stale answers), not about a model's out-of-sample skill.

## Reproducing

```bash
pip install -c constraints.txt -e ".[dev]"
make bench          # results/*.json and RESULTS.md
pytest -q tests/test_bench.py   # reruns and compares with the committed files
```

The bench records no timings, so every field is deterministic. The comparison allows a relative 1e-6 on floats for differences between platforms and BLAS builds.

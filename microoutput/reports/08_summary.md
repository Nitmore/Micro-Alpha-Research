# 08 · Robustness, controls and the one-time test unlock

protocol sha256 0224381a70665d880a38c72f35f2b250fa3c97b7281b56e817d71372b22234af; frozen choices sha256 c540fa09a5111069488bcebca00cae46a03d591ffce33a93abac819a22ab762e; test unlock ledger: test_unlock.json (first unlock 2026-10-11T09:58:27+00:00, re-runs 0)

signal diagnostics (pre-unlock): D6 R² -0.0069 with IC 0.157 and calibration slope 0.47 → ranking information is intact (IC > 0) but the forecast is mis-scaled: slope 0.47 (1 = well scaled), intercept 0.0036 ticks; condition number 45.2; VIF qi 9.5, micro 9.6; half-life D6 > 1000 ms (IC(1 s)/IC(0) = 0.63, CI 0.56–0.69)
Track A pre-unlock (D6, L=130): timed 0.205 [0.176, 0.237]; last-second 0.000 [-0.042, 0.041]; matched delay 0.016 [-0.013, 0.049]; signal value 0.189 [0.151, 0.229]
θ_A extended grid (dev, diagnostic): {0.1: 0.243, 0.25: 0.249, 0.5: 0.267, 1.0: 0.287, 2.0: 0.338, 3.0: 0.344, 5.0: 0.375, 8.0: 0.174, 13.0: 0.098}; LODO θ choices [2.0, 2.0, 2.0, 2.0, 2.0]

TEST (D7–D8):
  H1: dev S → test SUPPORTED (same verdict) — β>0 & t>1.96 both days: True; OFI R² > TI R² on 2/2; OFI R² [0.429, 0.5] vs TI [0.372, 0.263]
  H2: dev S → test SUPPORTED (same verdict) — slope -0.868 CI [-0.946, -0.773] (includes −1: False); open-snapshot -0.32 CI [-0.364, -0.274]
  H3: dev S → test SUPPORTED (same verdict) — RW-significant horizons [0.1, 0.25, 0.5, 1.0, 5.0, 30.0]; peak 0.1 s; OOS R² at 0.1 s 0.0230
  H4: dev NS → test NOT SUPPORTED (same verdict) — mid-change beats clock: True; volume beats clock: False
  H5: dev NS → test NOT SUPPORTED (same verdict) — winning: none; levels kept {0.1: 7, 0.25: 1, 0.5: 1, 1.0: 2, 5.0: 2, 30.0: 1}
  H6: dev S → test SUPPORTED (same verdict) — γ {'2026-09-13': 0.439, '2026-09-14': 0.355}; kernel β {'2026-09-13': 0.2, '2026-09-14': 0.175} (pooled 0.175)
  H7: dev S → test SUPPORTED (same verdict) — significant horizons [0.1, 0.25, 0.5, 1.0]; max gain 0.00279
  signal (frozen): R² [0.0257, 0.0593], IC [0.182, 0.247], half-life 750 ms
  Track A (L=130): timed 0.339 [0.309, 0.370]; last-second 0.020 [-0.056, 0.095]; signal value 0.318 [0.272, 0.357] → the OFI signal adds value beyond timing (signal-value CI excludes 0 on the test days)
  Track A by latency (timed vs TWAP): L=0: 0.373; L=50: 0.358; L=130: 0.339; L=250: 0.313
  Track B headline: net -4242.6 USDT at 2.0 bp, break-even -0.911 bp, 5 s markout -26.22 ticks/ETH; kill: ['max drawdown > 3 × daily PnL std', '5 s markout worse than the spread captured (markout_5s < 0)']
  Track C trades: 0
  deflated Sharpe: Track A saving (test, per parent): SR 0.8708, sr0 0.0289, DSR 1.000; Track A signal value (test, per parent; no selection): SR 0.6349, sr0 0.0000, DSR 1.000; Track B headline (test, per 5 min): SR -0.3230, sr0 0.7897, DSR 0.000
Funding not modelled (no funding-rate data); holding periods are sub-hour.
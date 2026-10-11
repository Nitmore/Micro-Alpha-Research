# 04 · RPI test, features, protocol: summary

QU trades (unexplained, |Δmid| ≤ 1 tick): 18,134; better than visible 95.3%; 1–3 tick improvement 24.0%; covering queue drop 82.3% (control 79.7%); single fill 5.9% (control 32.3%)
PASS R1  ≥ 70% better than the visible book (pooled and ≥ 6 days)
FAIL R2  ≥ 80% improve by 1–3 ticks (pooled and ≥ 6 days)
FAIL R3  ≤ 20% with a covering visible queue drop (pooled and ≥ 6 days)
STAGE 0: NOT PASSED

feature grid: 6,912,000 bins of 100 ms per clock; valid ex 97.88%, rx 97.85%
rpi-flagged trades: 18,134 (0.004%)
protocol sha256: 0224381a70665d880a38c72f35f2b250fa3c97b7281b56e817d71372b22234af
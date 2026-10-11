# 02 · Trades and trade–book tests: summary

trades: 35,255,841 over 8 days; missing IDs 139,453 (0.394%)
matched to a book batch: 99.999%; usable (valid book): 98.37%
at touch 43.01% | deeper top-10 27.08% | beyond top-10 27.96% | 1-batch slack 1.00%
UNEXPLAINED 0.943% of trades, 2.475% of volume
sweeps preceded 95.76% (beyond top-10: 94.48%); monotone 98.25%
test 3: V <= q_pre 97.63%; refill within batch 31.43%
missing-ID test: depletion ratio gap/other 1.042, P(dep) 0.267 vs 0.258
quality windows: {'invalid_book': 231.0, 'latency': 4.8, 'resync_gap': 13.4}

PASS BOOK  all 192 hours reconstructed
PASS BOOK  C++ == numba on every batch of every hour
PASS BOOK  zero crossed books in valid batches
PASS BOOK  every pu break followed by a resync snapshot in the same hour
PASS BOOK  exact snapshot audits wherever replay and snapshot share an update id
PASS TRADES all days loaded, no duplicate/backward IDs, time never decreases
PASS TRADES on grid and quote_qty == price × qty exactly
FAIL JOIN  unexplained ≤ 0.5% of usable trades, every day
FAIL JOIN  unexplained ≤ 1.0% of usable volume, every day
PASS JOIN  sweep fills preceded by a touch fill ≥ 95%, every day
FAIL JOIN  sweeps walk the book in order ≥ 99%, every day
PASS JOIN  ≥ 95% of trades in valid book periods, every day
PASS JOIN  ≥ 99.9% of each day's trades matched to a book batch

STAGE 0: NOT PASSED
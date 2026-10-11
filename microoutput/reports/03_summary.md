# 03 · Events and OFI: summary

valid batches: 25,206,056 of 25,720,661; identity violations: 0
taker orders: 5,861,027; walk the book in order: worst day 99.685%
OFI variance shares (all days): limit 0.178, market 0.017, move 0.805
trading-and-growth at the touch: 33.83%
unexplained trades: quiet-batch rate 0.2296%; share in fast batches 90.1%; found within ±5 batches 18.8%
hand-check: PASS

PASS notebook 02: every BOOK and TRADES check passed
PASS event identity: level-1 OFI == limit + market + move on every batch
FAIL quiet batches (|Δmid| ≤ 1 tick): unexplained rate ≤ 0.05%, every day
PASS ≥ 80% of unexplained trades in fast batches (|Δmid| ≥ 4 ticks), every day
PASS taker orders walk the book in order ≥ 99%, every day

STAGE 0: NOT PASSED
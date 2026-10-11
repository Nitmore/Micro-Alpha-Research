# 07 · Deep vs linear, strategy & execution (fit D1–D5, choices on D6; test days unopened)

Part A: complex model accepted = none; trading signal = linear
  linear_ofi1 1s: dev-WF 0.00920 / D6 0.00808; linear_ofi1 5s: dev-WF 0.00493 / D6 0.00572; linear_ofi1 30s: dev-WF 0.00125 / D6 0.00099; ridge_window 1s: dev-WF 0.03609 / D6 -0.00077; ridge_window 5s: dev-WF 0.02083 / D6 0.01797; ridge_window 30s: dev-WF 0.00357 / D6 0.00650; cnn 1s: dev-WF 0.01971 / D6 -0.01796; cnn 5s: dev-WF 0.01333 / D6 0.01225; cnn 30s: dev-WF 0.00266 / D6 0.00399
signal OOS R² on D6 (received clock, 1 s): -0.00691; alpha half-life inf ms
Track A (L=130 ms, 50 ETH / 10 min, θ_A=2.0): TWAP -0.161 bp, OFI-timed -0.366 bp, saving 0.205 bp [95% CI 0.173, 0.235]
  by latency: L=0: 0.215 [0.181,0.246]; L=50: 0.211 [0.178,0.243]; L=130: 0.205 [0.173,0.235]; L=250: 0.194 [0.161,0.227]
Track B (L=130, pessimistic, skew θ_B=1.0): net -790.65 USDT at maker 2.0 bp; break-even maker fee -1.350 bp; 5 s markout -24.742 ticks/ETH; kill criteria triggered: ['max drawdown > 3 × daily PnL std', '5 s markout worse than the spread captured (markout_5s < 0)']
Track C: bins where |forecast| > round-trip cost: 0
Funding not modelled (no funding-rate data); holding periods are sub-hour.
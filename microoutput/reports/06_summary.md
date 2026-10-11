# 06 · Multi-level OFI and Hawkes (development days)

H5: NOT SUPPORTED — winning candidates (beat OFI1 and microprice, Holm 5%): none; levels kept by horizon: {0.1: 5, 0.25: 1, 0.5: 1, 1.0: 1, 5.0: 1, 30.0: 1}
H7: SUPPORTED — horizons with significant gain: [0.1, 0.25, 0.5, 1.0]; max OOS R² gain 0.005899

OOS R² (selected models):
  h=0.1s: micro 0.02477 | OFI1 0.01827 | L1-10 0.01983 | ridge 0.01983 | integrated 0.01490 | OFI1+micro 0.03428
  h=0.25s: micro 0.02914 | OFI1 0.01418 | L1-10 0.01432 | ridge 0.01431 | integrated 0.00888 | OFI1+micro 0.03411
  h=0.5s: micro 0.03380 | OFI1 0.01197 | L1-10 0.01186 | ridge 0.01185 | integrated 0.00661 | OFI1+micro 0.03600
  h=1.0s: micro 0.03600 | OFI1 0.00919 | L1-10 0.00916 | ridge 0.00919 | integrated 0.00595 | OFI1+micro 0.03655
  h=5.0s: micro 0.02527 | OFI1 0.00315 | L1-10 0.00281 | ridge 0.00247 | integrated 0.00180 | OFI1+micro 0.02519
  h=30.0s: micro 0.00520 | OFI1 0.00072 | L1-10 -0.00157 | ridge -0.00065 | integrated 0.00094 | OFI1+micro 0.00505

taker Hawkes: best kernel by held-out LL = multiscale_12exp; hourly branching ratio mean 0.417 [0.400, 0.434] (upper bound: constant baseline per hour); jitter std 0.0011
six-stream: sell taker → bid cancel (adverse-selection flight): 0.018±0.012; sell taker → bid add (replenishment): 0.024±0.022; buy taker → ask cancel (adverse-selection flight): 0.024±0.011; buy taker → ask add (replenishment): 0.040±0.038; buy taker → buy taker (herding/splitting): 0.438±0.071; sell taker → sell taker: 0.459±0.074; bid cancel → sell taker: 0.026±0.012; ask cancel → buy taker: 0.037±0.009
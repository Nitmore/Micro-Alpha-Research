# 01 · Book reconstruction: summary

files: 192 hours, 204 segments, 25,720,662 batches

PASS all 192 hours present
PASS zero crossed books in valid segments
FAIL zero pu-chain breaks
FAIL every segment snapshot-linked or continued
PASS u increasing & T non-decreasing everywhere
PASS C++ and numba identical on every batch

boundaries: 203; chain continues across boundary: 22
exact audits: 22; touch equal: 22
L - prev_last_u: {'count': 34.0, 'mean': 2631620.0, 'std': 8970131.0, 'min': 0.0, '25%': 0.0, '50%': 0.0, '75%': 331871.0, 'max': 46756786.0}
modes: {'continued': 169, 'snapshot_linked': 23, 'snapshot_bracket': 12}

latency median of hourly medians: 126.6 ms; worst p99: 30745.3 ms; max 48804 ms
levels outside price range ignored: 2132
C++ vs numba: 192 / 192 hours identical
files with extra snapshots / leading updates: 173
# Microstructure Alpha: Binance ETHUSDT perpetual, 7–14 Sep 2026

Measure order-flow imbalance exactly on real data, test what predicts price, and show honestly what survives fees, latency and adverse selection.

## Layout
```
microalpha/
  cpp/                      C++17 order-book builder (CMake; MSVC or g++)
    include/l2_format.hpp   binary formats shared with Python (.l2bin input, .bookbin output)
    include/book.hpp        tick-indexed price-level book
    src/book.cpp
    src/book_builder.cpp    CLI: futures sync rules, pu-chain checks, snapshot audits, top-K per batch
    tests/test_book.cpp     unit tests (ctest)
  notebooks/
    ma_lib.py               shared Python library (exact parsing, numba reference engine, C++ bridge)
    01_book_reconstruction.ipynb
```

## Build the C++ builder (Windows, MSVC)
From "x64 Native Tools Command Prompt for VS 2022":
```
cd microalpha\cpp
cmake -S . -B build -G "Visual Studio 17 2022" -A x64
cmake --build build --config Release
ctest --test-dir build -C Release --output-on-failure
```
Output: `cpp\build\Release\book_builder.exe`. Linux/macOS: `cmake -S . -B build && cmake --build build && ctest --test-dir build`.

## Run notebook 01
`pip install pyarrow pandas numpy numba matplotlib`, open `notebooks/01_book_reconstruction.ipynb`, set `ROOT_DIR` / `OUT_DIR` in the config cell, and run all cells. Send back `OUT_DIR/reports/01_summary.md`, `01_sync_log.csv`, `01_sync_plots.png` and `01_engine_crosscheck.csv`.

## Data rules
- Prices are integer ticks (0.01 USDT) and sizes integer lots (0.001 ETH), parsed exactly from the strings.
- L2 quantity is the new absolute level size; 0 removes the level.
- Futures sync: after a snapshot with lastUpdateId L, the first update needs `pu == L`, then `pu ==` the previous `u`. Update IDs skip between messages, so `U == L+1` is not the test.
- Never open the trade CSVs in Excel (it destroys millisecond timestamps).

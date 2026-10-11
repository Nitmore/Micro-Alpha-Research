// l2_format.hpp — binary formats shared between the Python ingest step and the C++ book builder.
#pragma once
#include <cstdint>

namespace ma {

// One row of an ingested hourly L2 file (.l2bin), written by ma_lib.ingest_hour().
// Prices are integer ticks (0.01 USDT), sizes integer lots (0.001 ETH). Little-endian, packed.
// Snapshot rows (snap=1) come first and carry u = lastUpdateId L; U, pu are -1 for them.
#pragma pack(push, 1)
struct L2Row {
    int64_t u, U, pu;   // final_update_id, first_update_id, prev_final_update_id
    int64_t T;          // transaction_time (ms, matching engine)
    int64_t R;          // received_time (ns, local capture)
    int64_t px;         // price in ticks
    int64_t qty;        // NEW ABSOLUTE size in lots (0 = remove level)
    int8_t side;        // 0 = bid, 1 = ask
    int8_t snap;        // 1 = snapshot row, 0 = update row
};
#pragma pack(pop)
static_assert(sizeof(L2Row) == 58, "L2Row must be 58 bytes");

constexpr int64_t kMaxTick   = 2000000;   // 20,000 USDT: levels at/above (or <= 0) are ignored and counted
constexpr int64_t kScanLimit = 200000;    // max ticks scanned when collecting top-K levels

// Book output file (.bookbin): 16-byte header "MABOOK01" + int32 K + int32 reserved, then one packed
// record per batch: u, T, R (int64) | valid (int8) | n_rows (int32) | bp[K] (int32) | bq[K] (int64)
//                                    | ap[K] (int32) | aq[K] (int64)
constexpr char kBookMagic[9] = "MABOOK01";

}  // namespace ma

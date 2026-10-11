// test_book.cpp — deterministic unit tests for the Book class (no external framework; exit code != 0 on failure).
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <vector>
#include "book.hpp"
using namespace ma;

static int failures = 0;
#define CHECK(cond) do { if (!(cond)) { std::printf("FAIL %s:%d  %s\n", __FILE__, __LINE__, #cond); ++failures; } } while (0)

static L2Row row(int64_t px, int64_t q, int side) { L2Row r{}; r.px = px; r.qty = q; r.side = (int8_t)side; r.snap = 1; return r; }

int main() {
    Book b(1000);
    // snapshot: bids 100x5, 99x3 ; asks 101x4, 102x6  (same book as ma_lib.self_test)
    std::vector<L2Row> snap = {row(100, 5, 0), row(99, 3, 0), row(101, 4, 1), row(102, 6, 1)};
    CHECK(b.load_snapshot(snap.data(), snap.size()) == 0);
    CHECK(b.best_bid() == 100 && b.best_ask() == 101);
    int32_t bp[3], ap[3]; int64_t bq[3], aq[3];

    // batch 1: ask 101 -> 0 (remove best ask), bid 100 -> 7 (absolute size, not a delta)
    b.apply(101, 0, 1); b.apply(100, 7, 0);
    b.top_k(3, bp, bq, ap, aq);
    CHECK(bp[0] == 100 && bq[0] == 7 && bp[1] == 99 && bq[1] == 3 && bp[2] == 0);
    CHECK(ap[0] == 102 && aq[0] == 6 && ap[1] == 0);

    // batch 2: bid 101 -> 2 (improves), ask 102 -> 0 (best-ask rescan), ask 104 -> 1
    b.apply(101, 2, 0); b.apply(102, 0, 1); b.apply(104, 1, 1);
    b.top_k(3, bp, bq, ap, aq);
    CHECK(bp[0] == 101 && bq[0] == 2 && bp[1] == 100 && bp[2] == 99);
    CHECK(ap[0] == 104 && aq[0] == 1 && ap[1] == 0);
    CHECK(b.best_ask() == 104);

    // removing a non-best level does not move the best; removing a missing level is a no-op
    b.apply(99, 0, 0); b.apply(50, 0, 0);
    CHECK(b.best_bid() == 101 && b.levels(0) == 2);

    // emptying a side: best bid falls to 0, best ask to max_tick
    b.apply(101, 0, 0); b.apply(100, 0, 0);
    CHECK(b.best_bid() == 0 && b.levels(0) == 0);
    b.apply(104, 0, 1);
    CHECK(b.best_ask() == 1000);

    // out-of-range prices are rejected
    CHECK(!b.apply(0, 5, 0) && !b.apply(1000, 5, 1) && !b.apply(-3, 1, 1));

    // state save/load round trip
    b.apply(200, 9, 0); b.apply(205, 4, 1); b.apply(210, 1, 1);
    const auto path = (std::filesystem::temp_directory_path() / "ma_state_test.bin").string();
    b.save(path, 4242, 1);
    Book c(1000); int64_t lu = 0; int v = 0;
    CHECK(c.load(path, lu, v));
    CHECK(lu == 4242 && v == 1 && c.best_bid() == 200 && c.best_ask() == 205 && c.qty_at(1, 210) == 1 && c.levels(1) == 2);
    std::filesystem::remove(path);

    if (failures == 0) std::printf("all Book tests passed\n");
    return failures ? 1 : 0;
}

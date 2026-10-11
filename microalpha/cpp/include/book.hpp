// book.hpp — price-level order book on a dense tick-indexed array with incrementally tracked best prices.
#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include "l2_format.hpp"

namespace ma {

class Book {
public:
    explicit Book(int64_t max_tick = kMaxTick);

    void clear();
    // Replace the whole book with snapshot rows; returns number of rows ignored (price out of range).
    int64_t load_snapshot(const L2Row* rows, size_t n);
    // Set one level to an absolute size (0 removes it). Returns false if the price is out of range.
    bool apply(int64_t px, int64_t qty, int side);

    int64_t best_bid() const { return bb_; }   // 0 if the bid side is empty
    int64_t best_ask() const { return ba_; }   // max_tick if the ask side is empty
    int64_t qty_at(int side, int64_t px) const;
    // Best-first top K levels; unused slots are zero.
    void top_k(int K, int32_t* bp, int64_t* bq, int32_t* ap, int64_t* aq) const;
    int64_t levels(int side) const;

    // Sparse state persistence (non-empty levels only) so the book can be carried across process runs.
    void save(const std::string& path, int64_t last_u, int valid) const;
    bool load(const std::string& path, int64_t& last_u, int& valid);

private:
    int64_t max_tick_;
    std::vector<int64_t> bid_, ask_;
    int64_t bb_, ba_;
};

}  // namespace ma

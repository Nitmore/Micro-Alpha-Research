#include "book.hpp"
#include <algorithm>
#include <cstring>
#include <fstream>
#include <stdexcept>

namespace ma {

Book::Book(int64_t max_tick) : max_tick_(max_tick), bid_(max_tick, 0), ask_(max_tick, 0), bb_(0), ba_(max_tick) {}

void Book::clear() {
    std::fill(bid_.begin(), bid_.end(), 0);
    std::fill(ask_.begin(), ask_.end(), 0);
    bb_ = 0;
    ba_ = max_tick_;
}

int64_t Book::load_snapshot(const L2Row* rows, size_t n) {
    clear();
    int64_t dropped = 0;
    for (size_t i = 0; i < n; ++i) {
        const int64_t p = rows[i].px, q = rows[i].qty;
        if (p <= 0 || p >= max_tick_) { ++dropped; continue; }
        if (rows[i].side == 1) { ask_[p] = q; if (q > 0 && p < ba_) ba_ = p; }
        else                   { bid_[p] = q; if (q > 0 && p > bb_) bb_ = p; }
    }
    return dropped;
}

bool Book::apply(int64_t p, int64_t q, int side) {
    if (p <= 0 || p >= max_tick_) return false;
    if (side == 1) {
        ask_[p] = q;
        if (q > 0) { if (p < ba_) ba_ = p; }
        else if (p == ba_) { while (ba_ < max_tick_ && ask_[ba_] == 0) ++ba_; }
    } else {
        bid_[p] = q;
        if (q > 0) { if (p > bb_) bb_ = p; }
        else if (p == bb_) { while (bb_ > 0 && bid_[bb_] == 0) --bb_; }
    }
    return true;
}

int64_t Book::qty_at(int side, int64_t p) const {
    if (p <= 0 || p >= max_tick_) return 0;
    return side == 1 ? ask_[p] : bid_[p];
}

void Book::top_k(int K, int32_t* bp, int64_t* bq, int32_t* ap, int64_t* aq) const {
    std::memset(bp, 0, sizeof(int32_t) * K); std::memset(bq, 0, sizeof(int64_t) * K);
    std::memset(ap, 0, sizeof(int32_t) * K); std::memset(aq, 0, sizeof(int64_t) * K);
    int n = 0;
    for (int64_t p = bb_, lim = bb_ - kScanLimit; n < K && p > 0 && p > lim; --p)
        if (bid_[p] > 0) { bp[n] = static_cast<int32_t>(p); bq[n] = bid_[p]; ++n; }
    n = 0;
    for (int64_t p = ba_, lim = ba_ + kScanLimit; n < K && p < max_tick_ && p < lim; ++p)
        if (ask_[p] > 0) { ap[n] = static_cast<int32_t>(p); aq[n] = ask_[p]; ++n; }
}

int64_t Book::levels(int side) const {
    const auto& v = side == 1 ? ask_ : bid_;
    return std::count_if(v.begin(), v.end(), [](int64_t q) { return q > 0; });
}

void Book::save(const std::string& path, int64_t last_u, int valid) const {
    std::ofstream f(path, std::ios::binary | std::ios::trunc);
    if (!f) throw std::runtime_error("cannot write state file " + path);
    const char magic[8] = {'M', 'A', 'S', 'T', 'A', 'T', 'E', '1'};
    f.write(magic, 8);
    int64_t hdr[4] = {max_tick_, last_u, valid, 0};
    f.write(reinterpret_cast<const char*>(hdr), sizeof(hdr));
    for (int side = 0; side < 2; ++side) {
        const auto& v = side == 1 ? ask_ : bid_;
        std::vector<int64_t> buf;
        for (int64_t p = 1; p < max_tick_; ++p) if (v[p] > 0) { buf.push_back(p); buf.push_back(v[p]); }
        int64_t n = static_cast<int64_t>(buf.size() / 2);
        f.write(reinterpret_cast<const char*>(&n), 8);
        f.write(reinterpret_cast<const char*>(buf.data()), static_cast<std::streamsize>(buf.size() * 8));
    }
}

bool Book::load(const std::string& path, int64_t& last_u, int& valid) {
    std::ifstream f(path, std::ios::binary);
    if (!f) return false;
    char magic[8];
    f.read(magic, 8);
    if (std::memcmp(magic, "MASTATE1", 8) != 0) throw std::runtime_error("bad state file " + path);
    int64_t hdr[4];
    f.read(reinterpret_cast<char*>(hdr), sizeof(hdr));
    if (hdr[0] != max_tick_) throw std::runtime_error("state file max_tick mismatch");
    last_u = hdr[1];
    valid = static_cast<int>(hdr[2]);
    clear();
    for (int side = 0; side < 2; ++side) {
        int64_t n = 0;
        f.read(reinterpret_cast<char*>(&n), 8);
        std::vector<int64_t> buf(static_cast<size_t>(n * 2));
        f.read(reinterpret_cast<char*>(buf.data()), static_cast<std::streamsize>(buf.size() * 8));
        for (int64_t i = 0; i < n; ++i) {
            const int64_t p = buf[2 * i], q = buf[2 * i + 1];
            if (side == 1) { ask_[p] = q; if (p < ba_) ba_ = p; }
            else           { bid_[p] = q; if (p > bb_) bb_ = p; }
        }
    }
    if (!f) throw std::runtime_error("truncated state file " + path);
    return true;
}

}  // namespace ma

// book_builder.cpp — replay one ingested L2 hour on top of the carried book state.
//
// Usage:
//   book_builder --in HOUR.l2bin --book-out HOUR.bookbin --log sync_log.csv --state book_state.bin
//                [--k 10] [--audit 20] [--reset]
//
// Binance USD-M futures sync rules (update IDs are NOT contiguous between messages; pu carries continuity):
//   snapshot_linked  : first update has pu == L            -> reset to snapshot, chain-check from L
//   continued        : pu == previous file's last u        -> keep replayed book (snapshot only audited)
//   snapshot_bracket : after dropping u < L, U <= L <= u   -> reset to snapshot (documented fallback)
//   unsynced         : none of the above                   -> reset to snapshot, whole hour flagged invalid
// Within the hour every message must satisfy pu == previous u; a break flags the book invalid until the
// next snapshot reset.
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "book.hpp"
#include "l2_format.hpp"

using namespace ma;
namespace fs = std::filesystem;

namespace {

std::vector<L2Row> read_rows(const std::string& path) {
    std::ifstream f(path, std::ios::binary | std::ios::ate);
    if (!f) throw std::runtime_error("cannot open " + path);
    const auto bytes = static_cast<size_t>(f.tellg());
    if (bytes % sizeof(L2Row) != 0) throw std::runtime_error("file size is not a multiple of 58: " + path);
    std::vector<L2Row> rows(bytes / sizeof(L2Row));
    f.seekg(0);
    f.read(reinterpret_cast<char*>(rows.data()), static_cast<std::streamsize>(bytes));
    return rows;
}

// numpy-compatible percentile (linear interpolation, numpy's default)
double percentile(std::vector<double> v, double q) {
    if (v.empty()) return 0.0;
    std::sort(v.begin(), v.end());
    const double pos = q / 100.0 * static_cast<double>(v.size() - 1);
    const size_t lo = static_cast<size_t>(pos);
    const size_t hi = std::min(lo + 1, v.size() - 1);
    return v[lo] + (v[hi] - v[lo]) * (pos - static_cast<double>(lo));
}

struct Snapshot { std::vector<int64_t> px[2], q[2]; };   // best-first top-N per side

Snapshot snapshot_topn(const L2Row* rows, size_t n, int N) {
    Snapshot s;
    std::vector<std::pair<int64_t, int64_t>> side[2];
    for (size_t i = 0; i < n; ++i)
        if (rows[i].qty > 0) side[rows[i].side].emplace_back(rows[i].px, rows[i].qty);
    std::sort(side[0].begin(), side[0].end(), [](auto& a, auto& b) { return a.first > b.first; });
    std::sort(side[1].begin(), side[1].end(), [](auto& a, auto& b) { return a.first < b.first; });
    for (int k = 0; k < 2; ++k) {
        s.px[k].assign(N, 0); s.q[k].assign(N, 0);
        for (int i = 0; i < N && i < static_cast<int>(side[k].size()); ++i) {
            s.px[k][i] = side[k][i].first; s.q[k][i] = side[k][i].second;
        }
    }
    return s;
}

const std::vector<std::string> kLogCols = {
    "file", "rows", "snapshot_levels", "L", "prev_last_u", "L_minus_prev_last_u", "first_pu_eq_prev_last_u",
    "audit_bid_px_equal", "audit_bid_px_qty_equal", "audit_ask_px_equal", "audit_ask_px_qty_equal",
    "audit_touch_equal", "audit_exact", "mode", "dropped_stale_rows", "first_U", "first_u", "first_pu",
    "first_U_minus_L", "batches", "chain_breaks", "first_break_u", "invalid_batches", "crossed_batches",
    "one_sided_batches", "u_strictly_increasing", "T_non_decreasing", "lat_med_ms", "lat_p99_ms",
    "lat_max_ms", "lat_min_ms", "batch_gap_med_ms", "batch_gap_max_ms", "spread_med_ticks",
    "spread_p99_ticks", "end_best_bid_ticks", "end_best_ask_ticks", "out_of_range_levels", "replay_ms"};

}  // namespace

int main(int argc, char** argv) {
    std::string in, book_out, log_path, state_path;
    int K = 10, N = 20;
    bool reset = false;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto next = [&]() -> std::string {
            if (i + 1 >= argc) throw std::runtime_error("missing value for " + a);
            return argv[++i];
        };
        if (a == "--in") in = next();
        else if (a == "--book-out") book_out = next();
        else if (a == "--log") log_path = next();
        else if (a == "--state") state_path = next();
        else if (a == "--k") K = std::stoi(next());
        else if (a == "--audit") N = std::stoi(next());
        else if (a == "--reset") reset = true;
        else { std::cerr << "unknown argument " << a << "\n"; return 2; }
    }
    if (in.empty() || book_out.empty() || log_path.empty() || state_path.empty()) {
        std::cerr << "usage: book_builder --in H.l2bin --book-out H.bookbin --log log.csv --state state.bin "
                     "[--k 10] [--audit 20] [--reset]\n";
        return 2;
    }
    try {
        const std::vector<L2Row> rows = read_rows(in);
        size_t ns = 0;
        while (ns < rows.size() && rows[ns].snap == 1) ++ns;
        for (size_t i = ns; i < rows.size(); ++i)
            if (rows[i].snap == 1) throw std::runtime_error("snapshot row after update rows (split segments first)");
        if (ns == rows.size()) throw std::runtime_error("no update rows");
        const bool has_snap = ns > 0;                 // segment without its own snapshot = leading updates
        const int64_t L = has_snap ? rows[0].u : -1;
        const L2Row* upd = rows.data() + ns;
        const size_t nu = rows.size() - ns;

        std::map<std::string, std::string> log;
        auto put = [&](const std::string& k, auto v) { std::ostringstream o; o << std::setprecision(12) << v; log[k] = o.str(); };
        auto putb = [&](const std::string& k, bool v) { log[k] = v ? "True" : "False"; };
        put("file", fs::path(in).stem().string());
        put("rows", rows.size()); put("snapshot_levels", ns);
        if (has_snap) put("L", L);

        // carried state
        Book book;
        int64_t last_u = -1;
        int valid = 0;
        const bool has_book = !reset && book.load(state_path, last_u, valid);

        auto write_log = [&]() {
            const bool new_log = !fs::exists(log_path) || fs::file_size(log_path) == 0;
            std::ofstream lf(log_path, std::ios::app);
            if (!lf) throw std::runtime_error("cannot write " + log_path);
            if (new_log) { for (size_t i = 0; i < kLogCols.size(); ++i) lf << (i ? "," : "") << kLogCols[i]; lf << "\n"; }
            for (size_t i = 0; i < kLogCols.size(); ++i) {
                auto it = log.find(kLogCols[i]);
                lf << (i ? "," : "") << (it == log.end() ? "" : it->second);
            }
            lf << "\n";
        };
        // leading updates with no book to apply them to: write an empty book file, leave state untouched
        if (!has_snap && !has_book) {
            std::ofstream bo(book_out, std::ios::binary | std::ios::trunc);
            bo.write(kBookMagic, 8);
            const int32_t hdr0[2] = {K, 0};
            bo.write(reinterpret_cast<const char*>(hdr0), 8);
            put("mode", "skipped_no_book"); put("batches", 0); put("chain_breaks", 0); put("invalid_batches", 0);
            put("crossed_batches", 0); put("one_sided_batches", 0); putb("u_strictly_increasing", true);
            putb("T_non_decreasing", true); put("out_of_range_levels", 0); put("replay_ms", 0);
            write_log();
            std::cout << log["file"] << "  skipped_no_book  batches 0  breaks 0  crossed 0  replay 0 ms\n";
            return 0;
        }

        // 1) audit replayed book vs this snapshot
        const Snapshot snap = snapshot_topn(rows.data(), ns, N);
        if (has_book && has_snap) {
            put("prev_last_u", last_u); put("L_minus_prev_last_u", L - last_u);
            putb("first_pu_eq_prev_last_u", upd[0].pu == last_u);
            std::vector<int32_t> bp(N), ap(N); std::vector<int64_t> bq(N), aq(N);
            book.top_k(N, bp.data(), bq.data(), ap.data(), aq.data());
            int eq_px[2] = {0, 0}, eq_pq[2] = {0, 0};
            for (int i = 0; i < N; ++i) {
                if (snap.px[0][i] == bp[i]) { ++eq_px[0]; if (snap.q[0][i] == bq[i]) ++eq_pq[0]; }
                if (snap.px[1][i] == ap[i]) { ++eq_px[1]; if (snap.q[1][i] == aq[i]) ++eq_pq[1]; }
            }
            put("audit_bid_px_equal", eq_px[0]); put("audit_bid_px_qty_equal", eq_pq[0]);
            put("audit_ask_px_equal", eq_px[1]); put("audit_ask_px_qty_equal", eq_pq[1]);
            putb("audit_touch_equal", snap.px[0][0] == bp[0] && snap.px[1][0] == ap[0] &&
                                      snap.q[0][0] == bq[0] && snap.q[1][0] == aq[0]);
            putb("audit_exact", eq_pq[0] == N && eq_pq[1] == N);
        }

        // 2) choose how to start the hour
        size_t start = 0;
        bool do_reset = true;
        std::string mode;
        int64_t chain_from;
        if (!has_snap) {
            do_reset = false;
            if (valid && upd[0].pu == last_u) { mode = "continued"; chain_from = last_u; }
            else { mode = "unsynced_no_snapshot"; chain_from = -1; valid = 0; }
        }
        else if (upd[0].pu == L) { mode = "snapshot_linked"; chain_from = L; valid = 1; }
        else if (has_book && valid && upd[0].pu == last_u) { mode = "continued"; chain_from = last_u; do_reset = false; }
        else {
            while (start < nu && upd[start].u < L) ++start;
            if (start < nu && upd[start].U <= L && L <= upd[start].u) { mode = "snapshot_bracket"; valid = 1; }
            else { mode = "unsynced"; valid = 0; }
            chain_from = -1;
        }
        if (start >= nu) throw std::runtime_error("no usable updates after dropping u < L");
        put("mode", mode); put("dropped_stale_rows", start);
        put("first_U", upd[start].U); put("first_u", upd[start].u); put("first_pu", upd[start].pu);
        if (has_snap) put("first_U_minus_L", upd[start].U - L);
        int64_t out_of_range = 0;
        if (do_reset) out_of_range += book.load_snapshot(rows.data(), ns);

        // 3) replay, writing one record per batch
        std::ofstream bo(book_out, std::ios::binary | std::ios::trunc);
        if (!bo) throw std::runtime_error("cannot write " + book_out);
        bo.write(kBookMagic, 8);
        const int32_t hdr[2] = {K, 0};
        bo.write(reinterpret_cast<const char*>(hdr), 8);
        const size_t rec = 3 * 8 + 1 + 4 + K * (4 + 8) * 2;
        std::vector<char> buf(rec);
        std::vector<int32_t> bp(K), ap(K); std::vector<int64_t> bq(K), aq(K);

        const auto t0 = std::chrono::steady_clock::now();
        int64_t prev = chain_from, breaks = 0, first_break_u = -1, batches = 0, invalid = 0, crossed = 0, one_sided = 0;
        int64_t prev_u = INT64_MIN, prev_T = INT64_MIN, gap_max = 0;
        bool u_inc = true, t_nd = true;
        std::vector<double> lat, gaps, spreads;
        for (size_t s = start; s < nu;) {
            size_t e = s;
            while (e < nu && upd[e].u == upd[s].u) ++e;
            if (prev >= 0 && upd[s].pu != prev) { ++breaks; valid = 0; if (first_break_u < 0) first_break_u = upd[s].u; }
            for (size_t i = s; i < e; ++i)
                if (!book.apply(upd[i].px, upd[i].qty, upd[i].side)) ++out_of_range;
            prev = upd[s].u;
            book.top_k(K, bp.data(), bq.data(), ap.data(), aq.data());
            // stats
            const int64_t u = upd[s].u, T = upd[s].T, R = upd[s].R;
            if (prev_u != INT64_MIN) {
                if (u <= prev_u) u_inc = false;
                if (T < prev_T) t_nd = false;
                gaps.push_back(static_cast<double>(T - prev_T));
                gap_max = std::max(gap_max, T - prev_T);
            }
            prev_u = u; prev_T = T;
            lat.push_back(static_cast<double>(R) / 1e6 - static_cast<double>(T));
            if (!valid) ++invalid;
            if (bp[0] > 0 && ap[0] > 0) { if (bp[0] >= ap[0]) ++crossed; spreads.push_back(static_cast<double>(ap[0] - bp[0])); }
            else ++one_sided;
            // record
            char* w = buf.data();
            auto put_raw = [&](const void* src, size_t n) { std::memcpy(w, src, n); w += n; };
            const int8_t v8 = static_cast<int8_t>(valid);
            const int32_t nrows = static_cast<int32_t>(e - s);
            put_raw(&u, 8); put_raw(&T, 8); put_raw(&R, 8); put_raw(&v8, 1); put_raw(&nrows, 4);
            put_raw(bp.data(), 4 * K); put_raw(bq.data(), 8 * K); put_raw(ap.data(), 4 * K); put_raw(aq.data(), 8 * K);
            bo.write(buf.data(), static_cast<std::streamsize>(rec));
            ++batches;
            s = e;
        }
        const double replay_ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
        bo.close();
        book.save(state_path, prev, valid);

        put("batches", batches); put("chain_breaks", breaks);
        if (first_break_u >= 0) put("first_break_u", first_break_u);
        put("invalid_batches", invalid); put("crossed_batches", crossed); put("one_sided_batches", one_sided);
        putb("u_strictly_increasing", u_inc); putb("T_non_decreasing", t_nd);
        put("lat_med_ms", percentile(lat, 50)); put("lat_p99_ms", percentile(lat, 99));
        put("lat_max_ms", *std::max_element(lat.begin(), lat.end()));
        put("lat_min_ms", *std::min_element(lat.begin(), lat.end()));
        if (!gaps.empty()) { put("batch_gap_med_ms", percentile(gaps, 50)); put("batch_gap_max_ms", gap_max); }
        if (!spreads.empty()) { put("spread_med_ticks", percentile(spreads, 50)); put("spread_p99_ticks", percentile(spreads, 99)); }
        put("end_best_bid_ticks", book.best_bid()); put("end_best_ask_ticks", book.best_ask());
        put("out_of_range_levels", out_of_range); put("replay_ms", replay_ms);

        // 4) append to sync log
        write_log();
        std::cout << log["file"] << "  " << mode << "  batches " << batches << "  breaks " << breaks
                  << "  crossed " << crossed << "  replay " << std::fixed << std::setprecision(0) << replay_ms << " ms\n";
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "ERROR: " << ex.what() << "\n";
        return 1;
    }
}

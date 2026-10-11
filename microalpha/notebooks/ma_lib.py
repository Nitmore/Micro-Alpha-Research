"""
ma_lib.py — shared library for the Microstructure Alpha project (Binance USD-M ETHUSDT perp).

Part 1 (Notebook 01): file inventory, exact L2 parsing, numba order-book replay engine,
                      Binance-futures sync rules, hour-boundary snapshot audits.

Conventions
  * Prices are integer ticks  (1 tick = 0.01 USDT)   -> exact, no floats anywhere in the book.
  * Sizes are integer lots    (1 lot  = 0.001 ETH).
  * L2 quantity is the NEW ABSOLUTE size of a level; 0 removes the level.
  * Times: T = transaction_time (matching engine, ms), R = received_time (local capture, ns).
  * One "batch" = one diff-depth message = all rows sharing final_update_id u.

Binance USD-M futures sync rules (NOT the spot rules):
  * update IDs are NOT contiguous between messages (median jump ~143); continuity is carried by pu.
  * after loading a snapshot with lastUpdateId L, the stream is linked iff the first applied message
    has pu == L (or, per Binance docs, U <= L <= u after dropping messages with u < L).
  * every later message must have pu == u of the previous message; a break invalidates the book
    until the next snapshot.
"""
from __future__ import annotations

import re
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from numba import njit

MAX_TICK = 2_000_000          # 20,000 USDT; levels at/above this (or <= 0) are ignored and counted
SCAN_LIMIT = 200_000          # max ticks scanned when collecting top-K levels
L2_RE = re.compile(r'(?P<sym>[A-Z0-9]+)_orderbook_(?P<d>\d{8})_(?P<h>\d{2})\.parquet$')
TR_RE = re.compile(r'(?P<sym>[A-Z0-9]+)-trades-(?P<d>\d{4}-\d{2}-\d{2})\.(?P<ext>zip|csv)$')
L2_COLS = ['received_time', 'event_time', 'transaction_time', 'event_type', 'first_update_id',
           'final_update_id', 'prev_final_update_id', 'side', 'price', 'quantity']


# ----------------------------------------------------------------------------------------------
# Inventory
# ----------------------------------------------------------------------------------------------
def inventory(root, symbol: str = 'ETHUSDT') -> pd.DataFrame:
    """Find every L2 hour file and trades file under `root` (any folder layout) by filename."""
    rows = []
    for p in Path(root).rglob('*'):
        if not p.is_file():
            continue
        m = L2_RE.search(p.name)
        if m and m['sym'] == symbol:
            rows.append(dict(kind='l2', date=f"{m['d'][:4]}-{m['d'][4:6]}-{m['d'][6:]}", hour=int(m['h']),
                             path=str(p), mb=p.stat().st_size / 2**20))
            continue
        m = TR_RE.search(p.name)
        if m and m['sym'] == symbol:
            rows.append(dict(kind='trades', date=m['d'], hour=-1, path=str(p), mb=p.stat().st_size / 2**20))
    df = pd.DataFrame(rows, columns=['kind', 'date', 'hour', 'path', 'mb'])
    return df.sort_values(['kind', 'date', 'hour']).reset_index(drop=True)


def check_inventory(inv: pd.DataFrame, dates) -> pd.DataFrame:
    """One row per date: L2 hours present/missing, duplicates, tiny files, trades present."""
    out = []
    for d in dates:
        l2 = inv[(inv.kind == 'l2') & (inv.date == d)]
        tr = inv[(inv.kind == 'trades') & (inv.date == d)]
        hours = set(l2.hour)
        out.append(dict(date=d, l2_files=len(l2), missing_hours=sorted(set(range(24)) - hours),
                        duplicate_hours=sorted(l2.hour[l2.hour.duplicated()].unique().tolist()),
                        smallest_l2_mb=round(l2.mb.min(), 1) if len(l2) else None,
                        l2_total_mb=round(l2.mb.sum()), trades_files=len(tr),
                        trades_mb=round(tr.mb.sum(), 1)))
    return pd.DataFrame(out)


# ----------------------------------------------------------------------------------------------
# Exact parsing
# ----------------------------------------------------------------------------------------------
def dec_to_int(arr, scale: int) -> np.ndarray:
    """Exact decimal string -> int64 scaled by 10**scale ('2513.47', 2 -> 251347).
    Raises pyarrow.ArrowInvalid if any value has more decimals than `scale` (off-grid)."""
    if isinstance(arr, pa.ChunkedArray):
        arr = arr.combine_chunks()
    if pa.types.is_dictionary(arr.type):
        arr = arr.dictionary_decode()
    if arr.null_count:
        raise ValueError('null price/quantity')
    d = pc.cast(arr, pa.decimal128(18, scale))
    raw = np.frombuffer(d.buffers()[1], dtype='<i8')
    return raw[2 * d.offset: 2 * (d.offset + len(d)): 2].copy()   # low 64 bits of each int128


def load_l2_hour(path) -> dict:
    """Read one hourly diff-depth parquet into exact integer arrays and split it into SEGMENTS.

    A normal file is one segment: snapshot rows, then update rows. If the recorder took another snapshot
    inside the hour (e.g. after a reconnect), every snapshot starts a new segment. Update rows that come
    before the first snapshot form a 'leading' segment with no snapshot of its own.
    Returns dict(path, n, segments=[view, ...]); each view has the single-segment layout used by the engines:
    n_snap, L, snap_time, u/U/pu/T/R (update rows), px_all/q_all/side_all (snapshot rows first)."""
    t = pq.read_table(path, columns=L2_COLS)
    col = lambda c: t[c].combine_chunks()
    et = col('event_type')
    snap = pc.equal(et, 'snapshot').to_numpy(zero_copy_only=False)
    upd = pc.equal(et, 'update').to_numpy(zero_copy_only=False)
    if not (snap | upd).all():
        raise ValueError(f'{path}: unknown event_type values')
    side = col('side')
    is_ask = pc.equal(side, 'ask').to_numpy(zero_copy_only=False)
    is_bid = pc.equal(side, 'bid').to_numpy(zero_copy_only=False)
    if not (is_ask | is_bid).all():
        raise ValueError(f'{path}: unknown side values')
    i64 = lambda c: col(c).fill_null(-1).to_numpy().astype(np.int64)
    u, U, pu = i64('final_update_id'), i64('first_update_id'), i64('prev_final_update_id')
    T, R, E = i64('transaction_time'), i64('received_time'), i64('event_time')
    px, q, sd = dec_to_int(col('price'), 2), dec_to_int(col('quantity'), 3), is_ask.astype(np.int8)
    n = t.num_rows
    # a snapshot block starts where a snapshot row follows an update row or the snapshot id changes
    prev_snap = np.r_[False, snap[:-1]]
    prev_u = np.r_[-2, u[:-1]]
    bstart = np.nonzero(snap & (~prev_snap | (u != prev_u)))[0]
    segs = []
    def view(seg, a, ns, b):
        return dict(path=str(path), seg=seg, n=b - a, n_snap=ns, L=int(u[a]) if ns else None,
                    snap_time=int(E[a]) if ns else None, first_row=a,
                    u=u[a + ns:b], U=U[a + ns:b], pu=pu[a + ns:b], T=T[a + ns:b], R=R[a + ns:b],
                    px_all=px[a:b], q_all=q[a:b], side_all=sd[a:b])
    first = bstart[0] if len(bstart) else n
    if first > 0:
        segs.append(view(0, 0, 0, first))                      # leading updates, no snapshot
    for k, a in enumerate(bstart):
        e = a
        while e < n and snap[e] and u[e] == u[a]:
            e += 1
        b = bstart[k + 1] if k + 1 < len(bstart) else n
        if snap[e:b].any():
            raise ValueError(f'{path}: malformed snapshot block at row {a}')
        segs.append(view(len(segs), a, e - a, b))
    return dict(path=str(path), n=n, segments=segs, n_snapshots=len(bstart))


def snapshot_blocks(path) -> dict:
    """Cheap structure scan (two columns): how many snapshots a file has and where they sit."""
    t = pq.read_table(path, columns=['event_type', 'final_update_id', 'event_time'])
    snap = pc.equal(t['event_type'].combine_chunks(), 'snapshot').to_numpy(zero_copy_only=False)
    u = t['final_update_id'].combine_chunks().fill_null(-1).to_numpy()
    E = t['event_time'].combine_chunks().to_numpy()
    prev_snap = np.r_[False, snap[:-1]]; prev_u = np.r_[-2, u[:-1]]
    bs = np.nonzero(snap & (~prev_snap | (u != prev_u)))[0]
    return dict(n_snapshots=len(bs), leading_update_rows=int(bs[0]) if len(bs) else len(snap),
                snapshot_rows=[int(x) for x in bs],
                snapshot_times=[str(pd.to_datetime(E[x], unit='ms').time()) for x in bs])


# ----------------------------------------------------------------------------------------------
# Numba engine
# ----------------------------------------------------------------------------------------------
@njit(cache=True)
def _load_snapshot(bidq, askq, px, qty, side, max_tick):
    bidq[:] = 0
    askq[:] = 0
    bb = 0
    ba = max_tick
    dropped = 0
    for i in range(px.shape[0]):
        p = px[i]
        if p <= 0 or p >= max_tick:
            dropped += 1
            continue
        q = qty[i]
        if side[i] == 1:
            askq[p] = q
            if q > 0 and p < ba:
                ba = p
        else:
            bidq[p] = q
            if q > 0 and p > bb:
                bb = p
    return bb, ba, dropped


@njit(cache=True)
def _fill_topk(bidq, askq, bb, ba, K, max_tick, scan_limit, bp, bq, ap, aq, r):
    n = 0
    p = bb
    lim = bb - scan_limit
    while n < K and p > 0 and p > lim:
        q = bidq[p]
        if q > 0:
            bp[r, n] = p
            bq[r, n] = q
            n += 1
        p -= 1
    n = 0
    p = ba
    lim = ba + scan_limit
    while n < K and p < max_tick and p < lim:
        q = askq[p]
        if q > 0:
            ap[r, n] = p
            aq[r, n] = q
            n += 1
        p += 1


@njit(cache=True)
def _replay(bidq, askq, bb, ba, last_u, valid, starts, ends, u, pu, T, R, px, qty, side,
            K, max_tick, scan_limit):
    nb = starts.shape[0]
    o_u = np.empty(nb, np.int64)
    o_T = np.empty(nb, np.int64)
    o_R = np.empty(nb, np.int64)
    o_valid = np.empty(nb, np.int8)
    o_n = np.empty(nb, np.int32)
    bp = np.zeros((nb, K), np.int32)
    bq = np.zeros((nb, K), np.int64)
    ap = np.zeros((nb, K), np.int32)
    aq = np.zeros((nb, K), np.int64)
    breaks = 0
    first_break = -1
    dropped = 0
    for b in range(nb):
        s = starts[b]
        e = ends[b]
        if last_u >= 0 and pu[s] != last_u:          # futures continuity rule
            breaks += 1
            valid = 0
            if first_break < 0:
                first_break = b
        for i in range(s, e):
            p = px[i]
            if p <= 0 or p >= max_tick:
                dropped += 1
                continue
            q = qty[i]
            if side[i] == 1:
                askq[p] = q
                if q > 0:
                    if p < ba:
                        ba = p
                elif p == ba:
                    while ba < max_tick and askq[ba] == 0:
                        ba += 1
            else:
                bidq[p] = q
                if q > 0:
                    if p > bb:
                        bb = p
                elif p == bb:
                    while bb > 0 and bidq[bb] == 0:
                        bb -= 1
        last_u = u[s]
        o_u[b] = u[s]
        o_T[b] = T[s]
        o_R[b] = R[s]
        o_valid[b] = valid
        o_n[b] = e - s
        _fill_topk(bidq, askq, bb, ba, K, max_tick, scan_limit, bp, bq, ap, aq, b)
    return o_u, o_T, o_R, o_valid, o_n, bp, bq, ap, aq, bb, ba, last_u, valid, breaks, first_break, dropped


def snapshot_topn(px, q, side, n):
    """Top-n (price, qty) per side of a snapshot, best first, zero-padded."""
    out = {}
    for name, s, asc in (('bid', 0, False), ('ask', 1, True)):
        m = (side == s) & (q > 0)
        p, qq = px[m], q[m]
        o = np.argsort(p)
        o = o[:n] if asc else o[::-1][:n]
        pp = np.zeros(n, np.int64); qv = np.zeros(n, np.int64)
        pp[:len(o)] = p[o]; qv[:len(o)] = qq[o]
        out[name] = (pp, qv)
    return out


# ----------------------------------------------------------------------------------------------
# Book builder (stateful across hours and days)
# ----------------------------------------------------------------------------------------------
class BookBuilder:
    def __init__(self, K: int = 10, audit_n: int = 20, max_tick: int = MAX_TICK):
        self.K, self.audit_n, self.max_tick = K, audit_n, max_tick
        self.bidq = np.zeros(max_tick, np.int64)
        self.askq = np.zeros(max_tick, np.int64)
        self.bb, self.ba = 0, max_tick
        self.last_u, self.valid, self.has_book = -1, 0, False

    # -- top-n of the current replayed book
    def topn(self, n):
        bp = np.zeros((1, n), np.int32); bq = np.zeros((1, n), np.int64)
        ap = np.zeros((1, n), np.int32); aq = np.zeros((1, n), np.int64)
        _fill_topk(self.bidq, self.askq, self.bb, self.ba, n, self.max_tick, SCAN_LIMIT, bp, bq, ap, aq, 0)
        return {'bid': (bp[0].astype(np.int64), bq[0]), 'ask': (ap[0].astype(np.int64), aq[0])}

    def _audit(self, snap_top):
        rep = self.topn(self.audit_n)
        res = {}
        for s in ('bid', 'ask'):
            (sp, sq), (rp, rq) = snap_top[s], rep[s]
            res[f'audit_{s}_px_equal'] = int((sp == rp).sum())
            res[f'audit_{s}_px_qty_equal'] = int(((sp == rp) & (sq == rq)).sum())
        res['audit_touch_equal'] = bool(snap_top['bid'][0][0] == rep['bid'][0][0] and
                                        snap_top['ask'][0][0] == rep['ask'][0][0] and
                                        snap_top['bid'][1][0] == rep['bid'][1][0] and
                                        snap_top['ask'][1][0] == rep['ask'][1][0])
        res['audit_exact'] = bool(res['audit_bid_px_qty_equal'] == self.audit_n and
                                  res['audit_ask_px_qty_equal'] == self.audit_n)
        return res

    def process_hour(self, path, date=None, hour=None):
        """Replay every segment of one hour file. `path` may be a parquet path or a load_l2_hour dict.
        Returns (per-batch frame with a 'seg' column, list of per-segment logs)."""
        t0 = time.perf_counter()
        d = path if isinstance(path, dict) else load_l2_hour(path)
        t_parse = time.perf_counter() - t0
        dfs, logs = [], []
        for v in d['segments']:
            df, log = self.process_segment(v, date, hour)
            log['parse_s'] = round(t_parse, 2) if v['seg'] == 0 else 0.0
            df.insert(0, 'seg', np.int8(v['seg']))
            dfs.append(df); logs.append(log)
        return pd.concat(dfs, ignore_index=True), logs

    def process_segment(self, d, date=None, hour=None):
        t_parse = 0.0
        path = d['path']
        ns, L = d['n_snap'], d['L']
        sp, sq, ss = d['px_all'][:ns], d['q_all'][:ns], d['side_all'][:ns]
        px, qty, side = d['px_all'][ns:], d['q_all'][ns:], d['side_all'][ns:]
        u, U, pu, T, R = d['u'], d['U'], d['pu'], d['T'], d['R']
        log = dict(date=date, hour=hour, file=Path(path).name, segment=d['seg'], rows=d['n'],
                   snapshot_levels=ns, L=L,
                   snapshot_time=pd.to_datetime(d['snap_time'], unit='ms') if ns else None)
        if len(u) == 0:
            raise ValueError(f'{path}: no update rows')

        # 1) audit: replayed book (state at previous file's last u) vs this file's snapshot (state at L)
        if ns == 0 and not self.has_book:                 # leading updates with nothing to apply them to
            log.update(mode='skipped_no_book', batches=0, chain_breaks=0, invalid_batches=0, crossed_batches=0,
                       one_sided_batches=0, u_strictly_increasing=True, T_non_decreasing=True,
                       out_of_range_levels=0, replay_s=0.0)
            return self._frame(np.zeros(0, np.int64), np.zeros(0, np.int64), np.zeros(0, np.int64),
                               np.zeros(0, np.int8), np.zeros(0, np.int32), *[np.zeros((0, self.K), t)
                               for t in (np.int32, np.int64, np.int32, np.int64)]), log
        snap_top = snapshot_topn(sp, sq, ss, self.audit_n) if ns else None
        if self.has_book and ns:
            log.update(prev_last_u=self.last_u, L_minus_prev_last_u=L - self.last_u,
                       first_pu_eq_prev_last_u=bool(pu[0] == self.last_u))
            log.update(self._audit(snap_top))

        # 2) choose how to start this hour
        start, reset = 0, True
        if ns == 0:
            reset = False
            if self.valid and pu[0] == self.last_u:
                mode, last_u, valid = 'continued', self.last_u, 1
            else:
                mode, last_u, valid = 'unsynced_no_snapshot', -1, 0
        elif pu[0] == L:
            mode, last_u, valid = 'snapshot_linked', L, 1
        elif self.has_book and self.valid and pu[0] == self.last_u:
            mode, last_u, valid, reset = 'continued', self.last_u, 1, False
        else:
            keep = np.nonzero(u >= L)[0]
            start = int(keep[0]) if len(keep) else len(u)
            if start < len(u) and U[start] <= L <= u[start]:
                mode, last_u, valid = 'snapshot_bracket', -1, 1
            else:
                mode, last_u, valid = 'unsynced', -1, 0
        log.update(mode=mode, dropped_stale_rows=start, first_U=int(U[start]), first_u=int(u[start]),
                   first_pu=int(pu[start]), first_U_minus_L=int(U[start] - L) if ns else None)
        if reset:
            self.bb, self.ba, dropped_snap = _load_snapshot(self.bidq, self.askq, sp, sq, ss, self.max_tick)
        else:
            dropped_snap = 0

        # 3) replay
        uu = u[start:]
        starts = start + np.r_[0, np.nonzero(np.diff(uu))[0] + 1]
        ends = np.r_[starts[1:], len(u)]
        t1 = time.perf_counter()
        (o_u, o_T, o_R, o_valid, o_n, bp, bq, ap, aq, self.bb, self.ba, self.last_u, self.valid,
         breaks, first_break, dropped) = _replay(self.bidq, self.askq, self.bb, self.ba, last_u, valid,
                                                 starts, ends, u, pu, T, R, px, qty, side,
                                                 self.K, self.max_tick, SCAN_LIMIT)
        t_replay = time.perf_counter() - t1
        self.has_book = True

        # 4) per-batch frame
        df = self._frame(o_u, o_T, o_R, o_valid, o_n, bp, bq, ap, aq)

        # 5) hour statistics
        lat = o_R / 1e6 - o_T
        both = (bp[:, 0] > 0) & (ap[:, 0] > 0)
        spread = (ap[:, 0] - bp[:, 0])[both]
        log.update(batches=len(df), chain_breaks=int(breaks),
                   first_break_u=int(o_u[first_break]) if first_break >= 0 else None,
                   invalid_batches=int((o_valid == 0).sum()),
                   crossed_batches=int((bp[:, 0] >= ap[:, 0])[both].sum()),
                   one_sided_batches=int((~both).sum()),
                   u_strictly_increasing=bool(np.all(np.diff(o_u) > 0)),
                   T_non_decreasing=bool(np.all(np.diff(o_T) >= 0)),
                   lat_med_ms=float(np.median(lat)), lat_p99_ms=float(np.percentile(lat, 99)),
                   lat_max_ms=float(lat.max()), lat_min_ms=float(lat.min()),
                   batch_gap_med_ms=float(np.median(np.diff(o_T))) if len(o_T) > 1 else None,
                   batch_gap_max_ms=int(np.diff(o_T).max()) if len(o_T) > 1 else None,
                   spread_med_ticks=float(np.median(spread)) if len(spread) else None,
                   spread_p99_ticks=float(np.percentile(spread, 99)) if len(spread) else None,
                   end_best_bid=self.bb / 100, end_best_ask=self.ba / 100,
                   out_of_range_levels=int(dropped + dropped_snap),
                   parse_s=round(t_parse, 2), replay_s=round(t_replay, 2))
        return df, log

    def _frame(self, o_u, o_T, o_R, o_valid, o_n, bp, bq, ap, aq):
        cols = {'u': o_u, 'T': o_T, 'R': o_R, 'valid': o_valid, 'n_rows': o_n}
        for k in range(self.K):
            cols[f'bp{k+1}'] = bp[:, k]; cols[f'bq{k+1}'] = bq[:, k]
        for k in range(self.K):
            cols[f'ap{k+1}'] = ap[:, k]; cols[f'aq{k+1}'] = aq[:, k]
        return pd.DataFrame(cols)

    def book_levels(self):
        """Number of non-empty levels per side in the current replayed book."""
        return int((self.bidq > 0).sum()), int((self.askq > 0).sum())


# ----------------------------------------------------------------------------------------------
# Self-test on a hand-made book (deterministic, run before touching real data)
# ----------------------------------------------------------------------------------------------
def self_test(verbose: bool = True) -> bool:
    K, MT = 3, 1000
    bidq = np.zeros(MT, np.int64); askq = np.zeros(MT, np.int64)
    # snapshot L=10: bids 100x5, 99x3 ; asks 101x4, 102x6
    sp = np.array([100, 99, 101, 102]); sq = np.array([5, 3, 4, 6]); ss = np.array([0, 0, 1, 1], np.int8)
    bb, ba, _ = _load_snapshot(bidq, askq, sp, sq, ss, MT)
    assert (bb, ba) == (100, 101)
    # batch 1 (u=12, pu=10): ask 101 -> 0 (removed), bid 100 -> 7
    # batch 2 (u=15, pu=12): bid 101 -> 2 (improves), ask 102 -> 0, ask 104 -> 1
    # batch 3 (u=20, pu=99): chain break -> valid flips to 0
    u = np.array([12, 12, 15, 15, 15, 20]); pu = np.array([10, 10, 12, 12, 12, 99])
    px = np.array([101, 100, 101, 102, 104, 99]); q = np.array([0, 7, 2, 0, 1, 0])
    sd = np.array([1, 0, 0, 1, 1, 0], np.int8); T = np.arange(6) * 10; R = T * 10**6
    starts = np.array([0, 2, 5]); ends = np.array([2, 5, 6])
    r = _replay(bidq, askq, bb, ba, 10, 1, starts, ends, u, pu, T, R, px, q, sd, K, MT, 500)
    o_valid, bp, bq, ap, aq, breaks = r[3], r[5], r[6], r[7], r[8], r[13]
    assert list(bp[0]) == [100, 99, 0] and list(bq[0]) == [7, 3, 0], (bp[0], bq[0])
    assert list(ap[0]) == [102, 0, 0] and list(aq[0]) == [6, 0, 0], (ap[0], aq[0])
    assert list(bp[1]) == [101, 100, 99] and list(bq[1]) == [2, 7, 3]
    assert list(ap[1]) == [104, 0, 0] and list(aq[1]) == [1, 0, 0]
    assert list(bp[2]) == [101, 100, 0]                         # 99 removed in batch 3
    assert list(o_valid) == [1, 1, 0] and breaks == 1
    if verbose:
        print('engine self-test passed: snapshot load, level removal, improvement, best-price rescans, '
              'pu-chain break detection')
    return True


# ----------------------------------------------------------------------------------------------
# C++ book builder bridge (cpp/book_builder): ingest -> run -> read -> cross-check with numba
# ----------------------------------------------------------------------------------------------
L2ROW_DTYPE = np.dtype([('u', '<i8'), ('U', '<i8'), ('pu', '<i8'), ('T', '<i8'), ('R', '<i8'),
                        ('px', '<i8'), ('qty', '<i8'), ('side', 'i1'), ('snap', 'i1')])   # 58 bytes, packed


def ingest_hour(parquet_path, out_path) -> dict:
    """Parquet hour (or a preloaded load_l2_hour dict) -> packed .l2bin rows for the C++ builder."""
    d = parquet_path if isinstance(parquet_path, dict) else load_l2_hour(parquet_path)['segments'][0]
    ns, n = d['n_snap'], d['n']
    a = np.zeros(n, L2ROW_DTYPE)
    if ns:
        a['u'][:ns] = d['L']; a['U'][:ns] = -1; a['pu'][:ns] = -1
        a['T'][:ns] = d['snap_time']; a['R'][:ns] = 0; a['snap'][:ns] = 1
    a['u'][ns:], a['U'][ns:], a['pu'][ns:], a['T'][ns:], a['R'][ns:] = d['u'], d['U'], d['pu'], d['T'], d['R']
    a['px'], a['qty'], a['side'] = d['px_all'], d['q_all'], d['side_all']
    a.tofile(out_path)
    return d


def book_dtype(K: int) -> np.dtype:
    return np.dtype([('u', '<i8'), ('T', '<i8'), ('R', '<i8'), ('valid', 'i1'), ('n_rows', '<i4'),
                     ('bp', '<i4', (K,)), ('bq', '<i8', (K,)), ('ap', '<i4', (K,)), ('aq', '<i8', (K,))])


def read_cpp_book(path) -> pd.DataFrame:
    """Read a .bookbin written by book_builder into the same column layout as BookBuilder.process_hour."""
    with open(path, 'rb') as f:
        hdr = f.read(16)
    if hdr[:8] != b'MABOOK01':
        raise ValueError(f'{path}: not a book file')
    K = int(np.frombuffer(hdr[8:12], '<i4')[0])
    a = np.fromfile(path, dtype=book_dtype(K), offset=16)
    cols = {'u': a['u'], 'T': a['T'], 'R': a['R'], 'valid': a['valid'], 'n_rows': a['n_rows']}
    for k in range(K):
        cols[f'bp{k+1}'] = a['bp'][:, k]; cols[f'bq{k+1}'] = a['bq'][:, k]
    for k in range(K):
        cols[f'ap{k+1}'] = a['ap'][:, k]; cols[f'aq{k+1}'] = a['aq'][:, k]
    return pd.DataFrame(cols)


def run_cpp_hour(exe, parquet_path, work_dir, state_path, log_path, K=10, audit_n=20, reset=False):
    """Run the C++ builder on every segment of one hour (path or load_l2_hour dict).
    Returns (per-batch frame with 'seg' column, info). Temp files are removed; book state carried in state_path."""
    import subprocess
    work_dir = Path(work_dir); work_dir.mkdir(parents=True, exist_ok=True)
    d = parquet_path if isinstance(parquet_path, dict) else load_l2_hour(parquet_path)
    stem = Path(d['path']).stem
    dfs, outs, t_ingest, t_cpp = [], [], 0.0, 0.0
    for v in d['segments']:
        l2bin, bookbin = work_dir / f"{stem}__s{v['seg']}.l2bin", work_dir / f"{stem}__s{v['seg']}.bookbin"
        t0 = time.perf_counter(); ingest_hour(v, l2bin); t_ingest += time.perf_counter() - t0
        cmd = [str(exe), '--in', str(l2bin), '--book-out', str(bookbin), '--log', str(log_path),
               '--state', str(state_path), '--k', str(K), '--audit', str(audit_n)]
        if reset and v['seg'] == 0:
            cmd.append('--reset')
        t1 = time.perf_counter(); res = subprocess.run(cmd, capture_output=True, text=True); t_cpp += time.perf_counter() - t1
        if res.returncode != 0:
            raise RuntimeError(f"book_builder failed on {l2bin.name}:\n{res.stdout}\n{res.stderr}")
        df = read_cpp_book(bookbin); df.insert(0, 'seg', np.int8(v['seg'])); dfs.append(df)
        outs.append(res.stdout.strip())
        l2bin.unlink(); bookbin.unlink(missing_ok=True)
    return pd.concat(dfs, ignore_index=True), dict(ingest_s=round(t_ingest, 2), cpp_s=round(t_cpp, 2),
                                                    segments=len(d['segments']), stdout='\n'.join(outs))


def compare_books(df_cpp: pd.DataFrame, df_ref: pd.DataFrame) -> dict:
    """Batch-by-batch equality of two engines' outputs (all columns, exact)."""
    if len(df_cpp) != len(df_ref):
        return dict(equal=False, reason=f'batch count {len(df_cpp)} vs {len(df_ref)}')
    cols = [c for c in df_ref.columns if c in df_cpp.columns]
    a = df_cpp[cols].to_numpy(np.int64); b = df_ref[cols].to_numpy(np.int64)
    bad_rows = np.nonzero((a != b).any(axis=1))[0]
    return dict(equal=len(bad_rows) == 0, mismatched_batches=int(len(bad_rows)),
                first_mismatch=int(bad_rows[0]) if len(bad_rows) else None,
                mismatched_columns=sorted({cols[j] for j in np.nonzero((a != b).any(axis=0))[0]}))


# ==============================================================================================
# Part 2 (Notebook 02): trades — exact loading, integrity, trade↔book join, quality windows
# ==============================================================================================
import zipfile
import pyarrow.csv as pacsv

TRADE_COLS = ['id', 'price', 'qty', 'quote_qty', 'time', 'is_buyer_maker']
CLS = {0: 'at_touch', 1: 'deeper_top10', 2: 'beyond_top10', 3: 'one_batch_slack',
       4: 'inside_spread', 5: 'wrong_side', 6: 'other'}
EXPLAINED = (0, 1, 2, 3)


def pick_trade_files(inv: pd.DataFrame, dates) -> dict:
    """date -> one trades file (prefer the CSV over the zip when both exist; else the largest)."""
    out = {}
    for d in dates:
        t = inv[(inv.kind == 'trades') & (inv.date == d)].copy()
        if len(t):
            t['is_csv'] = t.path.str.lower().str.endswith('.csv')
            out[d] = t.sort_values(['is_csv', 'mb'], ascending=False).path.iloc[0]
    return out


def _open_trades(path):
    p = Path(path)
    if p.suffix.lower() == '.zip':
        z = zipfile.ZipFile(p)
        names = [n for n in z.namelist() if n.lower().endswith('.csv')]
        if not names:
            raise ValueError(f'{path}: zip contains no csv (empty or broken download?)')
        return z.open(names[0])
    return open(p, 'rb')


def _count_offgrid(arr, dec):
    return int(pc.sum(pc.match_substring_regex(arr, r'\.\d{%d,}$' % (dec + 1))).as_py() or 0)


def load_trades_day(path):
    """Binance USD-M trades CSV (or zip) -> exact integer frame + integrity dict.
    Columns out: id, time (ms), px (ticks), qty (lots), sign (+1 taker buy, -1 taker sell)."""
    with _open_trades(path) as f:
        head = f.read(64)
    has_header = head.lstrip().lower().startswith(b'id')
    ro = pacsv.ReadOptions(block_size=1 << 26, column_names=None if has_header else TRADE_COLS)
    co = pacsv.ConvertOptions(column_types={'id': pa.int64(), 'price': pa.string(), 'qty': pa.string(),
                                            'quote_qty': pa.string(), 'time': pa.int64(),
                                            'is_buyer_maker': pa.string()})
    with _open_trades(path) as f:
        t = pacsv.read_csv(f, read_options=ro, convert_options=co)
    price, qty, qq = (t[c].combine_chunks() for c in ('price', 'qty', 'quote_qty'))
    info = dict(file=Path(path).name, rows=t.num_rows,
                offgrid_px=_count_offgrid(price, 2), offgrid_qty=_count_offgrid(qty, 3))
    if info['offgrid_px'] or info['offgrid_qty']:
        raise ValueError(f"{path}: off-grid values (px {info['offgrid_px']}, qty {info['offgrid_qty']})")
    px, q = dec_to_int(price, 2), dec_to_int(qty, 3)
    info['quote_sci_rows'] = int(pc.sum(pc.match_substring_regex(qq, '[eE]')).as_py() or 0)
    qv = dec_to_int(qq, 5)                                # exact, scientific notation handled by Arrow
    info['quote_mismatch'] = int((px * q != qv).sum())
    ibm = pc.utf8_lower(t['is_buyer_maker'].combine_chunks())
    is_true = pc.equal(ibm, 'true').to_numpy(zero_copy_only=False)
    is_false = pc.equal(ibm, 'false').to_numpy(zero_copy_only=False)
    info['bad_side_values'] = int((~(is_true | is_false)).sum())
    ids = t['id'].to_numpy(); tm = t['time'].to_numpy()
    d = np.diff(ids); dt = np.diff(tm)
    info.update(first_id=int(ids[0]), last_id=int(ids[-1]), missing_ids=int((d - 1).clip(min=0).sum()),
                id_gaps=int((d > 1).sum()), max_id_gap=int(d.max() - 1) if len(d) else 0,
                dup_or_backward_ids=int((d <= 0).sum()), time_decreases=int((dt < 0).sum()),
                first_time=pd.to_datetime(int(tm[0]), unit='ms'), last_time=pd.to_datetime(int(tm[-1]), unit='ms'),
                zero_qty=int((q == 0).sum()), buy_share=float(is_false.mean()),
                max_trade_eth=float(q.max() / 1000), max_trade_time=pd.to_datetime(int(tm[q.argmax()]), unit='ms'))
    info['missing_id_share'] = info['missing_ids'] / max(1, info['last_id'] - info['first_id'] + 1)
    df = pd.DataFrame({'id': ids, 'time': tm, 'px': px, 'qty': q, 'sign': np.where(is_false, 1, -1).astype(np.int8)})
    return df, info


def book_cols(K):
    return ([f'bp{k}' for k in range(1, K + 1)], [f'bq{k}' for k in range(1, K + 1)],
            [f'ap{k}' for k in range(1, K + 1)], [f'aq{k}' for k in range(1, K + 1)])


def join_hour(book: pd.DataFrame, prev_row, tr: pd.DataFrame, K: int = 10):
    """Acceptance tests 2 & 3 for one hour.
    book    : per-batch top-K book of this hour (Notebook 01 output, incl. seg/valid)
    prev_row: last row of the previous hour's book (pre-state of this hour's first batch) or None
    tr      : trades (exact ints) — the caller passes candidates; only time in (T_first_pre, T_last] are used
    A trade at time t belongs to the first batch b with T_b >= t; its pre-state is row b-1, post-state row b."""
    bk = book if prev_row is None else pd.concat([prev_row, book], ignore_index=True)
    first_cur = 0 if prev_row is None else 1
    BPc, BQc, APc, AQc = book_cols(K)
    T = bk['T'].to_numpy(); valid = bk['valid'].to_numpy().astype(bool); seg = bk['seg'].to_numpy()
    BP, BQ, AP, AQ = (bk[c].to_numpy(np.int64) for c in (BPc, BQc, APc, AQc))
    U = bk['u'].to_numpy()
    n = len(bk)
    # segment reset inside the hour: post row starts a new snapshot segment -> pre-state unknown
    reset_row = np.zeros(n, bool)
    reset_row[1:] = (seg[1:] > 0) & (seg[1:] != seg[:-1])
    sel = (tr.time.to_numpy() > T[0]) & (tr.time.to_numpy() <= T[-1])
    t = tr[sel]
    tt, px, q, s, tid = t.time.to_numpy(), t.px.to_numpy(), t.qty.to_numpy(), t.sign.to_numpy(), t.id.to_numpy()
    b = np.searchsorted(T, tt, side='left')
    b = np.maximum(b, 1)
    pre, post = b - 1, b
    pre2 = np.maximum(b - 2, 0)
    usable = valid[pre] & valid[post] & ~reset_row[post] & (post >= first_cur)
    buy = s > 0
    own_P_pre = np.where(buy[:, None], AP[pre], BP[pre])
    own_Q_pre = np.where(buy[:, None], AQ[pre], BQ[pre])
    own_P_post = np.where(buy[:, None], AP[post], BP[post])
    opp_touch_pre = np.where(buy, BP[pre, 0], AP[pre, 0])
    touch = own_P_pre[:, 0]
    at_touch = px == touch
    beyond = np.where(buy, px > touch, px < touch)
    in_pre = (own_P_pre == px[:, None]).any(axis=1)
    lastK = own_P_pre[:, K - 1]
    past_k = beyond & ~in_pre & (lastK > 0) & np.where(buy, px > lastK, px < lastK)
    slack = (px == np.where(buy, AP[pre2, 0], BP[pre2, 0])) | (own_P_post == px[:, None]).any(axis=1)
    wrong = np.where(buy, px <= opp_touch_pre, px >= opp_touch_pre)
    inside = np.where(buy, (px < touch) & (px > opp_touch_pre), (px > touch) & (px < opp_touch_pre))
    cls = np.full(len(t), 6, np.int8)
    for code, m in ((5, wrong), (4, inside), (3, slack), (2, past_k), (1, beyond & in_pre), (0, at_touch)):
        cls[m] = code                                  # later assignments win: at_touch has priority
    cls = np.where(usable, cls, -2).astype(np.int8)

    # --- sweep sanity: deeper fills come after a touch fill of the same batch & side, and walk the book
    key = U[post] * 2 + (s > 0)
    df = pd.DataFrame({'key': key, 'id': tid, 'aggr': px * s, 'cls': cls})
    dfu = df[usable]
    first_touch = dfu[dfu.cls == 0].groupby('key').id.min()
    deep = dfu[dfu.cls.isin([1, 2])]
    preceded = (deep.key.map(first_touch) < deep.id)
    dd = dfu.sort_values(['key', 'id'])
    step = dd.groupby('key')['aggr'].diff()
    mono_groups = (step.fillna(0) >= 0).groupby(dd.key).all()

    # --- test 3: volume at the pre-batch touch vs the resting queue
    at = usable & at_touch
    g = pd.DataFrame({'key': key[at], 'v': q[at], 'q_pre': own_Q_pre[at, 0], 'px': px[at],
                      'q_post': np.where(own_P_post[at] == px[at][:, None],
                                         np.where(buy[at][:, None], AQ[post[at]], BQ[post[at]]), 0).sum(axis=1)})
    tq = g.groupby('key').agg(V=('v', 'sum'), q_pre=('q_pre', 'first'), q_post=('q_post', 'first'))

    # --- missing trade IDs (exploratory): in batches with NO public trade, touch depletion can only be
    #     cancels -- or hidden executions. Compare quiet batches inside an ID-gap time window with other
    #     quiet batches. If the missing IDs are executions against visible liquidity, the first should
    #     show more depletion.
    rows = np.arange(1, n)
    ok_rows = valid[rows - 1] & valid[rows] & ~reset_row[rows] & (rows >= first_cur)
    has_trade = np.zeros(n, bool); has_trade[post] = True
    dep = np.zeros(n)
    for P, Q in ((AP, AQ), (BP, BQ)):
        p0, q0 = P[rows - 1, 0], Q[rows - 1, 0]
        qpost = np.where(P[rows] == p0[:, None], Q[rows], 0).sum(axis=1)
        dep[rows] += np.maximum(q0 - qpost, 0)
    o = np.argsort(tid, kind='stable')
    gi = np.nonzero((np.diff(tid[o]) > 1) & usable[o][1:] & usable[o][:-1])[0]
    lo, hi = post[o][gi], post[o][gi + 1]                  # missing trades executed within batches (lo, hi]
    mark = np.zeros(n + 1, np.int64)
    short = (hi - lo) <= 40                                # windows up to ~1 s
    np.add.at(mark, lo[short] + 1, 1); np.add.at(mark, hi[short] + 1, -1)
    in_gap = np.cumsum(mark)[:n] > 0
    quiet = np.zeros(n, bool); quiet[rows] = ok_rows & ~has_trade[rows]
    qg, qo = quiet & in_gap, quiet & ~in_gap
    stats = dict(trades=int(len(t)), usable=int(usable.sum()), vol=int(q.sum()), vol_usable=int(q[usable].sum()))
    for c, name in CLS.items():
        m = cls == c
        stats[f'n_{name}'] = int(m.sum()); stats[f'v_{name}'] = int(q[m].sum())
    beyond = deep[deep.cls == 2]
    stats.update(deeper_preceded=int(preceded.sum()), deeper_total=int(len(deep)),
                 beyond_preceded=int(preceded[deep.cls == 2].sum()), beyond_total=int(len(beyond)),
                 mono_groups=int(mono_groups.sum()), groups=int(len(mono_groups)),
                 touch_groups=int(len(tq)), touch_v_le_qpre=int((tq.V <= tq.q_pre).sum()),
                 touch_refill_needed=int((tq.V > (tq.q_pre - tq.q_post).clip(lower=0)).sum()),
                 id_gaps=int(len(gi)), id_gap_windows_used=int(short.sum()),
                 quiet_gap=int(qg.sum()), quiet_other=int(qo.sum()),
                 dep_gap_sum=float(dep[qg].sum()), dep_other_sum=float(dep[qo].sum()),
                 dep_gap_pos=int((dep[qg] > 0).sum()), dep_other_pos=int((dep[qo] > 0).sum()))
    ann = pd.DataFrame({'idx': np.nonzero(sel)[0], 'u_post': U[post], 'cls': cls})
    unexplained = t[(cls >= 4)].assign(cls=cls[cls >= 4], pre_bid=BP[pre[cls >= 4], 0], pre_ask=AP[pre[cls >= 4], 0])
    return ann, stats, unexplained


def quality_windows(book: pd.DataFrame, lat_bad_ms: float = 1000.0):
    """Contiguous exchange-time windows where the book is not trustworthy for research or trading:
    valid == 0 (after a pu break, until resync) or feed latency above lat_bad_ms (received-time clock)."""
    T = book['T'].to_numpy(); lat = book['R'].to_numpy() / 1e6 - T
    out = []
    for reason, m in (('invalid_book', book['valid'].to_numpy() == 0), ('latency', lat > lat_bad_ms)):
        if not m.any():
            continue
        edges = np.diff(np.r_[0, m.astype(np.int8), 0])
        for a, e in zip(np.nonzero(edges == 1)[0], np.nonzero(edges == -1)[0]):
            out.append(dict(reason=reason, start_T=int(T[a]), end_T=int(T[e - 1]), batches=int(e - a),
                            max_latency_ms=float(lat[a:e].max())))
    return out


def per_second(book: pd.DataFrame):
    """1-second sample (last batch in each second) for sanity plots."""
    sec = book['T'].to_numpy() // 1000
    last = np.r_[np.nonzero(np.diff(sec))[0], len(sec) - 1]
    b = book.iloc[last]
    return pd.DataFrame({'sec': sec[last], 'mid': (b.bp1.to_numpy() + b.ap1.to_numpy()) / 200.0,
                         'spread': (b.ap1 - b.bp1).to_numpy(), 'depth': (b.bq1 + b.aq1).to_numpy() / 1000.0,
                         'lat': b.R.to_numpy() / 1e6 - b['T'].to_numpy(), 'valid': b.valid.to_numpy(),
                         'batches': np.diff(np.r_[-1, last])})


# ==============================================================================================
# Part 3 (Notebook 03): event reconstruction — taker orders, touch decomposition, multi-level OFI
# ==============================================================================================
BIG_ASK = np.int64(2 ** 62)          # missing ask level = +infinity (missing bid level = price 0)
MOVE = {0: 'unchanged', 1: 'improve', 2: 'sweep_out', 3: 'cancel_out'}


def ofi_levels(BPp, BQp, APp, AQp, BPn, BQn, APn, AQn):
    """Cont–Kukanov–Stoikov OFI per level (arrays n x K, ints). p = pre-state, n = post-state.
    e = 1{Pb_n>=Pb_p} qb_n - 1{Pb_n<=Pb_p} qb_p - 1{Pa_n<=Pa_p} qa_n + 1{Pa_n>=Pa_p} qa_p"""
    APp = np.where(APp > 0, APp, BIG_ASK); APn = np.where(APn > 0, APn, BIG_ASK)
    return ((BPn >= BPp) * BQn - (BPn <= BPp) * BQp - (APn <= APp) * AQn + (APn >= APp) * AQp).astype(np.int64)


def cks_scalar(pb0, qb0, pa0, qa0, pb1, qb1, pa1, qa1):
    """Independent scalar reference for level-1 OFI, written as plain if-statements (used to hand-check)."""
    e = 0
    if pb1 >= pb0: e += qb1
    if pb1 <= pb0: e -= qb0
    if pa1 <= pa0: e -= qa1
    if pa1 >= pa0: e += qa0
    return e


def build_taker_orders(tr: pd.DataFrame) -> pd.DataFrame:
    """Aggressive (taker) orders = runs of consecutive trades (id order) with the same timestamp and side.
    Each fill is one resting maker order at the front of the queue, so fill sizes give queue composition."""
    t = tr.sort_values('id', kind='stable')
    tm, s, px, q = t.time.to_numpy(), t.sign.to_numpy(), t.px.to_numpy(), t.qty.to_numpy()
    new = np.r_[True, (tm[1:] != tm[:-1]) | (s[1:] != s[:-1])]
    st = np.nonzero(new)[0]
    order_of = np.cumsum(new) - 1
    px_first = px[st][order_of]
    agg = px * s
    step = np.r_[0, np.diff(agg)]; step[st] = 0
    lvl_change = np.r_[0, (px[1:] != px[:-1]).astype(np.int64)]; lvl_change[st] = 0
    out = pd.DataFrame({
        'oid': t.id.to_numpy()[st], 'time': tm[st], 'sign': s[st],
        'n_fills': np.diff(np.r_[st, len(t)]),
        'qty': np.add.reduceat(q, st),
        'n_levels': np.add.reduceat(lvl_change, st) + 1,
        'px_first': px[st], 'px_last': px[np.r_[st[1:], len(t)] - 1],
        'vwap': np.add.reduceat(px.astype(np.float64) * q, st) / np.add.reduceat(q, st),
        'qty_first_level': np.add.reduceat(np.where(px == px_first, q, 0), st),
        'nonmonotone': np.add.reduceat((step < 0).astype(np.int64), st) > 0,
        'u_post': t.u_post.to_numpy()[st] if 'u_post' in t else -1,
        'usable': np.minimum.reduceat(t.cls.to_numpy(), st) >= 0 if 'cls' in t else True})
    return out


def batch_events(book: pd.DataFrame, prev_tail, trh: pd.DataFrame, K: int = 10):
    """Per-batch events for one hour. book: this hour; prev_tail: last rows of the previous hour (or None);
    trh: trades of this hour (with u_post). Returns (events frame, row index of each trade or -1)."""
    bk = book if prev_tail is None else pd.concat([prev_tail, book], ignore_index=True)
    f = 0 if prev_tail is None else len(prev_tail)
    BPc, BQc, APc, AQc = book_cols(K)
    BP, BQ, AP, AQ = (bk[c].to_numpy(np.int64) for c in (BPc, BQc, APc, AQc))
    U, valid, seg = bk.u.to_numpy(), bk.valid.to_numpy().astype(bool), bk.seg.to_numpy()
    n = len(bk)
    reset = np.zeros(n, bool); reset[1:] = (seg[1:] > 0) & (seg[1:] != seg[:-1])
    rows = np.arange(max(f, 1), n); pre = rows - 1
    ok = valid[pre] & valid[rows] & ~reset[rows]
    # trades -> rows
    up = trh.u_post.to_numpy()
    r = np.searchsorted(U, up); r = np.clip(r, 0, n - 1)
    hit = (U[r] == up) & (r >= max(f, 1))
    r = np.where(hit, r, -1)
    tq, ts, tp = trh.qty.to_numpy(), trh.sign.to_numpy(), trh.px.to_numpy()
    m = r >= 0
    def acc(w):
        return np.bincount(r[m], weights=w[m], minlength=n)[rows]
    buy = ts > 0
    v_buy, v_sell = acc(tq * buy), acc(tq * ~buy)
    nb_fill, ns_fill = acc(buy.astype(float)), acc((~buy).astype(float))
    rr = np.where(m, r, 0)
    at_ask = m & buy & (tp == AP[np.maximum(rr - 1, 0), 0])
    at_bid = m & ~buy & (tp == BP[np.maximum(rr - 1, 0), 0])
    Va = acc(tq * at_ask); Vb = acc(tq * at_bid)
    # touch decomposition
    bp0, bq0, bp1, bq1 = BP[pre, 0], BQ[pre, 0], BP[rows, 0], BQ[rows, 0]
    ap0, aq0, ap1, aq1 = AP[pre, 0], AQ[pre, 0], AP[rows, 0], AQ[rows, 0]
    ub, ua = bp1 == bp0, ap1 == ap0
    dqb, dqa = np.where(ub, bq1 - bq0, 0), np.where(ua, aq1 - aq0, 0)
    Nb, Na = np.where(ub, dqb + Vb, 0), np.where(ua, dqa + Va, 0)
    mvb = np.select([ub, bp1 > bp0, Vb >= bq0], [0, 1, 2], 3).astype(np.int8)
    mva = np.select([ua, ap1 < ap0, Va >= aq0], [0, 1, 2], 3).astype(np.int8)
    move_b = np.select([mvb == 1, mvb >= 2], [bq1, -bq0], 0)
    move_a = np.select([mva == 1, mva >= 2], [-aq1, aq0], 0)
    limit_flow = Nb - Na
    market_flow = np.where(ua, Va, 0) - np.where(ub, Vb, 0)
    move_flow = move_b + move_a
    OFI = ofi_levels(BP[pre], BQ[pre], AP[pre], AQ[pre], BP[rows], BQ[rows], AP[rows], AQ[rows])
    ev = pd.DataFrame({'u': U[rows], 'T': bk['T'].to_numpy()[rows], 'R': bk['R'].to_numpy()[rows], 'ok': ok,
                       'seg': seg[rows], 'bp1': bp1, 'ap1': ap1, 'bq1': bq1, 'aq1': aq1})
    for k in range(K):
        ev[f'ofi{k+1}'] = OFI[:, k]
    ev = ev.assign(limit_flow=limit_flow, market_flow=market_flow, move_flow=move_flow, mv_b=mvb, mv_a=mva,
                   N_b=Nb, N_a=Na, V_b_touch=Vb.astype(np.int64), V_a_touch=Va.astype(np.int64),
                   v_buy=v_buy.astype(np.int64), v_sell=v_sell.astype(np.int64),
                   n_buy_fills=nb_fill.astype(np.int32), n_sell_fills=ns_fill.astype(np.int32))
    # rows of the hour's own book only (drop prepended tail); the first row of the very first hour has no pre-state
    ev = ev[ev.u.isin(book.u)].reset_index(drop=True)
    r_out = np.where(r >= 0, r - f, -1)                # row index within this hour's book
    return ev, r_out


def tolerance_scan(book: pd.DataFrame, prev_tail, trh: pd.DataFrame, K: int = 10, kmax: int = 5):
    """For trades the batch-level join could not explain: smallest |k| such that the trade price is on the
    trade's own side (within top K) of the book state k batches away from its pre-state (k<0 earlier).
    Returns array of k (or 99 if not found within ±kmax)."""
    bk = book if prev_tail is None else pd.concat([prev_tail, book], ignore_index=True)
    BPc, BQc, APc, AQc = book_cols(K)
    BP, AP = bk[BPc].to_numpy(np.int64), bk[APc].to_numpy(np.int64)
    U = bk.u.to_numpy(); n = len(bk)
    up = trh.u_post.to_numpy(); r = np.clip(np.searchsorted(U, up), 0, n - 1)
    pre = r - 1
    buy = trh.sign.to_numpy() > 0; px = trh.px.to_numpy()
    found = np.full(len(trh), 99)
    for k in sorted(range(-kmax, kmax + 1), key=abs):
        j = pre + k
        inr = (j >= 0) & (j < n) & (found == 99)
        if not inr.any():
            continue
        jj = np.where(inr, j, 0)
        own = np.where(buy[:, None], AP[jj], BP[jj])
        hitk = inr & (own == px[:, None]).any(axis=1)
        found[hitk] = k
    return found


def unexplained_diagnostics(book: pd.DataFrame, prev_tail, trh: pd.DataFrame):
    """For each usable trade: is its price inside the batch's pre/post price envelope, and how far did the
    mid move in its batch (ticks)? Used to test whether unexplained prints are intra-batch transients."""
    bk = book if prev_tail is None else pd.concat([prev_tail, book], ignore_index=True)
    U = bk.u.to_numpy(); n = len(bk)
    bp, ap = bk.bp1.to_numpy(np.int64), bk.ap1.to_numpy(np.int64)
    r = np.clip(np.searchsorted(U, trh.u_post.to_numpy()), 1, n - 1)
    lo = np.minimum(bp[r - 1], bp[r]); hi = np.maximum(ap[r - 1], ap[r])
    px = trh.px.to_numpy()
    inside_env = (px >= lo) & (px <= hi)
    dmid = np.abs((bp[r] + ap[r]) - (bp[r - 1] + ap[r - 1])) / 2.0
    return inside_env, dmid


HANDCHECK_TYPES = ['plain_fill', 'sweep_empties_level', 'cancel_at_touch', 'improvement_into_spread',
                   'mixed_fill_and_add', 'deep_level_only']


def pick_handcheck(ev: pd.DataFrame, exclude_u=None) -> dict:
    """First batch of each canonical type in an hour (deterministic; the hour's first batch is excluded so the
    choice does not depend on whether the previous hour was loaded: pass the hour's first two u). Returns {type: u}."""
    pre_spread = (ev.ap1 - ev.bp1).shift(1)                 # pre-state spread (previous row's post-state)
    excl = set(np.atleast_1d(exclude_u).tolist()) if exclude_u is not None else set()
    keep = ev.ok & ~ev.u.isin(excl) & pre_spread.notna()
    e = ev[keep].assign(pre_spread=pre_spread[keep])
    still = (e.mv_a == 0) & (e.mv_b == 0)
    deep_nz = (e.ofi2 != 0) | (e.ofi3 != 0)
    masks = {
        'plain_fill': still & (e.V_a_touch > 0) & (e.N_a == 0) & (e.N_b == 0) & (e.v_sell == 0) & (e.v_buy == e.V_a_touch),
        'sweep_empties_level': (e.mv_a == 2) & (e.v_sell == 0),
        'cancel_at_touch': still & (e.v_buy == 0) & (e.v_sell == 0) & (e.N_b < 0) & (e.N_a == 0),
        'improvement_into_spread': (((e.mv_b == 1) & (e.mv_a == 0)) | ((e.mv_a == 1) & (e.mv_b == 0))) &
                                   (e.v_buy == 0) & (e.v_sell == 0) & (e.pre_spread >= 2),
        'mixed_fill_and_add': still & (e.V_a_touch > 0) & (e.N_a > 0) & (e.N_b == 0) & (e.v_sell == 0),
        'deep_level_only': still & (e.ofi1 == 0) & (e.N_a == 0) & (e.N_b == 0) & (e.v_buy == 0) & (e.v_sell == 0) & deep_nz}
    return {k: (int(e.u[m].iloc[0]) if m.any() else None) for k, m in masks.items()}


def show_batch(book: pd.DataFrame, prev_tail, trh: pd.DataFrame, ev: pd.DataFrame, u: int, levels: int = 3):
    """Print pre/post book, the batch's trades and the level-1 OFI arithmetic; return the hand values."""
    bk = book if prev_tail is None else pd.concat([prev_tail, book], ignore_index=True)
    i = int(np.nonzero(bk.u.to_numpy() == u)[0][0]); p, q = bk.iloc[i - 1], bk.iloc[i]
    e = ev[ev.u == u].iloc[0]
    fmt = lambda px, qt: f"{px/100:.2f} x {qt/1000:.3f}"
    print(f"  {'level':<6}{'pre bid':>18}{'pre ask':>18}   |{'post bid':>18}{'post ask':>18}")
    for k in range(1, levels + 1):
        print(f"  {k:<6}{fmt(p[f'bp{k}'], p[f'bq{k}']):>18}{fmt(p[f'ap{k}'], p[f'aq{k}']):>18}   |"
              f"{fmt(q[f'bp{k}'], q[f'bq{k}']):>18}{fmt(q[f'ap{k}'], q[f'aq{k}']):>18}")
    t = trh[trh.u_post == u]
    print('  trades in batch:', 'none' if t.empty else
          ', '.join(f"{'BUY' if s > 0 else 'SELL'} {qq/1000:.3f}@{pp/100:.2f}" for pp, qq, s in zip(t.px, t.qty, t.sign)))
    pb0, qb0, pa0, qa0 = int(p.bp1), int(p.bq1), int(p.ap1), int(p.aq1)
    pb1, qb1, pa1, qa1 = int(q.bp1), int(q.bq1), int(q.ap1), int(q.aq1)
    terms = [(pb1 >= pb0, +qb1, 'Pb_n>=Pb_p: +qb_n'), (pb1 <= pb0, -qb0, 'Pb_n<=Pb_p: -qb_p'),
             (pa1 <= pa0, -qa1, 'Pa_n<=Pa_p: -qa_n'), (pa1 >= pa0, +qa0, 'Pa_n>=Pa_p: +qa_p')]
    print('  level-1 OFI = ' + ' '.join(f"{'+' if v >= 0 else '-'} {abs(v)/1000:.3f} [{lab}]" for on, v, lab in terms if on)
          + f"  = {cks_scalar(pb0, qb0, pa0, qa0, pb1, qb1, pa1, qa1)/1000:+.3f} ETH")
    print(f"  decomposition: limit {e.limit_flow/1000:+.3f} + market {e.market_flow/1000:+.3f} + move {e.move_flow/1000:+.3f}"
          f" = {(e.limit_flow + e.market_flow + e.move_flow)/1000:+.3f} | engine ofi1 {e.ofi1/1000:+.3f} | "
          f"N_bid {e.N_b/1000:+.3f}, N_ask {e.N_a/1000:+.3f}, executions bid {e.V_b_touch/1000:.3f} ask {e.V_a_touch/1000:.3f}"
          f" | moves bid={MOVE[int(e.mv_b)]} ask={MOVE[int(e.mv_a)]} | ofi2 {e.ofi2/1000:+.3f} ofi3 {e.ofi3/1000:+.3f}")
    return dict(u=u, ofi1=int(e.ofi1), scalar=int(cks_scalar(pb0, qb0, pa0, qa0, pb1, qb1, pa1, qa1)),
                limit=int(e.limit_flow), market=int(e.market_flow), move=int(e.move_flow), ofi2=int(e.ofi2))


def attach_touch(orders: pd.DataFrame, book: pd.DataFrame, prev_tail):
    """For taker orders of one hour: pre-batch touch price/size on the order's side, and whether the order
    emptied the touch (swept several levels, or its first-level quantity >= the resting touch queue)."""
    bk = book if prev_tail is None else pd.concat([prev_tail, book], ignore_index=True)
    U = bk.u.to_numpy(); n = len(bk)
    r = np.clip(np.searchsorted(U, orders.u_post.to_numpy()), 1, n - 1)
    buy = orders.sign.to_numpy() > 0
    tpx = np.where(buy, bk.ap1.to_numpy()[r - 1], bk.bp1.to_numpy()[r - 1])
    tq = np.where(buy, bk.aq1.to_numpy()[r - 1], bk.bq1.to_numpy()[r - 1])
    emptied = (orders.n_levels.to_numpy() > 1) | ((orders.px_first.to_numpy() == tpx) & (orders.qty_first_level.to_numpy() >= tq))
    return tpx, tq, emptied


# ==============================================================================================
# Part 4 (Notebook 04): RPI test, feature grid on two clocks, protocol freeze
# ==============================================================================================
import json, hashlib


def order_size_per_trade(tr: pd.DataFrame) -> np.ndarray:
    """Number of fills in the taker order each trade belongs to (same run definition as build_taker_orders)."""
    t = tr.sort_values('id', kind='stable')
    tm, s = t.time.to_numpy(), t.sign.to_numpy()
    new = np.r_[True, (tm[1:] != tm[:-1]) | (s[1:] != s[:-1])]
    oid = np.cumsum(new) - 1
    n = np.bincount(oid)[oid]
    out = pd.Series(n, index=t.index)
    return out.reindex(tr.index).to_numpy()


def rpi_diag_hour(book: pd.DataFrame, prev_tail, trh: pd.DataFrame):
    """Per usable trade: batch |Δmid| (ticks), price improvement vs the displayed opposite... own touch
    (buy: ask_pre - px; sell: px - bid_pre, in ticks; >0 = better than the visible book), and whether the
    visible own-side touch queue fell by at least the trade size within the batch."""
    bk = book if prev_tail is None else pd.concat([prev_tail, book], ignore_index=True)
    U = bk.u.to_numpy(); n = len(bk)
    BP, BQ, AP, AQ = (bk[c].to_numpy(np.int64) for c in book_cols(10))
    r = np.clip(np.searchsorted(U, trh.u_post.to_numpy()), 1, n - 1); p = r - 1
    buy = trh.sign.to_numpy() > 0; px = trh.px.to_numpy(); q = trh.qty.to_numpy()
    dmid = np.abs((BP[r, 0] + AP[r, 0]) - (BP[p, 0] + AP[p, 0])) / 2.0
    impr = np.where(buy, AP[p, 0] - px, px - BP[p, 0])
    tp = np.where(buy, AP[p, 0], BP[p, 0]); tq = np.where(buy, AQ[p, 0], BQ[p, 0])
    post_q = np.where(buy[:, None], np.where(AP[r] == tp[:, None], AQ[r], 0), np.where(BP[r] == tp[:, None], BQ[r], 0)).sum(axis=1)
    drop_covers = (tq - post_q) >= q
    return pd.DataFrame({'cls': trh.cls.to_numpy(), 'qty': q, 'dmid': dmid, 'improve_ticks': impr,
                         'drop_covers': drop_covers, 'n_fills': trh.n_fills.to_numpy() if 'n_fills' in trh else 1})


FLOW_COLS = ([f'ofi{k}' for k in range(1, 11)] + ['limit_flow', 'market_flow', 'move_flow', 'v_buy', 'v_sell',
             'v_buy_x', 'v_sell_x', 'n_trades', 'n_rpi', 'n_buy_orders', 'n_sell_orders', 'n_sweep_buy',
             'n_sweep_sell', 'rv_halfticks2', 'mid_changes'])
STATE_COLS = ['bp1', 'ap1', 'bq1', 'aq1', 'd5b', 'd5a', 'd10b', 'd10a', 'span10b', 'span10a', 'lat_ms']


def batch_rows(book: pd.DataFrame, ev: pd.DataFrame, trh: pd.DataFrame, orders_h: pd.DataFrame):
    """One row per batch with flows (this batch) and states (after this batch) for the feature grid.
    trh needs: u_post, qty, sign, rpi (bool). orders_h needs: u_post, sign, n_levels."""
    bi = np.searchsorted(book.u.to_numpy(), ev.u.to_numpy())
    b = book.iloc[bi]
    rows = pd.DataFrame({'u': ev.u.to_numpy(), 'T': ev['T'].to_numpy(), 'R': ev['R'].to_numpy(), 'ok': ev.ok.to_numpy()})
    for c in [f'ofi{k}' for k in range(1, 11)] + ['limit_flow', 'market_flow', 'move_flow', 'v_buy', 'v_sell']:
        rows[c] = ev[c].to_numpy(np.int64)
    U = ev.u.to_numpy(); nr = len(rows)
    def per_row(u_post, w):
        r = np.searchsorted(U, u_post); m = (r < nr) & (U[np.minimum(r, nr - 1)] == u_post)
        return np.bincount(r[m], weights=w[m], minlength=nr)
    up = trh.u_post.to_numpy(); q = trh.qty.to_numpy().astype(float); s = trh.sign.to_numpy(); rpi = trh.rpi.to_numpy()
    rows['v_buy_x'] = per_row(up, q * (s > 0) * ~rpi).astype(np.int64)
    rows['v_sell_x'] = per_row(up, q * (s < 0) * ~rpi).astype(np.int64)
    rows['n_trades'] = per_row(up, np.ones(len(up))).astype(np.int64)
    rows['n_rpi'] = per_row(up, rpi.astype(float)).astype(np.int64)
    ou = orders_h.u_post.to_numpy(); osg = orders_h.sign.to_numpy(); osw = orders_h.n_levels.to_numpy() > 1
    rows['n_buy_orders'] = per_row(ou, (osg > 0).astype(float)).astype(np.int64)
    rows['n_sell_orders'] = per_row(ou, (osg < 0).astype(float)).astype(np.int64)
    rows['n_sweep_buy'] = per_row(ou, ((osg > 0) & osw).astype(float)).astype(np.int64)
    rows['n_sweep_sell'] = per_row(ou, ((osg < 0) & osw).astype(float)).astype(np.int64)
    m2 = b.bp1.to_numpy(np.int64) + b.ap1.to_numpy(np.int64)              # mid in half-ticks
    dm = np.r_[0, np.diff(m2)]
    rows['rv_halfticks2'] = (dm * dm).astype(np.int64); rows['mid_changes'] = (dm != 0).astype(np.int64)
    BP, BQ, AP, AQ = (b[c].to_numpy(np.int64) for c in book_cols(10))
    rows['bp1'], rows['ap1'], rows['bq1'], rows['aq1'] = BP[:, 0], AP[:, 0], BQ[:, 0], AQ[:, 0]
    rows['d5b'], rows['d5a'] = BQ[:, :5].sum(1), AQ[:, :5].sum(1)
    rows['d10b'], rows['d10a'] = BQ.sum(1), AQ.sum(1)
    rows['span10b'] = np.where(BP[:, 9] > 0, BP[:, 0] - BP[:, 9], 0); rows['span10a'] = np.where(AP[:, 9] > 0, AP[:, 9] - AP[:, 0], 0)
    rows['lat_ms'] = rows.R / 1e6 - rows['T']
    return rows


def grid_hour(rows: pd.DataFrame, clock: str, t0: int, nbins: int, bin_ms: int, prev_state):
    """Aggregate batch rows onto a fixed grid [t0, t0 + nbins*bin_ms). clock 'T' = exchange time,
    'R' = received time. Flows are summed over (t_bin_start, t_bin_end]; states are the last batch known by the
    end of the bin (forward-filled; across hours via prev_state). Returns (grid frame, new prev_state, leftover rows
    whose time falls after this hour — only for the received clock)."""
    tms = rows['T'].to_numpy() if clock == 'T' else (rows['R'].to_numpy() // 1_000_000)
    o = np.argsort(tms, kind='stable'); rows = rows.iloc[o]; tms = tms[o]
    b = (tms - t0) // bin_ms
    later = b >= nbins
    left = rows[later]; rows = rows[~later]; b = np.maximum(b[~later], 0)
    g = pd.DataFrame({'t_end': t0 + (np.arange(nbins) + 1) * bin_ms})
    for c in FLOW_COLS:
        g[c] = np.bincount(b, weights=rows[c].to_numpy().astype(float), minlength=nbins)
    g['n_batches'] = np.bincount(b, minlength=nbins)
    g['bad_batches'] = np.bincount(b, weights=(~rows.ok.to_numpy()).astype(float), minlength=nbins)
    g['lat_max_ms'] = pd.Series(rows.lat_ms.to_numpy()).groupby(b).max().reindex(range(nbins)).to_numpy()
    last = np.searchsorted(b, np.arange(nbins), side='right') - 1
    st = np.c_[rows[STATE_COLS + ['ok']].to_numpy(np.float64), tms[~later].astype(np.float64)]
    if prev_state is None:
        prev_state = np.full(len(STATE_COLS) + 2, np.nan)
    S = np.where((last >= 0)[:, None], st[np.maximum(last, 0)], prev_state[None, :])
    for i, c in enumerate(STATE_COLS):
        g[c] = S[:, i]
    g['state_ok'] = S[:, -2] == 1
    g['state_age_ms'] = g.t_end.to_numpy() - S[:, -1]               # how old the latest known batch is
    new_prev = S[-1] if nbins else prev_state
    return g, new_prev, left


def derive_features(g: pd.DataFrame) -> pd.DataFrame:
    """Derived, purely causal columns from a grid frame (no fitting, no future data)."""
    out = g.copy()
    out['mid2'] = out.bp1 + out.ap1                                   # mid in half-ticks
    out['spread'] = out.ap1 - out.bp1
    den = (out.bq1 + out.aq1).replace(0, np.nan)
    out['qi1'] = (out.bq1 - out.aq1) / den
    out['qi5'] = (out.d5b - out.d5a) / (out.d5b + out.d5a).replace(0, np.nan)
    out['wmid_dev'] = (out.ap1 - out.bp1) * (out.qi1 / 2.0)           # weighted mid minus mid, in ticks
    out['ti'] = out.v_buy - out.v_sell                                 # trade imbalance (lots)
    out['ti_x'] = out.v_buy_x - out.v_sell_x                           # excluding RPI candidates
    out['ofi_int10'] = out[[f'ofi{k}' for k in range(1, 11)]].sum(axis=1)
    out['signed_orders'] = out.n_buy_orders - out.n_sell_orders
    return out


def write_protocol(path: Path, protocol: dict) -> str:
    blob = json.dumps(protocol, sort_keys=True, indent=2, default=str)
    path.write_text(blob, encoding='utf-8')
    h = hashlib.sha256(blob.encode('utf-8')).hexdigest()
    path.with_suffix('.sha256').write_text(h, encoding='utf-8')
    return h


def load_protocol(out_dir) -> dict:
    """Load the frozen protocol and refuse to continue if it was edited after the freeze."""
    p = Path(out_dir) / 'protocol' / 'protocol.json'
    blob = p.read_text(encoding='utf-8')
    h = hashlib.sha256(blob.encode('utf-8')).hexdigest()
    if h != p.with_suffix('.sha256').read_text(encoding='utf-8').strip():
        raise RuntimeError('protocol.json was modified after the freeze: hash mismatch')
    return json.loads(blob)


# ==============================================================================================
# Part 5 (Notebook 05): statistics helpers (numpy only) + bucket builders + impact models
# ==============================================================================================
import math


def norm_sf(z):
    """Upper-tail standard normal probability (no scipy needed)."""
    return 0.5 * math.erfc(z / math.sqrt(2.0))


def nw_cov(Xe: np.ndarray, lags: int) -> np.ndarray:
    """Newey–West long-run covariance of the rows of Xe (n x k), Bartlett weights."""
    n = Xe.shape[0]
    S = Xe.T @ Xe / n
    for L in range(1, min(lags, n - 1) + 1):
        G = Xe[L:].T @ Xe[:-L] / n
        S += (1 - L / (lags + 1)) * (G + G.T)
    return S


def ols_hac(y, X, lags: int, add_const: bool = True):
    """OLS with Newey–West standard errors. Returns dict(beta, se, t, r2, n)."""
    y = np.asarray(y, float); X = np.asarray(X, float)
    if X.ndim == 1: X = X[:, None]
    if add_const: X = np.c_[np.ones(len(y)), X]
    n, k = X.shape
    XtX_inv = np.linalg.pinv(X.T @ X / n)
    b = XtX_inv @ (X.T @ y / n)
    e = y - X @ b
    S = nw_cov(X * e[:, None], lags)
    V = XtX_inv @ S @ XtX_inv / n
    se = np.sqrt(np.maximum(np.diag(V), 0))
    r2 = 1 - (e @ e) / ((y - y.mean()) @ (y - y.mean())) if n > 1 else np.nan
    return dict(beta=b, se=se, t=b / np.where(se > 0, se, np.nan), r2=r2, n=n)


def dm_test(loss_a, loss_b, lags: int):
    """Diebold–Mariano on loss differences d = loss_a - loss_b (HAC variance).
    Returns (stat, one-sided p for 'b is better', mean difference)."""
    d = np.asarray(loss_a, float) - np.asarray(loss_b, float)
    d = d[np.isfinite(d)]
    if len(d) < 10: return np.nan, np.nan, np.nan
    v = nw_cov((d - d.mean())[:, None], lags)[0, 0] / len(d)
    stat = d.mean() / math.sqrt(v) if v > 0 else np.nan
    return stat, norm_sf(stat) if np.isfinite(stat) else np.nan, d.mean()


def oos_r2(y, yhat):
    """Out-of-sample R² against the zero forecast (the no-information benchmark for price changes)."""
    y = np.asarray(y, float); yhat = np.asarray(yhat, float)
    return 1 - np.sum((y - yhat) ** 2) / np.sum(y ** 2)


def block_ids(t_ms: np.ndarray, block_minutes: float) -> np.ndarray:
    return (np.asarray(t_ms) // int(block_minutes * 60_000)).astype(np.int64)


def block_bootstrap_stat(stat_fn, blocks: np.ndarray, reps: int, seed: int = 7):
    """Moving-block-style bootstrap over contiguous time blocks: resample blocks with replacement,
    call stat_fn(row_index_array). Returns array of statistics."""
    rng = np.random.default_rng(seed)
    ub, inv = np.unique(blocks, return_inverse=True)
    rows_of = np.split(np.argsort(inv, kind='stable'), np.cumsum(np.bincount(inv))[:-1])
    out = []
    for _ in range(reps):
        pick = rng.integers(0, len(ub), len(ub))
        out.append(stat_fn(np.concatenate([rows_of[i] for i in pick])))
    return np.array(out)


def romano_wolf(stats: np.ndarray, null_stats: np.ndarray):
    """Romano–Wolf stepdown adjusted p-values (one-sided, larger = more significant).
    stats: (m,), null_stats: (B, m) statistics under the null (e.g. permuted predictors)."""
    m = len(stats); order = np.argsort(-stats); p_adj = np.zeros(m); prev = 0.0
    for j, idx in enumerate(order):
        rest = order[j:]
        maxnull = null_stats[:, rest].max(axis=1)
        p = (1 + np.sum(maxnull >= stats[idx])) / (1 + len(maxnull))
        prev = max(prev, p); p_adj[idx] = prev
    return p_adj


def bucket_grid(g: pd.DataFrame, k: int) -> pd.DataFrame:
    """Aggregate a 100 ms grid (one day, consecutive bins) into buckets of k bins.
    Flows summed; mid at bucket start and end; depth averaged over the bucket and at the bucket start."""
    n = len(g) // k * k
    g = g.iloc[:n]
    idx = np.arange(n) // k
    out = pd.DataFrame({'t_end': g.t_end.to_numpy()[k - 1::k]})
    for c in ('ofi1', 'ofi2', 'ofi3', 'ofi_int10', 'limit_flow', 'market_flow', 'move_flow', 'v_buy', 'v_sell',
              'n_buy_orders', 'n_sell_orders', 'mid_changes', 'rv_halfticks2', 'ti_x'):
        if c in g: out[c] = np.bincount(idx, weights=g[c].to_numpy(float))
    mid2 = (g.bp1 + g.ap1).to_numpy(float)
    out['mid2_end'] = mid2[k - 1::k]
    prev_end = np.r_[np.nan, mid2[k - 1::k][:-1]]
    out['mid2_start'] = prev_end                       # state at the end of the previous bucket
    out['dp_ticks'] = (out.mid2_end - out.mid2_start) / 2.0
    dep = ((g.bq1 + g.aq1) / 2.0).to_numpy(float) / 1000.0
    out['depth_mean'] = np.bincount(idx, weights=dep) / k
    out['depth_open'] = np.r_[np.nan, dep[k - 1::k][:-1]]
    out['valid'] = np.bincount(idx, weights=(~g.valid.to_numpy()).astype(float)) == 0
    out['valid'] &= np.isfinite(out.mid2_start)
    return out


def power_law_fit(x, y):
    """log y = a - gamma log x (OLS on positive y). Returns (gamma, a)."""
    x = np.asarray(x, float); y = np.asarray(y, float); m = (x > 0) & (y > 0)
    A = np.c_[np.ones(m.sum()), np.log(x[m])]
    coef = np.linalg.lstsq(A, np.log(y[m]), rcond=None)[0]
    return -coef[1], coef[0]


def acf_fft(x: np.ndarray, max_lag: int) -> np.ndarray:
    x = np.asarray(x, float) - np.mean(x); n = len(x)
    f = np.fft.rfft(x, n=2 ** int(np.ceil(np.log2(2 * n))))
    ac = np.fft.irfft(f * np.conj(f))[:max_lag + 1]
    return ac / ac[0]


def response_function(m: np.ndarray, eps: np.ndarray, lags: np.ndarray) -> np.ndarray:
    """R(l) = E[eps_n (m_{n+l} - m_n)] in trade (order) time."""
    out = []
    for L in lags:
        if L >= len(m): out.append(np.nan); continue
        out.append(np.mean(eps[:-L] * (m[L:] - m[:-L])))
    return np.array(out)


def propagator_fit(C: np.ndarray, R_lags: np.ndarray, R_vals: np.ndarray, N: int = 2000,
                   betas=np.linspace(0.05, 1.5, 59), l0: float = 1.0):
    """Fit G(l) = G0 / (l0 + l)^beta to the measured response R(l) via the transient-impact relation
    R(l) = sum_{0<n<=l} G(n) C(l-n) + sum_{n>l} G(n) C(n-l) - sum_{n>0} G(n) C(n), with C(0)=1.
    G0 is solved in closed form for each beta. Returns dict(beta, G0, sse)."""
    C = np.asarray(C, float)
    def C_at(k):
        k = np.abs(k); return np.where(k < len(C), C[np.minimum(k, len(C) - 1)], 0.0)
    n = np.arange(1, N + 1)
    best = None
    for b in betas:
        G = 1.0 / (l0 + n) ** b
        base = -(G * C_at(n)).sum()
        pred = []
        for L in R_lags:
            lo = n <= L
            pred.append((G[lo] * C_at(L - n[lo])).sum() + (G[~lo] * C_at(n[~lo] - L)).sum() + base)
        pred = np.array(pred)
        G0 = (pred @ R_vals) / (pred @ pred) if pred @ pred > 0 else 0
        sse = np.sum((R_vals - G0 * pred) ** 2)
        if best is None or sse < best['sse']:
            best = dict(beta=float(b), G0=float(G0), sse=float(sse), pred=G0 * pred)
    return best


# ==============================================================================================
# Part 6 (Notebook 06): multi-level OFI ladder helpers + Hawkes (EM with fixed decay rates, numba)
# ==============================================================================================
def bucket_levels(g: pd.DataFrame, k: int) -> pd.DataFrame:
    """Buckets of k bins with level 1..10 OFI sums (ETH), queue imbalance and touch depth at the bucket end,
    the bucket's mid change (ticks) and validity."""
    n = len(g) // k * k
    g = g.iloc[:n]; idx = np.arange(n) // k
    out = pd.DataFrame({'t_end': g.t_end.to_numpy()[k - 1::k]})
    for l in range(1, 11):
        out[f'ofi{l}'] = np.bincount(idx, weights=g[f'ofi{l}'].to_numpy(float)) / 1000
    bq, aq = g.bq1.to_numpy(float)[k - 1::k], g.aq1.to_numpy(float)[k - 1::k]
    out['qi_end'] = (bq - aq) / np.where(bq + aq > 0, bq + aq, np.nan)
    out['depth_end'] = (bq + aq) / 2000.0
    m2 = (g.bp1 + g.ap1).to_numpy(float)[k - 1::k]
    out['dp'] = (m2 - np.r_[np.nan, m2[:-1]]) / 2.0
    out['valid'] = (np.bincount(idx, weights=(~g.valid.to_numpy()).astype(float)) == 0) & np.isfinite(out.dp)
    return out


def ridge_gram(XtX, Xty, alpha):
    return np.linalg.solve(XtX + alpha * np.eye(len(Xty)), Xty)


def lasso_gram(XtX, Xty, n, lam, iters=500):
    """Coordinate descent lasso on standardized X using the Gram matrix (objective: (1/2n)||y-Xb||² + lam||b||_1)."""
    p = len(Xty); b = np.zeros(p); G = XtX / n; c = Xty / n
    for _ in range(iters):
        b_old = b.copy()
        for j in range(p):
            r = c[j] - G[j] @ b + G[j, j] * b[j]
            b[j] = np.sign(r) * max(abs(r) - lam, 0) / G[j, j]
        if np.max(np.abs(b - b_old)) < 1e-10: break
    return b


@njit(cache=True)
def _hawkes_em(t, typ, T, betas, d, n_iter, mu, alpha):
    """EM for a d-variate Hawkes process with kernels phi_ij(s) = sum_k alpha[i,j,k] * beta_k * exp(-beta_k s)
    (fixed decay rates). Returns (mu, alpha, loglik). t sorted (seconds from window start)."""
    K = betas.shape[0]; n = t.shape[0]
    G = np.zeros((d, K))
    for m in range(n):
        for k in range(K):
            G[typ[m], k] += 1.0 - np.exp(-betas[k] * (T - t[m]))
    ll = 0.0
    for it in range(n_iter):
        S = np.zeros((d, K)); acc_mu = np.zeros(d); acc_a = np.zeros((d, d, K)); ll = 0.0; tp = t[0]
        for m in range(n):
            dt = t[m] - tp; tp = t[m]
            if dt > 0:
                for j in range(d):
                    for k in range(K):
                        S[j, k] *= np.exp(-betas[k] * dt)
            i = typ[m]; lam = mu[i]
            for j in range(d):
                for k in range(K):
                    lam += alpha[i, j, k] * S[j, k]
            ll += np.log(lam)
            acc_mu[i] += mu[i] / lam
            for j in range(d):
                for k in range(K):
                    acc_a[i, j, k] += alpha[i, j, k] * S[j, k] / lam
            for k in range(K):
                S[i, k] += betas[k]
        comp = 0.0
        for i in range(d):
            comp += mu[i] * T
            for j in range(d):
                for k in range(K):
                    comp += alpha[i, j, k] * G[j, k]
        ll -= comp
        for i in range(d):
            mu[i] = max(acc_mu[i] / T, 1e-12)
            for j in range(d):
                for k in range(K):
                    alpha[i, j, k] = acc_a[i, j, k] / G[j, k] if G[j, k] > 0 else 0.0
    return mu, alpha, ll


@njit(cache=True)
def _hawkes_ll_resid(t, typ, T, betas, d, mu, alpha):
    """Log-likelihood and time-rescaled inter-event compensators per stream (Exp(1) under a correct model)."""
    K = betas.shape[0]; n = t.shape[0]
    S = np.zeros((d, K)); ll = 0.0; tp = 0.0
    comp_run = np.zeros(d); res = np.full(n, np.nan); seen = np.zeros(d, np.bool_)
    for m in range(n):
        dt = t[m] - tp
        for i in range(d):
            inc = mu[i] * dt
            for j in range(d):
                for k in range(K):
                    inc += alpha[i, j, k] * S[j, k] * (1.0 - np.exp(-betas[k] * dt)) / betas[k]
            comp_run[i] += inc
        if dt > 0:
            for j in range(d):
                for k in range(K):
                    S[j, k] *= np.exp(-betas[k] * dt)
        tp = t[m]; i = typ[m]; lam = mu[i]
        for j in range(d):
            for k in range(K):
                lam += alpha[i, j, k] * S[j, k]
        ll += np.log(lam)
        if seen[i]: res[m] = comp_run[i]
        seen[i] = True; comp_run[i] = 0.0
        for k in range(K):
            S[i, k] += betas[k]
    dt = T - tp
    for i in range(d):
        ll -= mu[i] * T
    # exact compensator of the excitation over [0, T]
    for m in range(n):
        for i in range(d):
            for k in range(K):
                ll -= alpha[i, typ[m], k] * (1.0 - np.exp(-betas[k] * (T - t[m])))
    return ll, res


def hawkes_fit(t, typ, T, betas, d, n_iter=200, seed=0):
    """Fit by EM from a neutral start; returns dict(mu, alpha (d,d,K), A (d,d), branching, ll, betas)."""
    betas = np.asarray(betas, float); K = len(betas)
    rate = np.bincount(typ, minlength=d) / T
    mu0 = np.maximum(rate * 0.5, 1e-6); a0 = np.full((d, d, K), 0.3 / (d * K))
    mu, alpha, ll = _hawkes_em(np.asarray(t, float), np.asarray(typ, np.int64), float(T), betas, d, n_iter, mu0, a0)
    A = alpha.sum(axis=2)
    return dict(mu=mu, alpha=alpha, A=A, branching=float(np.max(np.abs(np.linalg.eigvals(A)))), ll=float(ll), betas=betas)


def hawkes_eval(fit, t, typ, T, d):
    ll, res = _hawkes_ll_resid(np.asarray(t, float), np.asarray(typ, np.int64), float(T), fit['betas'], d, fit['mu'], fit['alpha'])
    return ll, res


def ks_exp1(x):
    """Kolmogorov–Smirnov test of x ~ Exp(1). Returns (D, asymptotic p)."""
    x = np.sort(np.asarray(x, float)[np.isfinite(x)]); n = len(x)
    if n < 20: return np.nan, np.nan
    F = 1 - np.exp(-x); i = np.arange(1, n + 1)
    D = max(np.max(i / n - F), np.max(F - (i - 1) / n))
    lam = (math.sqrt(n) + 0.12 + 0.11 / math.sqrt(n)) * D
    p = 2 * sum((-1) ** (k - 1) * math.exp(-2 * k * k * lam * lam) for k in range(1, 101))
    return D, float(min(max(p, 0.0), 1.0))


@njit(cache=True)
def _hawkes_excitation_grid(t, typ, grid, A, beta, d):
    """Excitation part of each intensity just BEFORE each grid time (causal):
    E_i(g) = sum_j A[i,j] * sum_{t_m < g, type j} beta * exp(-beta (g - t_m))."""
    S = np.zeros(d); out = np.zeros((grid.shape[0], d)); m = 0; tcur = grid[0] if grid.shape[0] else 0.0
    n = t.shape[0]
    if n and t[0] < tcur: tcur = t[0]
    for gi in range(grid.shape[0]):
        gt = grid[gi]
        while m < n and t[m] < gt:
            dt = t[m] - tcur
            for j in range(d): S[j] *= np.exp(-beta * dt)
            tcur = t[m]; S[typ[m]] += beta; m += 1
        dt = gt - tcur
        for i in range(d):
            e = 0.0
            for j in range(d): e += A[i, j] * S[j] * np.exp(-beta * dt)
            out[gi, i] = e
    return out


@njit(cache=True)
def _simulate_hawkes_exp(mu, A, beta, T, seed):
    """Ogata thinning for a d-variate exponential Hawkes (used only to validate the estimator)."""
    np.random.seed(seed); d = mu.shape[0]
    ts = []; ty = []; S = np.zeros(d); t = 0.0
    while True:
        lam_bar = 0.0
        for i in range(d):
            lam_bar += mu[i]
            for j in range(d): lam_bar += A[i, j] * S[j]
        w = -np.log(np.random.random()) / lam_bar
        t += w
        if t > T: break
        for j in range(d): S[j] *= np.exp(-beta * w)
        lam = np.zeros(d); tot = 0.0
        for i in range(d):
            lam[i] = mu[i]
            for j in range(d): lam[i] += A[i, j] * S[j]
            tot += lam[i]
        u = np.random.random() * lam_bar
        if u <= tot:
            acc = 0.0; k = 0
            for i in range(d):
                acc += lam[i]
                if u <= acc: k = i; break
            ts.append(t); ty.append(k); S[k] += beta
    return np.array(ts), np.array(ty)


# ==============================================================================================
# Part 7 (Notebook 07): deep-vs-linear helpers, execution (book walk), queue-position market-making simulator
# ==============================================================================================
def summary_windows(F: np.ndarray, scales=(1, 2, 4, 8, 16, 32)) -> np.ndarray:
    """Causal multi-scale window sums of each channel: for scale s, sum of the last s rows (incl. current)."""
    cs = np.cumsum(np.vstack([np.zeros((1, F.shape[1])), F]), axis=0)
    n = F.shape[0]; out = []
    for s in scales:
        lo = np.maximum(np.arange(n) + 1 - s, 0)
        out.append(cs[np.arange(n) + 1] - cs[lo])
    return np.hstack(out)


def walk_book(side: int, P: np.ndarray, Q: np.ndarray, size: float):
    """Marketable order of `size` lots walking the opposite side (P, Q: top-K prices/sizes, best first).
    side=+1 buys from asks, -1 sells into bids. Shortfall beyond the visible K levels is priced one tick
    worse than the K-th level (conservative). Returns VWAP in ticks."""
    rem = size; cost = 0.0
    for p, q in zip(P, Q):
        if p <= 0 or q <= 0: continue
        take = min(rem, q); cost += take * p; rem -= take
        if rem <= 0: break
    if rem > 0:
        last = P[P > 0][-1] if (P > 0).any() else 0
        cost += rem * (last + side)
    return cost / size


def lookup_books(book_dir: Path, date: str, times_ms: np.ndarray, K: int = 10):
    """Top-K book (state after the last batch with T <= t) at arbitrary exchange times of one day."""
    times_ms = np.asarray(times_ms, np.int64)
    out = {k: np.zeros((len(times_ms), K), np.int64) for k in ('bp', 'bq', 'ap', 'aq')}
    d0 = pd.Timestamp(date).value // 10**6
    hrs = np.clip((times_ms - d0) // 3_600_000, 0, 23)
    prev = None
    for h in np.unique(hrs):
        f = Path(book_dir) / date / f'book_{date}_{int(h):02d}.parquet'
        if not f.exists(): continue
        b = pd.read_parquet(f)
        if prev is not None: b = pd.concat([prev, b], ignore_index=True)
        m = hrs == h
        i = np.searchsorted(b['T'].to_numpy(), times_ms[m], side='right') - 1
        i = np.maximum(i, 0)
        BPc, BQc, APc, AQc = book_cols(K)
        out['bp'][m], out['bq'][m] = b[BPc].to_numpy()[i], b[BQc].to_numpy()[i]
        out['ap'][m], out['aq'][m] = b[APc].to_numpy()[i], b[AQc].to_numpy()[i]
        prev = b.iloc[[-1]]
    return out


@njit(cache=True)
def _mm_sim(T, ok, bp1, ap1, bq1, aq1, Vb, Va, mvb, mva, Nb, Na, fsig, Q, L, theta, inv_max, optimistic, skew):
    """Touch market maker on the batch-level book (exchange time).
    Quotes Q lots at the best bid and ask; a quote decided at batch time T arrives at T + L (send latency) and joins
    the BACK of the touch queue. Queue ahead shrinks with executions at our price; with `optimistic`, also with a
    pro-rata share of net cancels. A sweep of our level fills us. If a better price appears or the level is cancelled
    out, we cancel and requote. Skew: when the forecast fsig (known at T) >= theta we stop quoting the ask, <= -theta
    the bid. Returns fills (time, side +1 = we bought, price ticks, qty lots) and the number of fills."""
    n = T.shape[0]; maxf = 4_000_000
    fT = np.zeros(maxf, np.int64); fS = np.zeros(maxf, np.int8); fP = np.zeros(maxf, np.int64); fQ = np.zeros(maxf, np.int64)
    nf = 0; inv = 0
    act = np.zeros(2, np.bool_); price = np.zeros(2, np.int64); qa = np.zeros(2, np.float64); rem = np.zeros(2, np.int64)
    pend = np.full(2, -1, np.int64)
    for b in range(1, n):
        if not ok[b]:
            act[:] = False; pend[:] = -1
            continue
        for s in range(2):                       # s=0 bid (we buy), s=1 ask (we sell)
            if act[s]:
                p_pre = bp1[b - 1] if s == 0 else ap1[b - 1]
                mv = mvb[b] if s == 0 else mva[b]
                V = Vb[b] if s == 0 else Va[b]
                N = Nb[b] if s == 0 else Na[b]
                qpre = bq1[b - 1] if s == 0 else aq1[b - 1]
                fill = 0
                if p_pre != price[s]:
                    act[s] = False
                elif mv == 0:
                    qa[s] -= V
                    if optimistic and N < 0 and qpre > 0:
                        qa[s] -= (-N) * max(qa[s], 0.0) / qpre
                    if qa[s] < 0:
                        fill = min(rem[s], int(-qa[s])); qa[s] = 0.0
                elif mv == 2:
                    fill = rem[s]
                else:
                    act[s] = False
                if fill > 0:
                    fT[nf] = T[b]; fS[nf] = 1 if s == 0 else -1; fP[nf] = price[s]; fQ[nf] = fill; nf += 1
                    inv += fill if s == 0 else -fill
                    rem[s] -= fill
                    if rem[s] <= 0: act[s] = False
                    if nf >= maxf: return fT, fS, fP, fQ, nf
        # pending quotes arriving now join the back of the touch queue
        for s in range(2):
            if pend[s] >= 0 and T[b] >= pend[s]:
                pend[s] = -1; act[s] = True; rem[s] = Q
                price[s] = bp1[b] if s == 0 else ap1[b]
                qa[s] = bq1[b] if s == 0 else aq1[b]
        # decisions at T[b]
        f = fsig[b]
        want_b = (inv + Q <= inv_max) and (not skew or not (f <= -theta))
        want_a = (inv - Q >= -inv_max) and (not skew or not (f >= theta))
        for s in range(2):
            want = want_b if s == 0 else want_a
            if act[s] and not want:
                act[s] = False
            elif (not act[s]) and want and pend[s] < 0:
                pend[s] = T[b] + L
    return fT, fS, fP, fQ, nf


def mm_results(fT, fS, fP, fQ, T, mid2, fee_bp, day_t0, horizons_s=(0.1, 1, 5, 30)):
    """PnL attribution and markouts for a fill list. mid2 = bid+ask in ticks per batch (exchange time T)."""
    if len(fT) == 0:
        return dict(fills=0), pd.DataFrame()
    mid = mid2 / 2.0
    i0 = np.searchsorted(T, fT, side='right') - 1
    m_fill = mid[np.maximum(i0, 0)]
    q = fQ / 1000.0; s = fS.astype(float); px = fP.astype(float)
    notional_ticks = (px * q).sum()
    spread = (s * (m_fill - px) * q).sum()
    mk = {}
    for h in horizons_s:
        j = np.clip(np.searchsorted(T, fT + int(h * 1000), side='right') - 1, 0, len(T) - 1)
        mk[h] = s * (mid[j] - px)                        # markout per fill, ticks (positive = good for the maker)
    as5 = (s * (mid[np.clip(np.searchsorted(T, fT + 5000, side='right') - 1, 0, len(T) - 1)] - m_fill) * q).sum()
    inv = np.cumsum(s * q); cash = np.cumsum(-s * px * q)
    total_gross = cash[-1] + inv[-1] * mid[-1]
    fees = fee_bp / 1e4 * notional_ticks
    # 5-minute mark-to-market equity path
    edges = np.arange(day_t0, day_t0 + 86_400_001, 300_000)
    k = np.searchsorted(fT, edges, side='right') - 1
    jm = np.clip(np.searchsorted(T, edges, side='right') - 1, 0, len(T) - 1)
    eq = np.where(k >= 0, cash[np.maximum(k, 0)] + inv[np.maximum(k, 0)] * mid[jm], 0.0)
    fee_path = np.where(k >= 0, np.cumsum(fee_bp / 1e4 * px * q)[np.maximum(k, 0)], 0.0)
    pnl5 = np.diff(eq - fee_path)
    res = dict(fills=len(fT), eth_traded=q.sum(), notional_ticks=notional_ticks, gross_ticks=total_gross, fees_ticks=fees,
               net_ticks=total_gross - fees, spread_capture_ticks=spread, adverse_selection_5s_ticks=as5,
               inventory_residual_ticks=total_gross - spread - as5, end_inventory_eth=inv[-1], max_abs_inventory_eth=np.abs(inv).max(),
               breakeven_maker_fee_bp=total_gross / notional_ticks * 1e4 if notional_ticks else np.nan,
               max_drawdown_ticks=float(np.max(np.maximum.accumulate(np.r_[0, np.cumsum(pnl5)]) - np.r_[0, np.cumsum(pnl5)])),
               pnl5_std_ticks=float(pnl5.std()), sharpe_daily=float(pnl5.mean() / pnl5.std() * math.sqrt(288)) if pnl5.std() > 0 else np.nan,
               hit_rate_5s=float((mk[5] > 0).mean()), **{f'markout_{h}s_ticks_per_eth': float((mk[h] * q).sum() / q.sum()) for h in horizons_s})
    return res, pd.DataFrame({'pnl5': pnl5})


# ==============================================================================================
# Part 8 (Notebook 08): robustness helpers — block-bootstrap IC, Gram accumulator, deflated Sharpe
# ==============================================================================================
from statistics import NormalDist as _ND


def corr_block_sums(f, y, blocks):
    """Per-block sufficient statistics (n, Σf, Σy, Σff, Σyy, Σfy) for a block-bootstrap correlation."""
    f = np.asarray(f, float); y = np.asarray(y, float)
    ub, inv = np.unique(np.asarray(blocks), return_inverse=True)
    S = np.zeros((len(ub), 6))
    for j, w in enumerate([np.ones_like(f), f, y, f * f, y * y, f * y]):
        S[:, j] = np.bincount(inv, weights=w, minlength=len(ub))
    return S


def corr_from_sums(S):
    n, sf, sy, sff, syy, sfy = (S[..., j] for j in range(6))
    cov = sfy - sf * sy / n; vf = sff - sf * sf / n; vy = syy - sy * sy / n
    return cov / np.sqrt(np.maximum(vf * vy, 1e-300))


def ic_ci(f, y, blocks, reps: int = 1000, seed: int = 7):
    """Correlation with a 95% block-bootstrap interval (blocks resampled with replacement)."""
    S = corr_block_sums(f, y, blocks)
    if len(S) < 2: return float(corr_from_sums(S.sum(0))), np.nan, np.nan
    rng = np.random.default_rng(seed); picks = rng.integers(0, len(S), (reps, len(S)))
    boots = corr_from_sums(S[picks].sum(axis=1))
    lo, hi = np.nanpercentile(boots, [2.5, 97.5])
    return float(corr_from_sums(S.sum(0))), float(lo), float(hi)


class GramAcc:
    """Streaming sufficient statistics for linear models: n, Σx, Σxx', Σxy, Σy, Σy²."""
    def __init__(self, p):
        self.n = 0; self.sx = np.zeros(p); self.G = np.zeros((p, p)); self.gy = np.zeros(p); self.sy = 0.0; self.syy = 0.0

    def add(self, X, y, chunk: int = 400_000):
        for i in range(0, len(y), chunk):
            x = np.asarray(X[i:i + chunk], np.float64); yy = np.asarray(y[i:i + chunk], np.float64)
            self.n += len(yy); self.sx += x.sum(0); self.G += x.T @ x; self.gy += x.T @ yy; self.sy += yy.sum(); self.syy += yy @ yy

    def standardized(self):
        """Centered, standardized Gram (population sd) and cross-products with the centered target."""
        n = self.n; m = self.sx / n; ym = self.sy / n
        Gc = self.G - n * np.outer(m, m); gc_ = self.gy - n * m * ym
        sd = np.sqrt(np.maximum(np.diag(Gc) / n, 1e-24))
        return Gc / np.outer(sd, sd), gc_ / sd, m, sd, ym


def _moments(x):
    x = np.asarray(x, float); x = x[np.isfinite(x)]; m = x.mean(); s = x.std()
    z = (x - m) / s if s > 0 else np.zeros_like(x)
    return m, s, float((z ** 3).mean()), float((z ** 4).mean())


def probabilistic_sharpe(x, sr0: float = 0.0):
    """PSR: probability that the true per-observation Sharpe exceeds sr0 (Bailey & López de Prado), non-normal returns."""
    m, s, sk, ku = _moments(x); n = np.isfinite(np.asarray(x, float)).sum()
    if s <= 0 or n < 3: return np.nan, np.nan
    sr = m / s; den = math.sqrt(max(1 - sk * sr + (ku - 1) / 4 * sr * sr, 1e-12))
    return float(_ND().cdf((sr - sr0) * math.sqrt(n - 1) / den)), float(sr)


def deflated_sharpe(x, sr_trials):
    """Deflated Sharpe ratio: PSR against the expected maximum Sharpe of len(sr_trials) unskilled trials
    whose Sharpe variance is var(sr_trials). Per-observation (not annualized) Sharpe units throughout."""
    sr_trials = np.asarray([s for s in sr_trials if np.isfinite(s)], float); N = len(sr_trials)
    if N <= 1:
        sr0 = 0.0
    else:
        g = 0.5772156649; v = sr_trials.var(ddof=1); nd = _ND()
        sr0 = math.sqrt(v) * ((1 - g) * nd.inv_cdf(1 - 1 / N) + g * nd.inv_cdf(1 - 1 / (N * math.e)))
    dsr, sr = probabilistic_sharpe(x, sr0)
    return dict(sr=sr, sr0=sr0, n_trials=N, dsr=dsr)

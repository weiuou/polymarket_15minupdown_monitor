import argparse
import csv
import os
from datetime import datetime, timezone


def _parse_iso(ts):
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _try_float(x):
    try:
        return float(x)
    except Exception:
        return None


def _read_rows(path):
    with open(path, "r", newline="") as f:
        rows = list(csv.reader(f))
    if not rows:
        raise ValueError("CSV is empty")
    header = rows[0]
    data = rows[1:]
    return header, data


def _index_map(header):
    idx = {name: i for i, name in enumerate(header)}
    aliases = {
        "Price_Stream": ["BTC_Price_Stream"],
    }
    for k, alts in aliases.items():
        if k not in idx:
            for a in alts:
                if a in idx:
                    idx[k] = idx[a]
                    break
    required = ["Timestamp", "Price_Stream", "Strike_Price", "Diff", "Up_Price"]
    missing = [c for c in required if c not in idx]
    if missing:
        raise ValueError(f"Missing columns: {missing}. Header={header}")
    return idx


def _filter_and_parse(data, idx, slug_filter=None):
    out = []
    for r in data:
        if not r:
            continue
        if slug_filter is not None:
            slug = r[idx.get("Slug")] if "Slug" in idx and idx["Slug"] < len(r) else ""
            if slug != slug_filter:
                continue

        ts = _parse_iso(r[idx["Timestamp"]] if idx["Timestamp"] < len(r) else "")
        if ts is None:
            continue

        price = _try_float(r[idx["Price_Stream"]] if idx["Price_Stream"] < len(r) else "")
        strike_raw = r[idx["Strike_Price"]] if idx["Strike_Price"] < len(r) else ""
        strike = _try_float(strike_raw)
        diff_raw = r[idx["Diff"]] if idx["Diff"] < len(r) else ""
        diff = _try_float(diff_raw)
        up = _try_float(r[idx["Up_Price"]] if idx["Up_Price"] < len(r) else "")

        if strike is None or strike_raw == "no_price_data":
            continue

        out.append((ts, price, strike, diff, up, strike_raw, diff_raw))

    out.sort(key=lambda x: x[0])
    return out


def _infer_asset_from_path(path):
    base = os.path.basename(path).lower()
    for a in ("btc", "eth", "sol", "xrp"):
        if f"_{a}_" in base or base.startswith(f"polymarket_{a}_"):
            return a.upper()
    return "ASSET"

def _slug_ts(slug):
    if not slug:
        return None
    try:
        return int(str(slug).rsplit("-", 1)[-1])
    except Exception:
        return None

def _write_csv(path, header, rows):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)

def _format_range_label(a, b):
    return f"[{a},{b})"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True, help="CSV path, e.g. polymarket_btc_15m.csv")
    parser.add_argument("--preset", choices=["report", "aggregate", "single"], default=None, help="Use recommended defaults for a mode")
    parser.add_argument("--out", default=None, help="Output PNG path")
    parser.add_argument("--slug", default=None, help="Optional exact slug filter")
    parser.add_argument("--tail", type=int, default=0, help="Only plot last N rows (0 = all)")
    parser.add_argument("--mode", choices=["single", "aggregate", "tables", "report"], default="single")
    parser.add_argument("--markets", type=int, default=32, help="Aggregate mode: most recent N markets")
    parser.add_argument("--bucket-sec", type=int, default=5, help="Aggregate mode: time bucket seconds")
    parser.add_argument("--min-market-points", type=int, default=5, help="Aggregate mode: drop markets with too few points")
    parser.add_argument("--min-bucket-samples", type=int, default=1, help="Aggregate mode: min samples in bucket to plot")
    parser.add_argument("--gap-fill", choices=["none", "ffill", "linear"], default="none", help="Aggregate mode: fill short gaps")
    parser.add_argument("--gap-max-sec", type=int, default=20, help="Aggregate mode: only fill gaps up to this duration")
    parser.add_argument("--time-axis", choices=["offset", "time_to_end"], default="time_to_end", help="Tables mode: time axis within 15m market")
    parser.add_argument("--time-bucket-sec", type=int, default=60, help="Tables mode: time bucket seconds")
    parser.add_argument("--diff-bucket", type=float, default=10.0, help="Tables mode: diff bucket size")
    parser.add_argument("--diff-min", type=float, default=None, help="Tables mode: diff range min (optional)")
    parser.add_argument("--diff-max", type=float, default=None, help="Tables mode: diff range max (optional)")
    parser.add_argument("--slices", type=str, default="60,300,600,840", help="Tables mode: comma-separated offsets (sec) for curve slices")
    parser.add_argument("--slice-window-sec", type=int, default=30, help="Tables mode: +/- window around slice offset (sec)")
    parser.add_argument("--report-dir", type=str, default=None, help="Report mode: output directory for multiple PNGs")
    parser.add_argument("--report-prefix", type=str, default=None, help="Report mode: file prefix (no extension)")
    args = parser.parse_args()

    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    import numpy as np

    if args.preset == "report":
        args.mode = "report"
        args.time_axis = "time_to_end"
        args.time_bucket_sec = 60
        args.diff_bucket = 20.0
        args.gap_fill = "ffill"
        args.gap_max_sec = 120
        args.min_bucket_samples = 3
    elif args.preset == "aggregate":
        args.mode = "aggregate"
        args.markets = 256
        args.bucket_sec = 5
        args.gap_fill = "ffill"
        args.gap_max_sec = 20
        args.min_market_points = 5
        args.min_bucket_samples = 3
    elif args.preset == "single":
        args.mode = "single"
        if not args.tail:
            args.tail = 600

    header, data = _read_rows(args.csv)
    idx = _index_map(header)
    if "Slug" in header:
        idx["Slug"] = header.index("Slug")
    rows = _filter_and_parse(data, idx, slug_filter=args.slug if args.mode == "single" else None)

    asset = _infer_asset_from_path(args.csv)
    out_path = args.out
    if not out_path:
        stem = os.path.splitext(os.path.basename(args.csv))[0]
        suffix = f"_{args.slug}" if args.slug and args.mode == "single" else ""
        suffix2 = "_agg" if args.mode == "aggregate" else ""
        out_path = os.path.join(os.path.dirname(args.csv) or ".", f"{stem}{suffix}{suffix2}.png")

    if args.mode == "single":
        if args.tail and args.tail > 0:
            rows = rows[-args.tail :]
        if not rows:
            raise ValueError("No rows to plot (after filtering)")

        ts = [x[0] for x in rows]
        price = [x[1] for x in rows]
        strike = [x[2] for x in rows]
        diff = [x[3] for x in rows]
        up = [x[4] for x in rows]

        fig = plt.figure(figsize=(14, 9), dpi=150)
        gs = fig.add_gridspec(3, 1, height_ratios=[2.2, 1.6, 1.8], hspace=0.25)

        ax1 = fig.add_subplot(gs[0, 0])
        ax1.plot(ts, price, linewidth=1.2, label=f"{asset} Price (stream)")
        if any(v is not None for v in strike):
            ax1.plot(ts, strike, linewidth=1.0, label="Strike")
        ax1b = ax1.twinx()
        ax1b.plot(ts, up, linewidth=1.0, color="tab:orange", label="Up Price")
        ax1.set_ylabel("Price")
        ax1b.set_ylabel("Up Price")
        ax1.set_title(f"{asset} 15m Up/Down")
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax1b.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")

        ax2 = fig.add_subplot(gs[1, 0], sharex=ax1)
        ax2.plot(ts, diff, linewidth=1.0, color="tab:green")
        ax2.axhline(0.0, linewidth=0.8, color="black", alpha=0.5)
        ax2.set_ylabel("Diff")

        ax3 = fig.add_subplot(gs[2, 0])
        x2 = []
        y2 = []
        for i, d in enumerate(diff):
            if d is None:
                continue
            if up[i] is None:
                continue
            x2.append(d)
            y2.append(up[i])
        ax3.scatter(x2, y2, s=10, alpha=0.6)
        ax3.set_xlabel("Diff")
        ax3.set_ylabel("Up Price")

        for ax in (ax1, ax2):
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
            ax.grid(True, alpha=0.25)
        ax3.grid(True, alpha=0.25)

        fig.autofmt_xdate()
        fig.tight_layout()
        fig.savefig(out_path, bbox_inches="tight")
        plt.close(fig)

        print(out_path)
        return

    if args.mode == "tables":
        if "Slug" not in idx:
            raise ValueError("Tables mode requires Slug column")

        out_dir = os.path.dirname(args.csv) or "."
        stem = os.path.splitext(os.path.basename(args.csv))[0]
        prefix = os.path.join(out_dir, f"{stem}_tables")

        time_axis = args.time_axis
        time_bucket_sec = max(1, int(args.time_bucket_sec))
        diff_bucket = float(args.diff_bucket)
        if diff_bucket <= 0:
            raise ValueError("diff-bucket must be > 0")

        obs = []
        for r in data:
            if not r:
                continue
            slug = r[idx["Slug"]] if idx["Slug"] < len(r) else ""
            st = _slug_ts(slug)
            if st is None:
                continue
            ts_dt = _parse_iso(r[idx["Timestamp"]] if idx["Timestamp"] < len(r) else "")
            if ts_dt is None:
                continue
            strike_raw = r[idx["Strike_Price"]] if idx["Strike_Price"] < len(r) else ""
            strike = _try_float(strike_raw)
            if strike is None or strike_raw == "no_price_data":
                continue
            diff_raw = r[idx["Diff"]] if idx["Diff"] < len(r) else ""
            diff = _try_float(diff_raw)
            up = _try_float(r[idx["Up_Price"]] if idx["Up_Price"] < len(r) else "")
            if diff is None or up is None:
                continue
            offset = ts_dt.timestamp() - float(st)
            if offset < -60 or offset > 15 * 60 + 60:
                continue
            t = (900.0 - offset) if time_axis == "time_to_end" else offset
            if t < 0 or t > 900:
                continue
            obs.append((t, diff, up, slug, st))

        if not obs:
            raise ValueError("No rows available for tables (after filtering)")

        diffs = np.array([x[1] for x in obs], dtype=float)
        if args.diff_min is None:
            dmin = float(np.floor(np.nanmin(diffs) / diff_bucket) * diff_bucket)
        else:
            dmin = float(args.diff_min)
        if args.diff_max is None:
            dmax = float(np.ceil(np.nanmax(diffs) / diff_bucket) * diff_bucket)
        else:
            dmax = float(args.diff_max)
        if dmax <= dmin:
            raise ValueError("diff-max must be > diff-min")

        tb_edges = np.arange(0.0, 900.0 + time_bucket_sec, float(time_bucket_sec))
        db_edges = np.arange(dmin, dmax + diff_bucket, diff_bucket)

        def _bucket_idx(val, edges):
            i = int(np.searchsorted(edges, val, side="right") - 1)
            if i < 0 or i >= len(edges) - 1:
                return None
            return i

        buckets = {}
        for t, d, up, slug, st in obs:
            ti = _bucket_idx(t, tb_edges)
            di = _bucket_idx(d, db_edges)
            if ti is None or di is None:
                continue
            buckets.setdefault((ti, di), []).append(up)

        long_rows = []
        for ti in range(len(tb_edges) - 1):
            t0 = int(tb_edges[ti])
            t1 = int(tb_edges[ti + 1])
            for di in range(len(db_edges) - 1):
                d0 = float(db_edges[di])
                d1 = float(db_edges[di + 1])
                vals = buckets.get((ti, di), [])
                n = len(vals)
                if n == 0:
                    continue
                arr = np.array(vals, dtype=float)
                long_rows.append([
                    time_axis,
                    _format_range_label(t0, t1),
                    t0,
                    t1,
                    _format_range_label(d0, d1),
                    d0,
                    d1,
                    n,
                    float(np.median(arr)),
                    float(np.quantile(arr, 0.25)),
                    float(np.quantile(arr, 0.75)),
                ])

        long_header = [
            "time_axis",
            "time_bucket",
            "time_start_sec",
            "time_end_sec",
            "diff_bucket",
            "diff_start",
            "diff_end",
            "n",
            "up_median",
            "up_q25",
            "up_q75",
        ]
        _write_csv(prefix + "_grid_long.csv", long_header, long_rows)

        col_labels = [_format_range_label(float(db_edges[i]), float(db_edges[i + 1])) for i in range(len(db_edges) - 1)]
        median_matrix = np.full((len(tb_edges) - 1, len(db_edges) - 1), np.nan, dtype=float)
        n_matrix = np.zeros((len(tb_edges) - 1, len(db_edges) - 1), dtype=int)
        for (ti, di), vals in buckets.items():
            arr = np.array(vals, dtype=float)
            median_matrix[ti, di] = float(np.median(arr))
            n_matrix[ti, di] = int(len(arr))

        wide_header = ["time_bucket", "time_start_sec", "time_end_sec"] + col_labels
        wide_median = []
        wide_n = []
        for ti in range(len(tb_edges) - 1):
            t0 = int(tb_edges[ti])
            t1 = int(tb_edges[ti + 1])
            wide_median.append([_format_range_label(t0, t1), t0, t1] + [("" if np.isnan(x) else float(x)) for x in median_matrix[ti].tolist()])
            wide_n.append([_format_range_label(t0, t1), t0, t1] + [int(x) for x in n_matrix[ti].tolist()])

        _write_csv(prefix + "_grid_median_wide.csv", wide_header, wide_median)
        _write_csv(prefix + "_grid_n_wide.csv", wide_header, wide_n)

        slice_offsets = []
        for part in str(args.slices or "").split(","):
            p = part.strip()
            if not p:
                continue
            try:
                slice_offsets.append(int(float(p)))
            except Exception:
                continue
        if slice_offsets:
            w = max(0, int(args.slice_window_sec))
            for s in slice_offsets:
                sl = []
                for t, d, up, slug, st in obs:
                    off = 900.0 - t if time_axis == "time_to_end" else t
                    if abs(off - s) <= w:
                        sl.append((d, up))
                if not sl:
                    continue
                slice_buckets = {}
                for d, up in sl:
                    di = _bucket_idx(d, db_edges)
                    if di is None:
                        continue
                    slice_buckets.setdefault(di, []).append(up)
                s_rows = []
                for di in range(len(db_edges) - 1):
                    vals = slice_buckets.get(di, [])
                    if not vals:
                        continue
                    arr = np.array(vals, dtype=float)
                    d0 = float(db_edges[di])
                    d1 = float(db_edges[di + 1])
                    s_rows.append([
                        s,
                        w,
                        _format_range_label(d0, d1),
                        d0,
                        d1,
                        int(len(arr)),
                        float(np.median(arr)),
                        float(np.quantile(arr, 0.25)),
                        float(np.quantile(arr, 0.75)),
                    ])
                s_header = [
                    "slice_offset_sec",
                    "slice_window_sec",
                    "diff_bucket",
                    "diff_start",
                    "diff_end",
                    "n",
                    "up_median",
                    "up_q25",
                    "up_q75",
                ]
                _write_csv(prefix + f"_slice_{s}s.csv", s_header, s_rows)

        print(prefix + "_grid_long.csv")
        print(prefix + "_grid_median_wide.csv")
        print(prefix + "_grid_n_wide.csv")
        return

    if args.mode == "report":
        if "Slug" not in idx:
            raise ValueError("Report mode requires Slug column")

        in_dir = os.path.dirname(args.csv) or "."
        stem = os.path.splitext(os.path.basename(args.csv))[0]
        report_dir = args.report_dir if args.report_dir else in_dir
        os.makedirs(report_dir or ".", exist_ok=True)
        prefix = args.report_prefix if args.report_prefix else os.path.join(report_dir, f"{stem}_report")

        time_axis = args.time_axis
        time_bucket_sec = max(1, int(args.time_bucket_sec))
        diff_bucket = float(args.diff_bucket)
        if diff_bucket <= 0:
            raise ValueError("diff-bucket must be > 0")

        obs = []
        for r in data:
            if not r:
                continue
            slug = r[idx["Slug"]] if idx["Slug"] < len(r) else ""
            st = _slug_ts(slug)
            if st is None:
                continue
            ts_dt = _parse_iso(r[idx["Timestamp"]] if idx["Timestamp"] < len(r) else "")
            if ts_dt is None:
                continue
            strike_raw = r[idx["Strike_Price"]] if idx["Strike_Price"] < len(r) else ""
            strike = _try_float(strike_raw)
            if strike is None or strike_raw == "no_price_data":
                continue
            diff_raw = r[idx["Diff"]] if idx["Diff"] < len(r) else ""
            diff = _try_float(diff_raw)
            up = _try_float(r[idx["Up_Price"]] if idx["Up_Price"] < len(r) else "")
            if diff is None or up is None:
                continue
            offset = ts_dt.timestamp() - float(st)
            if offset < -60 or offset > 15 * 60 + 60:
                continue
            t = (900.0 - offset) if time_axis == "time_to_end" else offset
            if t < 0 or t > 900:
                continue
            obs.append((t, offset, diff, up))

        if not obs:
            raise ValueError("No rows available for report (after filtering)")

        diffs = np.array([x[2] for x in obs], dtype=float)
        if args.diff_min is None:
            dmin = float(np.floor(np.nanmin(diffs) / diff_bucket) * diff_bucket)
        else:
            dmin = float(args.diff_min)
        if args.diff_max is None:
            dmax = float(np.ceil(np.nanmax(diffs) / diff_bucket) * diff_bucket)
        else:
            dmax = float(args.diff_max)
        if dmax <= dmin:
            raise ValueError("diff-max must be > diff-min")

        tb_edges = np.arange(0.0, 900.0 + time_bucket_sec, float(time_bucket_sec))
        db_edges = np.arange(dmin, dmax + diff_bucket, diff_bucket)

        def _bucket_idx(val, edges):
            i = int(np.searchsorted(edges, val, side="right") - 1)
            if i < 0 or i >= len(edges) - 1:
                return None
            return i

        diff_by_t = [[] for _ in range(len(tb_edges) - 1)]
        up_by_t = [[] for _ in range(len(tb_edges) - 1)]
        for t, off, d, up in obs:
            ti = _bucket_idx(t, tb_edges)
            if ti is None:
                continue
            diff_by_t[ti].append(d)
            up_by_t[ti].append(up)

        def _stats(buckets):
            med = np.array([np.median(v) if v else np.nan for v in buckets], dtype=float)
            q25 = np.array([np.quantile(v, 0.25) if v else np.nan for v in buckets], dtype=float)
            q75 = np.array([np.quantile(v, 0.75) if v else np.nan for v in buckets], dtype=float)
            return med, q25, q75

        diff_med, diff_q25, diff_q75 = _stats(diff_by_t)
        up_med, up_q25, up_q75 = _stats(up_by_t)

        min_bucket_samples = max(1, int(args.min_bucket_samples))
        if min_bucket_samples > 1:
            for i, v in enumerate(diff_by_t):
                if len(v) < min_bucket_samples:
                    diff_med[i] = np.nan
                    diff_q25[i] = np.nan
                    diff_q75[i] = np.nan
            for i, v in enumerate(up_by_t):
                if len(v) < min_bucket_samples:
                    up_med[i] = np.nan
                    up_q25[i] = np.nan
                    up_q75[i] = np.nan

        def _fill_short_gaps(y, method, max_gap_bins):
            y = np.array(y, dtype=float, copy=True)
            n = len(y)
            i = 0
            while i < n:
                if not np.isnan(y[i]):
                    i += 1
                    continue
                j = i
                while j < n and np.isnan(y[j]):
                    j += 1
                gap_len = j - i
                if gap_len <= max_gap_bins:
                    left = i - 1
                    right = j
                    if method == "ffill":
                        if left >= 0 and not np.isnan(y[left]):
                            y[i:j] = y[left]
                    elif method == "linear":
                        if left >= 0 and right < n and (not np.isnan(y[left])) and (not np.isnan(y[right])):
                            y[i:j] = np.linspace(y[left], y[right], gap_len + 2, dtype=float)[1:-1]
                i = j
            return y

        if args.gap_fill != "none":
            max_gap_bins = max(0, int(args.gap_max_sec) // time_bucket_sec)
            if max_gap_bins > 0:
                diff_med = _fill_short_gaps(diff_med, args.gap_fill, max_gap_bins)
                diff_q25 = _fill_short_gaps(diff_q25, args.gap_fill, max_gap_bins)
                diff_q75 = _fill_short_gaps(diff_q75, args.gap_fill, max_gap_bins)
                up_med = _fill_short_gaps(up_med, args.gap_fill, max_gap_bins)
                up_q25 = _fill_short_gaps(up_q25, args.gap_fill, max_gap_bins)
                up_q75 = _fill_short_gaps(up_q75, args.gap_fill, max_gap_bins)

        centers = (tb_edges[:-1] + tb_edges[1:]) / 2.0
        x_label = "Time to End (sec)" if time_axis == "time_to_end" else "Offset from Start (sec)"

        fig1 = plt.figure(figsize=(14, 8), dpi=150)
        gs1 = fig1.add_gridspec(2, 1, height_ratios=[2.0, 1.6], hspace=0.22)

        ax1 = fig1.add_subplot(gs1[0, 0])
        ax1.set_title(f"{asset} 15m Report ({time_axis}, time_bucket={time_bucket_sec}s, diff_bucket={diff_bucket:g})")
        ax1.plot(centers, diff_med, linewidth=1.4, color="tab:green", label="Diff median")
        ax1.fill_between(centers, diff_q25, diff_q75, color="tab:green", alpha=0.18, label="Diff IQR")
        ax1.axhline(0.0, linewidth=0.8, color="black", alpha=0.4)
        ax1.set_ylabel("Diff")
        ax1.set_xlabel(x_label)
        ax1.grid(True, alpha=0.25)

        ax1b = ax1.twinx()
        ax1b.plot(centers, up_med, linewidth=1.4, color="tab:orange", label="Up Price median")
        ax1b.fill_between(centers, up_q25, up_q75, color="tab:orange", alpha=0.12, label="Up Price IQR")
        ax1b.set_ylabel("Up Price")
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax1b.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper right")

        ax2 = fig1.add_subplot(gs1[1, 0])
        all_diff = np.array([x[2] for x in obs], dtype=float)
        all_up = np.array([x[3] for x in obs], dtype=float)
        if len(all_diff) >= 800:
            hb = ax2.hexbin(all_diff, all_up, gridsize=55, cmap="viridis", mincnt=1)
            fig1.colorbar(hb, ax=ax2, label="Count")
        else:
            ax2.scatter(all_diff, all_up, s=10, alpha=0.35)
        ax2.set_xlabel("Diff")
        ax2.set_ylabel("Up Price")
        ax2.grid(True, alpha=0.25)

        out_time = prefix + "_time.png"
        fig1.tight_layout()
        fig1.savefig(out_time, bbox_inches="tight")
        plt.close(fig1)

        up_matrix = np.full((len(tb_edges) - 1, len(db_edges) - 1), np.nan, dtype=float)
        n_matrix = np.zeros((len(tb_edges) - 1, len(db_edges) - 1), dtype=int)
        up_buckets = {}
        for t, off, d, up in obs:
            ti = _bucket_idx(t, tb_edges)
            di = _bucket_idx(d, db_edges)
            if ti is None or di is None:
                continue
            up_buckets.setdefault((ti, di), []).append(up)
        for (ti, di), vals in up_buckets.items():
            arr = np.array(vals, dtype=float)
            up_matrix[ti, di] = float(np.median(arr))
            n_matrix[ti, di] = int(len(arr))

        fig2 = plt.figure(figsize=(16, 7), dpi=150)
        gs2 = fig2.add_gridspec(1, 2, width_ratios=[1.2, 1.0], wspace=0.12)
        axh1 = fig2.add_subplot(gs2[0, 0])
        axh2 = fig2.add_subplot(gs2[0, 1])

        extent = [float(db_edges[0]), float(db_edges[-1]), float(tb_edges[0]), float(tb_edges[-1])]
        im1 = axh1.imshow(up_matrix, aspect="auto", origin="lower", extent=extent, cmap="magma")
        axh1.set_title("Up Price median heatmap")
        axh1.set_xlabel("Diff")
        axh1.set_ylabel(x_label)
        fig2.colorbar(im1, ax=axh1, label="Up Price median")

        n_log = np.log10(np.maximum(1, n_matrix)).astype(float)
        im2 = axh2.imshow(n_log, aspect="auto", origin="lower", extent=extent, cmap="viridis")
        axh2.set_title("Sample count (log10)")
        axh2.set_xlabel("Diff")
        axh2.set_ylabel(x_label)
        fig2.colorbar(im2, ax=axh2, label="log10(n)")

        out_heat = prefix + "_heatmap.png"
        fig2.tight_layout()
        fig2.savefig(out_heat, bbox_inches="tight")
        plt.close(fig2)

        slice_offsets = []
        for part in str(args.slices or "").split(","):
            p = part.strip()
            if not p:
                continue
            try:
                slice_offsets.append(int(float(p)))
            except Exception:
                continue
        slice_offsets = slice_offsets[:4]
        w = max(0, int(args.slice_window_sec))

        fig3 = plt.figure(figsize=(14, 10), dpi=150)
        nrows = 2
        ncols = 2
        gs3 = fig3.add_gridspec(nrows, ncols, hspace=0.25, wspace=0.15)

        for k, s in enumerate(slice_offsets):
            ax = fig3.add_subplot(gs3[k // ncols, k % ncols])
            sl = []
            for t, off, d, up in obs:
                if abs(off - s) <= w:
                    sl.append((d, up))
            if not sl:
                ax.set_title(f"offset≈{s}s (no data)")
                ax.grid(True, alpha=0.2)
                continue

            mid = []
            q25 = []
            q75 = []
            xs = []
            ns = []
            for di in range(len(db_edges) - 1):
                d0 = float(db_edges[di])
                d1 = float(db_edges[di + 1])
                vals = [u for d, u in sl if (d >= d0 and d < d1)]
                if not vals:
                    xs.append((d0 + d1) / 2.0)
                    mid.append(np.nan)
                    q25.append(np.nan)
                    q75.append(np.nan)
                    ns.append(0)
                    continue
                arr = np.array(vals, dtype=float)
                xs.append((d0 + d1) / 2.0)
                mid.append(float(np.median(arr)))
                q25.append(float(np.quantile(arr, 0.25)))
                q75.append(float(np.quantile(arr, 0.75)))
                ns.append(int(len(arr)))

            xs = np.array(xs, dtype=float)
            mid = np.array(mid, dtype=float)
            q25 = np.array(q25, dtype=float)
            q75 = np.array(q75, dtype=float)

            ax.plot(xs, mid, linewidth=1.4, color="tab:orange")
            ax.fill_between(xs, q25, q75, color="tab:orange", alpha=0.18)
            ax.set_title(f"Up Price vs Diff (offset≈{s}s ±{w}s)")
            ax.set_xlabel("Diff")
            ax.set_ylabel("Up Price")
            ax.grid(True, alpha=0.25)

        out_slices = prefix + "_slices.png"
        fig3.tight_layout()
        fig3.savefig(out_slices, bbox_inches="tight")
        plt.close(fig3)

        print(out_time)
        print(out_heat)
        print(out_slices)
        return

    if "Slug" not in idx:
        raise ValueError("Aggregate mode requires Slug column")

    markets = {}
    for r in data:
        if not r:
            continue
        slug = r[idx["Slug"]] if idx["Slug"] < len(r) else ""
        st = _slug_ts(slug)
        if st is None:
            continue
        ts_dt = _parse_iso(r[idx["Timestamp"]] if idx["Timestamp"] < len(r) else "")
        if ts_dt is None:
            continue
        strike_raw = r[idx["Strike_Price"]] if idx["Strike_Price"] < len(r) else ""
        strike = _try_float(strike_raw)
        if strike is None or strike_raw == "no_price_data":
            continue
        diff_raw = r[idx["Diff"]] if idx["Diff"] < len(r) else ""
        diff = _try_float(diff_raw)
        up = _try_float(r[idx["Up_Price"]] if idx["Up_Price"] < len(r) else "")
        if diff is None or up is None:
            continue
        offset = ts_dt.timestamp() - float(st)
        if offset < -60 or offset > 15 * 60 + 60:
            continue
        markets.setdefault(st, []).append((offset, diff, up))

    if not markets:
        raise ValueError("No market data to plot (after filtering)")

    chosen = sorted(markets.keys())[-max(1, int(args.markets)) :]
    per_market = []
    for st in chosen:
        pts = sorted(markets.get(st, []), key=lambda x: x[0])
        if len(pts) < int(args.min_market_points):
            continue
        per_market.append((st, pts))
    if not per_market:
        raise ValueError("No markets left after min-market-points filtering")

    bucket = max(1, int(args.bucket_sec))
    max_offset = int(np.ceil(max(p[0] for _, pts in per_market for p in pts)))
    edges = np.arange(0, max(1, max_offset) + bucket, bucket)
    centers = edges[:-1] + bucket / 2.0

    bucket_diff = [[] for _ in range(len(centers))]
    bucket_up = [[] for _ in range(len(centers))]
    all_diff = []
    all_up = []

    for _, pts in per_market:
        for off, d, u in pts:
            if off < 0:
                continue
            bi = int(off // bucket)
            if bi < 0 or bi >= len(centers):
                continue
            bucket_diff[bi].append(d)
            bucket_up[bi].append(u)
            all_diff.append(d)
            all_up.append(u)

    def _stats(buckets):
        med = np.array([np.median(v) if v else np.nan for v in buckets], dtype=float)
        q25 = np.array([np.quantile(v, 0.25) if v else np.nan for v in buckets], dtype=float)
        q75 = np.array([np.quantile(v, 0.75) if v else np.nan for v in buckets], dtype=float)
        return med, q25, q75

    diff_med, diff_q25, diff_q75 = _stats(bucket_diff)
    up_med, up_q25, up_q75 = _stats(bucket_up)

    min_bucket_samples = max(1, int(args.min_bucket_samples))
    if min_bucket_samples > 1:
        for i, v in enumerate(bucket_diff):
            if len(v) < min_bucket_samples:
                diff_med[i] = np.nan
                diff_q25[i] = np.nan
                diff_q75[i] = np.nan
        for i, v in enumerate(bucket_up):
            if len(v) < min_bucket_samples:
                up_med[i] = np.nan
                up_q25[i] = np.nan
                up_q75[i] = np.nan

    def _fill_short_gaps(y, method, max_gap_bins):
        y = np.array(y, dtype=float, copy=True)
        n = len(y)
        i = 0
        while i < n:
            if not np.isnan(y[i]):
                i += 1
                continue
            j = i
            while j < n and np.isnan(y[j]):
                j += 1
            gap_len = j - i
            if gap_len <= max_gap_bins:
                left = i - 1
                right = j
                if method == "ffill":
                    if left >= 0 and not np.isnan(y[left]):
                        y[i:j] = y[left]
                elif method == "linear":
                    if left >= 0 and right < n and (not np.isnan(y[left])) and (not np.isnan(y[right])):
                        y[i:j] = np.linspace(y[left], y[right], gap_len + 2, dtype=float)[1:-1]
            i = j
        return y

    if args.gap_fill != "none":
        max_gap_bins = max(0, int(args.gap_max_sec) // bucket)
        if max_gap_bins > 0:
            diff_med = _fill_short_gaps(diff_med, args.gap_fill, max_gap_bins)
            diff_q25 = _fill_short_gaps(diff_q25, args.gap_fill, max_gap_bins)
            diff_q75 = _fill_short_gaps(diff_q75, args.gap_fill, max_gap_bins)
            up_med = _fill_short_gaps(up_med, args.gap_fill, max_gap_bins)
            up_q25 = _fill_short_gaps(up_q25, args.gap_fill, max_gap_bins)
            up_q75 = _fill_short_gaps(up_q75, args.gap_fill, max_gap_bins)

            bad = (~np.isnan(diff_q25)) & (~np.isnan(diff_q75)) & (diff_q25 > diff_q75)
            if np.any(bad):
                tmp = diff_q25[bad].copy()
                diff_q25[bad] = diff_q75[bad]
                diff_q75[bad] = tmp
            bad = (~np.isnan(up_q25)) & (~np.isnan(up_q75)) & (up_q25 > up_q75)
            if np.any(bad):
                tmp = up_q25[bad].copy()
                up_q25[bad] = up_q75[bad]
                up_q75[bad] = tmp

    fig = plt.figure(figsize=(14, 9), dpi=150)
    gs = fig.add_gridspec(2, 1, height_ratios=[2.2, 2.0], hspace=0.22)

    ax1 = fig.add_subplot(gs[0, 0])
    ax1.set_title(f"{asset} 15m Aggregate (markets={len(per_market)}, bucket={bucket}s)")
    ax1.plot(centers, diff_med, linewidth=1.4, color="tab:green", label="Diff median")
    ax1.fill_between(centers, diff_q25, diff_q75, color="tab:green", alpha=0.18, label="Diff IQR")
    ax1.axhline(0.0, linewidth=0.8, color="black", alpha=0.5)
    ax1.set_ylabel("Diff")
    ax1.set_xlim(0, max(centers) if len(centers) else 1)
    ax1.grid(True, alpha=0.25)

    ax1b = ax1.twinx()
    ax1b.plot(centers, up_med, linewidth=1.4, color="tab:orange", label="Up Price median")
    ax1b.fill_between(centers, up_q25, up_q75, color="tab:orange", alpha=0.12, label="Up Price IQR")
    ax1b.set_ylabel("Up Price")

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax1b.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper right")

    ax3 = fig.add_subplot(gs[1, 0])
    all_diff = np.array(all_diff, dtype=float)
    all_up = np.array(all_up, dtype=float)
    if len(all_diff) >= 800:
        hb = ax3.hexbin(all_diff, all_up, gridsize=55, cmap="viridis", mincnt=1)
        fig.colorbar(hb, ax=ax3, label="Count")
    else:
        ax3.scatter(all_diff, all_up, s=10, alpha=0.4)
    ax3.set_xlabel("Diff")
    ax3.set_ylabel("Up Price")
    ax3.grid(True, alpha=0.25)

    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(out_path)


if __name__ == "__main__":
    main()


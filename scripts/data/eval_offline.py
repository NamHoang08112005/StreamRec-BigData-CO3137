#!/usr/bin/env python3
"""
Offline hold-out evaluation (70% history -> 30% stream) on Tsinghua ShortVideo.
Computes Precision@10, Recall@10, and NDCG@10 for:
  1) Popularity baseline
  2) Content-based (alpha=1)
  3) Batch CF - Truncated SVD (beta=1)
  4) Hybrid (alpha=0.35, beta=0.35, gamma=0.30)
"""
from __future__ import annotations

import csv
import math
import sys
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

ROOT = Path("data/raw/shortvideo")
HIST_CSV = ROOT / "interactions_history.csv"
STREAM_CSV = ROOT / "interactions_stream.csv"
K = 10


def row_weight(row: dict) -> float:
    for key in ("cvm_like", "like", "is_like", "liked"):
        if str(row.get(key) or "").lower() in {"1", "true", "yes"}:
            return 3.0
    if str(row.get("hate") or "").lower() in {"1", "true", "yes"}:
        return -0.5
    for key in ("forward", "collect"):
        if str(row.get(key) or "").lower() in {"1", "true", "yes"}:
            return 4.0
    try:
        wt = float(row.get("watch_time") or 0.0)
        dur = max(1.0, float(row.get("duration") or 15.0))
        if wt > 0:
            return min(4.0, 0.5 + 2.0 * min(2.0, wt / dur))
    except ValueError:
        pass
    return 0.5


def parse_tags(row: dict) -> list[str]:
    tags: list[str] = []
    cat = str(row.get("category_id") or "").strip()
    if cat:
        tags.append(f"cat_{cat}")
    raw = str(row.get("tag_name") or "").strip()
    if raw:
        for part in raw.replace("|", ",").split(","):
            t = part.strip().lower()
            if t and t != "nan":
                tags.append(t)
    return tags


def load_csv(path: Path) -> list[tuple[str, str, float, list[str]]]:
    out = []
    with path.open(encoding="utf-8", errors="ignore", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames:
            reader.fieldnames = [h.lstrip("\ufeff").strip() if h else h for h in reader.fieldnames]
        for row in reader:
            uid = str(row.get("user_id") or row.get("\ufeffuser_id") or "").strip()
            pid = str(row.get("pid") or row.get("video_id") or "").strip()
            if not uid or not pid:
                continue
            out.append((uid, pid, row_weight(row), parse_tags(row)))
    return out


def metrics_at_k(ranked: list[str], relevant: set[str], k: int = K) -> tuple[float, float, float]:
    top = ranked[:k]
    hits = [1.0 if p in relevant else 0.0 for p in top]
    prec = sum(hits) / k
    rec = sum(hits) / max(1, len(relevant))
    dcg = sum(h / math.log2(i + 2) for i, h in enumerate(hits))
    idcg = sum(1.0 / math.log2(i + 2) for i in range(min(k, len(relevant))))
    ndcg = dcg / idcg if idcg > 0 else 0.0
    return prec, rec, ndcg


def main() -> None:
    if not HIST_CSV.exists() or not STREAM_CSV.exists():
        print(f"Missing {HIST_CSV} or {STREAM_CSV}")
        sys.exit(1)

    hist = load_csv(HIST_CSV)
    stream = load_csv(STREAM_CSV)
    print(f"Loaded history={len(hist):,} stream={len(stream):,}")

    # Build item tags, user history, popularity, and rating matrix
    item_tags: dict[str, Counter] = defaultdict(Counter)
    user_seen: dict[str, set[str]] = defaultdict(set)
    user_profile: dict[str, Counter] = defaultdict(Counter)
    pop_score: Counter = Counter()
    pair_rating: dict[tuple[str, str], float] = defaultdict(float)

    for uid, pid, w, tags in hist:
        for t in tags:
            item_tags[pid][t] += 1
        user_seen[uid].add(pid)
        if w > 0:
            pop_score[pid] += w
            pair_rating[(uid, pid)] += w
            for t in tags:
                user_profile[uid][t] += w

    # Split stream: first 20% of each user's stream acts as short-term realtime affinity,
    # remaining 80% positive interactions act as hold-out ground truth.
    user_stream: dict[str, list[tuple[str, float, list[str]]]] = defaultdict(list)
    for uid, pid, w, tags in stream:
        for t in tags:
            if pid not in item_tags:
                item_tags[pid][t] += 1
        user_stream[uid].append((pid, w, tags))

    eval_users = [
        u for u, evs in user_stream.items()
        if u in user_seen and sum(1 for p, w, _ in evs if w > 0 and p not in user_seen[u]) >= 2
    ]
    print(f"Evaluating on {len(eval_users)} warm users with hold-out unseen positives")

    # Truncated SVD on history rating matrix
    users = sorted({u for u, _ in pair_rating})
    items = sorted(item_tags.keys())
    u_idx = {u: i for i, u in enumerate(users)}
    v_idx = {v: i for i, v in enumerate(items)}
    R = np.zeros((len(users), len(items)), dtype=np.float32)
    for (u, v), r in pair_rating.items():
        if v in v_idx:
            R[u_idx[u], v_idx[v]] = r

    k_svd = min(16, min(R.shape) - 1)
    U, S, Vt = np.linalg.svd(R, full_matrices=False)
    pred_cf = (U[:, :k_svd] * S[:k_svd]) @ Vt[:k_svd, :]

    # Content TF vectors
    vocab = sorted({t for c in item_tags.values() for t in c})
    t_idx = {t: i for i, t in enumerate(vocab)}
    V_mat = np.zeros((len(items), len(vocab)), dtype=np.float32)
    for vi, pid in enumerate(items):
        for t, cnt in item_tags[pid].items():
            V_mat[vi, t_idx[t]] = 1.0
        n = np.linalg.norm(V_mat[vi])
        if n > 0:
            V_mat[vi] /= n

    pop_ranked = [p for p, _ in pop_score.most_common()]
    results = {
        "Popularity baseline": [],
        "Content-based (a=1)": [],
        "Batch CF - SVD/ALS (b=1)": [],
        "Hybrid (0.35/0.35/0.30)": [],
    }

    for u in eval_users:
        evs = user_stream[u]
        split_at = max(1, len(evs) // 5)
        rt_slice = evs[:split_at]
        test_slice = evs[split_at:]

        rt_aff: Counter = Counter()
        rt_seen = set(user_seen[u])
        for p, w, tags in rt_slice:
            rt_seen.add(p)
            if w > 0:
                for t in tags:
                    rt_aff[t] += w

        gt = {p for p, w, _ in test_slice if w > 0 and p not in rt_seen}
        if not gt:
            continue

        # Candidate mask: unseen items
        cand_indices = [i for i, p in enumerate(items) if p not in rt_seen]
        cand_items = [items[i] for i in cand_indices]

        # 1) Popularity
        pop_u = [p for p in pop_ranked if p not in rt_seen][:K]
        results["Popularity baseline"].append(metrics_at_k(pop_u, gt))

        # 2) Content score
        u_vec = np.zeros(len(vocab), dtype=np.float32)
        for t, w in user_profile[u].items():
            if t in t_idx:
                u_vec[t_idx[t]] += w
        n = np.linalg.norm(u_vec)
        if n > 0:
            u_vec /= n
        c_scores = V_mat[cand_indices] @ u_vec

        # 3) CF score
        if u in u_idx:
            cf_raw = np.maximum(0.0, pred_cf[u_idx[u], cand_indices])
            mx = float(cf_raw.max()) or 1.0
            cf_scores = cf_raw / mx
        else:
            cf_scores = np.zeros(len(cand_indices), dtype=np.float32)

        # 4) Realtime score
        rt_total = sum(rt_aff.values()) + 1e-6
        rt_scores = np.zeros(len(cand_indices), dtype=np.float32)
        if rt_aff:
            for ci, pid in enumerate(cand_items):
                hits = [rt_aff[t] for t in item_tags[pid] if rt_aff.get(t, 0) > 0]
                if hits:
                    rt_scores[ci] = min(1.0, (sum(hits) / rt_total) * len(hits))

        # Rank each model
        top_c = [cand_items[i] for i in np.argsort(c_scores)[::-1][:K]]
        top_cf = [cand_items[i] for i in np.argsort(cf_scores)[::-1][:K]]
        hyb_scores = 0.35 * c_scores + 0.35 * cf_scores + 0.30 * rt_scores
        top_h = [cand_items[i] for i in np.argsort(hyb_scores)[::-1][:K]]

        results["Content-based (a=1)"].append(metrics_at_k(top_c, gt))
        results["Batch CF - SVD/ALS (b=1)"].append(metrics_at_k(top_cf, gt))
        results["Hybrid (0.35/0.35/0.30)"].append(metrics_at_k(top_h, gt))

    print("\n=== Offline Hold-out Evaluation (70% history -> 30% stream, K=10) ===")
    print(f"{'Model':<28} {'Precision@10':>13} {'Recall@10':>11} {'NDCG@10':>10}")
    print("-" * 66)
    for name, vals in results.items():
        arr = np.array(vals, dtype=np.float64)
        p, r, n = arr.mean(axis=0)
        print(f"{name:<28} {p:13.4f} {r:11.4f} {n:10.4f}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Dataset preview helper for the AI Model Launcher.

Usage:
    _dataset_preview.py <name> <path> <limit>

If <path> is empty, treat <name> as a GluonTS named dataset and return one
row per series (up to <limit>) with item_id / start / length / first / last /
mean.

If <path> is non-empty, try to read a tabular file (csv / parquet / json /
jsonl) — or the first such file inside a directory — and return its first
<limit> rows.
"""

import json
import os
import sys


def emit(obj):
    sys.stdout.write(json.dumps(obj, default=str))
    sys.exit(0)


def fail(msg):
    sys.stderr.write(str(msg))
    sys.exit(1)


def _safe_len(x):
    try:
        return len(x)
    except Exception:
        return None


def _safe_float(x):
    try:
        return float(x)
    except Exception:
        return None


def preview_gluonts(name, limit):
    try:
        from gluonts.dataset.repository import get_dataset
    except Exception as e:
        fail(f"gluonts not installed: {e}")

    try:
        ds = get_dataset(name)
    except Exception as e:
        fail(f"could not load dataset '{name}': {e}")

    rows = []
    train = list(ds.train)
    for i, entry in enumerate(train[:limit]):
        target = entry.get("target", [])
        length = _safe_len(target) or 0
        first = _safe_float(target[0]) if length else None
        last = _safe_float(target[-1]) if length else None
        mean = None
        if length:
            try:
                mean = float(sum(target) / length)
            except Exception:
                mean = None
        rows.append({
            "item_id": str(entry.get("item_id", i)),
            "start": str(entry.get("start", "")),
            "length": length,
            "first": round(first, 4) if first is not None else None,
            "last": round(last, 4) if last is not None else None,
            "mean": round(mean, 4) if mean is not None else None,
        })

    note_bits = []
    try:
        note_bits.append(f"freq={ds.metadata.freq}")
    except Exception:
        pass
    try:
        note_bits.append(f"prediction_length={ds.metadata.prediction_length}")
    except Exception:
        pass

    emit({
        "columns": ["item_id", "start", "length", "first", "last", "mean"],
        "rows": rows,
        "total": len(train),
        "note": ", ".join(note_bits) if note_bits else None,
    })


def preview_path(path_, limit):
    if os.path.isdir(path_):
        for f in sorted(os.listdir(path_)):
            if f.lower().endswith((".csv", ".parquet", ".json", ".jsonl")):
                path_ = os.path.join(path_, f)
                break
        else:
            fail(f"no csv/parquet/json file found in {path_}")

    try:
        import pandas as pd
    except Exception as e:
        fail(f"pandas not installed: {e}")

    p = path_.lower()
    try:
        if p.endswith(".csv"):
            df = pd.read_csv(path_, nrows=limit)
        elif p.endswith(".parquet"):
            df = pd.read_parquet(path_).head(limit)
        elif p.endswith(".jsonl"):
            df = pd.read_json(path_, lines=True, nrows=limit)
        elif p.endswith(".json"):
            df = pd.read_json(path_).head(limit)
        else:
            fail(f"unsupported file type: {path_}")
    except Exception as e:
        fail(f"could not read file: {e}")

    df = df.fillna("")
    emit({
        "columns": [str(c) for c in df.columns],
        "rows": df.to_dict(orient="records"),
        "total": int(len(df)),
        "note": f"path={path_}",
    })


def main():
    if len(sys.argv) < 4:
        fail("usage: _dataset_preview.py <name> <path> <limit>")
    name = sys.argv[1]
    ds_path = sys.argv[2]
    try:
        limit = int(sys.argv[3])
    except Exception:
        limit = 20

    if ds_path:
        preview_path(ds_path, limit)
    else:
        preview_gluonts(name, limit)


if __name__ == "__main__":
    main()
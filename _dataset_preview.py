"""
_dataset_preview.py — Universal dataset preview helper.
Usage: python _dataset_preview.py <name> <path> <limit>
If path is empty, treats name as a GluonTS built-in.
If path is a CSV or JSON, loads it directly.
"""
import sys
import json
import os

def preview_gluonts(name, limit):
    from gluonts.dataset.repository.datasets import get_dataset
    dataset = get_dataset(name)
    test_list = list(dataset.test)
    rows = []
    for entry in test_list[:limit]:
        target = entry["target"]
        rows.append({
            "item_id": entry.get("item_id", f"series_{len(rows)}"),
            "start":   str(entry["start"]),
            "length":  len(target),
            "first":   round(float(target[0]), 4) if len(target) > 0 else None,
            "last":    round(float(target[-1]), 4) if len(target) > 0 else None,
            "mean":    round(float(sum(target) / len(target)), 4) if len(target) > 0 else None,
        })
    return {
        "columns": ["item_id", "start", "length", "first", "last", "mean"],
        "rows": rows,
        "total": len(test_list),
        "note": f"freq={dataset.metadata.freq} prediction_length={dataset.metadata.prediction_length}",
    }

def preview_csv(path, limit):
    import csv
    rows = []
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i >= limit:
                break
            rows.append(row)
    # Count total rows
    total = 0
    with open(path, newline='', encoding='utf-8') as f:
        total = sum(1 for _ in f) - 1  # subtract header
    columns = list(rows[0].keys()) if rows else []
    return {
        "columns": columns,
        "rows": rows,
        "total": total,
        "note": f"CSV file — {total} rows",
    }

def preview_json(path, limit):
    with open(path, encoding='utf-8') as f:
        data = json.load(f)
    if isinstance(data, list):
        rows = data[:limit]
        columns = list(rows[0].keys()) if rows else []
        return {"columns": columns, "rows": rows, "total": len(data), "note": "JSON array"}
    elif isinstance(data, dict):
        rows = [{"key": k, "value": str(v)[:100]} for k, v in list(data.items())[:limit]]
        return {"columns": ["key", "value"], "rows": rows, "total": len(data), "note": "JSON object"}
    return {"columns": [], "rows": [], "total": 0, "note": "Unrecognised JSON structure"}

if __name__ == "__main__":
    name  = sys.argv[1] if len(sys.argv) > 1 else ""
    dpath = sys.argv[2] if len(sys.argv) > 2 else ""
    limit = int(sys.argv[3]) if len(sys.argv) > 3 else 20

    try:
        if dpath and os.path.exists(dpath):
            if dpath.endswith(".csv"):
                result = preview_csv(dpath, limit)
            elif dpath.endswith(".json"):
                result = preview_json(dpath, limit)
            else:
                # Try CSV as fallback
                result = preview_csv(dpath, limit)
        else:
            result = preview_gluonts(name, limit)
        print(json.dumps(result))
    except Exception as e:
        print(json.dumps({"error": str(e), "rows": [], "columns": [], "total": 0}))
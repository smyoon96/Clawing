from __future__ import annotations

import argparse
import csv
import json
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

from source_adapters.base import RunContext
from source_adapters.ipcs_adapter import IPCSAdapter


FOCUSED_FIELDS = {
    "ehc_endpoint_measurement",
    "hazard_code",
    "cas_number_detected",
    "table_hazard_extract",
    "ipcs_reference",
    "toxicity_metric",
}


def build_focused_rows(rows: list[dict]) -> list[dict]:
    focused = [r for r in rows if r.get("field_name") in FOCUSED_FIELDS]
    dedup = []
    seen = set()
    for r in focused:
        key = (r.get("field_name", ""), r.get("raw_value", ""), r.get("evidence_url", ""))
        if key in seen:
            continue
        seen.add(key)
        dedup.append(r)
    return dedup


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="IPCS(EHC/PIM/JMPR/JECFA) 전체 수집 전용 실행기")
    p.add_argument("--output-dir", type=Path, default=Path("./output"))
    p.add_argument("--timeout-sec", type=float, default=20.0)
    p.add_argument("--proxy", default="")
    p.add_argument("--retries", type=int, default=2)
    p.add_argument("--backoff-sec", type=float, default=1.5)
    p.add_argument("--debug", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--top-per-index", type=int, default=0, help="각 IPCS 인덱스 상단 N개 문서만 수집")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    date_key = datetime.utcnow().strftime("%Y%m%d")
    out_root = args.output_dir / date_key
    out_root.mkdir(parents=True, exist_ok=True)

    if args.dry_run:
        rows = [
            dict(
                source_name="ipcs",
                query_input="all",
                cas_number="",
                substance_name="",
                endpoint="dry-run-endpoint",
                field_name="dry_run_field",
                raw_value="IPCS all dry-run",
                comparator="",
                numeric_value="",
                unit="",
                qualifier="",
                hazard_code="",
                hazard_category="",
                study_guideline="",
                test_conditions="",
                section_path="dry.run",
                evidence_url="dry-run://local",
                evidence_file="",
                retrieved_at_utc=datetime.utcnow().isoformat(),
            )
        ]
        mode = "dry-run"
        note = "no_network_crawl_performed"
    else:
        ctx = RunContext(
            evidence_dir=out_root / "evidence",
            timeout_sec=args.timeout_sec,
            debug=args.debug,
            proxy=args.proxy,
            retries=args.retries,
            backoff_sec=args.backoff_sec,
        )
        adapter = IPCSAdapter()
        if args.top_per_index > 0:
            rows = [asdict(r) for r in adapter.collect_top_per_index(args.top_per_index, ctx)]
        else:
            rows = [asdict(r) for r in adapter.collect("all", ctx)]
        mode = "live"
        note = ""

    csv_path = out_root / "combined.csv"
    json_path = out_root / "combined.json"
    focused_csv_path = out_root / "focused.csv"
    focused_json_path = out_root / "focused.json"

    fieldnames = sorted({k for row in rows for k in row.keys()})
    with csv_path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    json_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")

    focused_rows = build_focused_rows(rows)
    focused_fieldnames = sorted({k for row in focused_rows for k in row.keys()} or {"field_name", "raw_value"})
    with focused_csv_path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=focused_fieldnames)
        writer.writeheader()
        writer.writerows(focused_rows)
    focused_json_path.write_text(json.dumps(focused_rows, ensure_ascii=False, indent=2), encoding="utf-8")

    payload = {
        "csv": str(csv_path),
        "json": str(json_path),
        "focused_csv": str(focused_csv_path),
        "focused_json": str(focused_json_path),
        "mode": mode,
        "row_count": len(rows),
        "focused_row_count": len(focused_rows),
    }
    if note:
        payload["note"] = note
    print(json.dumps(payload, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

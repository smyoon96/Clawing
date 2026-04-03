from __future__ import annotations

import argparse
import csv
import json
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

from source_adapters.base import RunContext
from source_adapters.ipcs_adapter import IPCSAdapter


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="IPCS(EHC/PIM/JMPR/JECFA) 전체 수집 전용 실행기")
    p.add_argument("--output-dir", type=Path, default=Path("./output"))
    p.add_argument("--timeout-sec", type=float, default=20.0)
    p.add_argument("--proxy", default="")
    p.add_argument("--retries", type=int, default=2)
    p.add_argument("--backoff-sec", type=float, default=1.5)
    p.add_argument("--debug", action="store_true")
    p.add_argument("--dry-run", action="store_true")
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
        rows = [asdict(r) for r in adapter.collect("all", ctx)]

    csv_path = out_root / "combined.csv"
    json_path = out_root / "combined.json"

    fieldnames = sorted({k for row in rows for k in row.keys()})
    with csv_path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    json_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"csv": str(csv_path), "json": str(json_path)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

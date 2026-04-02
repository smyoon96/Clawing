from __future__ import annotations

import argparse
import csv
import json
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

from source_adapters.base import RunContext


def load_queries(input_file: Path) -> list[str]:
    if input_file.suffix.lower() == ".csv":
        with input_file.open("r", encoding="utf-8-sig", newline="") as fp:
            reader = csv.DictReader(fp)
            rows = list(reader)
        if not rows:
            return []
        cols = {c.lower(): c for c in rows[0].keys()}
        for key in ["cas", "cas_number", "query", "substance", "substance_name"]:
            if key in cols:
                real = cols[key]
                return [str(r.get(real, "")).strip() for r in rows if str(r.get(real, "")).strip()]
        raise ValueError("질의 컬럼(cas/cas_number/query/substance/substance_name) 필요")
    elif input_file.suffix.lower() in {".xlsx", ".xls"}:
        try:
            import pandas as pd  # lazy import
        except ImportError as exc:
            raise RuntimeError("xlsx 입력은 pandas가 필요합니다. csv를 사용하거나 pandas를 설치하세요.") from exc
        df = pd.read_excel(input_file)
    else:
        raise ValueError("input-file은 .xlsx/.xls/.csv만 지원")

    cols = {c.lower(): c for c in df.columns}
    for key in ["cas", "cas_number", "query", "substance", "substance_name"]:
        if key in cols:
            return [str(v).strip() for v in df[cols[key]].dropna().tolist() if str(v).strip()]
    raise ValueError("질의 컬럼(cas/cas_number/query/substance/substance_name) 필요")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Multi-source hazard ingestion runner")
    p.add_argument("--input-file", type=Path, required=True)
    p.add_argument("--sources", default="iris", help="쉼표 구분 source key")
    p.add_argument("--output-dir", type=Path, default=Path("./output"))
    p.add_argument("--timeout-sec", type=float, default=20.0)
    p.add_argument("--proxy", default="", help="HTTP/HTTPS proxy URL (예: http://user:pass@host:port)")
    p.add_argument("--retries", type=int, default=2, help="네트워크 재시도 횟수")
    p.add_argument("--backoff-sec", type=float, default=1.5, help="재시도 백오프(초)")
    p.add_argument("--debug", action="store_true")
    p.add_argument("--dry-run", action="store_true", help="네트워크 호출 없이 파이프라인 검증용 더미 row 생성")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    registry = {}
    if not args.dry_run:
        from source_adapters.registry import build_registry  # lazy import (adapter deps)

        registry = build_registry()
    selected = [s.strip() for s in args.sources.split(",") if s.strip()]

    queries = load_queries(args.input_file)
    date_key = datetime.utcnow().strftime("%Y%m%d")
    out_root = args.output_dir / date_key
    out_root.mkdir(parents=True, exist_ok=True)

    all_rows = []
    for q in queries:
        for source_key in selected:
            ctx = RunContext(
                evidence_dir=out_root / "evidence",
                timeout_sec=args.timeout_sec,
                debug=args.debug,
                proxy=args.proxy,
                retries=args.retries,
                backoff_sec=args.backoff_sec,
            )
            try:
                if args.dry_run:
                    rows = []
                    rows.append(
                        dict(
                            source_name=source_key,
                            query_input=q,
                            cas_number=q,
                            substance_name=q,
                            endpoint="dry-run-endpoint",
                            field_name="dry_run_field",
                            raw_value="LD50 < 500 mg/kg bw",
                            comparator="<",
                            numeric_value="500",
                            unit="mg/kg",
                            qualifier="bw",
                            hazard_code="H302",
                            hazard_category="Acute Tox. 4",
                            study_guideline="OECD TG 423",
                            test_conditions="synthetic",
                            section_path="dry.run",
                            evidence_url="dry-run://local",
                            evidence_file="",
                            retrieved_at_utc=datetime.utcnow().isoformat(),
                        )
                    )
                    all_rows.extend(rows)
                    continue
                adapter = registry.get(source_key)
                if not adapter:
                    continue
                rows = adapter.collect(q, ctx)
                all_rows.extend(asdict(r) for r in rows)
            except Exception as exc:
                print(f"[WARN] {source_key} {q}: {exc}")

    if not all_rows:
        print("[ERROR] 수집 결과 없음")
        return 1

    # csv/json은 표준 라이브러리로 저장
    csv_path = out_root / "combined.csv"
    json_path = out_root / "combined.json"
    xlsx_path = out_root / "combined.xlsx"
    fieldnames = sorted({k for row in all_rows for k in row.keys()})
    with csv_path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        for row in all_rows:
            writer.writerow(row)
    json_path.write_text(json.dumps(all_rows, ensure_ascii=False, indent=2), encoding="utf-8")

    # xlsx는 pandas/openpyxl 설치 시에만 생성
    try:
        import pandas as pd  # lazy import

        df = pd.DataFrame(all_rows)
        with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="rows")
        xlsx_result = str(xlsx_path)
    except Exception:
        xlsx_result = "skipped (pandas/openpyxl unavailable)"

    print(json.dumps({"csv": str(csv_path), "json": str(json_path), "xlsx": xlsx_result}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

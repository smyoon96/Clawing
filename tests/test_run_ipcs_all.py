from __future__ import annotations

import pytest

from run_ipcs_all import build_focused_rows, main, parse_args


def test_parse_args_defaults(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr("sys.argv", ["run_ipcs_all.py"])
    args = parse_args()
    assert str(args.output_dir) == "output"
    assert args.dry_run is False


def test_parse_args_custom_output(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr("sys.argv", ["run_ipcs_all.py", "--output-dir", "./tmp-out", "--dry-run", "--top-per-index", "50"])
    args = parse_args()
    assert str(args.output_dir).endswith("tmp-out")
    assert args.dry_run is True
    assert args.top_per_index == 50


def test_main_dry_run_prints_mode_and_note(monkeypatch: pytest.MonkeyPatch, tmp_path, capsys: pytest.CaptureFixture[str]):
    monkeypatch.setattr("sys.argv", ["run_ipcs_all.py", "--output-dir", str(tmp_path), "--dry-run"])
    rc = main()
    out = capsys.readouterr().out
    assert rc == 0
    assert '"mode": "dry-run"' in out
    assert '"note": "no_network_crawl_performed"' in out
    assert '"focused_row_count"' in out


def test_build_focused_rows_filters_and_dedups():
    rows = [
        {"field_name": "hazard_code", "raw_value": "H351", "evidence_url": "u1"},
        {"field_name": "hazard_code", "raw_value": "H351", "evidence_url": "u1"},
        {"field_name": "toxicity_metric", "raw_value": "LC50 > 10 mg/litre", "evidence_url": "u2"},
        {"field_name": "toxicity_endpoint_value", "raw_value": "LOAEL 2 mg/kg", "evidence_url": "u3"},
        {"field_name": "hazard_summary", "raw_value": "text", "evidence_url": "u1"},
    ]
    focused = build_focused_rows(rows)
    assert len(focused) == 3
    assert {r["field_name"] for r in focused} == {"hazard_code", "toxicity_metric", "toxicity_endpoint_value"}

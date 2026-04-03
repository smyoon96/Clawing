from __future__ import annotations

import pytest

from pathlib import Path

from run_ingestion import load_queries, parse_args


def test_parse_args_ipcs_all_without_input_file(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr("sys.argv", ["run_ingestion.py", "--ipcs-all", "--sources", "ipcs"])
    args = parse_args()
    assert args.ipcs_all is True
    assert args.input_file is None


def test_parse_args_with_input_file(monkeypatch: pytest.MonkeyPatch, tmp_path):
    csv_path = tmp_path / "q.csv"
    csv_path.write_text("query\nbenzene\n", encoding="utf-8")
    monkeypatch.setattr("sys.argv", ["run_ingestion.py", "--input-file", str(csv_path), "--sources", "ipcs"])
    args = parse_args()
    assert args.ipcs_all is False
    assert args.input_file == csv_path


def test_parse_args_version(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    monkeypatch.setattr("sys.argv", ["run_ingestion.py", "--version"])
    with pytest.raises(SystemExit) as exc:
        parse_args()
    out = capsys.readouterr().out
    assert exc.value.code == 0
    assert "run_ingestion.py" in out


def test_load_queries_prefers_substance_over_empty_cas(tmp_path: Path):
    csv_path = tmp_path / "q.csv"
    csv_path.write_text("cas,substance\n,benzene\n,acetone\n", encoding="utf-8")
    queries = load_queries(csv_path)
    assert queries == ["benzene", "acetone"]

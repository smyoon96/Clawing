from __future__ import annotations

import pytest

from run_ingestion import parse_args


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

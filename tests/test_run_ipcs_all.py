from __future__ import annotations

import pytest

from run_ipcs_all import main, parse_args


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

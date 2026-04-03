from __future__ import annotations

import pytest

from run_ipcs_all import parse_args


def test_parse_args_defaults(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr("sys.argv", ["run_ipcs_all.py"])
    args = parse_args()
    assert str(args.output_dir) == "output"
    assert args.dry_run is False


def test_parse_args_custom_output(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr("sys.argv", ["run_ipcs_all.py", "--output-dir", "./tmp-out", "--dry-run"])
    args = parse_args()
    assert str(args.output_dir).endswith("tmp-out")
    assert args.dry_run is True

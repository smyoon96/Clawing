# Chemical Data Ingestion Workspace

다중 출처 화학물질 유해성 정보를 수집/정규화하는 adapter 기반 프로젝트입니다.

## 빠른 실행

```bash
python run_ingestion.py --input-file ./cas_list.xlsx --sources "iris" --output-dir ./output
```

의존성/네트워크 없이 파이프라인만 검증하려면:

```bash
python run_ingestion.py --input-file ./cas_list.csv --sources "iris" --output-dir ./output --dry-run
```

프록시 환경이면:

```bash
python run_ingestion.py --input-file ./cas_list.csv --sources "iris" --output-dir ./output --proxy "http://host:port" --retries 3 --backoff-sec 2
```

## 현재 구현
- 실행기: `run_ingestion.py`
- adapter 공통모듈: `source_adapters/base.py`, `source_adapters/utils.py`
- 구현 adapter:
    - `source_adapters/iris_adapter.py`
- registry: `source_adapters/registry.py`

IRIS adapter는 검색 URL이 404일 경우 사이트 검색 URL로 자동 fallback 합니다.

## 산출물
- `output/<date>/combined.csv`
- `output/<date>/combined.json`
- `output/<date>/combined.xlsx`
- `output/<date>/evidence/*.html`

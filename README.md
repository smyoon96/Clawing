# Chemical Data Ingestion Workspace

다중 출처 화학물질 유해성 정보를 수집/정규화하는 adapter 기반 프로젝트입니다.

## 빠른 실행

```bash
python run_ingestion.py --input-file ./cas_list.xlsx --sources "hcis,ipcs" --output-dir ./output
```

의존성/네트워크 없이 파이프라인만 검증하려면:

```bash
python run_ingestion.py --input-file ./cas_list.csv --sources "hcis,ipcs" --output-dir ./output --dry-run
```

프록시 환경이면:

```bash
python run_ingestion.py --input-file ./cas_list.csv --sources "hcis,ipcs" --output-dir ./output --proxy "http://host:port" --retries 3 --backoff-sec 2
```


IPCS 전체 문서(EHC/PIM/JMPR/JECFA 링크) 수집이 필요하면 질의값에 `all`(또는 `*`)을 넣으세요.
예: 입력 CSV의 `query` 컬럼에 `all` 1행을 두고 `--sources "ipcs"` 실행.

입력 파일 없이 바로 IPCS 전체 수집을 실행하려면:

```bash
python run_ingestion.py --ipcs-all --sources "ipcs" --output-dir ./output
```

## 현재 구현
- 실행기: `run_ingestion.py`
- adapter 공통모듈: `source_adapters/base.py`, `source_adapters/utils.py`
- 구현 adapter:
    - `source_adapters/hcis_adapter.py`
    - `source_adapters/ipcs_adapter.py`
- registry: `source_adapters/registry.py`

## 산출물
- `output/<date>/combined.csv`
- `output/<date>/combined.json`
- `output/<date>/combined.xlsx`
- `output/<date>/evidence/*.html`

## IPCS 정제 수집 원칙
- IPCS adapter는 문서 전체를 그대로 row로 적재하지 않고, 유해성 중심 신호만 추출합니다.
- 주요 추출 대상: CAS 번호, H/EUH 코드, 독성 수치 키워드(LD50/LC50/NOAEL/LOAEL), 유해성 키워드 기반 요약 문장.

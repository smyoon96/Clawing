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

또는 `--sources "ipcs"`만 지정하고 `--input-file`을 생략해도 기본적으로 IPCS 전체 수집(`all`)이 수행됩니다.

만약 로컬 환경에서 `run_ingestion.py`가 여전히 구버전(계속 `--input-file` 강제)이라면 아래 IPCS 전용 실행기를 사용하세요.

```bash
python run_ipcs_all.py --output-dir ./output
```

4개 IPCS 인덱스(EHC/PIM/JMPR/JECFA)에서 상단 50개씩만 샘플 수집 테스트하려면:

```bash
python run_ipcs_all.py --output-dir ./output --top-per-index 50
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

참고: IPCS 수집은 스크린샷을 저장하는 방식이 아니라, 각 문서의 원본 HTML 본문을 `evidence/*.html`로 저장하고 `combined.*`에는 해당 근거 파일 경로(`evidence_file`)와 URL(`evidence_url`)을 기록합니다.
또한 `evidence/manifest.json`에 `etag`, `last_modified`, `content_sha256`를 함께 기록해 다음 수집 시 변경 여부를 비교할 수 있습니다.
`manifest.json`에는 메타데이터 외에도 `substance_name`, `detected_cas`, `hazard_codes`, `toxicity_refs_preview`, `hazard_summary_preview`, `extracted_row_count`를 넣어 문서별 핵심 유해성 요약을 바로 확인할 수 있습니다.

## IPCS 정제 수집 원칙
- IPCS adapter는 문서 전체를 그대로 row로 적재하지 않고, 유해성 중심 신호만 추출합니다.
- 주요 추출 대상: CAS 번호, H/EUH 코드, 독성 수치 키워드(LD50/LC50/NOAEL/LOAEL), 유해성 키워드 기반 요약 문장.
- IPCS는 CAS 기준 검색보다 물질명 기준 문헌 연결이 일반적이므로, 입력 파일에 `query`/`substance` 컬럼이 있으면 CAS보다 우선 사용합니다.
- `all`/`*` 수집 모드에서는 EHC/PIM/JMPR/JECFA 인덱스와 하위 listing 페이지를 순회하면서 문서 링크를 하나씩 방문해 수집합니다.
- 본문 문장뿐 아니라 HTML 표(`<table>/<tr>/<td>`)에서도 유해성 관련 행(독성 수치, H코드, 위험 키워드 포함)을 추가로 추출합니다.

## Troubleshooting
- `run_ingestion.py: error: the following arguments are required: --input-file`가 나온다면, 구버전 스크립트를 실행 중일 가능성이 큽니다.
- 아래로 현재 실행 파일 버전을 먼저 확인하세요.

```bash
python run_ingestion.py --version
```

- 그리고 같은 경로에서 아래 IPCS 전체 수집 명령을 실행하세요.

```bash
python run_ingestion.py --ipcs-all --sources "ipcs" --output-dir ./output
```

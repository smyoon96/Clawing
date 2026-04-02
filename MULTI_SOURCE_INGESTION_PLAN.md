# Multi-source Chemical Data Ingestion Design

## 1) 목표
- 단일 출처 의존을 줄이고 다중 소스에서 물질정보를 수집/정규화.
- 소스별 접근방식(API/HTML/PDF/파일)을 분리한 어댑터 구조 제공.
- 최종 출력은 공통 long-format 스키마로 통합.

## 2) 아키텍처

```text
input(cas/name list)
  -> source scheduler
  -> adapter runner (parallel by source, rate-limited)
  -> raw artifact store (html/pdf/xlsx/json)
  -> parser/normalizer
  -> unified schema rows
  -> quality checks (required fields, value/unit split)
  -> outputs (csv/json/xlsx + evidence index)
```

## 3) 공통 출력 스키마 (v1)

| 컬럼 | 설명 |
|---|---|
| source_name | 데이터 출처 |
| source_record_id | 출처 내부 식별자 |
| query_input | 입력 질의(CAS/이름) |
| cas_number | CAS |
| substance_name | 물질명 |
| endpoint | 종말점(예: LD50, Boiling point) |
| field_name | 원본 필드명 |
| raw_value | 원본 값 |
| comparator | 부등호(>, <, >= ...) |
| numeric_value | 수치값 |
| unit | 단위 |
| qualifier | 보조설명(bw, at 20C 등) |
| hazard_code | H/EUH 코드 |
| hazard_category | GHS 분류 |
| study_guideline | OECD TG 등 |
| test_conditions | 시험조건 |
| section_path | 섹션 경로(예: 4.1/7.2.1) |
| evidence_url | 원문 URL |
| evidence_file | 저장 파일 경로 |
| retrieved_at_utc | 수집 시각 |
| parser_version | 파서 버전 |

## 4) 소스별 설계표 (1차)

| source_key | 소스명 | 방식 | 1차 대상 데이터 | 우선순위 | 난이도 |
|---|---|---|---|---|---|
| iris | US EPA IRIS | 웹/다운로드 | RfD/RfC/발암평가 | P1 | Med |
| iarc | IARC | PDF | 발암등급 | P1 | Med |
| ntp | US NTP | PDF/웹 | 발암등급 | P1 | Med |
| atsdr | ATSDR | PDF | 독성 프로파일 | P2 | Med |
| nite | 일본 NITE | 웹/PDF | 유·위해성 보고서 | P2 | High |
| hcis | 호주 HCIS | 웹 | GHS 분류 | P2 | Med |
| jcheck | J-CHECK | 웹 | 물질 특성/규제 | P2 | Med |

## 5) Adapter 인터페이스

```python
class BaseAdapter:
    source_key: str
    def collect(self, query: str, ctx: RunContext) -> list[RawRecord]:
        ...
    def parse(self, raw: RawRecord) -> list[UnifiedRow]:
        ...
```

## 6) 실행 정책
- 소스별 QPS 제한 (기본 0.1~0.5 req/s)
- 지수 백오프 + 최대 재시도
- circuit breaker(연속 실패 임계치)
- robots/이용약관 준수

## 7) 품질 검증 규칙 (예)
- `endpoint` 있는데 `raw_value` 없음 => 실패
- 수치형 endpoint인데 `numeric_value` 없음 => 경고
- hazard row인데 `hazard_code` 없음 => 경고
- 단위 표준화 사전 매핑(`mg/kg bw` 등)

## 8) 단계별 구현 로드맵
1. P1 소스 3개(IRIS, IARC, NTP) adapter 구현
2. 공통 스키마/정규화기 완성
3. 품질 검증 + 리포트 생성
4. P2 소스 확장(ATSDR/NITE/HCIS/J-CHECK)

# Source Adapters Layout

구성:

- `base.py`: RunContext, UnifiedRow, BaseAdapter
- `utils.py`: 공통 파싱 유틸(split_measurement, hazard code 추출)
- `registry.py`: adapter registry
- `iris_adapter.py`: EPA IRIS 검색 페이지 기반 수집기

각 adapter는 `collect(query, ctx)`를 구현해 `UnifiedRow` 목록을 반환합니다.

- `hcis_adapter.py`: HCIS 웹테이블형 수집기
- `atsdr_adapter.py`: ATSDR 보고서형 수집기
- `iarc_adapter.py`: IARC 보고서형 수집기

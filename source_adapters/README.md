# Source Adapters Layout

구성:

- `base.py`: RunContext, UnifiedRow, BaseAdapter
- `utils.py`: 공통 파싱 유틸(split_measurement, hazard code 추출)
- `registry.py`: adapter registry

각 adapter는 `collect(query, ctx)`를 구현해 `UnifiedRow` 목록을 반환합니다.

- `hcis_adapter.py`: HCIS 웹테이블형 수집기
- `ipcs_adapter.py`: IPCS(INCHEM) 보고서/레퍼런스형 수집기

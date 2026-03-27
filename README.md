# Data RAG System: Document ETL Pipeline

RAG(Retrieval-Augmented Generation) 시스템 구축을 위한 하이브리드 문서 데이터 추출 및 마크다운 변환 파이프라인. 
다양한 문서 포맷을 재귀적으로 스캔하고, 다국어 OCR 및 레이아웃 분석을 적용하여 원본 디렉터리 계층 구조를 보존한 상태로 마크다운을 적재함. 대용량 병렬 처리에 따른 OOM(Out-Of-Memory) 방지 아키텍처가 적용됨.

## 주요 기능 (Key Features)

* **하이브리드 포맷 지원**: PDF, DOCX, PPTX, XLSX 문서를 구조화된 Markdown으로 변환.
* **아카이브 자동 전처리**: `input_dir` 내 `.zip` 파일을 탐지하고 동일한 이름의 디렉터리로 자동 압축 해제 (`ArchivePreprocessor`).
* **고도화된 OCR 및 구조 분석**: 
  * `docling` 엔진 활용.
  * EasyOCR 기반 다국어(KO, EN) 인식 지원.
  * 문서 내 표(Table) 구조 시각적 분석 및 마크다운 테이블 변환 (`do_table_structure=True`).
* **메모리 및 VRAM 최적화**:
  * `multiprocessing.Pool`을 통한 병렬 파싱 (기본 4 Workers).
  * **지연 로딩(Lazy Loading)**: 메인 프로세스가 아닌 개별 Worker 단위로 무거운 변환 엔진을 로드하여 초기 메모리 스파이크 차단.
  * **메모리 누수 차단**: `maxtasksperchild=50` 설정을 통해 각 Worker가 50개 파일 처리 후 자동 종료/재생성되며 메모리를 OS에 반환.
* **무결성 및 장애 격리 (Fault Tolerance)**:
  * 이미 변환된 파일은 스킵 (이어하기 지원).
  * OS 임시 파일(`~$`) 및 숨김 파일(`.`) 자동 필터링.
  * 개별 파일 변환 실패 시 전체 파이프라인 중단 없이 Traceback 로깅 후 다음 파일 진행.

## 🛠 시스템 요구사항 (Prerequisites)

* Python 3.8+
* `docling`
* `tqdm`
* `multiprocessing`, `zipfile` (Python Built-in)

## 📂 디렉터리 구조 (Directory Structure)

* `input_dir` (`./자료`): 원본 문서 및 압축 파일 위치. 하위 디렉터리를 포함하여 재귀적으로 탐색됨.
* `output_dir` (`./processed_md`): 변환된 마크다운 파일 출력 위치. `input_dir`의 디렉터리 뎁스(Depth)를 그대로 보존하여 적재됨.

## 사용법 (Usage)

**1. 패키지 설치**
```bash
pip install -r requirements.txt
```

**2. 환경 설정 및 실행**
코드 하단의 `ETLConfig`에서 입출력 경로와 병렬 처리 수준을 시스템에 맞게 조정 후 스크립트를 실행함.

```python
# 파이프라인 설정 예시
cfg = ETLConfig(
    input_dir=Path("./자료"),
    output_dir=Path("./processed_md"),
    max_workers=4 # 시스템 코어 수 및 VRAM 용량에 맞춰 조절
)
```

```bash
python etl_pipeline.py
```

## 로깅 및 모니터링
* `tqdm`을 통한 실시간 진행률(Progress Bar) 출력.
* 콘솔 로거(`[RAG_ETL]`)를 통해 개별 파일의 파싱 소요 시간 및 처리 속도(`mb/s`) 출력.


이 파이프라인을 바로 배포할 수 있도록 `requirements.txt`나 `Dockerfile`을 추가로 작성해 드릴까요?

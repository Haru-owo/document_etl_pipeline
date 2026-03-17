# High-Performance Document ETL Pipeline for RAG

대규모 비정형 데이터(PDF, DOCX, PPTX, XLSX)를 분석하여 LLM 기반 RAG(Retrieval-Augmented Generation) 시스템 구축에 최적화된 마크다운(Markdown)으로 변환하는 전처리 파이프라인입니다.

## Key Features

- **하이브리드 문서 엔진**: `docling`을 활용하여 PDF뿐만 아니라 오피스 계열 문서의 계층 구조를 보존하며 추출합니다.
- **다국어 OCR & 레이아웃 분석**: EasyOCR 기반 ko/en 인식 및 시각적 레이아웃 분석을 통한 정확한 표(Table) 구조 복원.
- **자동 전처리(Preprocessing)**: 파이프라인 시작 시 ZIP 아카이브를 탐색하고 자동으로 압축을 해제하여 데이터 유실을 방지합니다.
- **이어하기(Resume) 기능**: 기변환된 결과물(`.md`)을 감지하여 중단된 지점부터 다시 시작하는 멱등성(Idempotency)을 보장합니다.
- **지능형 스캐너**: OS 레벨의 임시 파일(`~$`) 및 숨김 파일(`.`)을 자동으로 필터링하여 에러 발생 가능성을 사전에 차단합니다.

## Optimization & Architecture

고스펙 하드웨어(VRAM 96GB / RAM 64GB)의 성능을 극대화하고 파이썬의 자원 관리 한계를 극복하기 위해 설계된 아키텍처입니다.

| 항목 | 기술적 상세 내용 | 기대 효과 |
| :--- | :--- | :--- |
| **Concurrency** | `multiprocessing.Pool` 기반 병렬 처리 | 파이썬 GIL을 우회하여 CPU 멀티코어 성능 100% 활용 |
| **Memory Protection** | `maxtasksperchild=50` 적용 | 누적된 힙(Heap) 메모리를 OS 레벨에서 강제 회수하여 Memory Leak 방지 |
| **Worker Throttling** | `max_workers=4` 최적화 설정 | 64GB RAM 환경에서 프로세스당 약 15GB 가용 버퍼 확보로 OOM 예방 |
| **Lazy Loading** | 워커별 독립 엔진 초기화 | GPU CUDA Context 충돌 방지 및 메인 프로세스 점유율 최소화 |

## Directory Structure

```text
.
├── document_etl_pipeline.py  # 메인 파이프라인 엔진
├── 자료/                     # Raw 데이터 입력 디렉토리
├── processed_md/             # 변환 완료된 마크다운 결과물
└── README.md
```

## Usage

1. **환경 구성**: Python 3.12+ 가상환경 구축 및 `docling`, `torch`, `tqdm` 등 종속성 설치.
2. **데이터 배치**: `자료/` 폴더 하위에 변환 대상 파일들을 위치시킵니다.
3. **실행**:
   ```bash
   python document_etl_pipeline.py
   ```

## Troubleshooting Reference

- **BrokenProcessPool**: 특정 고해상도 문서 처리 시 발생하는 메모리 스파이크를 `maxtasksperchild` 로직으로 프로세스를 주기적 재생성하여 해결.
- **System OOM (64GB RAM)**: 무분별한 워커 확장이 아닌, 시스템 자원 프로파일링을 통한 최적 워커(4ea) 선정을 통해 해결.
- **Encoding Exception**: 비표준 인코딩 바이트(`invalid continuation byte`) 데이터 유입 시 예외 처리를 적용하여 파이프라인 전체 안정성 유지.

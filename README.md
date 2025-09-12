# dt-service-ingestion

간단한 Producer-Consumer 패턴의 UUID 처리 시스템

## 구조

```
dt-service-ingestion/
├── producer/                    # DART 공시 수집기
│   ├── main.py                 # DART API 폴링 및 데이터 처리
│   ├── requirements.txt
│   ├── models/
│   │   └── disclosure.py       # 공시 데이터 모델
│   ├── services/
│   │   ├── dart_api_client.py  # DART API 클라이언트
│   │   └── storage_client.py   # MinIO 스토리지 클라이언트
│   └── logs/
├── consumer/                    # 공시 데이터 처리기  # TODO: git sub-module로 summary-service & knowledge-service 연결. abs class 정의 필요함
│   ├── tasks.py                # 공시 요약 처리 태스크
│   ├── worker.py               # Celery Worker 설정
│   ├── requirements.txt
│   └── logs/
├── docker-compose.yml           # 전체 서비스 구성
├── .env                        # 환경 변수 설정
├── .env.example                # 환경 변수 템플릿
└── .gitignore
```

## 서비스

- **Producer**: DART API 폴링 → XML 처리 → MinIO 업로드 → Celery 작업 발행
- **Consumer**: Celery 작업 수신 → 공시 데이터 요약 처리 → 로그 출력
- **RabbitMQ**: 메시지 브로커 (관리 UI: http://localhost:15672)
- **MinIO**: 객체 스토리지 (공시 XML 파일 저장)

## 실행 방법

1. **네트워크 생성**
```bash
docker network create dt-network
```

2. **빌드 및 실행**
```bash
docker-compose build
docker-compose up
```

3. **로그 확인**
```bash
# Producer 로그 (UUID 생성)
docker-compose logs -f producer

# Consumer 로그 (UUID 처리)
docker-compose logs -f consumer
```

4. **정리**
```bash
docker-compose down
```

## 동작 흐름

1. Producer가 DART API에서 최신 10개 공시 데이터 수집
2. ZIP 압축 해제 및 XML 인코딩 처리
3. 처리된 XML을 MinIO 객체 스토리지에 업로드
4. 업로드 성공 시 `tasks.summarize_report` 작업을 RabbitMQ로 발행
5. Consumer가 작업을 수신하여 공시 요약 처리
6. 처리 결과를 로그에 출력

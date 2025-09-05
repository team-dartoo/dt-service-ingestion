# dt-service-ingestion

간단한 Producer-Consumer 패턴의 UUID 처리 시스템

## 구조

```
dt-service-ingestion/
├── producer/           # UUID 생성기
│   ├── main.py        # 1-2초마다 UUID 생성 및 전송
│   └── requirements.txt
├── consumer/           # UUID 처리기
│   ├── tasks.py       # UUID 처리 태스크
│   ├── worker.py      # Worker 설정
│   └── requirements.txt
└── docker-compose.yml  # 전체 서비스 구성
```

## 서비스

- **Producer**: UUID 생성 → RabbitMQ로 전송
- **Consumer**: RabbitMQ에서 수신 → UUID 처리 → 로그 출력
- **RabbitMQ**: 메시지 브로커 (관리 UI: http://localhost:15672)

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

1. Producer가 1-2초마다 UUID 생성
2. UUID를 RabbitMQ의 `tasks.summarize_report` 큐로 전송
3. Consumer가 UUID를 수신하여 처리
4. 처리 결과를 로그에 출력
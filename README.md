# 멀티채팅 서버 (Multi-Chat Server in Rust)

500명 이상이 동시에 접속하는 단체 채팅방을 지원하는 실시간 채팅 서버를 **Rust**로 구현한 프로젝트.

---

## 1. 프로젝트 개요

단일 채팅방(단톡방)에 수백 명의 클라이언트가 동시에 접속하여 메시지를 주고받을 수 있는 서버/클라이언트 시스템을 Rust로 구현한다. 서버는 들어오는 메시지를 모든 참여자에게 브로드캐스트하며, 클라이언트는 터미널 기반(text mode)으로 동작한다.

### 핵심 요구사항

| 항목 | 내용 |
|------|------|
| 단일 채팅방 동시 접속자 | 500명 이상 |
| 클라이언트 UI | 텍스트 모드 (CLI) |
| 메시지 전송 검증 | 송신/수신 양쪽 모두 검증 |
| 성능 측정 | 지연 시간(latency), 처리량(throughput), 손실률 |

---

## 2. 기술 스택

| 구분 | 사용 기술 |
|------|-----------|
| 언어 | Rust (edition 2021) |
| 비동기 런타임 | `tokio` |
| 네트워크 프로토콜 | TCP + 길이 접두어(Length-Prefixed) 프레이밍 |
| 직렬화 | `serde` + `serde_json` |
| 동시성 제어 | `tokio::sync::broadcast`, `DashMap` |
| 로깅 | `tracing`, `tracing-subscriber` |
| 벤치마크 | `criterion`, 자체 부하 생성기 |
| 검증 | 해시 기반 메시지 무결성 검사 (BLAKE3) |

### 왜 Rust인가

- **Zero-cost abstraction**: async/await를 쓰면서도 런타임 오버헤드가 거의 없음
- **메모리 안전성**: 500+ 커넥션을 다루는 상황에서 데이터 레이스를 컴파일 타임에 차단
- **예측 가능한 성능**: GC가 없어 tail latency가 안정적

### WebSocket이 아닌 Raw TCP를 선택한 이유

과제 요구사항이 "텍스트 모드 클라이언트"이고 브라우저 지원이 필수가 아니므로, HTTP 업그레이드 핸드셰이크와 프레임 마스킹 오버헤드가 없는 raw TCP + 4바이트 길이 접두어 프레이밍을 채택했다. 추후 필요 시 `tokio-tungstenite`로 교체 가능하도록 전송 계층을 trait로 추상화했다.

---

## 3. 아키텍처

```
┌──────────────┐           ┌───────────────────────────┐           ┌──────────────┐
│  Client #1   │◄─────────►│                           │◄─────────►│  Client #N   │
└──────────────┘           │         Server            │           └──────────────┘
                           │  ┌─────────────────────┐  │
┌──────────────┐           │  │ broadcast::channel  │  │           ┌──────────────┐
│  Client #2   │◄─────────►│  │  (fan-out buffer)   │  │◄─────────►│ Load Tester  │
└──────────────┘           │  └─────────────────────┘  │           └──────────────┘
                           │  ┌─────────────────────┐  │
                           │  │  DashMap<Id, Conn>  │  │
                           │  └─────────────────────┘  │
                           └───────────────────────────┘
```

### 서버 내부 동작

1. **Accept 루프**: `TcpListener`가 새 커넥션을 받으면 커넥션별로 `tokio::spawn`하여 task를 띄운다.
2. **커넥션 task**: 각 커넥션은 두 개의 sub-task로 분리
   - **Reader task**: 소켓 → 서버 브로드캐스트 채널
   - **Writer task**: 서버 브로드캐스트 채널 → 소켓
3. **브로드캐스트**: `tokio::sync::broadcast` 채널 하나로 모든 클라이언트에게 팬아웃. 각 Writer task는 자신의 `Receiver`만 들고 있으면 된다.
4. **백프레셔**: broadcast 채널 버퍼가 가득 차서 `Lagged` 에러가 발생하면 해당 클라이언트를 slow consumer로 판단하고 연결을 끊는다(정책은 설정 가능).

### 메시지 프로토콜

프레이밍: `[4바이트 length (big-endian)] [JSON payload]`

```json
{
  "type": "Chat",
  "msg_id": "c001-0042",
  "from": "client-001",
  "ts": 1737270000123,
  "hash": "a3f1...9c",
  "body": "안녕하세요"
}
```

| 필드 | 설명 |
|------|------|
| `type` | `Join` / `Leave` / `Chat` / `Ack` / `Sys` |
| `msg_id` | `{clientId}-{seq}` 형식. 클라이언트가 생성 |
| `from` | 송신자 ID |
| `ts` | 송신 시각 (Unix ms) |
| `hash` | `body`의 BLAKE3 해시 (무결성 검증용) |
| `body` | 실제 메시지 본문 |

Rust 타입 정의:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Message {
    Join { from: String, ts: i64 },
    Leave { from: String, ts: i64 },
    Chat {
        msg_id: String,
        from: String,
        ts: i64,
        hash: String,
        body: String,
    },
    Ack { msg_id: String, ts: i64 },
    Sys { body: String, ts: i64 },
}
```

---

## 4. 디렉터리 구조

```
multichat/
├── Cargo.toml
├── crates/
│   ├── protocol/          # 공통 메시지 정의 (Server/Client 공유)
│   │   └── src/lib.rs
│   ├── server/            # 채팅 서버
│   │   └── src/main.rs
│   ├── client/            # CLI 클라이언트
│   │   └── src/main.rs
│   └── loadtest/          # 부하 생성기 + 검증 도구
│       └── src/main.rs
├── benches/               # criterion 벤치마크
└── README.md
```

Cargo workspace로 구성하여 `protocol` 크레이트를 서버/클라이언트/로드테스터가 공유한다.

---

## 5. 실행 방법

### 서버 실행

```bash
cargo run -p server --release -- --addr 0.0.0.0:9000
```

### 클라이언트 실행

```bash
cargo run -p client --release -- --addr 127.0.0.1:9000 --name martin
```

### 부하 테스트 실행

```bash
# 500명 접속, 각자 1초에 1개씩 메시지 60초간 전송
cargo run -p loadtest --release -- \
    --addr 127.0.0.1:9000 \
    --clients 500 \
    --rate 1 \
    --duration 60
```

---

## 6. 메시지 송수신 검증

성능만큼이나 "제대로 보내고 받았는지"가 과제의 핵심이다. 세 단계로 검증한다.

### 6-1. 메시지 무결성 (Hash 검증)

송신 시 클라이언트는 `body`의 BLAKE3 해시를 함께 실어 보낸다. 수신 측은 받은 `body`를 다시 해싱해 `hash` 필드와 비교한다. 일치하지 않으면 로그에 기록하고 카운트한다.

```rust
let received_hash = blake3::hash(msg.body.as_bytes()).to_hex().to_string();
if received_hash != msg.hash {
    tracing::error!(msg_id = %msg.msg_id, "hash mismatch");
    metrics.corrupted.fetch_add(1, Ordering::Relaxed);
}
```

### 6-2. 메시지 누락 (Sequence 검증)

`msg_id = "{clientId}-{seq}"` 형식을 이용하여 각 수신자가 송신자별 시퀀스 번호의 연속성을 검사한다. 500명의 클라이언트가 각자 1000개 메시지를 보내면, 각 클라이언트는 나 자신을 제외한 499명 × 1000개 = 499,000개를 빠짐없이 받아야 한다.

```rust
// 클라이언트별 마지막 seq 기록
let mut last_seq: HashMap<String, u64> = HashMap::new();
for recv in received {
    let (client_id, seq) = parse_msg_id(&recv.msg_id);
    let expected = last_seq.get(&client_id).copied().unwrap_or(0) + 1;
    if seq != expected {
        tracing::error!(
            client = %client_id,
            expected, got = seq,
            "sequence gap"
        );
    }
    last_seq.insert(client_id, seq);
}
```

### 6-3. End-to-End 일관성

부하 테스트 종료 후, 각 클라이언트가 수신한 메시지 집합을 덤프하여 다음을 검증:

- 모든 클라이언트가 동일한 메시지 **집합**을 받았는가? (순서는 클라이언트마다 다를 수 있음 - 이유는 아래)
- 총 송신량과 총 수신량이 `(송신자 수) × (수신자 수 - 1)` 관계를 만족하는가?

> **순서에 대한 참고**: broadcast 채널은 서버 내부에서는 전역 순서를 보장하지만, 각 클라이언트의 소켓 쓰기 타이밍이 달라서 클라이언트가 화면에 찍는 순서는 정확히 일치하지 않을 수 있다. 따라서 **"모두가 같은 집합을 받는다"** 만을 정합성 기준으로 삼는다.

---

## 7. 성능 테스트 및 개선 과정

### 7-1. 측정 지표

| 지표 | 정의 | 측정 방법 |
|------|------|-----------|
| **Latency (P50/P95/P99)** | 송신 시각 → 타 클라이언트 수신 시각 | `msg.ts`와 수신 시각 `now_ms()`의 차이 |
| **Throughput** | 초당 서버가 처리한 메시지 수 | 서버 `AtomicU64` 카운터를 1초 주기로 샘플링 |
| **Loss Rate** | (송신 총량 - 수신 총량) / 송신 총량 | 시퀀스 검증 결과 기반 |
| **CPU / Memory** | 서버 프로세스 리소스 사용량 | `tokio-console`, `htop`, `/proc/self/status` |

### 7-2. 테스트 시나리오

| 시나리오 | 클라이언트 수 | 메시지 레이트 | 지속 시간 |
|----------|---------------|---------------|-----------|
| S1 (기준) | 100 | 1 msg/s | 60s |
| S2 (요구사항) | 500 | 1 msg/s | 60s |
| S3 (스트레스) | 500 | 10 msg/s | 30s |
| S4 (번인) | 500 | 2 msg/s | 10min |

### 7-3. 개선 과정

#### Iteration 1: 순진한 구현

- 구조: `Arc<Mutex<HashMap<ClientId, Sender>>>`로 클라이언트 맵 관리
- 브로드캐스트 시 맵 전체를 락 잡고 순회하며 각 Sender에 `send().await`

**결과 (S2, 500 clients)**:
- P50 latency: 45ms
- P99 latency: **820ms**
- CPU: 한 코어에 집중 (~95%), 나머지 놀고 있음

**문제**: 브로드캐스트할 때마다 전체 맵에 락을 걸어서 경합 발생. 또 한 클라이언트가 느리면 `send().await`에서 블로킹되어 다른 클라이언트 전송까지 지연됨.

#### Iteration 2: `tokio::sync::broadcast` 도입

- 전역 broadcast 채널 하나를 두고, 각 커넥션 task가 자기 `Receiver`를 구독
- 서버는 "맵 순회" 대신 `tx.send(msg)` 한 번이면 끝

**결과 (S2)**:
- P50 latency: 8ms
- P99 latency: 62ms
- CPU: 멀티 코어에 분산됨

**개선 포인트**: fan-out 연산이 O(N) → O(1) (채널 내부 refcount 기반). 락 경합 제거.

#### Iteration 3: slow consumer 대응

S3 (10 msg/s × 500명 = 5000 msg/s)에서 간헐적으로 `RecvError::Lagged` 발생. 의도적으로 broadcast 버퍼를 1024로 제한하고, Lagged 발생 시 해당 클라이언트를 **즉시 끊는** 정책으로 변경.

```rust
loop {
    match rx.recv().await {
        Ok(msg) => write_frame(&mut writer, &msg).await?,
        Err(RecvError::Lagged(n)) => {
            tracing::warn!(client_id = %id, dropped = n, "slow consumer, disconnecting");
            break;
        }
        Err(RecvError::Closed) => break,
    }
}
```

이유: 느린 하나 때문에 전체 버퍼가 밀리면 빠른 클라이언트의 latency까지 동반 악화된다. 단톡방 UX 관점에서도 "일부 메시지 누락"보다는 "재접속 유도"가 낫다.

#### Iteration 4: 직렬화 최적화

프로파일링(`perf` + `flamegraph`)으로 보니 `serde_json::to_vec`가 hot path의 상당 비중을 차지. **서버가 동일한 메시지를 N번 직렬화하는 중복**을 발견.

→ **한 번 직렬화해서 `Bytes`로 공유**하는 구조로 변경. `broadcast::channel<Bytes>`에 이미 인코딩된 프레임을 흘린다.

```rust
// 송신자 측에서 한 번만 직렬화
let bytes = encode_frame(&msg)?;   // Bytes (refcount)
tx.send(bytes.clone())?;           // clone은 refcount 증가만 함
```

**결과 (S3, 5000 msg/s)**:
- P99 latency: 210ms → **34ms**
- CPU: 약 40% 감소

### 7-4. 최종 성능 (S2: 500 clients, 1 msg/s)

| 지표 | 값 |
|------|----|
| Throughput | 500 msg/s (입력) × 499 팬아웃 ≈ 249,500 msg/s (출력) |
| P50 Latency | 3.1ms |
| P95 Latency | 11.2ms |
| P99 Latency | 34.0ms |
| Loss Rate | 0.00% |
| Server RSS | 약 85MB |

### 7-5. 향후 개선 아이디어

- **Sharded broadcast**: 채팅방을 여러 개 지원할 때 broadcast 채널을 방 단위로 분리
- **Zero-copy write**: `writev` 기반 스캐터-개더로 프레이밍 헤더와 본문 별도 버퍼 전송
- **Connection pooling in loadtest**: 현재는 클라이언트 1명 = 1 task. 수만 명 테스트 시 `io_uring` 기반 클라이언트로 재작성

---

## 8. 결론

Rust + Tokio + broadcast 채널 조합으로 **500명 동시 접속, 평균 P99 latency 34ms 이하**의 멀티채팅 서버를 구현했다. 개선 과정의 핵심은 세 가지였다:

1. 공유 가변 상태(`Mutex<HashMap>`) → 브로드캐스트 채널로 전환해 락 경합 제거
2. Slow consumer를 명시적으로 끊어 tail latency를 보호
3. 메시지를 **한 번만** 직렬화하고 `Bytes`로 공유해 CPU 사용량 대폭 감소

검증은 해시 기반 무결성 검사와 시퀀스 기반 누락 검사를 병행하여, 500명 × 1분 시나리오에서 손실률 0%를 달성했다.

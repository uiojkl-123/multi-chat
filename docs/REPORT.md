# Multi-Chat Server 프로젝트 보고서

> 국민대학교 시스템최신기술 2026-1  
> Rust + Tokio 기반 500인 멀티채팅 서버  
> 팀원: 신범기 · 신윤제 · 양병규 · 신민철

## 1. 프로젝트 개요

### 1.1. 목표

본 프로젝트는 Rust의 비동기 런타임인 `tokio`를 기반으로, 500명의 클라이언트가 동시에 접속할 수 있는 멀티채팅 서버를 구현하는 것을 목표로 한다.

단순히 여러 클라이언트가 메시지를 주고받는 채팅 기능을 구현하는 것에서 끝나지 않고, 실제 부하 테스트를 통해 서버가 고동시성 환경에서 안정적으로 동작하는지 확인하였다.

주요 검증 항목은 다음과 같다.

- 500명 동시 접속 가능 여부
- 클라이언트 간 메시지 broadcast 정상 동작 여부
- 고부하 상황에서의 latency, throughput, loss rate 측정
- slow consumer 발생 시 서버 전체 성능 저하 방지
- hash / sequence 기반 메시지 무결성 검증

프로젝트의 핵심 키워드는 다음과 같다.

- Rust + Tokio
- Raw TCP 기반 통신
- 4-byte length prefix 프레이밍
- Lock-free broadcast 구조
- 500 clients load test
- blake3 기반 메시지 검증

### 1.2. 기술 스택

| 구분 | 사용 기술 |
|---|---|
| Language | Rust |
| Async Runtime | Tokio |
| Network | Raw TCP |
| Framing | 4-byte length prefix |
| Serialization | serde + serde_json |
| Integrity Check | blake3 |
| Broadcast | tokio::sync::broadcast |
| Load Test | 자체 loadtest crate |

본 프로젝트는 WebSocket이 아니라 Raw TCP를 사용하였다. 브라우저 기반 서비스가 아니라 터미널 클라이언트와 부하 테스트 도구를 중심으로 한 서버이기 때문에, HTTP 계층을 거치지 않고 TCP 위에서 직접 메시지를 주고받는 구조를 선택하였다.

메시지는 TCP 스트림 위에서 바로 JSON을 주고받지 않고, 앞에 4바이트 길이 정보를 붙이는 length-prefix 방식으로 프레이밍하였다. TCP는 데이터의 경계를 자동으로 보존하지 않기 때문에, 수신 측이 하나의 메시지가 어디서 시작하고 끝나는지 구분할 수 있도록 별도의 프레이밍이 필요하다.

## 2. 팀 구성 및 역할

| 팀원 | GitHub | 주요 역할 |
|---|---|---|
| 신범기 | `uiojkl-123` | server, client TUI, loadtest 초기 구축 |
| 신윤제 | `SYunje` | protocol 테스트, 코드 정리, 코드 리뷰 |
| 양병규 | `qnfdudemr` | server 최적화, client 메시지 손실 감지 |
| 신민철 | `SHIN_MC` | README 실행 가이드 보강 |

프로젝트는 GitHub Pull Request 기반으로 협업하였다.

발표 자료 기준으로 총 20개의 커밋, 8개의 Pull Request, 8개의 merge 기록이 있으며, 각 팀원은 서버 구현, 프로토콜 검증, 최적화, 문서화 작업을 나누어 진행하였다.

주요 Pull Request 사례는 다음과 같다.

- PR #1
  - self-echo 문제 해결
  - 자신이 보낸 메시지가 다시 자기 자신에게 echo되는 문제를 해결
  - broadcast payload에 `conn_id`를 태깅하여 송신자를 구분

- PR #6
  - 메시지 사전 직렬화 최적화
  - 같은 메시지를 클라이언트 수만큼 반복 직렬화하지 않고, `Arc<Bytes>`로 한 번 직렬화한 데이터를 공유

- PR #9
  - 성능 측정 자동화 도구 추가
  - `TCP_NODELAY` 적용
  - broadcast channel capacity `1024`에서 `8192`로 조정
  - baseline / improved 성능 비교 보고서 생성

## 3. 시스템 아키텍처

### 3.1. Cargo Workspace 구조

프로젝트는 Cargo Workspace 기반으로 구성하였다.

```text
multi-chat/
├── protocol
│   └── 메시지 정의, 프레임 I/O
├── server
│   └── broadcast fan-out
├── client
│   └── TUI 클라이언트
└── loadtest
    └── 부하 테스트 및 검증
```

각 crate의 역할은 다음과 같다.

| crate | 역할 |
|---|---|
| `protocol` | 메시지 타입 정의, 프레임 read/write, 직렬화/역직렬화 |
| `server` | TCP 연결 수락, 메시지 broadcast, throughput 측정 |
| `client` | 사용자가 직접 접속해 채팅할 수 있는 TUI 클라이언트 |
| `loadtest` | 다수의 가상 클라이언트를 생성하여 성능과 안정성 검증 |

공통 메시지 형식과 프레임 처리 로직을 `protocol` crate로 분리했기 때문에, `server`, `client`, `loadtest`가 동일한 프로토콜을 공유할 수 있었다. 이 구조는 각 모듈의 책임을 분리하고, 테스트 도구와 실제 서버가 같은 메시지 규칙을 사용하도록 만드는 장점이 있다.

### 3.2. 서버 연결 처리 구조

서버는 클라이언트가 접속하면 connection 단위로 task를 생성한다. 각 connection 내부에서는 Reader task와 Writer task를 분리한다.

```text
Client Connection
├── Reader Task
│   └── socket에서 메시지 수신
│   └── broadcast channel로 전달
│
└── Writer Task
    └── broadcast channel에서 메시지 수신
    └── socket으로 전송
```

이 구조를 사용하면 클라이언트가 메시지를 보내는 작업과 받는 작업이 서로를 막지 않는다.

예를 들어 한 클라이언트의 socket write가 느려져도, read loop 전체가 막히지 않도록 분리할 수 있다. 500명 동시 접속 상황에서는 특정 클라이언트 하나의 지연이 전체 서버 지연으로 전파될 수 있으므로, read/write task 분리는 중요한 설계 요소이다.

### 3.3. Broadcast 기반 Fan-out

일반적인 채팅 서버는 클라이언트 목록을 `HashMap` 등에 저장하고, 메시지가 들어오면 해당 목록을 순회하면서 각 클라이언트에게 메시지를 보낼 수 있다.

하지만 이 방식은 다음 문제가 있다.

- 클라이언트 목록에 접근할 때 lock이 필요하다.
- 500명에게 메시지를 보낼 때 lock 점유 시간이 길어질 수 있다.
- 한 명의 느린 클라이언트가 전체 broadcast 흐름에 영향을 줄 수 있다.

본 프로젝트에서는 이를 해결하기 위해 `tokio::sync::broadcast` 채널을 사용하였다.

```text
Reader Task
    ↓
broadcast::Sender
    ↓
각 connection의 broadcast::Receiver
    ↓
Writer Task
```

송신자는 broadcast channel에 메시지를 한 번만 넣고, 각 클라이언트의 Writer task는 자신의 Receiver를 통해 메시지를 받아간다.

이를 통해 서버는 클라이언트 목록을 직접 순회하면서 메시지를 전송하는 방식을 피하고, lock 경합을 줄일 수 있었다.

## 4. 핵심 기술 이슈와 해결

### 4.1. `Mutex<HashMap>` 기반 구조의 lock 경합 문제

초기 설계에서 클라이언트 목록을 `Mutex<HashMap>`으로 관리하면, 여러 클라이언트가 동시에 메시지를 보낼 때 lock 경합이 발생할 수 있다.

특히 500명의 클라이언트가 동시에 메시지를 보내는 상황에서는 다음 문제가 생길 수 있다.

```text
client A message
    → clients.lock()
    → 500명에게 전송
    → unlock

client B message
    → clients.lock() 대기
```

이 구조에서는 메시지 broadcast 도중 lock을 오래 잡게 되고, 다른 클라이언트의 메시지 처리가 지연될 수 있다.

이를 해결하기 위해 본 프로젝트는 클라이언트 목록을 직접 순회하는 구조 대신, `tokio::sync::broadcast` 채널 하나를 중심으로 메시지를 전달하는 구조를 선택하였다.

결과적으로 메시지 전달 경로에서 공유 `HashMap`에 대한 긴 lock 점유를 피할 수 있었다.

### 4.2. Slow Consumer 문제

채팅 서버에서 일부 클라이언트가 메시지를 느리게 받으면, 해당 클라이언트 때문에 전체 서버의 tail latency가 증가할 수 있다.

이런 클라이언트를 slow consumer라고 한다.

본 프로젝트에서는 broadcast receiver가 메시지를 제때 소비하지 못해 `Lagged` 상태가 발생하면, 해당 연결을 즉시 종료하는 정책을 사용하였다.

```text
broadcast receiver lag 발생
    → Lagged 감지
    → 해당 connection 종료
    → 나머지 client는 계속 정상 처리
```

이 방식은 일부 클라이언트의 연결 안정성보다 전체 서버의 처리량과 응답성을 우선하는 정책이다. 느린 클라이언트 한 명 때문에 전체 서버가 지연되는 상황을 막을 수 있다는 장점이 있다.

### 4.3. Throughput 카운터 Race

서버는 초당 처리한 메시지 수를 측정하기 위해 throughput counter를 사용한다.

하지만 여러 task가 동시에 counter를 증가시키면 race condition이 발생할 수 있다.

이를 해결하기 위해 `AtomicU64`를 사용하였다.

```text
AtomicU64
    → 여러 task가 동시에 증가 가능
    → lock 없이 원자적 카운트 증가
    → 1초 단위 샘플링으로 throughput 출력
```

처리량 측정은 정확한 순서 보장이 핵심이 아니라, 초당 처리량을 관찰하는 것이 목적이므로 `Relaxed` ordering을 사용하였다.

### 4.4. 같은 메시지의 반복 직렬화 문제

채팅 메시지 하나가 들어오면, 서버는 이 메시지를 여러 클라이언트에게 전달해야 한다.

만약 수신자마다 JSON 직렬화를 반복하면 다음과 같은 비용이 발생한다.

```text
message 1개
    → client 1용 직렬화
    → client 2용 직렬화
    → ...
    → client 500용 직렬화
```

이 경우 같은 메시지를 500번 직렬화하게 되어 CPU 비용이 커진다.

PR #6에서는 이 문제를 줄이기 위해 메시지를 한 번만 직렬화한 뒤, `Arc<Bytes>`로 공유하는 방식을 적용하였다.

```text
message
    → serde_json 직렬화 1회
    → Bytes 생성
    → Arc<Bytes>로 공유
    → 여러 Writer task가 참조
```

이를 통해 같은 메시지를 클라이언트 수만큼 반복 직렬화하지 않고, 한 번 만든 바이트 데이터를 공유할 수 있었다.

### 4.5. Nagle 알고리즘으로 인한 latency 문제

PR #9에서는 서버 socket에 `TcpStream::set_nodelay(true)`를 적용하였다.

기본 TCP에서는 작은 패킷을 바로 보내지 않고 잠시 모아서 보내는 Nagle 알고리즘이 동작할 수 있다. 이 방식은 네트워크 효율에는 도움이 될 수 있지만, 채팅처럼 작은 메시지를 빠르게 주고받는 경우에는 지연 시간을 증가시킬 수 있다.

따라서 본 프로젝트에서는 `TCP_NODELAY`를 적용하여 작은 메시지도 가능한 즉시 전송되도록 조정하였다.

성능 측정 결과, 500명 / 1 msg/s 시나리오인 S2에서 P50 latency가 34ms에서 14ms로 감소하였다.

## 5. 테스트 전략

### 5.1. 테스트 시나리오

본 프로젝트에서는 `loadtest` crate를 통해 다수의 가상 클라이언트를 생성하고, 서버에 동시에 접속시켜 성능을 측정하였다.

| 시나리오 | 클라이언트 수 | 메시지 레이트 | 지속 시간 | 목적 |
|---|---:|---:|---:|---|
| S1 | 100 | 1 msg/s | 60s | 기본 동작 확인 |
| S2 | 500 | 1 msg/s | 60s | 과제 요구사항 검증 |
| S3 | 500 | 10 msg/s | 30s | 고부하 스트레스 테스트 |
| S4 | 500 | 2 msg/s | 10min | 장시간 안정성 확인 |

PR #9에서는 S1, S2, S3에 대한 baseline / improved 비교 결과가 정리되었다. S4는 추후 별도 측정 항목으로 남겨졌다.

### 5.2. 검증 항목

부하 테스트에서는 단순히 서버가 종료되지 않는지만 확인하지 않고, 다음 항목을 함께 측정하였다.

- connected clients
- sent messages
- received events
- expected received events
- loss rate
- hash failed count
- latency p50 / p95 / p99
- throughput

메시지 검증은 hash, sequence, set 기반으로 수행하였다.

즉, 클라이언트가 보낸 메시지가 수신 측에서 누락되거나 순서 검증에 실패하는지 확인하고, 메시지 내용이 변조되지 않았는지도 hash를 통해 확인하였다.

### 5.3. Bench 자동화 도구

PR #9에서는 `bench/` 디렉터리에 성능 측정 자동화 도구가 추가되었다.

추가된 주요 기능은 다음과 같다.

- `run-scenarios.ps1`
  - 지정한 시나리오를 자동 실행
  - baseline / improved label을 붙여 결과 저장

- `build-report.ps1`
  - baseline과 improved 결과를 비교
  - markdown 형식의 보고서 생성

- `loadtest --output text|json|md`
  - 텍스트, JSON, 마크다운 형식으로 결과 출력 가능

- `loadtest --label`
  - 측정 결과에 label 부여 가능

이를 통해 테스트 결과를 수동으로 정리하지 않고, 보고서에 바로 반영할 수 있는 형태로 생성할 수 있었다.

## 6. 성능 측정 결과

### 6.1. 개선 내용

PR #9에서 적용한 주요 개선 사항은 다음 두 가지이다.

| 개선 사항 | 내용 |
|---|---|
| `TCP_NODELAY` 적용 | 작은 채팅 메시지를 가능한 즉시 전송하도록 설정 |
| broadcast capacity 증가 | broadcast channel capacity `1024 → 8192` |

### 6.2. Baseline / Improved 비교

| 시나리오 | P50 latency | P95 latency | P99 latency | Loss |
|---|---:|---:|---:|---:|
| S1 baseline → improved | 5ms → 4ms | 6ms → 6ms | 7ms → 6ms | 0.83% → 0.83% |
| S2 baseline → improved | 34ms → 14ms | 83ms → 70ms | 101ms → 98ms | 0.83% → 0.85% |
| S3 baseline → improved | 47ms → 295ms | 85ms → 1426ms | 130ms → 1559ms | 38.03% → 31.00% |

## 7. 결과 분석

### 7.1. S1 — 기본 부하

S1은 100명의 클라이언트가 초당 1개의 메시지를 보내는 기본 부하 시나리오이다.

측정 결과 P50 latency는 5ms에서 4ms로 소폭 감소했고, P99 latency도 7ms에서 6ms로 줄었다. Loss rate는 0.83%로 동일하였다.

따라서 낮은 부하 상황에서는 기존 구조도 비교적 안정적으로 동작했으며, 개선 후에도 큰 변화보다는 소폭의 latency 감소가 확인되었다.

### 7.2. S2 — 500명 동시 접속 요구사항 검증

S2는 본 프로젝트의 핵심 요구사항인 500명 동시 접속, 클라이언트당 초당 1개 메시지 전송 시나리오이다.

이 구간에서 개선 효과가 가장 명확하게 나타났다.

```text
P50 latency: 34ms → 14ms
```

이는 중간값 기준 지연 시간이 절반 이하로 줄어든 결과이다.

`TCP_NODELAY` 적용으로 작은 채팅 메시지를 더 빠르게 전송할 수 있었고, 그 결과 일반적인 500명 부하 상황에서 latency가 개선된 것으로 볼 수 있다.

다만 loss rate는 0.83%에서 0.85%로 거의 동일하였다. 따라서 이번 개선은 메시지 손실률을 줄이는 개선이라기보다는, 500명 동시 접속 상황에서 일반적인 지연 시간을 줄이는 개선으로 보는 것이 적절하다.

### 7.3. S3 — 고부하 스트레스 테스트

S3는 500명의 클라이언트가 초당 10개의 메시지를 보내는 고부하 시나리오이다.

이 시나리오에서는 broadcast capacity 증가의 장단점이 명확하게 드러났다.

```text
Loss rate: 38.03% → 31.00%
```

즉, broadcast channel capacity를 1024에서 8192로 늘린 것이 고부하 상황에서 메시지 손실을 줄이는 데는 효과가 있었다.

하지만 latency는 크게 증가하였다.

```text
P50: 47ms → 295ms
P95: 85ms → 1426ms
P99: 130ms → 1559ms
```

이는 capacity가 커지면서 메시지가 바로 버려지지 않고 큐에 더 오래 머무르게 되었기 때문으로 볼 수 있다.

즉, capacity 증가는 loss rate를 줄이는 대신, queueing delay를 증가시켜 tail latency를 악화시켰다.

이 결과는 다음 trade-off를 보여준다.

```text
작은 capacity
    → Lagged client를 더 빨리 끊음
    → latency는 낮아질 수 있음
    → 대신 loss rate 증가 가능

큰 capacity
    → 메시지를 더 오래 보관
    → loss rate 감소 가능
    → 대신 queueing delay 증가
```

따라서 고부하 상황에서는 단순히 버퍼 크기를 키우는 것만으로는 충분하지 않고, slow consumer 정책을 더 정교하게 조정해야 한다.

## 8. GitHub 협업 흔적

본 프로젝트는 GitHub Pull Request 기반으로 기능 개발과 개선 작업을 진행하였다.

발표 자료 기준 주요 협업 결과는 다음과 같다.

- 총 20 commits
- 총 8 Pull Requests
- 총 8 merged Pull Requests
- 총 4 contributors

협업 과정에서 서버 구현, 프로토콜 검증, 최적화, 문서화가 병렬로 진행되었다.

특히 PR #5, PR #6, PR #4, PR #7이 동시에 머지되어야 하는 상황에서는 Slack을 통해 의존관계를 합의하고, 머지 순서를 조율하였다.

다만 Issue tracker를 적극적으로 사용하지 못한 점은 아쉬운 부분이다. 작업의 대부분이 PR description에만 남아 있어, 전체 진행 상황을 한눈에 파악하기 어려웠다.

## 9. 결론

본 프로젝트는 Rust와 Tokio를 활용하여 500명 동시 접속을 처리하는 멀티채팅 서버를 구현하였다.

서버는 Raw TCP 기반으로 동작하며, 4-byte length prefix를 통해 메시지 경계를 구분하고, serde_json을 통해 메시지를 직렬화하였다. 또한 blake3 hash를 이용해 메시지 무결성을 검증하였다.

동시성 측면에서는 다음 구조를 적용하였다.

- connection별 Reader / Writer task 분리
- `tokio::sync::broadcast` 기반 fan-out
- `AtomicU64` 기반 throughput counter
- `Lagged` 발생 시 slow consumer 연결 종료
- `Arc<Bytes>` 기반 메시지 사전 직렬화 공유

성능 측정 결과, 500명 / 1 msg/s 시나리오인 S2에서 P50 latency가 34ms에서 14ms로 감소하였다. 이를 통해 `TCP_NODELAY` 적용이 일반적인 500명 부하 상황에서 latency 개선에 효과가 있음을 확인하였다.

반면 500명 / 10 msg/s 고부하 시나리오인 S3에서는 broadcast capacity 증가로 loss rate가 38.03%에서 31.00%로 감소했지만, P99 latency는 130ms에서 1559ms로 크게 증가하였다.

따라서 본 프로젝트는 500명 동시 접속 멀티채팅 서버 구현이라는 기본 목표를 달성했으며, 동시에 고부하 상황에서 loss rate와 tail latency 사이의 trade-off를 확인하였다.

## 10. 회고 및 향후 개선 방향

### 10.1. 잘한 점

첫째, 단순 기능 구현에 그치지 않고 `loadtest` crate를 통해 실제 부하 테스트를 수행하였다.

둘째, 서버 구조를 `Mutex<HashMap>` 중심이 아니라 broadcast channel 중심으로 설계하여 lock 경합을 줄였다.

셋째, PR #6의 `Arc<Bytes>` 최적화와 PR #9의 `TCP_NODELAY`, broadcast capacity 조정을 통해 측정 기반 개선을 진행하였다.

넷째, Cargo Workspace를 사용하여 protocol, server, client, loadtest의 책임을 분리하였다.

### 10.2. 아쉬운 점

첫째, Issue tracker를 거의 사용하지 않아 작업 추적성이 부족했다. 다음 프로젝트에서는 Issue를 먼저 만들고, PR을 해당 Issue와 연결하는 방식이 필요하다.

둘째, S4 장시간 테스트는 PR #9 기준으로 아직 별도 측정 항목으로 남아 있다. 단기 성능은 확인했지만, 장시간 운영 안정성은 추가 검증이 필요하다.

셋째, S3 고부하 상황에서 tail latency가 크게 증가하였다. 이는 capacity 증가만으로는 고부하 문제를 해결할 수 없다는 것을 보여준다.

### 10.3. 향후 개선 방향

향후 개선 방향은 다음과 같다.

- Issue → PR 연결 규칙 적용
- CI에 `cargo clippy -D warnings` 추가
- CI에 간단한 loadtest smoke test 추가
- S4 10분 burn-in 테스트 수행
- flamegraph 기반 병목 분석
- slow consumer 정책 개선
- write-side backpressure 적용 검토
- Lagged disconnect 임계값 조정

최종적으로 본 프로젝트를 통해 Rust의 소유권 모델, Tokio의 비동기 task 구조, broadcast channel 기반 fan-out, atomic counter, 부하 테스트 자동화 과정을 경험할 수 있었다.

특히 500명 동시 접속이라는 목표를 단순히 코드로만 구현한 것이 아니라, 실제 측정 결과를 기반으로 latency와 loss rate를 분석했다는 점에서 의미가 있다.

## 참고 자료

- 프로젝트 저장소: https://github.com/uiojkl-123/multi-chat
- 성능 측정 PR: https://github.com/uiojkl-123/multi-chat/pull/9

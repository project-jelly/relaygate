# SPEC 002: SDK와 Pipe 계약

## API

| 호출 | 결과 |
| --- | --- |
| `Relay::connect(Config)` | active Relay |
| `Relay::listen(Destination, AccessTokenSource)` | active Listener |
| `Relay::dial(Destination, AccessTokenSource)` | established Pipe |
| `Relay::status()` / `Relay::subscribe_status()` | latest Relay status snapshot/subscription |
| `Relay::wait_ready()` | current Relay session active 또는 closed error |
| `Listener::accept()` | distinct incoming Pipe |
| `Listener::status()` / `Listener::subscribe_status()` | latest Listener status snapshot/subscription |
| `Listener::close()` | 해당 Listener 종료 |
| `Relay::close()` | 전체 SDK runtime 종료 |

| 설정 | 동작 |
| --- | --- |
| `Config` | Gateway transport, timeout, heartbeat, reconnect, `ResourceLimits` 지정 |
| `Config::new("host:port")` / `tls://host:port` | 공인 CA, endpoint 이름·SNI, `relaygate/3` ALPN 검증 |
| `Config::new("tcp://host:port")` | 명시적 평문; TLS 실패 후 fallback 없음 |
| `Config::with_ca_certificate` | 사설 CA 지정; 평문 endpoint에는 사용 불가 |
| `Config::with_transport` | 명시적 transport 사용; CA·검증 이름·client 인증은 `ClientTlsConfig`가 설정 |
| IPv6 endpoint | `[::1]:port` 형식 |

Application은 config source, Agent 풀과 작업 분배 정책을 소유합니다. `HELLO/WELCOME`에는 credential이 없습니다.

### SDK resource hierarchy

```text
Relay
├── live Pipe 20,000
├── buffered inbound bytes 64 MiB
└── Listener 0..N
    ├── pending Pipe 64
    └── live Pipe 10,000
        └── Pipe
            ├── buffered inbound frame 64
            └── buffered inbound bytes 1 MiB
```

| 범위 | 규칙 |
| --- | --- |
| `ResourceLimits::default()` | process-local 보호 상한; 처리량 보장 아님 |
| `Config::with_resource_limits` | 모든 값 양수; Listener live ≤ Relay live, Pipe bytes ≤ Relay bytes |
| incoming pending Pipe | pending·live slot 동시 점유; 작은 상한이 queue를 제한 |
| Gateway admission | 비신뢰 client에 대한 cluster-side 권위 상한 |

## AccessTokenSource

```text
static token -----------------------> PUBLISH 또는 DIAL
async callback(action, Destination) -> PUBLISH 또는 DIAL
```

| ID | 계약 |
| --- | --- |
| `SDK-001` | `Relay::connect`는 지정 transport와 credential-free `HELLO/WELCOME` 완료 뒤 반환한다. |
| `SDK-002` | network loss, heartbeat timeout과 bounded writer failure는 current session을 끝낸다. |
| `SDK-003` | 실행 중 session loss는 bounded exponential backoff와 runtime별 jitter로 재연결한다. |
| `SDK-004` | 새 session은 이미 반환된 live Listener를 새 AccessToken으로 자동 republish한다. |
| `SDK-005` | recovery는 새 session·Binding을 만든다. existing Pipe, committed dial, payload와 PUBLISH가 commit된 initial listen은 terminal이다. PUBLISH pre-commit initial listen은 원래 deadline 안에서 재시도한다. |
| `SDK-006` | 초기 config·transport·handshake 실패는 `Relay::connect`의 `Err`다. Gateway의 `SESSION_REJECTED`(drain 중 `UNAVAILABLE`)도 같은 `Err`다. 실행 중 session·protocol·transport failure는 current session을 끝내고 bounded backoff 재연결로 수렴한다. |
| `SDK-007` | explicit close는 같은 runtime의 terminal `CLOSED`로 수렴한다. |
| `SDK-014` | AccessToken은 비어 있지 않은 최대 4,096 bytes이고 Debug 출력은 값을 redaction한다. |
| `SDK-015` | dynamic AccessTokenSource는 `AccessAction`과 exact Destination을 받아 application-owned future를 실행한다. |
| `SDK-016` | Listener는 AccessTokenSource를 보관하고 initial publish와 republish마다 다시 호출한다. |
| `SDK-017` | dial은 API 호출당 AccessTokenSource를 정확히 한 번 resolve하며 committed operation을 SDK가 replay하지 않는다. |
| `SDK-018` | SDK runtime은 token cache, singleflight, refresh token, private key와 token issuer를 소유하지 않는다. Backend가 필요하면 `relaygate-token-issuer`로 AccessToken을 생성해 `AccessTokenSource`에 공급한다. |
| `SDK-019` | returned Listener의 republish token source 실패는 Relay당 하나의 bounded exponential backoff+jitter timer로 병합한다. timer가 준비되기 전 다른 reconcile trigger는 suspended Listener를 재시도하지 않는다. 전체 Listener가 다시 active이면 backoff를 초기화하고 대기 중 timer를 무효화한다. |
| `SDK-020` | Relay live Pipe 상한은 outgoing DIAL의 pending 단계부터 returned Pipe 수명까지와 incoming Pipe를 함께 계산하고 모든 실패·cancel·drop·terminal 경로에서 점유를 반환한다. |
| `SDK-022` | Relay와 Listener status subscription은 SDK 소유 wrapper이며 raw watch channel을 노출하지 않는다. `current()`는 latest snapshot을 반환하고 subscription cursor를 소비하며, `changed()`는 그 이후 coalescing된 latest state를 반환한다. Relay `ACTIVE`는 current `HELLO/WELCOME` transport session 설치를 뜻하며 Listener republish/`BLOCKED`와 분리된다. Relay `CLOSED`는 terminal이고 `ACTIVE`로 역행하지 않는다. |

| 상황 | 결과 |
| --- | --- |
| token source 실패·deadline | 해당 operation만 `UNAVAILABLE` 또는 `DEADLINE_EXCEEDED/NOT_OBSERVED` |
| initial PUBLISH pre-commit session 종료 | 원래 deadline 안에서 재시도 |
| initial PUBLISH post-commit session 종료 | `MAYBE_OBSERVED` 오류 |
| Gateway의 initial PUBLISH 실패 응답 | `Relay::listen`의 `Err` |
| returned Listener의 republish token source 실패 | `SUSPENDED`; bounded delay 뒤 재공급 요청 |
| returned Listener의 영구적 PUBLISH 실패 | `BLOCKED`; `INVALID_ARGUMENT`, `UNAUTHENTICATED`, `PERMISSION_DENIED`, `FAILED_PRECONDITION`, `ALREADY_EXISTS` |
| reconnect episode 종료 | 모든 returned Listener가 `ACTIVE` 또는 `BLOCKED`로 settled |
| episode 중 `BLOCKED` 발생 | Relay session이 `ACTIVE`이거나 Listener가 즉시 drop되어도 outcome은 `degraded` |

`BLOCKED` 복구는 application이 새 token source 또는 Relay/Listener를 구성합니다.

## Relay runtime

Relay 상태와 전이는 [SPEC 007](007-error-and-state-model.md#sdk와-binding-상태)이 소유합니다.

## Listener

Listener 상태와 전이는 [SPEC 007](007-error-and-state-model.md#sdk와-binding-상태)이 소유합니다.

| ID | 계약 |
| --- | --- |
| `SDK-008` | `listen`은 Gateway가 Binding을 확인한 뒤 Listener를 반환한다. |
| `SDK-009` | incoming OFFER는 terminal queue compaction 뒤 Listener별 bounded queue에 즉시 admission할 수 있을 때만 성공한다. queue 포화는 session frame loop를 기다리게 하지 않는다. |
| `SDK-010` | `accept`는 distinct Pipe를 정확히 한 번 반환한다. |
| `SDK-011` | session 종료는 old unaccepted Pipe를 제거한다. |
| `SDK-012` | Listener close는 신규 수신과 unaccepted Pipe를 끝내고 returned Pipe는 독립 유지한다. |
| `SDK-013` | 같은 Relay의 동일 Destination 중복 listen은 `ALREADY_EXISTS`다. |
| `SDK-021` | Listener pending/live Pipe 상한 초과는 해당 OFFER/DIAL만 즉시 `RESOURCE_EXHAUSTED`로 끝내고 Relay, Listener, 기존 Pipe와 sibling Listener를 유지한다. |

## Pipe

Pipe 상태와 전이는 [SPEC 007](007-error-and-state-model.md#authorization과-pipe-상태)이 소유합니다.

| ID | 계약 |
| --- | --- |
| `PIPE-001` | Pipe는 full-duplex opaque byte stream이다. |
| `PIPE-002` | `FIN`은 한 방향 write half-close이고 반대 방향은 계속 사용한다. |
| `PIPE-003` | `CLOSE`는 정상 종료, `RESET`은 오류 종료다. |
| `PIPE-004` | frame, queue와 buffer는 모두 bounded다. |
| `PIPE-005` | Pipe terminal cleanup은 해당 Pipe에 한정되고 sibling Pipe, Listener와 Binding은 유지된다. |
| `PIPE-006` | Pipe I/O success는 RelayGate byte path의 성공이며 application acknowledgement는 application protocol이 정의한다. |
| `PIPE-007` | inbound DATA는 Pipe별 frame·byte 상한과 Relay 전체 byte 상한을 함께 예약한다. 부분 읽기 중인 frame도 전부 소비될 때까지 점유한다. 초과는 해당 Pipe만 `RESET(RESOURCE_EXHAUSTED)`하고 읽기·drop·terminal cleanup은 점유를 반환한다. |

# TEST 001: requirement와 실행 증거 대응표

```text
SPEC requirement -> Test ID -> exact cargo test name
```

| Test ID | Requirement | 검증 |
| --- | --- | --- |
| `T-MODEL-01` | `TERM-001`, `TERM-002`, `TERM-003`, `TERM-004`, `TERM-005`, `TERM-006`, `TERM-007`, `TERM-008`, `TERM-009`, `TERM-010`, `TERM-011`, `TERM-012` | 대칭 Relay, canonical Destination, exact routing, N:M Binding과 1:1 Pipe cardinality |
| `T-SDK-01` | `SDK-001`, `SDK-002`, `SDK-003`, `SDK-004`, `SDK-005`, `SDK-006`, `SDK-007`, `SDK-019`, `SDK-022`, `SDK-023`, `SDK-024`, `SDK-025`, `SDK-026`, `SDK-027`, `SDK-028` | 초기 연결·token 공급의 deadline 내 자동 복구와 cancel, 공급자의 인증·권한 실패 즉시 반환과 Listener BLOCKED·sibling 격리·수동 복구, heartbeat, bounded reconnect·republish, shared republish retry, status·마지막 오류·발생 위치 관측과 복구 시 초기화, pre-commit listen 재시도와 committed operation·old Pipe no replay |
| `T-SDK-02` | `SDK-008`, `SDK-009`, `SDK-010`, `SDK-011`, `SDK-012`, `SDK-013`, `SDK-020`, `SDK-021` | listen/accept/close, Listener·Relay Pipe 상한과 회수, 초과 DIAL 격리, Relay-local 중복 Listener |
| `T-AUTH-01` | `SDK-014`, `SDK-015`, `SDK-016`, `SDK-017`, `SDK-018`, `AUTH-001`, `AUTH-002`, `AUTH-003`, `AUTH-004`, `AUTH-005`, `AUTH-006`, `AUTH-007`, `AUTH-008`, `AUTH-009`, `AUTH-010`, `AUTH-011`, `AUTH-012`, `AUTH-013`, `AUTH-014`, `AUTH-015`, `AUTH-016` | static/dynamic token source, token issuer helper, custom `typ` equivalence, `crit` fail-closed, ES256·kid·claim·Exact/Subtree/All, Namespace 격리, bounded verifier, response correlation·redaction과 admission-only state 격리 |
| `T-PIPE-01` | `PIPE-001`, `PIPE-002`, `PIPE-003`, `PIPE-004`, `PIPE-005`, `PIPE-006`, `PIPE-007` | full-duplex, FIN/CLOSE/RESET, frame·byte backpressure, 자원 회수와 sibling 격리 |
| `T-BIND-01` | `BIND-001`, `BIND-002`, `BIND-003`, `BIND-004`, `BIND-005`, `BIND-006`, `BIND-007`, `BIND-008`, `BIND-009`, `BIND-010` | hierarchical destination, live-only Binding index와 session cleanup |
| `T-RT-01` | `RT-001`, `RT-002`, `RT-003`, `RT-004`, `RT-005`, `RT-006`, `RT-007`, `RT-008`, `RT-009`, `RT-010`, `RT-011`, `RT-012`, `RT-013` | Destination shard authority, lease/revision, expiry/restart/재수렴, bounded memory |
| `T-DIAL-01` | `DIAL-001`, `DIAL-002`, `DIAL-003`, `DIAL-004`, `DIAL-005`, `DIAL-006`, `DIAL-007`, `DIAL-008`, `DIAL-009`, `DIAL-010`, `DIAL-011`, `DIAL-012` | authorized local/remote dial, self exclusion, 단일 선택, timeout/cancel/observation과 bounded admission |
| `T-PEER-01` | `PEER-001`, `PEER-002`, `PEER-003`, `PEER-004`, `PEER-005`, `PEER-006`, `PEER-007`, `PEER-008`, `PEER-009`, `PEER-010`, `PEER-011`, `PEER-012` | token-free one-hop multiplexing, direction arbitration, heartbeat/idle/terminal cleanup |
| `T-STATE-01` | `STATE-001`, `STATE-002`, `STATE-003`, `STATE-004`, `STATE-005`, `STATE-006`, `STATE-007`, `STATE-008`, `STATE-009`, `STATE-010` | terminal no-resurrection, owner-scoped cleanup, RT 독립, status 관측, admission·SDK resource 격리와 idempotent convergence |
| `T-SEC-01` | `SEC-001`, `SEC-002`, `SEC-003`, `SEC-004`, `SEC-005`, `SEC-006`, `SEC-007`, `SEC-008`, `SEC-009`, `SEC-010`, `SEC-011` | SDK TLS/TCP, credential-free HELLO, server name/ALPN, 내부 mTLS 역할 SAN·신뢰 CA, plaintext, Secret 분리, L4 passthrough |
| `T-SEC-02` | `SEC-012`, `SEC-013` | TLS 전 handshake 상한, zero-payload HELLO, stalled read/write deadline, slot 회수, pipelined frame 보존 |
| `T-SEC-03` | `SEC-014` | burst·fractional refill·유휴 상한, clone 공유, TLS 전 rate 거절·기존 session 유지·회복, env 검증과 metric |
| `T-SEC-04` | `SEC-015`, `STATE-008` | PUBLISH/DIAL 예산, RT·crypto 전 거절, ID fence, bounded rejection response와 sibling 격리 |
| `T-OBS-01` | `OBS-001`, `OBS-002`, `OBS-003`, `OBS-004`, `OBS-005`, `OBS-006`, `OBS-007`, `OBS-008`, `OBS-009`, `OBS-010`, `OBS-011`, `OBS-012`, `OBS-013`, `OBS-014` | health·RED/USE·authorization latency·SDK resource·cleanup·redaction; PromQL과 DATA probe 증거는 [TEST 006](006-local-observability-test-plan.md) |

모든 requirement는 위 표와 [실행 증거 인덱스](001-executable-coverage.toml)에 연결됩니다. 인덱스는 exact
Rust test 이름의 존재를 검증합니다. [TEST 004](004-rt2-gw3-closed-loop-test-plan.md)는 L4 passthrough와
rolling/fault runtime acceptance를, TEST 006은 PromQL과 DATA probe를 검증합니다.

## Authorization·admission 검증 경계

| 계층 | 검증 내용 | 증거 범위 |
| --- | --- | --- |
| Destination unit | grammar·byte bound·canonical key·whole-label descendant | exact routing key와 scope 기반 자료구조 |
| JWT unit | JWS `typ`·`crit`·alg·signature·kid, closed claims, issuer·audience·nbf·exp, action·Namespace·Exact/Subtree/All | [SPEC 009](../spec/009-operation-jwt-authorization-contract.md)의 static public-key custom profile; OAuth 2.0·RFC 9068 아님 |
| Config unit | version 1, public ES256 JWK, Namespace당 issuer 하나, key 1..2, verifier bounds | startup parse와 fail-closed |
| GatewayState unit | drain/rate/ConnectionId fence, auth failure state 격리 | crypto 이전·이후 commit 경계 |
| SDK unit | bounded/redacted token, callback request, supply 횟수 | cache·refresh·issuer 없음 |
| SDK–GW integration | invalid token operation만 거절, session·sibling state 유지, TLS 선행 | 실제 socket/wire 경로 |
| TokenBucket unit | rate/burst·refill·clone 공유 | 결정적 schedule |
| Kind | valid/invalid authorization과 local/one-hop Pipe, rolling/fault cleanup | 배포 wiring과 runtime acceptance |

테스트는 제한된 입력·사건 순서를 증명합니다. 무제한 공격 트래픽, 외부 issuer availability, private-key 보관,
token 발급·refresh·revocation과 application payload authorization은 RelayGate 실행 증거 범위 밖입니다.

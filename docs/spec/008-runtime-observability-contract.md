# SPEC 008: 관측 계약

Log·metric·probe와 수집 해석을 소유합니다. 전송 보안, admission 보호 상한과 환경변수는
[SPEC 010](010-transport-and-admission-contract.md)이 소유합니다.

## Operation authorization

```text
PUBLISH(Destination, AccessToken) --+
                                     +--> SPEC 009 verification --> state operation
DIAL(Destination, AccessToken) -----+                |
                                                      `-- raw token drop
```

[SPEC 009](009-operation-jwt-authorization-contract.md)가 custom JWT profile, protected header, claims JSON
Schema, static JWK trust, permission, verification ordering, response와 state 영향을 소유합니다. 이 문서는 그 결과를
관측하는 경계만 소유합니다.

Authorization은 `PUBLISH`와 `DIAL`의 admission 단계입니다. Raw token, decoded claim과 permission은
log·metric label 또는 error body에 기록하지 않습니다. 인증 성공 자체에는 별도 ACK가 없으며 이후 operation의
기존 `Published/PublishFailed` 또는 `Opened/DialFailed` 흐름을 관측합니다. 인증 실패는 해당 operation만 끝내고
existing session·Binding·Pipe를 유지합니다.

## 로그와 metric

Lifecycle log는 `debug`가 기본이며, `info`에서는 drain과 `warn` 이상 사건만 기록합니다.

| category | lifecycle event |
| --- | --- |
| session | rejected, removed |
| Listener | active, suspended, blocked, closed |
| authorization | metric 전용: operation, terminal outcome, stable code |
| dial | result, code, observation |
| dependency | peer/RT connect, handshake, loss, recovery |
| shutdown | drain start, deadline, complete |
| protection | TLS, queue, capacity rejection |

Lifecycle field는 bounded identifier와 outcome을 사용합니다. AccessToken, decoded claim, credential, private key,
payload와 free-form error body는 redaction합니다. DATA RTT와 payload goodput은 명시적으로 실행한 SDK probe가
측정합니다.

| 운영 질문 | metric | 판정 |
| --- | --- | --- |
| process scrape | Prometheus `up` | process/endpoint reachability |
| SDK admission | `relaygate_gateway_sdk_admission_ready`, `relaygate_gateway_draining` | non-draining + transport·handshake capacity + rate budget 여유 |
| SDK handshake 포화 | `relaygate_gateway_resource_used{resource="sdk_handshakes"}`, `relaygate_gateway_resource_limit{resource="sdk_handshakes"}` | TLS/HELLO 진행 수·상한 |
| SDK transport 거절 | `relaygate_gateway_sdk_transport_rejections_total{reason}` | `rate_limit|session_limit|handshake_limit` |
| operation authorization | `relaygate_gateway_authorization_results_total{operation,outcome,code}` | `publish|dial`의 terminal verification result |
| authorization latency | `relaygate_gateway_authorization_duration_seconds{operation,outcome}` | bounded verification duration |
| SDK 제어 요청 거절 | `relaygate_gateway_control_rejections_total{operation,scope}` | `publish|dial` x `session|gateway` |
| RT dependency | `relaygate_gateway_route_dependency{state}` | `DISABLED|READY|DEGRADED|TERMINAL` one-hot. 전이 counter는 별도 값 `starting|ready|degraded|terminal`을 사용(`DISABLED` 없음) |
| RT convergence | `relaygate_gateway_route_registrations_unsynced` | pending registration 수 |
| peer state | `relaygate_gateway_peer_transports_connecting`, `relaygate_gateway_peer_transports_ready` | connecting·reusable transport 수 |
| liveness failure | `relaygate_gateway_heartbeat_timeouts_total{transport}` | SDK/peer timeout 누계 |
| SDK process 자원 | `relaygate_sdk_resource_used/limit{resource}` | `live_pipes|buffered_bytes` 현재 점유·설정 상한 |
| SDK process 포화 | `relaygate_sdk_resource_rejections_total{resource}` | `listener_pending_pipes|listener_live_pipes|relay_live_pipes|pipe_buffered_frames|pipe_buffered_bytes|relay_buffered_bytes` 상한 거절 누계 |

### RED와 latency

| 구간 | metric | 측정 경계 |
| --- | --- | --- |
| GW DIAL | `relaygate_gateway_dial_requests_total`, `relaygate_gateway_dial_results_total`, `relaygate_gateway_dial_duration_seconds` | precheck 진입 -> OPENED/failure/cancel |
| SDK 접속·DIAL | `relaygate_sdk_operation_results_total`, `relaygate_sdk_operation_duration_seconds` | `session_connect`: transport·TLS·HELLO/WELCOME; `dial`: API 진입 -> Pipe/실패/cancel |
| publish | `relaygate_gateway_publish_results_total` | terminal result counter |
| authorization | `relaygate_gateway_authorization_results_total`, `relaygate_gateway_authorization_duration_seconds` | verifier start -> success/failure |
| heartbeat | timeout counter + `relaygate_gateway_heartbeat_duration_seconds{transport}` | committed PING -> matching PONG |
| Gateway->RT | `relaygate_gateway_route_table_requests_total`, `relaygate_gateway_route_table_request_duration_seconds` | client queue admission -> response/failure |
| RT actor | `relaygate_route_table_requests_total`, `relaygate_route_table_request_duration_seconds` | actor service start -> result |
| peer | `relaygate_gateway_peer_handshakes_total`, `relaygate_gateway_peer_transport_closed_total` | transport lifecycle outcome |
| GW->RT 연결 | `relaygate_gateway_route_connection_attempts_total{outcome,code}` | connect·handshake 시도 결과 |
| RT dependency 전이 | `relaygate_gateway_route_dependency_transitions_total{previous,current}`, `relaygate_gateway_route_recovery_duration_seconds` | `starting|ready|degraded|terminal` 전이; degraded 진입 -> ready 복귀 |
| RT handshake | `relaygate_route_table_handshakes_total{outcome,code}` | RT가 수락한 Gateway connection handshake 결과. connection 상한 거절(`resource_exhausted`) 포함 |
| SDK reconnect | `relaygate_sdk_reconnect_attempts_total{outcome}`, `relaygate_sdk_reconnect_episodes_total{outcome}`, `relaygate_sdk_reconnect_episode_duration_seconds{outcome}` | episode start -> `recovered|degraded|closed|aborted`; attempt `outcome`은 `success|error` |
| SDK 복구 시간 | `relaygate_sdk_reconnect_duration_seconds` | episode start -> `recovered|degraded`만 기록. 전체 terminal outcome 분포는 `episode_duration_seconds{outcome}` |
| SDK 미복구 | `relaygate_sdk_reconnect_in_progress` | process 내 진행 중 episode 수. `recovered|degraded|closed|aborted` terminal outcome 뒤 0으로 수렴 |

```text
SDK session_connect -> token source -> SDK dial -> established Pipe DATA RTT
                                      `-> GW precheck -> authorization -> local/RT resolve -> OFFER
```

GW->RT와 RT actor histogram은 표본 경계가 다릅니다. 두 p95 차이를 network p95로 해석하지 않습니다.
Heartbeat는 liveness RTT, DATA RTT는 payload 왕복입니다. SDK dial 시간은 token source와 session·queue 대기를
포함합니다.

| DIAL `class` | code |
| --- | --- |
| `success` | `ok` |
| `request` | `invalid_argument`, `unauthenticated`, `permission_denied`, `not_found`, `failed_precondition`, `protocol_error`, `already_exists` |
| `capacity` | `resource_exhausted` |
| `availability` | `unavailable`, `deadline_exceeded` |
| `internal` | `internal` |
| `cancelled` | `cancelled` |

### USE와 current state

| 영역 | 관측값 |
| --- | --- |
| Gateway | `relaygate_gateway_sessions`, `relaygate_gateway_bindings`, `relaygate_gateway_pending_offers`, `relaygate_gateway_live_pipes`(GW-local Pipe state; remote Pipe는 양쪽 GW에 존재), `relaygate_gateway_remote_dial_attempts` |
| 고유 Pipe | `relaygate_gateway_originated_pipes`: 호출 SDK의 GW에서 한 번 집계 |
| Peer | `relaygate_gateway_peer_transports_connecting`, `relaygate_gateway_peer_transports_ready`, `relaygate_gateway_peer_streams`; one-hop 양단 포함 |
| Capacity | `relaygate_gateway_resource_used/limit{resource}`: 현재 점유 / 설정 상한 |
| RT 수렴 | `relaygate_gateway_route_registrations_synced`, `relaygate_gateway_route_registrations_unsynced`: session-shard registration 수 |
| RouteTable | `relaygate_route_table_registrations`, `relaygate_route_table_bindings`, `relaygate_route_table_destinations`, `relaygate_route_table_expiry_records`, `relaygate_route_table_expired_registrations_total` |
| Saturation | `relaygate_gateway_writer_queue_rejections_total{reason}`(`full|closed|timeout`), control·authorization rejection, `RESOURCE_EXHAUSTED` result |
| Recovery | reconnect 진행 개수·종료 시간, dependency transition, lease expiry, drain |

| `resource` | used | limit |
| --- | --- | --- |
| `sessions` | handshake 포함 SDK transport semaphore 점유 | max sessions |
| `sdk_handshakes` | TLS/HELLO 진행 점유 | max pending handshakes |
| `bindings` | local bindings | max bindings |
| `pending_opens` | pending offers + remote attempts | max pending offers |
| `remote_dials` | remote attempts | max remote dial attempts |
| `pipes` | GW-local open Pipe states | max live Pipes |

Authorization concurrency는 authorization result의 `resource_exhausted`와 duration으로 관측하며 raw token·Namespace·
Destination을 metric label로 사용하지 않습니다. 설정 상한은 지속 가능한 처리량이 아닙니다. Snapshot은 GW별
순간 관측이고 cluster 합계는 전역 원자적 값이 아닙니다.

Metric label set은 `operation`, `outcome`, `code`, `class`, `reason`, `scope`, `resource`, `state`, `direction`,
`transport`, `previous`, `current` 같은 bounded enumeration입니다. Instance identity는 Prometheus target metadata, request identity는
lifecycle log가 담당합니다.

## 수집과 해석

```text
운영 개요: 준비율 · 세션/Pipe -> 요청 · 지연 · 결과 · 용량
  |-- GW·RT 진단: 연결/큐 -> 라우팅/수렴 -> Kubernetes
  `-- SDK 복구: 진행 중 재연결 <-> 종료 시간, 접속/DIAL
```

| 대상 | 수집 계약 |
| --- | --- |
| GW·RT | Prometheus target의 `cluster`, `namespace`, `instance` label 유지 |
| SDK | application이 recorder와 scrape endpoint, 배포 범위 target label 설치 |
| Kubernetes | cAdvisor CPU·throttling·working set·RSS·network와 kube-state-metrics limit·replica |
| 미수집 | No data로 표현; `up`은 발견 target, desired replica는 별도 비교 |
| 메모리 | logical baseline 복귀와 반복 부하 뒤 working set/RSS 추세 함께 관측 |
| 네트워크 | Pod RX/TX와 DATA probe echo payload goodput 구분 |

화면 구성은 [Grafana dashboard JSON](../../monitoring/grafana/dashboards/)에,
화면·쿼리 검증은 [TEST 006](../test/006-local-observability-test-plan.md)에 있습니다.

## Probe

| probe | 판정 범위 | 상위 검증 |
| --- | --- | --- |
| startup/readiness `check` | TLS + credential-free `HELLO/WELCOME` | topology test가 authorization·RT·Destination·Pipe 검증 |
| liveness TCP | process socket reachability | health metric이 control/data plane 구분 |
| topology test | authorization/local/one-hop/dial/Pipe byte | application test가 업무 성공 검증 |
| Pipe latency probe | 고정 workload의 established Pipe RTT | application benchmark가 실제 payload 특성 검증 |

| ID | 계약 |
| --- | --- |
| `OBS-001` | snapshot gauge는 current value를 기록한다. |
| `OBS-002` | cleanup 뒤 gauge는 baseline으로 수렴한다. |
| `OBS-003` | access token·private key·payload marker의 log·metric·error 출현은 0이다. |
| `OBS-004` | Helm scrape surface는 metrics endpoint로 한정된다. |
| `OBS-005` | process, SDK transport admission, operation authorization, RT dependency와 peer health를 독립 지표로 관측한다. |
| `OBS-006` | authorization, DIAL, GW->RT, RT actor와 heartbeat는 각 측정 경계의 histogram을 가진다. |
| `OBS-007` | SDK/peer heartbeat timeout은 bounded `transport` label counter다. |
| `OBS-008` | DATA RTT는 explicit established-Pipe latency probe가 측정한다. |
| `OBS-009` | SDK admission ready는 non-draining, transport·handshake slot 여유와 rate budget 여유의 conjunction이다. |
| `OBS-010` | Dashboard runtime selector는 cluster·namespace 범위를 일관되게 적용한다. |
| `OBS-011` | 고유 Pipe와 GW-local Pipe state를 구분하며 resource used/limit 집계 기준을 일치시킨다. |
| `OBS-012` | SDK 계측은 error·polled future cancel·reconnect 미완료와 종료를 구분한다. |
| `OBS-013` | 결과 분류와 gauge/rate 단위를 유지하고 실제 PromQL 기대값으로 검증한다. |
| `OBS-014` | SDK live Pipe·buffered byte 점유는 cleanup 뒤 기준값으로 수렴하고 resource rejection은 bounded `resource` label로 구분한다. |

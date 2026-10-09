# CLAUDE.md

## 작업 원칙

- 변경 전 관련 코드와 ADR/SPEC을 읽고 가정을 밝힌다.
- 현재 요구에 필요한 최소 구현을 선택하고 speculative abstraction을 만들지 않는다.
- 기존 worktree 변경을 보존하고, 수정 범위를 벗어난 파일을 정리하지 않는다.
- 의미가 달라지는 해석이 여럿이면 임의로 결정하지 않는다.

## 프로젝트 경계

- Production runtime과 public SDK는 Rust workspace가 소유한다.
- 루트에 단일 `src/`를 두지 않고 `crates/` 아래의 책임별 crate가 각자 `src/`와 `tests/`를 소유한다.
- `relaygate-server`는 process boot, config, observation, shutdown과 dependency wiring만 소유한다.
- `relaygate-gateway`는 대칭 Relay session, local Binding 소유권과 registration snapshot commit, dial admission과 Resolve 요청, Pipe relay와 cleanup을 소유한다. RT registration·KeepAlive·Resolve 수행은 `relaygate-gateway-routing`에 위임한다.
- `relaygate-gateway-peer`는 Gateway 간 one-hop PeerTransport, handshake, stream 다중화와 peer wire codec을 소유하는 workspace-internal crate이며 Gateway session state를 알지 못한다. Gateway가 `PeerHandle`로 구동하고 `PeerEvents`를 소비한다.
- `relaygate-gateway-routing`은 local Binding을 shard별 BindingSnapshot으로 projection해 RT registration·KeepAlive·Resolve를 수행하는 workspace-internal crate이며 local Binding 소유권과 Resolve 결과 cache를 갖지 않는다.
- `relaygate-sdk`는 public `Relay`, `Listener`, `Pipe` API와 managed reconnect·Listener republish를 소유하며 Gateway state type을 노출하지 않는다.
- `relaygate-protocol`은 SDK–Gateway wire contract와 Gateway 내부 crate가 함께 쓰는 `SessionId`·`BindingId`·`ErrorCode`·`PeerObservation` 어휘만 소유하며 socket, session policy와 routing state를 소유하지 않는다. SDK 의존성으로 crates.io에 배포되지만 application용 안정 API가 아니다.
- `relaygate-destination`은 `Namespace/DestinationName` 문법과 검증만, `relaygate-transport`는 TLS/mTLS handshake adapter만, `relaygate-token-issuer`는 application backend용 server-side ES256 JWT 발급 helper만 소유한다.
- Gateway와 SDK는 runtime dependency로 서로 직접 의존하지 않고(test dev-dependency 제외) `relaygate-protocol`, `relaygate-destination`, `relaygate-transport`만 공유한다.
- `relaygate-route-table`은 synchronous memory-only current-state core를, `relaygate-route-table-transport`는 bounded internal network/auth adapter를 소유하며 persistence를 포함하지 않는다.
- local-only mode는 Gateway 하나의 local Pipe 경로를 유지한다. distributed mode는 memory-only RouteTable과 one-hop peer relay를 사용하며 persistence를 포함하지 않는다.
- RelayGate는 payload를 opaque bytes로 취급하고 application 인증·인가, message 의미, delivery acknowledgement와 업무 retry를 소유하지 않는다.
- `Destination`은 application이 생성·보관하는 `Namespace/DestinationName` 라우팅 주소다. RelayGate는 중앙 발급·소유권 registry를 제공하지 않으며 routing은 exact match만 수행한다.
- SDK session 수립은 identity를 부여하지 않는다. `PUBLISH`와 `DIAL`은 operation마다 application이 공급한 JWT access token을 검증하며 raw token을 영속화하거나 RT·peer로 전달하지 않는다.
- Namespace 하나는 static ES256 issuer 하나에 대응하고 issuer는 회전용 공개키를 최대 두 개 가진다. private key·token 발급·갱신은 application/backend가 소유한다.
- SDK–Gateway TLS와 내부 mTLS가 기본값이다. 내부 plaintext는 명시적으로 선택하며 TLS 실패 시 평문 fallback을 제공하지 않는다. 인증서·trust 갱신 적용 정책은 platform이 소유하고 chart는 Secret mount와 범용 annotation을 전달한다.

## 문서

- `docs/` 아래 canonical 문서의 기본 언어는 한국어다. 코드 식별자, 프로토콜 메시지, 상태명은 구현과의 추적성을 위해 원문 표기를 유지할 수 있다.
- 장기 설계 결정은 `docs/adr/`, 상태와 동작 계약은 `docs/spec/`, 검증 계획은 `docs/test/`에 둔다.
- State/event 의미를 바꾸면 `SPEC 007`의 canonical table과 `TEST 001`의 대응 test를 함께 갱신한다.
- Accepted ADR의 의미가 바뀌면 기존 결정을 `Superseded`로 표시하고 새 ADR에 현재 결정을 기록한다.

## 검증

Rust workspace 변경:

```text
cargo fmt --all --check
cargo check --workspace
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

container 경로 변경:

```text
docker compose up --build --abort-on-container-exit --exit-code-from topology-probe
docker compose down --volumes --remove-orphans
```

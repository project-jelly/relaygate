# RelayGate 문서

`ADR`은 설계 결정, `SPEC`은 현재 계약, `TEST`는 실행 증거, `RFC`는 외부 표준의 근거입니다.

## 문서 지도

| 영역 | 문서 |
| --- | --- |
| 책임 경계 | [ADR 018](adr/018-operation-authorized-relay-boundary.md) |
| SDK·Destination·접근 | [ADR 002](adr/002-symmetric-relay-session.md), [ADR 015](adr/015-hierarchical-destination.md), [ADR 016](adr/016-per-operation-jwt-authorization.md), [ADR 017](adr/017-server-side-token-issuer-helper.md) |
| control·data plane | [ADR 005](adr/005-current-state-routing-topology.md), [ADR 019](adr/019-registration-snapshot-lifecycle.md), [ADR 007](adr/007-one-hop-peer-multiplexing.md) |
| 생존·운영 | [ADR 008](adr/008-transport-liveness-and-idle-retirement.md), [ADR 009](adr/009-operational-health-boundaries.md), [ADR 010](adr/010-bounded-gateway-drain-and-reconnect-jitter.md) |
| transport·certificate | [ADR 011](adr/011-sdk-transport-and-l4-boundary.md), [ADR 012](adr/012-public-edge-webpki-trust.md), [ADR 013](adr/013-cert-manager-internal-leaf-certificates.md), [ADR 014](adr/014-explicit-internal-transport-mode.md) |
| 현재 계약 | [SPEC](spec/) |
| 실행 증거 | [TEST 001](test/001-requirement-test-matrix.md) |
| 표준 근거 | [RFC](rfc/) |

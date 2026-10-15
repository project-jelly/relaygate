# SPEC 006: Gateway one-hop relay 계약

```text
GW A <===== PeerTransport A->B =====> GW B
         Stream 1 = Pipe 1
         Stream 2 = Pipe 2
         Stream N = Pipe N
```

| ID | 계약 |
| --- | --- |
| `PEER-001` | remote data path는 Entry Gateway → Owner Gateway 한 hop이다. |
| `PEER-002` | RT는 register·resolve control plane에 위치한다. |
| `PEER-003` | ordered Gateway pair의 PeerTransport는 방향별 0..1개다. |
| `PEER-004` | 두 방향 transport는 독립적이고 각각 여러 RelayStream을 multiplex한다. |
| `PEER-005` | StreamId는 Dialer/Acceptor bit와 방향별 monotonic counter로 유일하다. |
| `PEER-006` | stream FIN/CLOSE/RESET cleanup은 해당 stream에 한정된다. |
| `PEER-007` | writer commit failure와 transport loss는 해당 transport의 stream 전체를 terminal cleanup한다. |
| `PEER-008` | empty transport는 idle-retirement deadline에 정상 종료한다. |
| `PEER-009` | active transport heartbeat timeout은 transport와 소속 stream을 종료한다. 다른 OPEN deadline이 만료되어도 응답 대기 중인 PING의 nonce와 response deadline을 유지한다. |
| `PEER-010` | peer OPEN은 exact Destination, Binding과 origin open identity를 current state와 대조한다. |
| `PEER-011` | unknown·late·foreign frame은 current state를 유지하는 terminal/no-op 결과다. |
| `PEER-012` | peer connect, handshake, queue와 frame은 bounded다. |

PeerTransport는 다음 dial에서 재생성할 수 있는 availability optimization입니다. Established stream의
lifecycle은 해당 transport terminal 결과로 끝납니다.

Peer manager가 유효한 stream key로 수락한 `CLOSE`·`RESET`·`FAILED`를 transport command queue에
전달하지 못하면, 호출자가 응답을 계속 기다리는지와 무관하게 해당 transport를 닫아 `PEER-007` 범위로
정리합니다. queue에 정상 전달된 terminal command는 기존 stream 범위를 유지하며, `DATA`·`FIN`의
queue 거절은 이 강제 종료를 유발하지 않습니다.

Peer frame은 access token을 포함하지 않습니다. Entry Gateway의 operation authorization 결과는 선택된
Destination과 open identity로 축소되며 Owner Gateway는 current Binding을 다시 대조합니다.

## Peer wire format

`PEER-012`가 bounded라고 말하는 frame의 실제 형태입니다. 아래 layout이 계약이며 `relaygate-gateway-peer`
codec은 이 layout을 구현합니다. wire version은 codec 자체의 번호이고 TLS ALPN `relaygate/3`과는 무관합니다.

### Frame header (8 bytes, big-endian)

| offset | 크기 | 값 |
| --- | --- | --- |
| 0 | 2 | magic `GP` (불일치는 `InvalidMagic`으로 decode 실패) |
| 2 | 1 | version `3` (다른 값은 `UnsupportedVersion`으로 decode 실패) |
| 3 | 1 | frame kind |
| 4 | 4 | payload 길이(u32). peer frame payload 상한 1 MiB를 넘으면 buffer 확장 전에 거절 |

### Frame kinds

| kind | frame | payload |
| --- | --- | --- |
| 1 | `HELLO` | handshake |
| 2 | `WELCOME` | handshake |
| 3 | `HANDSHAKE_REJECTED` | `code`(u8) · `message`(string) |
| 4 | `OPEN` | `stream_id`(u64) · entry `gateway_id`(uuid) · origin `session_id`(uuid) · `connection_id`(u64) · Destination `namespace`(string) · Destination `name`(string) · owner `relay_session_id`(uuid) · `binding_id`(uuid) |
| 5 | `OPENED` | `stream_id`(u64) |
| 6 | `FAILED` | `stream_id`(u64) · `code`(u8) · `observation`(u8) · `message`(string) |
| 7 | `DATA` | `stream_id`(u64) · opaque payload(나머지 bytes) |
| 8 | `FIN` | `stream_id`(u64) |
| 9 | `CLOSE` | `stream_id`(u64) |
| 10 | `RESET` | `stream_id`(u64) · `code`(u8) · `message`(string) |
| 11 | `PING` | `nonce`(u64) |
| 12 | `PONG` | `nonce`(u64) |

- handshake는 `gateway_name`(string) · `gateway_id` · `expected_peer_gateway_id` · `dialer_gateway_id` ·
  `peer_transport_id`(각 uuid 16 bytes)입니다. `gateway_name`은 비어 있을 수 없습니다.
- string은 u16 길이 prefix + UTF-8 bytes이며 최대 65,535 bytes입니다. uuid는 16 raw bytes입니다.
- `OPEN`의 `namespace`·`name`은 `Destination` canonical key와 같은 bytes이며 각각 `Namespace`·`DestinationName`
  문법을 통과해야 합니다.
- `code`는 [SPEC 007](007-error-and-state-model.md) error 표의 순서대로 매긴 wire 값(`INVALID_ARGUMENT`=1 …
  `ALREADY_EXISTS`=12), `observation`은 `NOT_OBSERVED`=1 · `MAYBE_OBSERVED`=2 · `OBSERVED`=3입니다.
- 알 수 없는 magic·kind·enum 값, 길이 초과, 선언한 길이보다 짧은 payload, 남는 trailing bytes, UTF-8·UUID 위반,
  빈 `gateway_name`과 `namespace`·`name` 문법 위반은 모두 decode 실패입니다. 아직 도착하지 않은 bytes는 실패가 아니라 다음 read를 기다립니다. decode 실패는
  `PEER-011`의 terminal 결과로 transport를 닫고 `PEER-007`의 범위대로 소속 stream을 정리합니다.
- 읽지 않은 bytes를 남기는 frame은 거절하므로 기존 kind에 field를 덧붙이는 확장은 없습니다. kind 번호나 field 순서를
  바꾸면 wire version을 올려야 하며, 두 Gateway의 version이 다르면 첫 `HELLO` frame header에서 acceptor가 거절하고
  dialer는 handshake 중 transport 종료를 관측합니다(fallback 없음).

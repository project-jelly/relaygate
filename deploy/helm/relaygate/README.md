# RelayGate Helm chart

기본 구성: Gateway 1개, RouteTable shard 1개. 요구 Kubernetes 버전: 1.32 이상.

## 책임

| Chart | 배포자 / GitOps |
| --- | --- |
| GW·RT StatefulSet, Service, ShardDirectory, probe | replica·자원·배치·외부 노출 |
| TLS 설정과 기존 Secret mount | 인증서 발급·CA·Secret 공급·갱신 |
| 범용 `annotations`, `podAnnotations`, `extraEnv` | rollout controller·ArgoCD 보존 규칙 |
| metrics endpoint | 수집·대시보드·경보 |

차트는 인증서·access token을 발급하지 않는다.

## 사전 준비

release namespace에 다음 ConfigMap·Secret을 먼저 공급한다.

| values | 기본 리소스 | key |
| --- | --- | --- |
| `authorization.existingConfigMap` | `relaygate-authorization` ConfigMap | `authorization.json` |
| `tls.edge.existingSecret` | `relaygate-edge-tls` | `tls.crt`, `tls.key`; customCa는 `ca.crt` 추가 |
| `tls.internal.trustSecret` | `relaygate-internal-trust` | `ca.crt` |
| `tls.internal.gatewaySecret` | `relaygate-gw-internal-tls` | `tls.crt`, `tls.key` |
| `tls.internal.routeTableSecret` | `relaygate-rt-internal-tls` | `tls.crt`, `tls.key` |

`authorization.json`: Namespace별 issuer, ES256 public JWK, 검증 동시성·timeout. Private key·access token은 포함하지 않는다.

```json
{
  "version": 1,
  "audience": "relaygate",
  "issuers": [{
    "namespace": "example",
    "issuer": "https://issuer.example",
    "keys": [{
      "kid": "current", "kty": "EC", "crv": "P-256", "alg": "ES256", "use": "sig",
      "x": "<base64url-x>", "y": "<base64url-y>"
    }]
  }],
  "verification": { "concurrency": 32, "timeout_ms": 1000 }
}
```

| 인증서 | 요구 사항 |
| --- | --- |
| Gateway leaf | `gatewayServerName` SAN, clientAuth·serverAuth |
| RouteTable leaf | `routeTableServerName` SAN, serverAuth |
| CA private key | workload에 전달하지 않음 |

## 설치

```bash
helm upgrade --install relaygate deploy/helm/relaygate \
  --namespace relaygate --create-namespace --wait
```

| 설정 | 기본값 / 선택 |
| --- | --- |
| Gateway edge TLS | 필수; `tls.edge.existingSecret`의 server certificate·key 사용 |
| Gateway `check` trust | `tls.edge.trustMode`: `customCa` 기본값 또는 `webPkiRoots`; `tls.edge.serverName` 검증 |
| 외부 SDK TLS | SDK `Config`의 endpoint 이름·CA 설정으로 독립 검증; chart의 `tls.edge.serverName`을 전달받지 않음 |
| internal transport | `mtls` 기본값; 격리 테스트는 `plaintext` 명시 |
| SDK Service | `ClusterIP`; 필요 시 `LoadBalancer` |
| 외부 L4 | platform passthrough, Gateway에서 TLS 종료 |
| 데이터 | memory-only, persistent volume 없음 |
| runtime | Distroless `cc-debian13`, UID/GID `10001:10001`, read-only root filesystem; shell 없음 |

internal plaintext는 내부 인증·암호화·Secret mount만 제거한다. SDK edge TLS와 PUBLISH/DIAL token 검증은 유지한다.
Gateway startup/readiness는 `relaygate-server check`로 TLS·`relaygate/3` ALPN·`HELLO/WELCOME`을 검사한다.

## 운영 override

```yaml
tls:
  edge:
    existingSecret: public-edge-tls
    trustMode: webPkiRoots
    serverName: relaygate.example.com
  internal:
    trustSecret: internal-trust
    gatewaySecret: gateway-leaf
    routeTableSecret: route-table-leaf

authorization:
  existingConfigMap: relaygate-authorization
  configKey: authorization.json

gateway:
  replicaCount: 3
  annotations: {}
  podAnnotations: {}
  resources: {}

routeTable:
  shardCount: 2
  annotations: {}
  podAnnotations: {}
  resources: {}
```

`annotations`: StatefulSet metadata. `podAnnotations`: Pod template. Secret 갱신 watch와 ArgoCD의 controller 변경 필드 보존은 GitOps가 설정한다.
실행 가능한 platform override 예시는 [Kind fixture](../../../tests/kind/cert-manager-values.yaml)에 있다.

## 변경 계약

| 변경 | 적용 |
| --- | --- |
| Gateway/RT image | 해당 StatefulSet만 교체 |
| chart version만 변경 | Pod template 유지 |
| authorization public key·인증서 갱신 | platform rollout 정책 또는 해당 StatefulSet의 `rollout restart` |
| CA rotation | old/new trust overlap → leaf 교체 → old trust 제거 |
| 내부 mode·RT shard directory | maintenance window에서 coordinated restart |

Authorization key rotation: 새 public JWK 추가 → 전체 Gateway rollout → issuer의 새 `kid` 사용 → 기존 token 최대 수명·clock skew 경과 → 이전 JWK 제거 → 전체 Gateway rollout. 각 단계에서 모든 Gateway는 같은 Namespace issuer 설정을 사용한다.

RT shard 수는 immutable ShardDirectory를 바꾼다. 기존 workload 종료를 확인한 뒤 새 directory로 재설치한다.

인증서는 startup 시 읽는다. GW 교체: 신규 admission 중단 → active Pipe drain → deadline cleanup. SDK는 reconnect·republish하며 기존 Pipe의 연속성은 보장하지 않는다. `terminationGracePeriodSeconds > drainTimeoutMs`.

## 릴리스

chart version은 별도 release PR에서 변경한다. 성공한 main CI 뒤 [Release Helm Chart](../../../.github/workflows/release-chart.yml)가 새 버전의 immutable package를 발행한다.

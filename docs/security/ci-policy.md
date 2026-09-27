# CI/CD 보안 기준

이 문서는 세 project-jelly 제품의 공통 운영 기준을 RelayGate에 적용한다.
NIST/SLSA 준수 인증이나 특정 SLSA level 달성을 주장하지 않는다.

## PR과 소스 의존성

- Dependabot은 월요일 03:23 KST에 Cargo·Actions·Docker 업데이트를 제안한다.
  Cargo는 patch만 묶고 `relaygate-*` 및 package-consumer fixture 제외를 유지한다.
- `Dependency Review`는 PR이 추가하는 HIGH 이상 취약 의존성을 차단한다.
- CI의 `Vulnerability Check`는 cargo-audit 0.22.2로 Cargo.lock 전체를 검사한다.
  RustSec advisory DB 갱신/조회 실패도 검사 실패다. 알려진 취약 advisory는 severity에
  관계없이 cargo-audit 기본 정책으로 차단하며 unmaintained 등 advisory warning은 보고한다.
- CodeQL 분석 성공과 보안 알림에 따른 머지 차단은 다른 설정이다. 저장소 ruleset과
  required checks는 GitHub 설정에서 관리하고, workflow 추가만으로 활성화됐다고 간주하지 않는다.

## 이미지 릴리스와 발행 후 검사

`Release`는 같은 저장소 main push의 성공한 CI가 지목한 정확한 SHA를 checkout한다.
기존 component VERSION 파일과 GitHub Release를 비교해 미발행 버전만 처리하고,
기존 tag가 다른 commit을 가리키면 실패한다. 이미 완료한 릴리스와 부분 실패 재실행
규칙을 유지한다. 이미 존재하는 container version tag가 다른 digest면 재실행도
덮어쓰지 않고 실패한다. 이 경우 기존 digest를 검증해 복구하거나 새 버전이 필요하다.
보안 workflow 변경만으로 버전은 올리지 않는다.

1. amd64/arm64 이미지를 한 번 빌드해 tag 없이 GHCR의 digest로 보관한다.
2. SBOM과 BuildKit provenance를 이미지 index에 포함한다.
3. 두 platform manifest를 각각 Trivy 0.74.0으로 검사한다.
4. 서명된 custom release-evidence predicate에 실제 소스 SHA, 신뢰한 CI run, workflow revision과
   빌드 실행을 각각 기록하고 저장소/workflow identity·digest·predicate를 검증한다.
5. 전부 성공하면 검사한 동일 index digest에 version/latest tag를 붙이고 GitHub Release를 생성한다.

BuildKit의 `provenance: mode=max`는 빌드 재료에 대한 SLSA provenance를 남긴다.
별도 OIDC 서명은 `https://project-jelly.github.io/attestations/release-evidence/v1`
type의 릴리스 증거를 기록한다. 이 URI는 type 식별자이며 문서 페이지 주소가 아니다.
custom 증거는 소스·CI·workflow·실행 정보를 묶으며 GitHub의 표준 workflow build type이나
특정 SLSA level 충족을 주장하지 않는다. `workflow_run`의 workflow revision과 빌드한
source SHA를 구분하고, 검증된 statement 전체가 현재 릴리스와 정확히 일치해야 한다.

Trivy는 OS·library 취약점과 secret의 HIGH/CRITICAL을 검사하며, fix가 없는 취약점도
차단한다. 스캐너·DB·보고서 처리 실패나 SARIF 업로드 실패를 성공으로 숨기지 않는다.
`cargo-auditable` 0.7.6으로 production binary에 crate metadata를 포함하고, 보고서에
실제 Rust package inventory가 없으면 실패한다. OS package 검사만으로 Rust 검사를
완료했다고 판단하지 않는다. Docker Compose의 기존 topology/continuity 검증은 유지한다.

매일 03:23 KST 재검사는 두 latest 이미지의 amd64/arm64 digest를 고정해서 같은
정책을 적용한다. JSON/SARIF/TXT·스캐너 버전·immutable target을 실행 artifact로
30일 보관한다. 이는 팀 운영 정책이며 표준이 지정한 기간이 아니다. 이미 발행된
이미지의 실패를 알리지만 기존 운영 배포를 자동 중단하거나 rollback하지 않는다.
재검사는 latest 대상이며 실제 운영 digest나 모든 지원 버전의 검사를 보장하지 않는다.

이미 발행된 cargo-auditable metadata 없는 이미지의 일일 검사는 coverage gate에서
실패할 수 있다. 다음 정상 버전 릴리스에 metadata가 포함되기 전까지 이 실패를
취약점 0건/검사 성공으로 해석하지 않는다. 이 변경을 위해 기존 버전 tag를 덮어쓰지 않는다.

## 예외와 보안 알림

취약점을 무시하는 공통 allowlist는 두지 않는다. 예외가 필요하면 issue/PR에 근거,
담당자, 영향받는 digest/advisory와 만료일을 기록하고 별도 검토한다.

2026-09-27 CodeQL 알림 #1–#10의 정적 검토에서 heartbeat/peer jitter discriminator와
테스트 상수 및 Ping/Pong nonce가 발견됐다. 해당 nonce는 authentication secret이
아니며 liveness round 식별자다. `crates/relaygate-protocol/src/frame.rs`의 frame 계약과
session nonce 증가/echo 경로를 확인해 알림별로 분류해야 한다. 런타임 public 식별자를
scanner 회피 목적으로 변경하지 않으며, 이 PR은 원격 알림을 dismiss하지 않는다.

## 근거와 적용 범위

- [NIST SSDF SP 800-218](https://csrc.nist.gov/pubs/sp/800/218/final): PO.4의 검증 기준,
  PS.3의 릴리스 증거 보관, RV의 취약점 식별·대응을 운영 기준에 연결한다.
- [NIST SP 800-204D](https://csrc.nist.gov/pubs/sp/800/204/d/final): CI/CD 공급망의
  artifact·빌드·배포 단계 보안과 추적성을 반영한다.
- [SLSA v1.2](https://slsa.dev/spec/v1.2/): 출처 증명과 artifact 소비자 검증을 구분한다.
  이 저장소 검사는 외부 GitOps 소비자의 attestation 검증까지 대신하지 않는다.
- [OWASP CI/CD Security Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/CI_CD_Security_Cheat_Sheet.html):
  의존성 검사, trust boundary, token 최소 권한을 반영한다.
- [GitHub Actions secure use](https://docs.github.com/en/actions/reference/security/secure-use):
  외부 Action의 immutable SHA pin과 job별 필요한 권한만 부여한다.
- [Trivy Rust coverage](https://trivy.dev/docs/latest/coverage/language/rust/):
  Cargo.lock과 cargo-auditable 바이너리의 검사 범위를 구분한다.

주기·HIGH/CRITICAL 차단·30일 보관은 이 팀의 정책이며 위 문서가 동일 수치를 요구하지 않는다.

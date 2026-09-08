# Design

## Source of truth

- 상태: Active · 2026-09-08
- 대상: `monitoring/grafana/dashboards/`의 운영 화면
- 계약: [SPEC 008](docs/spec/008-runtime-observability-contract.md), [TEST 006](docs/test/006-local-observability-test-plan.md)
- 참고: GitOps의 `notegate-service-overview` / `notegate-internals-detail` 화면 분리와 범위·시간 유지 링크
- 원칙: [Grafana dashboard best practices](https://grafana.com/docs/grafana/latest/visualizations/dashboards/build-dashboards/best-practices/)

## Brand

차분한 운영 도구. 실제 측정 경계와 미수집 상태를 드러내 신뢰를 만든다.

## Product goals

| 목표 | 완료 기준 |
| --- | --- |
| 첫 화면에서 상태·부하 판단 | 개요는 숫자 4개 + 추이 4개 |
| 원인 후보 확인 | 개요 → GW·RT 진단 / SDK 복구 |
| 일관된 해석 | 단위·범위·측정 경계 유지, 미수집은 No data |

비목표: 업무 성공 판정, 설정 상한의 사용자 용량 환산, 자동 DATA probe 트래픽.

## Personas and jobs

운영자는 현재 상태를 확인하고, 장애 시 Gateway·RouteTable·SDK·플랫폼 중 조사할 경계를 선택한다.
SDK 개발자는 접속/DIAL 대기와 아직 끝나지 않은 재연결을 구분한다.

## Information architecture

```text
운영 개요 [cluster / namespace / GW]
  ├─ GW·RT 진단 [GW / RT / Pod / StatefulSet]
  │    ├─ 연결과 큐
  │    ├─ 라우팅과 수렴
  │    └─ Kubernetes 자원
  └─ SDK 복구 [SDK instance]
       └─ 접속 / DIAL / 재연결
```

상세 화면은 접힌 row로 영역을 선택한다. 상호 링크는 시간과 공통 변수를 유지한다.

## Design principles

| 원칙 | 적용 |
| --- | --- |
| 개요 → 상세 | 첫 화면은 최대 8개 데이터 패널 |
| 질문 하나 → 패널 하나 | 개수와 발생률, 접속 지연과 DATA RTT를 분리 |
| aggregate → instance | 개요는 집계, 상세는 개별 대상 |
| 현재 상태 → 추이 | 숫자는 instant query, 선 그래프는 range query |

## Visual language

Grafana 기본 테마·글꼴·간격·포커스를 사용한다. 24-column grid에서 숫자 카드는 6칸, 그래프는 12칸이다.
색은 결과 종류에 고정하며 빨강은 실패, 주황은 용량, 회색은 취소에 사용한다. 지연에는 임의 SLO 색상을 두지 않는다.
넓은 배경 색상과 장식 대신 값·선·제목으로 비교한다. 애니메이션·이미지·커스텀 플러그인은 없다.

## Components

Grafana 내장 stat / timeseries / row / text / dashboard link만 사용한다.
정상성 카드만 readiness threshold를 가지며 개수 카드는 중립 색상이다. 상세 설명은 panel description에 둔다.

## Accessibility

기본 키보드·포커스·화면읽기 동작을 유지한다. 색과 함께 결과명·단위를 표시한다.
제목과 범례를 짧게 유지하고 기본 테마 대비를 사용한다. 별도 접근성 적합성 인증은 제공하지 않는다.

## Responsive behavior

데스크톱은 2열 추이, 좁은 화면은 Grafana의 세로 재배치를 사용한다. 상세 범례는 펼친 화면에서 읽는다.

## Interaction states

| 상태 | 표현 |
| --- | --- |
| loading / error | Grafana 기본 로딩·query 오류 |
| missing / offline | No data; 과거 값을 현재 정상으로 재사용하지 않는 instant stat |
| zero traffic | 카운트 0, 정의되지 않은 비율·분위수는 No data |
| optional SDK / Kubernetes 수집 | 설명에 전제 표시, 연결된 exporter가 값을 제공 |
| success | 관측된 경계의 값 표시; application 성공 보장과 구분 |

## Content voice

한국어 제목·간결한 명사형 범례. SDK/GW/RT/Pipe/DIAL과 code·state는 추적 가능한 식별자로 유지한다.
정보 부족은 미수집으로, capacity는 설정 한도로 표현한다.

## Implementation constraints

- JSON을 canonical artifact로 유지한다. Rust/wire 변경과 릴리즈·운영 배포는 별도 범위다.
- 화면 ID·패널 배치의 이전 버전 호환을 요구하지 않는다.
- PromQL 범위·실제 기대값·중첩 패널·화면 링크·개요 패널 수·Grafana provisioning을 CI로 검증한다.
- 화면은 실제 Grafana에서 확인하고 미수집 상태와 펼침 동작을 검토한다.

## Open questions

- DATA RTT는 explicit probe 결과다. Grafana에 상시 측정 시계열을 추가하는 결정은 별도 계측 계약이다.

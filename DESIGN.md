# Design

## Source of truth

- Status: Active
- Last refreshed: 2026-09-04
- Primary product surfaces: local Grafana operations dashboard
- Evidence reviewed: `docs/spec/008-runtime-observability-contract.md`, `docs/test/006-local-observability-test-plan.md`, `monitoring/grafana/dashboards/relaygate-overview.json`

## Brand

- Personality: 기술적이고 절제된 운영 도구
- Trust signals: metric 의미와 한계를 함께 표시하고 상태를 합성하지 않는다.
- Avoid: 장식, 임의의 SLO, 원인을 숨기는 종합 health 점수

## Product goals

- Goals: 장애 구간을 생존성, OPEN, route, peer, RT 순서로 빠르게 좁힌다.
- Non-goals: application payload, 인증 결과, 업무 전달 성공을 관찰하지 않는다.
- Success signals: 첫 화면에서 전체 상태를 읽고 instance 필터로 상세 원인을 확인한다.

## Personas and jobs

- Primary personas: RelayGate 운영자와 개발자
- User jobs: 장애 위치 식별, 포화 확인, current-state cleanup 확인
- Key contexts of use: 로컬 Compose 검증과 운영 dashboard 초안 검증

## Information architecture

- Primary navigation: 단일 dashboard와 `All` 또는 한 instance를 고르는 Gateway/RouteTable 필터
- Core routes/screens: `RelayGate RED / USE`
- Content hierarchy: 생존성 → OPEN RED → Gateway/peer USE → RouteTable RED/current state

## Design principles

- 전체 상태는 상단에, 원인 분석은 아래에 둔다.
- current-state는 합계로 시작하고 instance filter로 drill-down한다.
- p95는 histogram bucket을 instance별로 계산한다.
- Tradeoffs: compact cluster view를 기본으로 하고 per-instance 비교는 filter와 p95 panel에 남긴다.

## Visual language

- Color: Grafana 기본 theme과 명시적인 healthy green만 사용한다.
- Typography: Grafana 기본 typography
- Spacing/layout rhythm: 24-column grid, 관련 panel을 같은 행에 배치한다.
- Shape/radius/elevation: Grafana 기본값
- Motion: 5초 refresh 외 별도 motion 없음
- Imagery/iconography: 사용하지 않음

## Components

- Existing components to reuse: Grafana stat, time series, variable
- New/changed components: 없음
- Variants and states: healthy, degraded, empty/no traffic
- Token/component ownership: Grafana가 소유하며 RelayGate는 별도 design token을 만들지 않는다.

## Accessibility

- Target standard: Grafana 기본 접근성 유지
- Keyboard/focus behavior: Grafana 기본 navigation과 variable control 사용
- Contrast/readability: 색만으로 결과를 구분하지 않고 label과 값도 표시한다.
- Screen-reader semantics: panel title과 description에 metric 의미를 기록한다.
- Reduced motion and sensory considerations: animation을 추가하지 않는다.

## Responsive behavior

- Supported breakpoints/devices: desktop 운영 화면 우선
- Layout adaptations: Grafana grid의 기본 responsive reflow 사용
- Touch/hover differences: 별도 동작 없음

## Interaction states

- Loading: Grafana 기본 loading
- Empty: 정상 무트래픽과 수집 실패를 `No data`와 `up`으로 구분한다.
- Error: outcome/code와 dependency state를 분리한다.
- Success: healthy ratio 100%와 success series
- Disabled: 해당 없음
- Offline/slow network: scrape ratio와 instance별 p95로 관찰한다.

## Content voice

- Tone: 짧고 사실 중심
- Terminology: 구현 identifier는 원문을 유지하고 설명은 한국어로 쓴다.
- Microcopy rules: 관찰하는 것과 보장하지 않는 것을 함께 적는다.

## Implementation constraints

- Framework/styling system: provisioned Grafana dashboard JSON
- Design-token constraints: Grafana 기본 theme만 사용
- Performance constraints: bounded low-cardinality PromQL만 사용
- Compatibility constraints: Prometheus histogram과 bounded label만 사용한다.
- Test/screenshot expectations: provisioning, query, RT 단절·복구와 실제 rendering을 검증한다.

## Open questions

- [ ] 실제 부하 측정 후 alert와 SLO threshold를 결정한다.
- [ ] Pipe terminal reason과 capacity denominator metric을 추가할지 결정한다.

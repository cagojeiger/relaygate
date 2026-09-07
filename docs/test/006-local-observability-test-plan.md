# TEST 006: 관측 검증

Compose observability profile은 장기 continuity traffic을 유지한 채 Prometheus target 5개(RT 2,
GW 3)와 Grafana dashboard provisioning을 확인합니다. `observability-probe`만 명시적으로 실행해
완료형 `topology-probe`가 먼저 종료되어 검증을 중단하지 않게 합니다.

검증 항목:

- DIAL result와 end-to-end duration 존재
- GW가 체감한 RT request result/latency와 RT actor service latency가 별도 metric으로 존재
- SDK/peer heartbeat RTT와 timeout이 같은 bounded `transport` 축으로 존재
- session/binding/pending offer/live Pipe/peer stream/RT mapping current gauge 존재
- recovery, lease expiry와 dependency transition counter 존재
- topology probe 종료 뒤 current state gauge가 baseline으로 수렴
- metric label에 Destination/session/Pipe/credential/error body 없음
- JSON lifecycle 로그에 component/event/outcome/code가 있고 payload/secret 없음
- 정상 DATA hot path에 per-frame info 로그 없음

established Pipe latency는 Gateway metric에서 추정하지 않습니다. 별도 probe는 Pipe를 먼저 연 뒤 warm-up과
측정 구간을 나누고, 고정된 payload 크기와 concurrency별로 p50/p95/p99/max RTT를 기록합니다. 이 probe의
결과는 RelayGate core가 application message 전달을 보장한다는 의미가 아닙니다.

관측 가능성은 correctness를 대신하지 않습니다. 먼저 topology/장애 acceptance가 성공한 뒤 metric과
로그가 그 결과와 같은 상태를 보고하는지 비교합니다.

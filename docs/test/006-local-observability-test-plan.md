# TEST 006: 관측 검증

Compose observability profile은 장기 continuity traffic을 유지한 채 Prometheus target 5개(RT 2,
GW 3)와 Grafana dashboard provisioning을 확인합니다. `observability-probe`만 명시적으로 실행해
완료형 `topology-probe`가 먼저 종료되어 검증을 중단하지 않게 합니다.

검증 항목:

- session/publish/dial/peer/RT result와 duration 존재
- session/binding/pending offer/live Pipe/peer stream/RT mapping current gauge 존재
- recovery, lease expiry와 dependency transition counter 존재
- topology probe 종료 뒤 current state gauge가 baseline으로 수렴
- metric label에 Destination/session/Pipe/credential/error body 없음
- JSON lifecycle 로그에 component/event/outcome/code가 있고 payload/secret 없음
- 정상 DATA hot path에 per-frame info 로그 없음

관측 가능성은 correctness를 대신하지 않습니다. 먼저 topology/장애 acceptance가 성공한 뒤 metric과
로그가 그 결과와 같은 상태를 보고하는지 비교합니다.

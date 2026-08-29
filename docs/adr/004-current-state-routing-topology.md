# ADR 004: RouteTable은 hash-sharded mapping authority다

| 항목 | 값 |
| --- | --- |
| 상태 | Proposed |
| 전제 | [ADR 003](003-client-id-listener-binding.md) |

## 맥락

Gateway가 여러 대일 때 Entry Gateway는 해당 `ClientId`의 live binding을 소유한 Gateway를 찾아야 한다. 모든 mapping을 모든 Gateway에 복제하지 않고 route key 수에 따라 수평 분산할 control plane이 필요하다.

## 결정

```text
Gateway × G ──► RouteTable shard × R

ShardDirectoryGeneration = SHA-256(exact ShardDirectory artifact bytes)
Authority(Generation, ClientId) = exactly 1 logical shard
Endpoint(Generation, ShardId)   = exactly 1 stable logical endpoint
Mappings(ClientId)              = 0..N live binding projections
```

`RouteTable`은 packet FIB가 아니라 identifier-to-locator Mapping System이다. `ClientId`의 deterministic hash partition이 하나의 logical shard authority를 정하고, 그 shard가 live binding에서 파생된 현재 mapping set을 관리한다.

각 Gateway와 RT process는 동일한 immutable shard directory artifact를 배포받고 그 exact bytes의 SHA-256을 `ShardDirectoryGeneration`으로 사용한다. generation은 운영자가 별도로 부여하거나 재사용하지 않는다. process는 시작할 때 generation과 directory를 고정하고, 모든 RT operation은 같은 generation일 때만 처리한다. directory artifact가 바뀌는 최초 운영 모델은 mixed-generation 전환이 아니라 coordinated restart와 current-state 재게시다.

최초 모델에서 하나의 logical shard record는 정확히 하나의 stable RT endpoint를 가진다. 그 endpoint는 하나의 process 주소 또는 하나의 logical service 주소일 수 있지만, 서로 독립적으로 쓰이는 여러 RT instance를 뜻하지 않는다. 한 shard의 복수 replica와 failover는 별도 합의 없이는 같은 authority가 아니므로 이 결정에 포함하지 않는다.

Gateway는 RT 전체 mapping을 복제하거나 구독하지 않는다. 자신이 소유한 binding은 authority shard에 게시하고, 원격 binding이 필요한 connect마다 해당 `ClientId`를 resolve한다. payload와 established Pipe는 RouteTable을 통과하지 않는다.

## 결과

- shard는 route key와 mapping 용량을 분산한다.
- 하나의 authority와 여러 destination binding을 구분한다.
- Gateway는 generation으로 식별되는 작은 불변 shard directory만 공유하고 remote mapping cache를 core authority로 사용하지 않는다.
- partition 수와 replica 수는 서로 다른 축이다.
- 최초 모델은 shard당 stable logical endpoint 하나만 허용한다.
- replication과 failover는 이 결정에서 자동으로 따라오지 않는다.
- 잘못 섞인 directory generation은 조용한 오라우팅 대신 명시적 실패가 된다.
- 같은 directory artifact는 같은 generation을 만들고 한 byte라도 바뀌면 generation을 다시 계산한다.
- directory 변경은 최초 버전에서 coordinated restart 비용을 가진다.

## 이 ADR에서 정하지 않는 것

- online 또는 rolling shard directory 변경
- logical shard의 replica와 failover
- publication과 resolve protocol, 오류와 timeout
- 구현 자료구조와 wire format

## 참고

- [RFC 9299](../rfc/rfc-9299-lisp-architecture.md)
- [RFC 9301](../rfc/rfc-9301-lisp-control-plane.md)

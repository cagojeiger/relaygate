# RFC 4254: The Secure Shell (SSH) Connection Protocol

- 원문: [RFC Editor](https://www.rfc-editor.org/rfc/rfc4254.html)
- 성격: Standards Track, 2006년 1월

## 범위

SSH Connection Protocol은 하나의 SSH transport connection 위에 여러 logical channel을
multiplex하는 방법을 정의한다.

## 핵심

- 어느 쪽이든 channel을 열 수 있고 여러 channel이 하나의 connection을 공유한다.
- channel identifier는 양 끝에서 서로 다른 local number일 수 있다.
- open request는 confirmation 또는 failure로 끝난다.
- data는 channel identifier로 demultiplex된다.
- channel별 receive window와 maximum packet size가 flow control을 제한한다.
- EOF와 CLOSE는 data 방향 종료와 channel state 제거를 구분한다.

## 구분할 점

- SSH transport의 인증, 암호화와 channel type은 SSH 고유 계약이다.
- channel flow control은 shared transport 전체의 congestion control을 대체하지 않는다.
- 이 문서는 여러 physical connection을 pool로 관리하는 방법을 정의하지 않는다.

## 읽을 절

- [§5 Channel Mechanism](https://www.rfc-editor.org/rfc/rfc4254.html#section-5)
- [§5.1 Opening a Channel](https://www.rfc-editor.org/rfc/rfc4254.html#section-5.1)
- [§5.2 Data Transfer](https://www.rfc-editor.org/rfc/rfc4254.html#section-5.2)
- [§5.3 Closing a Channel](https://www.rfc-editor.org/rfc/rfc4254.html#section-5.3)

# 기준정보와 본인 장비 설정 동기화

클라이언트는 인증한 Gateway의 REST 조회가 가능해지면 시작 동기화를 요청한다. 제어 WebSocket 연결·재연결 때도 요청하므로 연결 전 변경이나 단절 중 놓친 알림을 다시 조회한다.

## 변경 알림

제어 WebSocket `/ws/devices/gateways/<gateway_id>/`에서 다음 메시지를 받는다. 메시지는 재조회 힌트이며 payload나 명령별 ACK를 요구하지 않는다.

| type | 서버 변경 대상 |
| --- | --- |
| `sync.market_data` | 생산자·품목·RFID 카드 |
| `sync.gateway` | 본인 Gateway 설정 |
| `sync.weighing_stations` | 본인 Gateway 소속 Station |

세 알림 모두 같은 전체 동기화를 요청한다. 실시간 중량 연결이 Station 소유권 재확인을 요구할 때도 같은 작업을 사용한다. 서버는 DB commit 이후에 알림을 보내야 한다.

## 조회와 적용

각 REST 요청은 기존 `Authorization: Gateway <token>` 인증을 사용한다.

| GET 경로 | 응답 |
| --- | --- |
| `devices/api/gateways/self/` | 본인 Gateway 객체 |
| `devices/api/gateways/self/stations/` | Station 전체 배열 |
| `market/api/species/` | 품목 전체 배열 |
| `market/api/producers/` | 생산자 전체 배열 |
| `market/api/rfid-cards/` | 비활성 카드를 포함한 카드 전체 배열 |

전체 응답을 받은 뒤 필수 필드·중복 키·관계 참조·Gateway 소속을 검증한다. 정상 빈 목록은 삭제로 적용하며, 응답 누락이나 오류를 빈 목록으로 취급하지 않는다. 카드 API와 로컬 모델에는 `last_used_at`이 없다. 신규 DB로 설치하는 구성을 기준으로 한다.

검증한 설정은 하나의 SQLite 트랜잭션으로 저장하고 완성된 카드·Gateway 이름 캐시를 교체한다. Station manager가 추가·삭제·포트 변경을 적용하며 이름만 변경된 장비는 재시작하지 않는다. 계량 Record와 미전송 UUID는 이 교체 대상에 포함하지 않는다. 새 등록 토큰의 보존은 기존 인증 복구 흐름을 따른다.

동기화는 하나씩 실행하며 조회·적용 중 들어온 요청들은 후속 조회 한 번으로 합친다. REST 전체 조회에는 60초 제한이 있고 개별 HTTP timeout은 기존 10초다. 조회·검증·저장 실패 시 마지막 정상 설정을 유지하고 기존 재시도 간격으로 다시 시도한다. 성공 후에는 다음 요청까지 기다린다. 정기 조회나 영속 명령 큐는 없다.

WS 수신과 HTTP 작업은 분리한다. 장비 종료 대기는 별도 스레드에서 처리하고, 클라이언트 종료나 인증 복구 시 진행 중인 장비 적용이 끝난 뒤 다음 장비 관리 작업을 시작한다. 인증 거부는 기존 재등록 흐름으로 전달한다.

## 적용 범위와 검증 한계

여러 REST 응답이 서버의 동일 revision을 보장하는 계약은 아니다. 관계가 불완전한 응답은 적용을 거부하고 재조회하며, 조회 중 받은 알림은 후속 조회로 반영한다. 연결 중 알림이 유실되면 다음 변경 알림·재연결·재실행까지 이전 설정이 남을 수 있다.

전표 생성은 하나의 완성된 메모리 캐시를 사용한다. 카드 검증부터 출력까지 동일 배정을 고정하거나 과거 계량의 생산자·품목 귀속을 바꾸는 정책은 포함하지 않는다. 진행 중 카드 재배정·Station 삭제·포트 변경은 계량을 마친 뒤 수행한다.

자동 검증은 `tests/test_settings_sync.py`, `tests/test_settings_refresh.py`와 기존 재연결·오프라인·장비 manager 테스트를 따른다. 실제 Windows·계량 장비·실물 전표는 별도 인수 검증이 필요하다.

# HW1 구현 로직 설명

현재 `src/` 코드가 실제로 어떻게 동작하는지 정리한 문서. (요구사항 자체는 `HW1_설명정리.md` 참고)

---

## 1. 전체 구조

```
Master (외부 서버, 1개 프로세스)
  ├─ KVStore            : 5,000개 (key,value) + 진행 상태 관리
  ├─ WorkloadScheduler   : "다음 작업을 어느 Worker에게 보낼지" 결정
  ├─ VirtualClock        : Master의 가상 시계
  ├─ FileLogger          : Master.txt
  └─ ClientHandler × 4   : Worker 1개당 1개, 소켓 읽기 전담 Thread

Worker (로컬 PC, 1개 프로세스 = WorkerLauncher)
  └─ WorkerThread × 4    : Worker Node 1개 = Thread 1개
       ├─ MasterLink      : Master 소켓 "읽기" 전담 보조 Thread
       ├─ P2PServer       : 다른 Worker의 P2P 요청을 받는 보조 Thread
       ├─ ReadyQueue      : 최대 10개 대기 버퍼 (synchronized)
       ├─ VirtualClock    : 이 Worker만의 가상 시계
       └─ FileLogger      : WorkerN.txt
```

- Master ↔ Worker : TCP 소켓 (Master가 accept, Worker가 connect)
- Worker ↔ Worker(P2P) : TCP 소켓 (각 Worker가 자기 포트 6001~6004로 P2PServer를 띄우고, 서로에게 클라이언트로 접속)
- 모든 통신은 `Message`를 한 줄 텍스트로 직렬화해서 주고받음: `TYPE|k1=v1|k2=v2|...`

---

## 2. VirtualClock (가상 시계)

- `advance(delta)` : 실제로 sleep하지 않고 시간 값에 delta를 더함 (처리시간 1~3초, 통신지연 1초 등)
- `sync(incoming)` : 상대가 보내온 clock 값과 내 시계 중 큰 쪽을 채택 (Lamport Clock과 같은 아이디어)
- Master와 각 Worker는 서로 다른 프로세스라 시계를 물리적으로 공유할 수 없음 → 메시지마다 `clock` 필드를 실어 보내고 받는 쪽이 `sync()`로 느슨하게 맞춘다.
- 메시지 종류별 `clock` 처리:
  - `TASK`(Master→Worker), `RESULT`/`STATS`(Worker→Master), `P2P_TRANSFER`(Worker→Worker) : **실제 데이터 전송**이므로 보내기 전에 `advance(NETWORK_DELAY=1.0)` 후 그 값을 `clock` 필드에 실음
  - `QUEUE`(큐 크기 보고), `P2P_QUERY`/`P2P_STATUS`/`P2P_ACK` : 가벼운 상태 확인용 제어 메시지라 별도 지연 없이 현재 clock 값만 실어서 sync만 함 (모든 메시지에 1초씩 부과하면 텔레메트리만으로 총 수행시간이 의미 없이 부풀어서 제외)

---

## 3. Master 쪽 흐름 (`Master.java`)

1. **INIT**: `KVStore.generateAll()` — unique 16진수 4자리 key 5,000개 + value(1~100) 생성, 전부 `pendingQueue`에 적재
2. **CONNECT**: `acceptWorkers()`가 별도 Thread에서 4개 접속을 순서대로 accept → workerId(1~4) 부여, `ClientHandler` 시작, `WorkloadScheduler.registerWorker()`
3. **DISTRIB (동적 분배 루프, `runDispatchLoop`)**: `kvStore.isAllDone()`이 false인 동안 반복
   - `kvStore.nextPendingKey()` : **priorityQueue(재시도 대기)를 항상 먼저** 꺼내고, 없으면 `pendingQueue`에서 꺼냄 → "최우선 큐" 요구사항 구현
   - `scheduler.pickWorkerForDispatch()` : **최소 큐 우선(Least Queue First)** — 큐가 가장 여유로운(size가 가장 작은) Worker를 고름, 전부 10/10이면 `-1`
   - `-1`이면 그 key를 다시 priorityQueue로 되돌리고 20ms 대기 후 재시도 (busy-wait 방지용이지 System Clock과는 무관)
   - 정상 분배 시: `clock.advance(NETWORK_DELAY)` 후 `ClientHandler.sendTask()`로 TASK 전송, `scheduler.onDispatchedOptimistically()`로 낙관적으로 큐 크기 +1 (Worker가 실제 QUEUE 보고를 하기 전 몰아보내기 방지)
4. **RESULT 수신 (`onWorkerResult`)**: `ClientHandler`가 소켓에서 `RESULT` 메시지를 읽을 때마다 호출
   - `SUCCESS` → `kvStore.markSuccess(key)`로 저장 완료 처리
   - `FAIL` → `kvStore.requeueAsPriority(key)`로 최우선 재시도 등록 + `reassignCount` 1 증가 (이 카운터만 "장애로 인한 재할당" 수를 셈 — 큐가 꽉 차서 잠깐 미룬 건 포함 안 함)
5. **STATS 수신 (`onWorkerStats`)**: Worker가 종료 직전 자기 최종 통계(수신/성공/실패/평균대기/P2P송수신/총시간)를 보고하면 `workerFinalStats` 맵에 저장하고 `statsReceived` 래치 감소
6. **종료 (`shutdownAll`)**: 전체 5,000개 완료되면
   - 모든 Worker에 `SHUTDOWN` 전송
   - `statsReceived.await()`로 Worker들의 STATS 도착까지 대기 (전부 이미 끝난 상태라 사실상 즉시 도착, 30초는 안전장치)
   - Master `STAT` 로그: 총 처리 건수, 총 수행시간, 장애 재할당 총 횟수, Worker별(수신/성공/실패/평균대기/P2P송수신/총시간), P2P 이벤트 총합
   - `KVSTORE` 로그: 완료된 5,000쌍 전체를 key 정렬해서 한 줄씩 덤프 (`KVStore.snapshotStore()`)
   - `TERMINATE` : Graceful shutdown

---

## 4. Worker 쪽 흐름 (`WorkerThread.java`)

각 Worker는 독립 Thread(`WorkerThread.run()`)이며, 메인 루프는 아래 3가지를 반복:

```
while (!shutdownRequested) {
    1) inboxFromMaster에서 메시지 하나 꺼내 처리 (TASK / SHUTDOWN)
    2) readyQueue에서 작업 하나 꺼내 처리 (processTask)
    3) 1~3초 랜덤 주기가 지났으면 P2P 과부하 체크 (checkAndOffloadIfOverloaded)
    Thread.sleep(5)  // CPU busy-wait 방지용, System Clock과 무관
}
```

- **TASK 수신**: `readyQueue.offer()` 시도 → 꽉 찼으면(10/10) 즉시 `RESULT=FAIL` 응답(큐 오버플로), 성공하면 70% 초과 WARN 체크 + `QUEUE` 크기 보고
- **processTask**: 실제 sleep 없이 `clock.advance(procTime)`(1~3초 랜덤)만 하고, `random.nextDouble() < 0.8`로 성공/실패 판정 → `RESULT` 전송 (`sendResult()`가 전송 전 1초 네트워크 지연 반영)
- **P2P 과부하 체크 (`checkAndOffloadIfOverloaded`)**:
  1. 내 큐 크기 × 평균처리시간(2초) = 예상 대기시간, 15초 이하면 아무것도 안 함
  2. peer들을 순서대로 `P2P_QUERY`로 조회, **나보다 큐가 적은 첫 번째 peer**를 찾음
  3. `(내 큐 - peer 큐)/2` 만큼 `readyQueue.pollFromTail()`로 뒤쪽(늦게 처리될) 작업을 뽑아 `P2P_TRANSFER`로 전송
  4. peer가 다 못 받아주면(레이스 컨디션) 남은 건 내 큐로 롤백 → 유실 없음
  5. 매 이전마다 로컬 `p2pSent` 카운터 누적, WARN/QUEUE 로그 갱신
- **P2PServer**: 다른 Worker의 `P2P_QUERY`(내 큐 크기 응답), `P2P_TRANSFER`(작업 수신 후 `p2pReceived` 누적, ACK 응답) 처리
- **종료 (SHUTDOWN 수신)**: 루프 탈출 → `printFinalStatsAndClose()`
  - `STAT` 로그: 수신/성공/실패/평균대기시간/P2P송수신/총수행시간
  - `STATS` 메시지를 Master로 전송 (Master가 최종 STAT에 합산하도록)
  - `TERMINATE` 로그, 소켓/서버 정리

---

## 5. Ready Queue 규칙 (`ReadyQueue.java`)

- 최대 10개, `offer()`는 꽉 차면 `false` (즉시 FAIL 트리거)
- 70% 초과(=8개 이상) 상태에서 작업이 들고날 때마다(enqueue/dequeue 이벤트마다) `WARN` 로그 — 상태 전이 1회가 아니라 매번
- `pollFromTail()` : P2P로 넘길 때는 큐 **뒤쪽**(늦게 처리될 예정) 작업을 우선 이전 — 곧 처리될 앞쪽 작업까지 넘기면 오히려 지연이 커지기 때문

---

## 6. 메시지 프로토콜 요약 (`Message.java`)

| 방향 | TYPE | 필드 |
|---|---|---|
| Master→Worker | `TASK` | key, value, retry, clock |
| Master→Worker | `SHUTDOWN` | clock |
| Worker→Master | `RESULT` | key, status(SUCCESS/FAIL), clock |
| Worker→Master | `QUEUE` | size, clock |
| Worker→Master | `STATS` | received, success, fail, avgWait, p2pSent, p2pReceived, totalTime, clock |
| Worker↔Worker | `P2P_QUERY` | fromId, clock |
| Worker↔Worker | `P2P_STATUS` | size, clock |
| Worker↔Worker | `P2P_TRANSFER` | keys(`;`구분), values(`;`구분), clock |
| Worker↔Worker | `P2P_ACK` | count, clock |

---

## 7. 로그 형식

공통: `[clock] NODE | EVENT | STATUS | message` (STATUS는 INFO/SUCCESS/FAIL/WARN 중 하나)

현재 코드에서 실제로 쓰는 EVENT 목록: `INIT, CONNECT, DISTRIB, RESULT, DISCONNECT, STAT, KVSTORE, TERMINATE` (Master) / `INIT, CONNECT, RECV, PROC, QUEUE, LB, STAT, TERMINATE` (Worker) — 제출 시 `AllDefinedLogs.txt`에 이 목록을 그대로 정리하면 됨.

---

## 8. 알려진 설계상 트레이드오프

- `KVStore.priorityQueue`는 "장애 재시도"와 "일시적으로 보낼 Worker가 없어서 대기"라는 두 가지 경우를 같은 큐에 섞어서 처리함 → 로직상 문제는 없지만(둘 다 최우선으로 재시도하면 되므로), `reassignCount`는 앞의 경우(FAIL)만 세도록 분리해뒀음
- P2P 이전 시 목적지 peer는 "포트 번호 순서대로 처음 만난, 나보다 큐가 적은 곳" — 가장 여유로운 peer를 찾는 것도 아니고 라운드로빈도 아님 (Readme에 이 알고리즘 그대로 서술하면 됨)
- 네트워크 지연은 "실제 데이터 전송성" 메시지(TASK/RESULT/P2P_TRANSFER/STATS)에만 부과하고, 상태 조회성 메시지(QUEUE/P2P_QUERY/STATUS/ACK)에는 부과하지 않음 — 이유는 위 2장 참고

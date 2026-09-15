# HW#1 데이터통신 프로그래밍 과제

## 분산 환경에서의 Fault-Tolerant 키-값 저장소 구현
(Multi-Thread 소켓 통신 · 동적 부하 분산 · 장애 복구)

- **DUE**: 2026.9.23(수) 23:59  (Assigned: 2026.9.10(목) 09:00)
- **제출 방법**: 1개 파일 제출 — `G조이름HW1.zip` (조별로 한 명만 제출, 예: `G1HW1.zip`)
  모든 소스파일 및 결과파일 전체를 압축하여 제출.

---

## 0. 분산 키-값 저장소 기본 개념

분산 키-값 저장소(Distributed Key-Value Store)는 대규모 데이터를 여러 노드에 분산 저장하고 고가용성을 보장하는 저장 시스템. Master-Worker 구조에서 Master는 데이터 분배와 장애 처리를 조율하고, Worker는 실제 데이터를 저장·처리한다.

### 0-1. 핵심 개념

| 개념 | 설명 |
|---|---|
| **Key-Value Store** | Key는 고유 식별자(ID), Value는 저장할 데이터(1~100 정수). Map 자료구조 기반 O(1) 접근. Key는 unique한 16진수 4자리 (예: `a3f7`) |
| **Master Node** | System Clock 관리, 5,000개 KV 쌍 생성, Worker에 작업 동적 분배, 장애 감지 및 재할당 수행 |
| **Worker Node** | Ready Queue(최대 10개, 70% 초과 시 Warning, 10개 초과 시 FAIL)로 작업 수신, 1~3초 랜덤 처리시간, 80% 성공률, P2P 부하 분산 참여 |
| **Ready Queue** | 워커 노드의 대기 작업 버퍼. 최대 10개, 초과 요청은 즉시 FAIL 처리. WARN은 상태 전이 시 1회가 아니라 **큐가 70% 초과한 상태에서 작업이 들고날 때마다 매번** 기록 |
| **System Clock** | 실제 시간이 아닌 가상의 연산·네트워크 지연 누적 시간(단위: sec). 모든 이벤트 로그에 함께 기록. **Worker 처리시간(1~3초)과 노드 간 통신 지연(1초)은 실제 Thread sleep이 아니라 System Clock 값에 더하는 가상 누적치**이며, 프로그램 자체는 실시간 대기 없이 최대한 빠르게 실행됨 |
| **Fault Tolerance** | Worker 실패(20%) 시 Master가 감지하여 다른 Worker에 재할당, 데이터 유실 방지. 재할당된 작업에도 동일한 80/20 규칙 재적용, 또 실패하면 성공할 때까지 최우선 큐에 재등록해 재시도 |

### 0-2. 시스템 구조 및 데이터 흐름 (Master-Worker 구조도)

```
+---------------------------------------------+
|              Master Node                     |
|  (System Clock + KV 5,000)                   |
|  Workload Distribution                       |
|  Error Handling / Reassign                   |
+---+-----------+-----------+-----------+-------+
    |           |           |           |
 (Socket)    (Socket)    (Socket)    (Socket)
+--------+  +--------+  +--------+  +--------+
| Worker1|  | Worker2|  | Worker3|  | Worker4|
| Queue10|  | Queue10|  | Queue10|  | Queue10|
+--------+  +--------+  +--------+  +--------+
      <---- P2P Load Balancing (Worker <-> Worker) ---->
```

### 0-3. 동적 작업 분배 알고리즘 예시

Master는 각 Worker의 큐 상태를 모니터링하여 작업을 동적으로 분배한다.

| 시점 | Worker1 Queue | Worker2 Queue | Worker3 Queue | Master 판단 |
|---|---|---|---|---|
| t=1s | 3/10 | 2/10 | 1/10 | Worker3에 다음 작업 할당 (최소 큐) |
| t=5s | 8/10 | 5/10 | 2/10 | Worker3 우선 할당 / Worker1 포화 주의 |
| t=9s | 10/10 | 9/10 | 4/10 | Worker1 큐 초과 → 해당 요청 FAIL 처리 |
| t=12s | FAIL(장애) | 6/10 | 3/10 | Worker1 장애 → 작업 Worker3에 재할당 |

### 0-4. P2P 부하 분산 동작 예시

Worker 노드끼리 직접 소켓 통신으로 부하를 분산한다. 큐 대기 시간이 임계값(15sec)을 초과하면 인접 Worker에게 작업 이전을 요청.
예상 대기시간 = 현재 큐에 남은 작업 수 × 평균 처리시간(2초). 각 Worker는 1~3초 사이의 랜덤 주기로 반복 점검.

| 단계 | 주체 | 동작 |
|---|---|---|
| 1 | Worker1 | 자신의 큐 대기시간 모니터링: 큐=9개, 평균처리=2s → 예상 대기=18s (임계 초과) |
| 2 | Worker1 | 인접 Worker2, Worker3 큐 상태 조회. Worker3 큐=3개 확인 |
| 3 | Worker1 → Worker3 | 큐 하위 3개 작업을 Worker3으로 이전 요청 (P2P 소켓 전송) |
| 4 | Worker3 | 작업 수신 후 자신의 큐에 추가. 수신 확인(ACK) 전송 |
| 5 | Worker1 | ACK 수신 후 이전된 작업 큐에서 제거. 균형 복원 완료 |

### 0-5. 장애 처리(Fault Tolerance) 흐름 예시

Worker2가 작업 처리 중 실패(20% 확률)를 Master에 보고하는 경우:
1. Worker2가 작업(KV 쌍 저장) 처리 중 실패 발생 → Master에 FAIL 메시지 전송
2. Master는 실패한 작업 ID를 최우선 큐(Priority Queue)에 등록
3. Master는 현재 큐가 가장 여유 있는 Worker(예: Worker4)를 선택하여 재할당
4. 재할당된 Worker4가 동일 작업을 80/20 규칙으로 재처리 (재할당 작업도 실패 확률 동일 적용). SUCCESS 시 결과를 Master에 전송, FAIL 시 다시 최우선 큐에 등록되어 다른 Worker에 재재할당
5. Master는 KV 저장소/결과 기록을 업데이트하고 전체 진행률 갱신

---

## 1. 시뮬레이션 환경 및 구성요소

1개의 Master Node와 4개의 Worker Node로 구성된 분산 키-값 저장소 시스템 구현. 모든 노드 간 통신은 Socket(TCP), 각 Worker Node는 독립적인 Thread로 동작. Master Node는 외부 클라우드 서버(AWS/GCP)에 반드시 구현되어야 함. Worker Node(4개 Thread)는 조원의 로컬 PC에서 실행하며 Master의 공인(public) IP/포트로 접속. Master의 IP/포트는 실행 시 인자(argument) 또는 설정파일로 지정 가능해야 함.

```
[시스템 구성도]
+-----------------------------------------------+
|              Master Node                       |
|  System Clock | KV Store 5,000 | Workload Scheduler |
|  Error Handler | Priority Requeue | Log: Master.txt  |
+--------+-----------+-----------+-----------+----+
      (Socket)     (Socket)    (Socket)    (Socket)
+----------+ +----------+ +----------+ +----------+
| Worker1  | | Worker2  | | Worker3  | | Worker4  |
| Queue 10 | | Queue 10 | | Queue 10 | | Queue 10 |
| 80% succ | | 80% succ | | 80% succ | | 80% succ |
+----------+ +----------+ +----------+ +----------+
      <------- P2P Load Balancing (Socket) ------->
```

### ① Master Node (Controller)
- **외부 서버 구축**: 반드시 AWS, Google Cloud 등 물리적 외부 서버에 구현
- **데이터 생성**: 시작 시 5,000개의 (Key, Value) 생성. Key는 고유 ID, Value는 1~100 무작위 정수
- **System Clock 관리**: 전역 가상 시계 유지, 모든 이벤트 시각 로그 기록. Worker 처리·통신 지연 값도 이 가상 시계에 누적될 뿐, 실제 실행 속도(real time)와 무관
- **동적 작업 분배**: Worker의 현재 처리량과 Queue 상태를 모니터링하여 동적 분배
- **장애 처리(Fault Tolerance)**: Worker로부터 처리 실패 보고 시 Priority Queue에 등록 후 다른 Worker에 최우선 재할당
- **최종 출력**: 전체 처리 완료 시 KV 저장소 전체(5,000쌍), 총 수행시간, 노드별 처리 통계를 로그에 기록
- 모든 이벤트를 `Master.txt` 로그에 기록, 처리 완료 후 **Graceful Termination** 수행

### ② Worker Node (Storage & Processor, 4개)
각 Worker Node는 독립 Thread로 동작하며:
- **Ready Queue 운영**: 최대 10개 대기, 초과 요청은 즉시 FAIL
- **가상 처리 시간**: 작업당 1~3초 무작위 처리 시간(실제 sleep이 아니라 System Clock 값에 더하는 시뮬레이션)
- **성공 확률 시뮬레이션**: 80% 성공 / 20% 실패. 실패 시 Master에 FAIL 메시지 전송
- **P2P 부하 분산**: 큐 대기 시간이 임계값(15초) 초과 시 인접 Worker와 소켓 통신으로 작업 이전. 예상 대기시간 = (큐 남은 작업 수 × 평균 처리시간 2초), 1~3초 랜덤 주기로 반복 점검
- **통신 지연**: 모든 노드 간 네트워크 지연은 1초로 고정 시뮬레이션 (System Clock 누적값으로만 반영)
- **최종 출력**: 연결 종료 전 성공/실패 횟수, 작업 처리량, 평균 대기시간, 전체 수행시간 출력
- 각 Worker의 모든 이벤트를 `Worker1.txt` ~ `Worker4.txt` 로그에 기록

### ⚠ 필수 구현 조건 (하나라도 미충족 시 과제 0점 처리)
1. Master Node는 반드시 AWS, GCP 등 물리적 외부 서버에 구현되어야 함
2. Master Node와 모든 Worker Node 간 통신은 반드시 Socket으로 구현되어야 함
3. 모든 Worker Node는 독립적인 Thread를 통해 구현되어야 함

---

## 2. 시뮬레이션 시나리오

Master와 4개의 Worker 노드는 각각 `Master.txt`, `Worker1.txt`~`Worker4.txt`에 모든 이벤트와 결과를 기록한다.

- **Step 1 - 초기화 (System Clock: 0 sec)**
  - Master Node 구동, System Clock 0에서 시작
  - Master가 5,000개의 (Key, Value) 쌍 생성 후 메모리 저장
  - 4개 Worker Node가 순차적으로 소켓 연결 완료, Ready Queue 초기화
  - Master가 전체 초기화 완료 후 작업 분배 시작

- **Step 2 - 동적 작업 분배 (0 sec ~)**
  - Master가 각 Worker의 Queue 상태를 지속 모니터링
  - Queue 여유 공간이 가장 많은 Worker에게 우선 분배
  - Worker Queue가 최대(10개) 도달 시 해당 Worker 분배 일시 중단
  - 전체 5,000개 작업을 분배 완료할 때까지 반복, 통신 지연은 1sec로 가정

- **Step 3 - 작업 처리 및 결과 수집**
  - 각 Worker는 Queue에서 작업을 꺼내 1~3sec 가상 처리 후 결과를 Master에 전송
  - 성공(80%) 시 Master의 KV 저장소 업데이트
  - 실패(20%) 시 Master는 해당 KV 쌍을 Priority Queue에 등록 후 가장 여유 있는 Worker에 재할당
  - 재할당 시에도 동일한 80/20 규칙 적용, 실패 시 성공할 때까지 반복 재시도

- **Step 4 - P2P 부하 분산 (상시 동작)**
  - 각 Worker는 자신의 큐 대기 시간을 주기적으로 계산
  - 대기 시간이 임계값(15sec) 초과 시 인접 Worker에 큐 상태 조회 후 작업 이전 요청
  - **부하 분산 알고리즘(방법론)은 제약 없음. 단, 제출 시 사용한 알고리즘을 반드시 명시**
  - 부하 분산으로 이전된 모든 작업도 `Worker.txt` 로그에 기록

- **Step 5 - 종료**
  - 전체 5,000개 KV 쌍 처리 완료 시 Master가 완료 신호를 모든 Worker에 전송
  - 각 Worker는 성공/실패 횟수, 작업 처리량, 평균 대기시간, 전체 수행시간 출력 후 종료
  - Master는 KV 저장소 전체, 총 수행시간, 노드별 통계를 로그에 저장 후 Graceful Termination

---

## 3. 성능 평가 지표 (필수 측정 항목, 각 노드 최종 통계 로그(STAT)에 반드시 포함)

| 지표명 | 단위 | 설명 |
|---|---|---|
| Worker별 작업 처리량 | 건 | 각 Worker가 성공적으로 처리한 KV 쌍의 수 |
| Worker별 성공/실패 횟수 | 건 | 전체 처리 중 성공 및 20% 실패 케이스 카운트 |
| 작업 평균 대기시간 | sec | Queue 진입 후 처리 시작까지의 평균 대기 시간 |
| P2P 부하 분산 이벤트 횟수 | 회 | Worker 간 작업 이전이 발생한 총 횟수 |
| 장애 재할당 횟수 | 건 | 실패로 인해 다른 Worker에 재할당된 작업 수 |
| 전체 수행시간 | sec | System Clock 기준 전체 시나리오 소요 시간 |

※ 로그 예시에 등장하는 추가 항목(Queue overflow rejects, Retry tasks received, P2P 전송/수신 세부 건수 등)은 참고용 예시이며 필수는 아님.

---

## 4. 노드별 Log 출력 예시

각 노드는 모든 이벤트를 System Clock과 함께 아래 형식으로 기록한다.

- **공통 로그 형식**: `[clock] NODE | EVENT | STATUS | message`
- **STATUS**: `INFO` / `SUCCESS` / `FAIL` / `WARN` 중 하나만 사용
- **EVENT**: 고정 목록이 아닌 자유 형식 (예: INIT, CONNECT, DISTRIB, RESULT, RECV, PROC, LB, QUEUE, STAT, TERMINATE 등). 프로그램에서 정의한 모든 EVENT 종류는 제출물 `AllDefinedLogs.txt`에 명세·설명해야 함
- 로그 예시의 시각, 진행률, 통계 수치는 모두 예시 값이며 실제 실행 결과는 매번 달라짐

### 4-1. Master.txt 예시 (요약)
- `[0.00]` System Clock started / 5,000 KV pairs 생성 시작 → `[0.02]` 생성 완료
- `[0.51~0.55]` Worker1~4 순차 CONNECT, Ready Queue 초기화, 이후 workload distribution 시작
- `[1.00~]` DISTRIB 이벤트로 KV를 각 Worker에 큐 최소인 곳부터 분배
- RESULT SUCCESS/FAIL 로그, FAIL 시 WARN으로 Priority requeue → 다른 Worker에 재할당
- 큐 full(10/10) 시 WARN 후 해당 Worker로의 분배 중단, 다른 Worker로 리디렉션
- 진행률(Progress: n/5000) 주기적 기록
- 최종 `STAT`: 전체 KV pairs 처리, Total SUCCESS/FAIL, 재할당 수, P2P 이벤트 수, 전체 수행시간, Worker별 처리/성공/실패 통계
- `TERMINATE`: Graceful shutdown, 전체 Worker 연결 해제

### 4-2. Worker1.txt 예시 (정상 동작 + P2P 부하 분산)
- CONNECT → RECV/PROC(SUCCESS/FAIL) 반복
- 큐 nearing full(WARN) → 인접 Worker 큐 조회(LB) → 작업 이전(P2P 소켓) → ACK 수신 후 큐 갱신
- 큐 full 시 신규 요청 REJECT(FAIL, queue overflow)
- 누적 처리 통계 주기적 기록
- 최종 STAT: Total tasks received/SUCCESS/FAIL, Avg waiting time, P2P tasks 전송/수신 건수, 총 수행시간
- TERMINATE: gracefully disconnected

### 4-3. Worker2.txt 예시 (실패 재할당 수신 케이스)
- 작업 처리 실패(FAIL) → Master에 통지 → PRIORITY 재할당 작업 수신(RECV, reassigned) → 재처리 → SUCCESS
- P2P로 타 Worker의 작업 수신(RECV, LB SUCCESS) 및 자신 큐 여유 시 타 Worker로 전송
- 최종 STAT: Retry tasks received 항목 포함

### 4-4. Worker3.txt 예시 (Queue 초과 + 집중 수신 케이스)
- P2P로 Worker1로부터 다량 작업 수신 → 큐 full 도달 → 신규 요청 REJECT(queue overflow)
- 이후 처리로 큐 여유 회복, 계속 처리
- 최종 STAT: Queue overflow rejects 항목 포함

### 4-5. Worker4.txt 예시 (재할당 집중 처리 케이스)
- PRIORITY(재할당) 작업 수신 및 처리 비중이 높은 케이스
- P2P로 Worker2로부터 작업 수신 및 처리
- 최종 STAT: Priority(retry) tasks 항목 포함

---

## 5. 과제 제출물

**`G조이름HW1.zip`** (예: `G1HW1.zip`) — **압축 파일 오류 시 0점 처리**

제출물 구성:
1. **전체 소스 코드**
2. **`AllDefinedLogs.txt`** — 프로그램에서 정의한 모든 로그 메시지 명세 및 설명
3. **결과 로그 파일**: `Master.txt`, `Worker1.txt`, `Worker2.txt`, `Worker3.txt`, `Worker4.txt` — 각 노드에서 출력된 전체 로그
4. **`download.txt`** — 5분 이내 시연 동영상(`G조이름HW1.mp4`) 다운로드 가능한 링크 포함 (예: `G1HW1.mp4`)
   - 링크 오류, 공유 권한 없음, 재생 오류 등은 해당 조 과실로 감점
   - 영상은 전체 5,000건 처리가 끝날 때까지 보여줄 필요 없음. **초기화·동적 분배·장애 재할당·P2P 부하분산·로그 출력 등 핵심 동작 과정만 확인되면 됨**
5. **`Readme.txt`** — 아래 항목 포함:
   - 조원 이름·학번·역할 명시
   - 프로그램 구성요소 설명
   - 소스코드 컴파일 및 실행 방법 (**컴파일/실행 불가 시 과제 전체 0점 처리**)
   - 프로그램 실행 환경 및 실행 방법
   - 동적 작업 분배 알고리즘 설명(장단점 분석 포함) — **알고리즘 명시 필수**
   - P2P 부하 분산 알고리즘 설명(장단점 분석 포함) — **알고리즘 명시 필수**
   - 장애 처리(Fault Tolerance) 메커니즘 설명
   - 추가 구현 사항 및 기타 언급할 내용

---

## ⚠ 감점/0점 유의사항 요약
- Master Node를 외부 클라우드 서버(AWS/GCP 등)에 구현하지 않으면 **0점**
- Master-Worker 간 통신을 Socket으로 구현하지 않으면 **0점**
- Worker Node를 독립 Thread로 구현하지 않으면 **0점**
- 제출 압축파일 오류 시 **0점**
- 소스코드 컴파일/실행 불가 시 **0점**
- 동적 작업 분배 및 P2P 부하 분산 알고리즘은 자유롭게 설계 가능하나, Readme에 **반드시 명시**해야 함
- 시연 영상 링크 오류/권한 문제/재생 오류는 조 과실로 감점

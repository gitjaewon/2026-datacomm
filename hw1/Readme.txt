====================================================================
HW#1 데이터통신 프로그래밍 과제
분산 환경에서의 Fault-Tolerant 키-값 저장소 구현
====================================================================

--------------------------------------------------------------------
1. 조원 정보
--------------------------------------------------------------------

이름:김재원
학번:20213086
역할:P2P 부하분산 로직 구현, 브랜치/PR 관리

이름:윤도훈
학번:20223122
역할:Master 통계/로깅 시스템 구현, Readme 및 로그 명세 문서화

이름:김치헌
학번:20223097
역할:장애 재할당(Fault Tolerance) 처리 및 통계 집계 구현

--------------------------------------------------------------------
2. 프로그램 구성요소 설명
--------------------------------------------------------------------

Master (1개 프로세스, 외부 클라우드 서버(AWS)에서 실행)
  - KVStore            : 5,000개 (key,value) 생성 및 진행 상태 관리
                          (pendingQueue / priorityQueue / store)
  - WorkloadScheduler   : 다음 작업을 어느 Worker에게 보낼지 결정
  - VirtualClock        : Master의 가상 시계
  - FileLogger          : Master.txt 기록
  - ClientHandler x4    : Worker 1개당 1개, 소켓 읽기 전담 Thread

Worker (1개 프로세스 = WorkerLauncher, 로컬 PC에서 실행,
        4개 Worker Node = 4개 Thread)
  - WorkerThread x4     : Worker Node 1개 = 독립 Thread 1개
       - MasterLink      : Master 소켓 "읽기" 전담 보조 Thread
       - P2PServer       : 다른 Worker의 P2P 요청을 받는 보조 Thread
       - ReadyQueue      : 최대 10개 대기 버퍼 (synchronized)
       - VirtualClock    : 이 Worker만의 가상 시계
       - FileLogger      : WorkerN.txt 기록

통신 구조
  - Master <-> Worker      : TCP 소켓 (Master가 accept, Worker가 connect)
  - Worker <-> Worker(P2P) : TCP 소켓 (각 Worker가 자기 포트 6001~6004에
                             P2PServer를 띄우고 서로 클라이언트로 접속)
  - 모든 메시지는 한 줄 텍스트로 직렬화: TYPE|key1=value1|key2=value2|...
    (TASK, RESULT, QUEUE, STATS, REGISTER, SHUTDOWN,
     P2P_QUERY, P2P_STATUS, P2P_TRANSFER, P2P_ACK)

--------------------------------------------------------------------
3. 소스코드 컴파일 및 실행 방법
--------------------------------------------------------------------

[공통] 저장소 받기
  git clone https://github.com/gitjaewon/2026-datacomm.git
  cd 2026-datacomm/hw1

[공통] 컴파일 (Master 서버, Worker PC 양쪽 모두 각자 실행)
  * Linux / macOS / Git Bash:
      javac -encoding UTF-8 -d out $(find src -name "*.java")
  * Windows PowerShell:
      javac -encoding UTF-8 -d out (Get-ChildItem -Recurse -Filter *.java src |
        Select-Object -ExpandProperty FullName)

[Master 서버(AWS)] 실행
  java -cp out kvstore.master.Master <포트번호>
  예) java -cp out kvstore.master.Master 5000
  (콘솔에는 아무것도 안 뜸, tail -f Master.txt로 확인)

[Worker PC(로컬)] 실행
  java -cp out kvstore.worker.WorkerLauncher <Master의 공인IP> <포트번호>
  예) java -cp out kvstore.worker.WorkerLauncher 12.34.56.78 5000

  4개 Worker Thread가 한 프로세스 안에서 동시에 뜨며, 자동으로
  Master에 접속 후 작업을 받기 시작한다.

--------------------------------------------------------------------
4. 프로그램 실행 환경 및 실행 방법
--------------------------------------------------------------------

- 언어/런타임 : Java (JDK 21 기준으로 개발 및 테스트)
- Master 실행 위치 : AWS EC2(Ubuntu) 등 물리적 외부 클라우드 서버
    - 보안 그룹(방화벽)에서 사용할 포트(예: 5000) 인바운드 TCP 허용 필요
    - P2P 포트(6001~6004)는 Worker들이 전부 같은 로컬 PC 안에서
      localhost로 통신
- Worker 실행 위치 : 로컬pc
- Master의 IP/포트는 실행 시 커맨드라인 인자로 지정 (3장 참고)
- 실행 순서 : Master를 먼저 실행해 대기 상태로 만든 뒤, Worker를 실행

--------------------------------------------------------------------
5. 동적 작업 분배 알고리즘 설명 (WorkloadScheduler.java)
--------------------------------------------------------------------

사용한 알고리즘: Least Queue First (최소 큐 우선)

Master가 각 Worker의 큐 크기를 계속 추적하다가, 매번 "추적된 큐 크기가
가장 작은 Worker"에게 다음 작업을 보낸다. 추적하는 큐 크기는
  Worker가 QUEUE로 보고한 크기 + (Master가 보낸 TASK 수 - 그 보고 시점까지
  Worker가 받은 TASK 수)
로 계산한다. 보고는 통신 지연만큼 늦게 오므로, 괄호 안(아직 도착하지 않은
작업)을 더하지 않으면 이미 가득 찬 Worker에게 계속 보내 거절이 쌓인다.
추적값이 10이 된 Worker에게는 보내지 않고, 보낼 수 있는 Worker가 없으면
해당 작업을 큐로 되돌리고 잠시 대기한다. 장애로 재할당된 작업도 같은 알고리즘을 쓰되,
priorityQueue를 pendingQueue보다 먼저 소비하고 직전에 그 작업을
실패시킨 Worker는 후보에서 제외한다.

장점: 구현이 단순하고, 큐 상태를 반영해 4개 Worker의 부하가 고르게
유지됨. 이미 보낸 작업을 빠뜨리지 않으므로 Master 전송 때문에 큐가
넘치는 일은 없음 (P2P 이전과 Master 전송이 같은 Worker에 동시에
몰릴 때만 드물게 큐 초과 거절이 생김)
아쉬운 점: Worker의 실제 처리 속도는 고려하지 않고 대기 개수만 봄.
그리고 분배 판단을 Master 혼자 다 하다 보니, Worker가 더 늘어나면
Master가 병목이 될 수 있고 Master가 죽으면 분배 자체가 멈춤

--------------------------------------------------------------------
6. P2P 부하 분산 알고리즘 설명 (WorkerThread.java)
--------------------------------------------------------------------

사용한 알고리즘: 처음 찾은 여유 Peer에게 절반씩 넘기기

1) 내 큐 크기 x 평균 처리시간(2초)으로 예상 대기시간을 계산, 15초를
   넘으면(큐 8개 이상) 부하분산 트리거
2) 자신의 VirtualClock 기준 1~3초 랜덤 주기로 이 체크를 반복 수행
3) peer들을 포트 번호 순서대로 조회해 나보다 큐가 적은 첫 번째
   peer를 찾음 (가장 여유로운 peer가 아니라 처음 만난 peer)
4) (내 큐 - peer 큐) / 2 만큼 큐 뒤쪽 작업부터 이전. 재할당 작업은
   최우선 처리 대상이므로 이전에서 제외
5) peer는 받은 만큼만 ACK로 확인해주고, 보낸 쪽은 ACK 개수 이후
   작업을 내 큐로 롤백 -> 유실·중복 없음

장점: Master 개입 없이 Worker끼리 자율적으로 처리함
아쉬운 점: 제일 처음 만난 여유 peer한테 넘기는 방식이라 전체적으로
최적의 분배는 아님. peer를 포트 순서대로 보다 보니 다들 Worker1부터
물어봐서, 부하가 높을 때는 번호가 작은 Worker에게 이전이 몰린다.

--------------------------------------------------------------------
7. 장애 처리(Fault Tolerance) 메커니즘 설명
--------------------------------------------------------------------

모든 작업은 처리될 때마다 80% 성공 / 20% 실패로 랜덤 판정된다.
실패하면 Worker가 RESULT=FAIL을 Master에 보내고, Master는 그 key를
KVStore의 priorityQueue(최우선 큐)에 등록하면서 실패시킨 Worker를
기록한다.

1) Master는 다음 작업을 꺼낼 때 priorityQueue를 항상 먼저 꺼낸다.
2) 받을 Worker는 Least Queue First로 고르되, 직전에 그 작업을
   실패시킨 Worker는 제외한다.
3) TASK 메시지에 retry=true를 실어 보내면, 받은 Worker는 이 작업을
   Ready Queue 맨 앞에 넣어 바로 처리하고 P2P 이전 대상에서도 뺀다.
4) 재할당된 작업에도 같은 80/20 규칙이 적용되고, 또 실패하면 1)부터
   반복한다. 성공할 때까지 반복하므로 데이터 유실은 없다.

큐 초과 거절은 장애와 구분한다. Worker의 Ready Queue가 이미 10개면
받은 작업을 즉시 거절하고 Master에 RESULT=REJECTED로 알린다. Master는
이 작업을 원래 있던 큐로 되돌리고, 장애 재할당 횟수(reassignCount)와
따로 큐 초과 거절 횟수(queueRejectCount)로 센다.

장점: 성공 확률이 80%라 몇 번 안에 대부분 성공하고, 구현도 단순함
아쉬운 점: 최대 재시도 횟수 제한이 없어서 이론상 계속 실패할 수도 있음

--------------------------------------------------------------------
8. 추가 구현 사항 및 기타 언급할 내용
--------------------------------------------------------------------

- 접속 직후 REGISTER 메시지로 자기 workerId를 Master에 알림
- 접속 후 5초 안에 REGISTER가 안 오면 그 연결은 끊음
- 장애 재할당 수 / 큐 초과 거절 수 / P2P 이전 횟수를 따로 집계해서 기록
- VirtualClock은 메시지 보낼 때마다 clock 값을 같이 보내서, 받는 쪽이
  자기 시계와 비교해 더 큰 값을 채택하는 방식으로 맞춤
- 통신 지연 1초는 실제 데이터를 옮기는 메시지(TASK, SHUTDOWN, RESULT,
  STATS, P2P_TRANSFER)에만 적용한다. 큐 크기 보고(QUEUE)와 P2P 조회/응답
  (P2P_QUERY, P2P_STATUS, P2P_ACK)은 상태 확인용 제어 메시지라 지연 없이
  현재 시각만 실어 보낸다 (작업마다 오가는 보고에까지 1초씩 붙이면 총
  수행시간이 의미 없이 부풀기 때문)
- 노드마다 시계를 따로 굴리므로 Master가 찍은 시각과 Worker가 찍은 시각의
  차이는 통신 지연 1초와 일치하지 않는다. 작업이 Worker 쪽에서 밀린
  만큼 더 벌어지는 것이고, "보낸 시각 <= 받은 시각"은 항상 지켜진다
- 한 노드 안에서도 여러 Thread가 같은 로그 파일에 쓰기 때문에, 시각을 읽은
  순서와 파일에 쓴 순서가 어긋날 수 있다. 이때는 직전 줄의 시각으로 올려
  찍어서 한 파일 안의 시각이 거꾸로 가지 않게 한다
- Total tasks received는 큐가 꽉 차 거절한 건도 포함된 값이라,
  실제로 큐에 들어간 수는 옆에 (accepted N)으로 같이 적는다
- P2P send events는 내가 이전을 건 횟수만 센다 (받은 쪽은 미포함)

package kvstore.worker;

import kvstore.common.Constants;
import kvstore.common.FileLogger;
import kvstore.common.Message;
import kvstore.common.VirtualClock;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Worker Node 1개 = Thread 1개로 구현한 클래스.
 *
 * 설계 요약:
 *  - 소켓에서 데이터를 "읽는" blocking 작업은 MasterLink / P2PServer라는 보조 Thread에 맡긴다.
 *  - 이 클래스의 run() 안에 있는 메인 루프가 진짜 "Worker의 두뇌" 역할을 한다:
 *      1) Master가 보낸 메시지 처리 (TASK 수신, SHUTDOWN)
 *      2) 큐에서 작업 하나 꺼내서 처리 (성공/실패 판정, 결과 전송)
 *      3) 주기적으로 "내가 과부하 상태인지" 체크해서 필요하면 P2P로 옆 Worker에 작업을 넘김
 */
public class WorkerThread extends Thread {

    private final int workerId;
    private final String masterHost;
    private final int masterPort;
    private final int myP2PPort;
    private final int[] peerP2PPorts; // 다른 Worker들의 P2P 포트 목록 (나 자신은 제외)
    private final String peerHost;    // 기본 가정: 모든 Worker가 같은 PC(localhost)에서 실행

    private final ReadyQueue readyQueue = new ReadyQueue();
    private final VirtualClock clock = new VirtualClock();
    private final FileLogger log;
    private final Random random = new Random();

    private final BlockingQueue<Message> inboxFromMaster = new LinkedBlockingQueue<>();
    private MasterLink masterLink;
    private P2PServer p2pServer;
    private volatile boolean shutdownRequested = false;

    // 성능 통계를 위한 누적 값
    private int totalReceived = 0;
    private int totalSuccess = 0;
    private int totalFail = 0;
    private int retryReceived = 0;  // Master에게 재할당(retry=true)으로 받은 작업 수
    private int queueRejected = 0;  // 큐가 가득 차서 거절한 작업 수
    private double totalWaitTime = 0.0; // 평균 대기시간 계산용 (합계)
    private int waitSampleCount = 0;
    private final AtomicInteger p2pSent = new AtomicInteger(0);
    private final AtomicInteger p2pReceived = new AtomicInteger(0);
    private final AtomicInteger p2pEvents = new AtomicInteger(0); // P2P 이전이 일어난 횟수 (p2pSent는 이전한 작업 건수)

    public WorkerThread(int workerId, String masterHost, int masterPort,
                         int myP2PPort, int[] peerP2PPorts, String peerHost) {
        this.workerId = workerId;
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.myP2PPort = myP2PPort;
        this.peerP2PPorts = peerP2PPorts;
        this.peerHost = peerHost;
        this.log = new FileLogger("Worker" + workerId + ".txt", "WORKER" + workerId);
        setName("Worker" + workerId);
    }

    @Override
    public void run() {
        try {
            log.log(clock.get(), "INIT", "INFO", "Worker" + workerId + " thread started. Connecting to Master...");

            connectToMaster();
            startP2PServer();

            double lastP2PCheckClock = clock.get();
            double nextP2PIntervalSec = randomP2PIntervalSeconds();

            while (!shutdownRequested) {
                // 1) Master로부터 온 메시지 전부 처리 (TASK / SHUTDOWN)
                // 1개만 꺼내면 "받자마자 바로 처리"되어 큐가 절대 쌓이지 못하므로,
                // 그 순간 inbox에 밀려있는 메시지를 전부 비운다 (처리는 여전히 2)에서 1개씩만).
                Message fromMaster;
                while ((fromMaster = inboxFromMaster.poll()) != null) {
                    handleMasterMessage(fromMaster);
                }

                // 2) 큐에 있는 작업 하나 처리
                Task task = readyQueue.poll();
                if (task != null) {
                    processTask(task);
                }

                // 3) 주기적으로 P2P 부하 분산 체크 (1~3초 랜덤 주기로 반복 점검)
                // 모든 시뮬레이션 시간은 가상 시계 기준이라, 진짜 벽시계(System.nanoTime())가
                // 아니라 이 Worker 자신의 VirtualClock 누적값을 기준으로
                // 점검 주기를 판단한다. (실시간 기준이면 프로그램이 실제로는 몇 초 만에 끝나버려서,
                // 점검 자체가 거의 실행될 기회가 없어짐)
                if (clock.get() - lastP2PCheckClock >= nextP2PIntervalSec) {
                    checkAndOffloadIfOverloaded();
                    lastP2PCheckClock = clock.get();
                    nextP2PIntervalSec = randomP2PIntervalSeconds();
                }

                // CPU를 100% 쓰는 busy-wait을 피하기 위한 아주 짧은 sleep.
                // (이건 "실시간 처리 지연 시뮬레이션"이 아니라 순수 엔지니어링 목적이라 System Clock과 무관하다)
                Thread.sleep(5);
            }

            printFinalStatsAndClose();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /** P2P 부하 체크 주기: 1~3초 사이 랜덤. VirtualClock 기준(가상 초). */
    private double randomP2PIntervalSeconds() {
        int seconds = Constants.P2P_CHECK_INTERVAL_MIN
                + random.nextInt(Constants.P2P_CHECK_INTERVAL_MAX - Constants.P2P_CHECK_INTERVAL_MIN + 1);
        return seconds;
    }

    private void connectToMaster() throws IOException {
        Socket socket = new Socket(masterHost, masterPort);
        masterLink = new MasterLink(socket, inboxFromMaster);

        // Master는 accept() 순서가 아니라 이 REGISTER 메시지로 워커를 구분하므로, 접속 직후
        // 내가 어떤 workerId인지 먼저 알려줘야 한다. (accept 순서로 번호를 매기면 Master.txt의
        // WorkerN과 실제 WorkerN.txt가 서로 다른 물리 스레드를 가리킬 수 있다)
        Message register = new Message("REGISTER");
        register.set("workerId", String.valueOf(workerId));
        masterLink.send(register);

        masterLink.start();
        log.log(clock.advance(0.01), "CONNECT", "SUCCESS",
                "Connected to Master. Ready Queue initialized (0/10).");
    }

    private void startP2PServer() {
        p2pServer = new P2PServer(myP2PPort, readyQueue, log, clock, p2pReceived);
        p2pServer.start();
    }

    private void handleMasterMessage(Message msg) {
        clock.sync(msg.getDouble("clock", 0));

        switch (msg.getType()) {
            case "TASK": {
                String key = msg.get("key");
                int value = msg.getInt("value");
                boolean isRetry = Boolean.parseBoolean(msg.get("retry"));
                totalReceived++;

                if (isRetry) {
                    retryReceived++;
                    log.log(clock.get(), "RECV", "INFO",
                            "Received PRIORITY task (reassigned): KV[" + key + "] (Value=" + value + ")");
                } else {
                    log.log(clock.get(), "RECV", "INFO",
                            "Received task: KV[" + key + "] (Value=" + value + ")");
                }

                boolean accepted = readyQueue.offer(new Task(key, value, isRetry, clock.get()));
                if (!accepted) {
                    // 큐 초과 -> 즉시 FAIL 처리 후 Master에 통지 (10개 초과는 즉시 FAIL)
                    // Master가 20% 처리 실패와 구분해서 셀 수 있도록 결과는 REJECTED로 보낸다.
                    queueRejected++;
                    log.log(clock.get(), "QUEUE", "WARN", "Queue full (10/10). New task request rejected.");
                    log.log(clock.get(), "RECV", "FAIL",
                            "KV[" + key + "] rejected - queue overflow (10/10). Master notified.");
                    sendResult(key, "REJECTED");
                } else {
                    logQueueWarnIfNeeded();
                    reportQueueSize();
                }
                break;
            }
            case "SHUTDOWN":
                shutdownRequested = true;
                break;
            default:
                break;
        }
    }

    /** 작업 하나를 "처리"한다. 실제 sleep 대신 VirtualClock에 가상의 처리시간만 더한다. */
    private void processTask(Task task) {
        int procTime = Constants.PROC_TIME_MIN + random.nextInt(Constants.PROC_TIME_MAX - Constants.PROC_TIME_MIN + 1);

        double startClock = clock.get();
        totalWaitTime += (startClock - task.enqueueClock);
        waitSampleCount++;

        double t = clock.advance(procTime);
        log.log(startClock, "PROC", "INFO", "Processing " + (task.isRetry ? "retry " : "") + "KV[" + task.key
                + "]... estimated time=" + procTime + "s" + (task.isRetry ? " (priority queue)" : ""));

        boolean success = random.nextDouble() < Constants.SUCCESS_RATE; // 80% 성공 / 20% 실패
        if (success) {
            totalSuccess++;
            log.log(t, "PROC", "SUCCESS", "KV[" + task.key + "] stored"
                    + (task.isRetry ? " on retry" : "") + ". time=" + procTime + "s");
            sendResult(task.key, "SUCCESS");
        } else {
            totalFail++;
            log.log(t, "PROC", "FAIL", "KV[" + task.key + "] FAILED (20% rule). Sending FAIL to Master.");
            sendResult(task.key, "FAIL");
        }
        logQueueWarnIfNeeded();
        reportQueueSize();
    }

    private void sendResult(String key, String status) {
        // 모든 노드 간 통신은 1초 지연을 가상으로 반영 (네트워크 지연 1초 고정 시뮬레이션)
        double t = clock.advance(Constants.NETWORK_DELAY);
        Message msg = new Message("RESULT");
        msg.set("key", key);
        msg.set("status", status);
        msg.set("clock", String.valueOf(t));
        masterLink.send(msg);
    }

    private void reportQueueSize() {
        double t = clock.advance(Constants.NETWORK_DELAY);
        Message msg = new Message("QUEUE");
        msg.set("size", String.valueOf(readyQueue.size()));
        msg.set("clock", String.valueOf(t));
        masterLink.send(msg);
    }

    /** 큐가 70%(=7/10)를 초과한 상태에서 작업이 들고날 때마다 매번 WARN을 남긴다. */
    private void logQueueWarnIfNeeded() {
        logQueueWarnIfNeeded(readyQueue.size());
    }

    private void logQueueWarnIfNeeded(int size) {
        if (size > Constants.QUEUE_MAX * Constants.QUEUE_WARN_RATIO) {
            log.log(clock.get(), "QUEUE", "WARN", "Queue over 70% (" + size + "/10).");
        }
    }

    /** 로그용: 작업 목록을 "KV[a3f7], KV[0b12]" 형태로 나열한다. */
    private static String describeKeys(List<Task> tasks) {
        StringBuilder sb = new StringBuilder();
        for (Task t : tasks) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append("KV[").append(t.key).append("]");
        }
        return sb.toString();
    }

    /**
     * P2P 부하 분산 판단부.
     *
     * ===== 알고리즘 =====
     *   1) 내 큐 크기 * 평균 처리시간(2초) = 예상 대기시간을 계산한다.
     *   2) 예상 대기시간이 임계값(15초)을 넘으면, peer들을 순서대로 조회해서
     *      나보다 큐가 더 적은 첫 번째 peer를 찾는다.
     *   3) 그 peer에게 (내 큐 크기 - peer 큐 크기)/2 만큼 작업을 넘긴다.
     */
    private void checkAndOffloadIfOverloaded() {
        int mySize = readyQueue.size();
        double expectedWait = mySize * Constants.P2P_AVG_PROC_TIME;
        if (expectedWait <= Constants.P2P_WAIT_THRESHOLD) {
            return; // 아직 여유 있음 -> 이번 주기엔 아무것도 안 함
        }

        for (int peerPort : peerP2PPorts) {
            Integer peerSize = queryPeerQueueSize(peerPort);
            if (peerSize != null && peerSize < mySize) {
                int moveCount = (mySize - peerSize) / 2;
                if (moveCount <= 0) {
                    continue;
                }

                List<Task> toMove = readyQueue.pollFromTail(moveCount);
                if (toMove.isEmpty()) {
                    // 뒤쪽이 전부 재할당 작업이라 넘길 게 없음 -> 이 peer는 건너뛰고 다음 peer 시도.
                    continue;
                }
                // 큐가 70% 초과한 상태에서 작업이 "들고날 때마다 매번" WARN.
                // 여러 개를 한 번에 뺐으므로, 한 개씩 빠져나간 직후의 크기마다 체크한다.
                int afterRemoval = readyQueue.size();
                for (int i = toMove.size() - 1; i >= 0; i--) {
                    logQueueWarnIfNeeded(afterRemoval + i);
                }

                int peerId = peerPort - (myP2PPort - workerId); // P2P 포트 = 기준 포트 + workerId
                int acked = transferTasksToPeer(peerPort, toMove);
                p2pSent.addAndGet(acked);
                if (acked > 0) {
                    p2pEvents.incrementAndGet();
                }

                // 동시성 문제 대응: peer에게 물어본 시점과 실제로 전송한 시점 사이에
                // 상황이 바뀌어서(Master가 그 사이 peer에게 새 작업을 배급했거나, 다른 Worker도
                // 동시에 같은 peer에게 넘기려고 했거나 등) peer 큐가 이미 차서 일부만 받아줬을 수 있다.
                // 못 받아들여진 나머지를 그냥 버리면 해당 KV가 영원히 유실되어 전체 시뮬레이션이
                // 끝나지 않게 되므로, 반드시 내 큐로 되돌려놓는다 (방금 뺀 자리라 100% 들어간다).
                // 받는 쪽(P2PServer)은 앞에서부터 받다가 멈추므로, acked 이후 작업이 못 받은 작업이다.
                if (acked < toMove.size()) {
                    for (int i = acked; i < toMove.size(); i++) {
                        readyQueue.offer(toMove.get(i));
                        logQueueWarnIfNeeded(); // 되돌아와서 다시 들어온 이벤트 (한 개마다)
                    }
                    log.log(clock.get(), "LB", "WARN",
                            (toMove.size() - acked) + " tasks rejected by Worker" + peerId
                                    + " (race condition), returned to my queue: "
                                    + describeKeys(toMove.subList(acked, toMove.size())));
                }
                reportQueueSize();

                log.log(clock.get(), "LB", "SUCCESS",
                        acked + " tasks transferred via P2P to Worker" + peerId + ": "
                                + describeKeys(toMove.subList(0, acked))
                                + ". Queue: " + mySize + "/10 -> " + readyQueue.size() + "/10");
                return; // 이번 주기엔 한 peer에게만 이전
            }
        }
    }

    /** peer Worker에게 "너 큐 몇 개야?" 물어보고 응답을 받는다 (짧은 동기 요청/응답). */
    private Integer queryPeerQueueSize(int peerPort) {
        try (Socket socket = new Socket(peerHost, peerPort);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            Message query = new Message("P2P_QUERY");
            query.set("fromId", String.valueOf(workerId));
            query.set("clock", String.valueOf(clock.advance(Constants.NETWORK_DELAY)));
            out.println(query.toLine());

            Message res = Message.parse(in.readLine());
            clock.sync(res.getDouble("clock", 0));
            return res.getInt("size");
        } catch (IOException e) {
            return null; // peer가 응답이 없으면 이번엔 넘기지 않는다.
        }
    }

    /** peer Worker에게 작업 목록을 실제로 전송하고, 몇 개가 받아들여졌는지(ACK) 확인한다. */
    private int transferTasksToPeer(int peerPort, List<Task> tasks) {
        if (tasks.isEmpty()) {
            return 0;
        }
        try (Socket socket = new Socket(peerHost, peerPort);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            StringBuilder keys = new StringBuilder();
            StringBuilder values = new StringBuilder();
            for (Task t : tasks) {
                if (keys.length() > 0) {
                    keys.append(";");
                    values.append(";");
                }
                keys.append(t.key);
                values.append(t.value);
            }

            // P2P 작업 이전도 노드 간 통신이므로 1초 지연을 가상으로 반영.
            double t = clock.advance(Constants.NETWORK_DELAY);
            Message transfer = new Message("P2P_TRANSFER");
            transfer.set("fromId", String.valueOf(workerId));
            transfer.set("keys", keys.toString());
            transfer.set("values", values.toString());
            transfer.set("clock", String.valueOf(t));
            out.println(transfer.toLine());

            Message ack = Message.parse(in.readLine());
            clock.sync(ack.getDouble("clock", 0));
            return ack.getInt("count");
        } catch (IOException e) {
            // 전송 자체가 실패해도(peer 다운 등) 0을 반환하면, 호출한 checkAndOffloadIfOverloaded()가
            // "acked(0) < toMove.size()" 조건에 걸려서 toMove 전체를 내 큐로 되돌려놓는다 -> 유실 없음.
            return 0;
        }
    }

    private void printFinalStatsAndClose() {
        double t = clock.get();
        double avgWait = waitSampleCount > 0 ? (totalWaitTime / waitSampleCount) : 0.0;

        log.log(t, "STAT", "INFO", "=== WORKER" + workerId + " FINAL STATISTICS ===");
        log.log(t, "STAT", "INFO", "Total tasks received    : " + totalReceived);
        log.log(t, "STAT", "INFO", "SUCCESS (처리량)        : " + totalSuccess);
        log.log(t, "STAT", "INFO", "FAIL (20% rule)         : " + totalFail);
        log.log(t, "STAT", "INFO", "Priority (retry) tasks  : " + retryReceived + " 건");
        log.log(t, "STAT", "INFO", "Queue overflow rejects  : " + queueRejected + " 건");
        log.log(t, "STAT", "INFO", String.format("Avg waiting time        : %.2f sec", avgWait));
        log.log(t, "STAT", "INFO", "P2P load balance events : " + p2pEvents.get() + " 회");
        log.log(t, "STAT", "INFO", "P2P tasks transferred   : " + p2pSent.get() + " (sent) / " + p2pReceived.get() + " (recv)");
        log.log(t, "STAT", "INFO", String.format("Total execution time    : %.2f sec", t));

        // Master가 최종 STAT 로그에 "노드별 처리 통계"를 남길 수 있도록 종료 전에 요약 통계를 보고한다.
        Message stats = new Message("STATS");
        stats.set("received", String.valueOf(totalReceived));
        stats.set("success", String.valueOf(totalSuccess));
        stats.set("fail", String.valueOf(totalFail));
        stats.set("avgWait", String.valueOf(avgWait));
        stats.set("p2pSent", String.valueOf(p2pSent.get()));
        stats.set("p2pReceived", String.valueOf(p2pReceived.get()));
        stats.set("p2pEvents", String.valueOf(p2pEvents.get()));
        stats.set("retryReceived", String.valueOf(retryReceived));
        stats.set("rejected", String.valueOf(queueRejected));
        stats.set("totalTime", String.valueOf(t));
        // STATS 전송도 다른 Worker->Master 통신(RESULT)과 동일하게 1초 네트워크 지연을 반영한다.
        // (totalTime은 이 지연이 섞이기 전, Worker 자신이 실제로 종료한 시각을 남기기 위해 t를 그대로 쓴다)
        double sendClock = clock.advance(Constants.NETWORK_DELAY);
        stats.set("clock", String.valueOf(sendClock));
        masterLink.send(stats);

        log.log(clock.get(), "TERMINATE", "SUCCESS",
                "Worker" + workerId + " gracefully disconnected from Master.");
        log.close();

        // 소켓을 명시적으로 닫아야 Master 쪽도 정상 종료되고, 이 프로세스의 JVM도 깨끗하게 끝난다.
        masterLink.close();
        p2pServer.shutdown();
    }
}

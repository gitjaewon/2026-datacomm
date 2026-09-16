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
 * Worker Node 1개 = Thread 1개.
 * "필수 구현 조건: 모든 Worker Node는 독립적인 Thread를 통해 구현되어야 함"을 만족시키는 클래스.
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
    private final String peerHost;    // 이번 과제 기본 가정: 모든 Worker가 같은 PC(localhost)에서 실행

    private final ReadyQueue readyQueue = new ReadyQueue();
    private final VirtualClock clock = new VirtualClock();
    private final FileLogger log;
    private final Random random = new Random();

    private final BlockingQueue<Message> inboxFromMaster = new LinkedBlockingQueue<>();
    private MasterLink masterLink;
    private P2PServer p2pServer;
    private volatile boolean shutdownRequested = false;

    // 3장 성능 평가 지표를 위한 누적 통계
    private int totalReceived = 0;
    private int totalSuccess = 0;
    private int totalFail = 0;
    private double totalWaitTime = 0.0; // 평균 대기시간 계산용 (합계)
    private int waitSampleCount = 0;
    private final AtomicInteger p2pSent = new AtomicInteger(0);
    private final AtomicInteger p2pReceived = new AtomicInteger(0);

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

            long lastP2PCheckNanos = System.nanoTime();
            long nextP2PIntervalNanos = randomP2PIntervalNanos();

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

                // 3) 주기적으로 P2P 부하 분산 체크 (과제 명세: "1~3초 랜덤 주기로 반복 점검")
                if (System.nanoTime() - lastP2PCheckNanos > nextP2PIntervalNanos) {
                    checkAndOffloadIfOverloaded();
                    lastP2PCheckNanos = System.nanoTime();
                    nextP2PIntervalNanos = randomP2PIntervalNanos();
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

    /** P2P 부하 체크 주기: 1~3초 사이 랜덤 (과제 명세 0-4 요구사항). */
    private long randomP2PIntervalNanos() {
        int seconds = Constants.P2P_CHECK_INTERVAL_MIN
                + random.nextInt(Constants.P2P_CHECK_INTERVAL_MAX - Constants.P2P_CHECK_INTERVAL_MIN + 1);
        return seconds * 1_000_000_000L;
    }

    private void connectToMaster() throws IOException {
        Socket socket = new Socket(masterHost, masterPort);
        masterLink = new MasterLink(socket, inboxFromMaster);

        // Master는 accept() 순서로만 워커를 구분하므로, 접속 직후 내가 어떤 workerId인지
        // 먼저 알려줘야 한다. (안 그러면 accept 순서가 뒤섞일 때 Master.txt의 WorkerN과
        // 실제 WorkerN.txt가 서로 다른 물리 스레드를 가리키게 된다)
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

                log.log(clock.get(), "RECV", "INFO",
                        "Received task: KV[" + key + "] (Value=" + value + ")" + (isRetry ? " [PRIORITY]" : ""));

                boolean accepted = readyQueue.offer(new Task(key, value, isRetry, false, clock.get()));
                if (!accepted) {
                    // 큐 초과 -> 즉시 FAIL 처리 후 Master에 통지 (과제 0-1: 10개 초과는 즉시 FAIL)
                    log.log(clock.get(), "QUEUE", "WARN", "Queue full (10/10). New task request rejected.");
                    sendResult(key, "FAIL");
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
        log.log(startClock, "PROC", "INFO", "Processing KV[" + task.key + "]... estimated time=" + procTime + "s");

        boolean success = random.nextDouble() < Constants.SUCCESS_RATE; // 80% 성공 / 20% 실패
        if (success) {
            totalSuccess++;
            log.log(t, "PROC", "SUCCESS", "KV[" + task.key + "] stored. time=" + procTime + "s");
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
        // 모든 노드 간 통신은 1초 지연을 가상으로 반영 (과제 명세: "네트워크 지연은 1초로 고정 시뮬레이션")
        double t = clock.advance(Constants.NETWORK_DELAY);
        Message msg = new Message("RESULT");
        msg.set("key", key);
        msg.set("status", status);
        msg.set("clock", String.valueOf(t));
        masterLink.send(msg);
    }

    private void reportQueueSize() {
        Message msg = new Message("QUEUE");
        msg.set("size", String.valueOf(readyQueue.size()));
        msg.set("clock", String.valueOf(clock.get()));
        masterLink.send(msg);
    }

    /** 과제 0-1 표: 큐가 70%(=7/10)를 초과한 상태에서 작업이 들고날 때마다 매번 WARN을 남긴다. */
    private void logQueueWarnIfNeeded() {
        int size = readyQueue.size();
        if (size > Constants.QUEUE_MAX * Constants.QUEUE_WARN_RATIO) {
            log.log(clock.get(), "QUEUE", "WARN", "Queue over 70% (" + size + "/10).");
        }
    }

    /**
     * P2P 부하 분산 판단부 (0-4 P2P 부하 분산 동작 예시에 대응).
     *
     * ===== 지금 구현되어 있는 알고리즘 (기본 예시) =====
     *   1) 내 큐 크기 * 평균 처리시간(2초) = 예상 대기시간을 계산한다.
     *   2) 예상 대기시간이 임계값(15초)을 넘으면, peer들을 순서대로 조회해서
     *      나보다 큐가 더 적은 첫 번째 peer를 찾는다.
     *   3) 그 peer에게 (내 큐 크기 - peer 큐 크기)/2 만큼 작업을 넘긴다.
     *
     * ===== TODO (채점 포인트 / 학습 포인트) =====
     *   위 알고리즘은 예시일 뿐이다. 예를 들어 "가장 여유로운 peer를 찾기", "한 번에 넘기는 개수를
     *   다르게 계산하기", "여러 peer에 나눠서 넘기기" 등으로 바꿔도 된다.
     *   단, Readme.txt에 어떤 알고리즘을 썼는지 + 장단점을 반드시 작성해야 한다 (필수 제출 항목).
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
                // 과제 0-1: 큐가 70% 초과한 상태에서 작업이 "들고날 때마다 매번" WARN.
                // 여기선 작업이 큐에서 빠져나가는(P2P로 이전되는) 이벤트이므로 나간 직후 체크한다.
                logQueueWarnIfNeeded();

                int acked = transferTasksToPeer(peerPort, toMove);
                p2pSent.addAndGet(acked);

                // 동시성 문제 대응: peer에게 물어본 시점과 실제로 전송한 시점 사이에
                // 상황이 바뀌어서(Master가 그 사이 peer에게 새 작업을 배급했거나, 다른 Worker도
                // 동시에 같은 peer에게 넘기려고 했거나 등) peer 큐가 이미 차서 일부만 받아줬을 수 있다.
                // 못 받아들여진 나머지를 그냥 버리면 해당 KV가 영원히 유실되어 전체 시뮬레이션이
                // 끝나지 않게 되므로, 반드시 내 큐로 되돌려놓는다 (방금 뺀 자리라 100% 들어간다).
                if (acked < toMove.size()) {
                    for (int i = acked; i < toMove.size(); i++) {
                        readyQueue.offer(toMove.get(i));
                    }
                    logQueueWarnIfNeeded(); // 되돌아와서 다시 들고난 이벤트
                    log.log(clock.get(), "LB", "WARN",
                            (toMove.size() - acked) + " tasks rejected by peer (race condition), returned to my queue.");
                }
                reportQueueSize();

                log.log(clock.get(), "LB", "SUCCESS",
                        acked + " tasks transferred via P2P. Queue: " + mySize + "/10 -> " + readyQueue.size() + "/10");
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
            query.set("clock", String.valueOf(clock.get()));
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
        log.log(t, "STAT", "INFO", "Total tasks received  : " + totalReceived);
        log.log(t, "STAT", "INFO", "SUCCESS                : " + totalSuccess);
        log.log(t, "STAT", "INFO", "FAIL                    : " + totalFail);
        log.log(t, "STAT", "INFO", String.format("Avg waiting time        : %.2f sec", avgWait));
        log.log(t, "STAT", "INFO", "P2P tasks transferred   : " + p2pSent.get() + " (sent) / " + p2pReceived.get() + " (recv)");
        log.log(t, "STAT", "INFO", "Total execution time    : " + t + " sec");

        // Master가 최종 STAT 로그에 "노드별 처리 통계"를 남길 수 있도록 종료 전에 요약 통계를 보고한다.
        Message stats = new Message("STATS");
        stats.set("received", String.valueOf(totalReceived));
        stats.set("success", String.valueOf(totalSuccess));
        stats.set("fail", String.valueOf(totalFail));
        stats.set("avgWait", String.valueOf(avgWait));
        stats.set("p2pSent", String.valueOf(p2pSent.get()));
        stats.set("p2pReceived", String.valueOf(p2pReceived.get()));
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

package kvstore.master;

import kvstore.common.Constants;
import kvstore.common.FileLogger;
import kvstore.common.Message;
import kvstore.common.VirtualClock;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Master Node.
 *
 * AWS/GCP 등 물리적 외부 서버에서 실행하는 것을 전제로 한다.
 *
 * 실행 예) java kvstore.master.Master 5000
 *
 * 흐름:
 *  1) 5,000개의 (Key, Value) 생성 (KVStore)
 *  2) Worker 4개의 접속을 받는다 (accept)
 *  3) 각 Worker의 큐 상태를 보면서 동적으로 작업을 분배한다 (WorkloadScheduler)
 *  4) Worker가 실패(FAIL)를 보고하면 최우선 큐에 등록 후 재할당한다
 *  5) 5,000개 모두 처리 완료되면 통계를 로그에 남기고 종료한다 (Graceful Termination)
 */
public class Master {

    private final KVStore kvStore = new KVStore();
    private final WorkloadScheduler scheduler = new WorkloadScheduler();
    private final VirtualClock clock = new VirtualClock();
    private final FileLogger log = new FileLogger("Master.txt", "MASTER");

    private final Map<Integer, ClientHandler> workers = new ConcurrentHashMap<>();
    private final CountDownLatch allWorkersConnected = new CountDownLatch(Constants.NUM_WORKERS);

    // 장애로 인해 다른 Worker에 재할당된 횟수 (FAIL -> requeue 시에만 카운트,
    // 큐가 가득 차서 잠시 대기시키는 경우는 "장애"가 아니므로 카운트하지 않는다)
    private final AtomicInteger reassignCount = new AtomicInteger(0);
    // Worker 큐가 가득 차서 거절된 횟수 (장애가 아니므로 reassignCount와 따로 센다)
    private final AtomicInteger queueRejectCount = new AtomicInteger(0);

    // Worker가 종료 직전에 STATS 메시지로 보고하는 최종 통계 (Master.txt 최종 STAT에 노드별로 함께 기록하기 위함)
    private final Map<Integer, WorkerFinalStat> workerFinalStats = new ConcurrentHashMap<>();
    private final CountDownLatch statsReceived = new CountDownLatch(Constants.NUM_WORKERS);

    /** Worker가 보고한 종료 시점 통계 한 벌 (Worker STATS 메시지 필드 그대로). */
    private static class WorkerFinalStat {
        final int received, success, fail, p2pSent, p2pReceived, p2pEvents, retryReceived, rejected;
        final double avgWait, totalTime;

        WorkerFinalStat(int received, int success, int fail, double avgWait,
                         int p2pSent, int p2pReceived, int p2pEvents,
                         int retryReceived, int rejected, double totalTime) {
            this.received = received;
            this.success = success;
            this.fail = fail;
            this.avgWait = avgWait;
            this.p2pSent = p2pSent;
            this.p2pReceived = p2pReceived;
            this.p2pEvents = p2pEvents;
            this.retryReceived = retryReceived;
            this.rejected = rejected;
            this.totalTime = totalTime;
        }
    }

    public VirtualClock getClock() {
        return clock;
    }

    public WorkloadScheduler getScheduler() {
        return scheduler;
    }

    /**
     * Worker가 QUEUE 메시지로 자기 큐 크기를 보고해올 때마다 호출됨.
     * 큐가 최대치(10)에 도달한 순간을 여기서 감지해서 WARN 로그를 남긴다.
     */
    public void onWorkerQueueUpdate(int workerId, int size, int recv) {
        scheduler.updateQueueSize(workerId, size, recv);
        if (size >= Constants.QUEUE_MAX) {
            log.log(clock.get(), "DISTRIB", "WARN",
                    "Worker" + workerId + " queue full (" + size + "/10). Pausing dispatch.");
        }
    }

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 5000;
        new Master().start(port);
    }

    public void start(int port) throws Exception {
        log.log(clock.get(), "INIT", "SUCCESS", "System Clock started.");

        log.log(clock.get(), "INIT", "INFO",
                "Generating 5,000 KV pairs... Key=hex4(unique), Value=rand(1~100)");
        kvStore.generateAll();
        log.log(clock.get(), "INIT", "SUCCESS",
                "5,000 KV pairs generated. Waiting for Worker connections.");

        acceptWorkers(port);
        allWorkersConnected.await(); // 4개 Worker가 전부 연결될 때까지 대기

        log.log(clock.get(), "INIT", "INFO", "All 4 Workers connected. Starting workload distribution.");

        runDispatchLoop();

        shutdownAll();
    }

    /** Worker 접속을 받아들이는 부분. Worker 1개당 ClientHandler Thread를 1개씩 만든다. */
    private void acceptWorkers(int port) throws IOException {
        ServerSocket serverSocket = new ServerSocket(port);
        Thread acceptThread = new Thread(() -> {
            int registered = 0;
            while (registered < Constants.NUM_WORKERS) {
                Socket socket = null;
                try {
                    socket = serverSocket.accept();

                    // AWS 등 공인 IP에 포트를 열어두면 포트 스캐너/헬스체크 같은 엉뚱한 접속이
                    // 먼저 들어올 수 있다. REGISTER를 안 보내는 연결에서 무한정 readLine()으로
                    // 블로킹되면 진짜 Worker 4개가 나중에 접속해도 영원히 처리되지 않으므로,
                    // 짧은 타임아웃을 걸어서 REGISTER를 안 보내는 연결은 버리고 계속 accept한다.
                    socket.setSoTimeout(5000);

                    // accept() 순서는 OS 스케줄링에 따라 스레드 시작 순서와 다를 수 있어서,
                    // Worker가 접속 직후 보내는 REGISTER 메시지로 "진짜" workerId를 받아야
                    // Master.txt의 WorkerN과 실제 WorkerN.txt가 같은 물리 스레드를 가리킨다.
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String line = in.readLine();
                    Message register = (line == null) ? null : Message.parse(line);
                    if (register == null || !"REGISTER".equals(register.getType())) {
                        socket.close();
                        continue; // 진짜 Worker가 아니므로 카운트하지 않고 다음 접속을 계속 받는다.
                    }
                    int workerId = register.getInt("workerId");
                    socket.setSoTimeout(0); // 등록 완료 -> 평상시처럼 무제한 대기로 복귀

                    scheduler.registerWorker(workerId);

                    ClientHandler handler = new ClientHandler(workerId, socket, in, this);
                    workers.put(workerId, handler);
                    handler.start();

                    log.log(clock.advance(0.01), "CONNECT", "SUCCESS",
                            "Worker" + workerId + " connected. Ready Queue initialized (0/10).");
                    allWorkersConnected.countDown();
                    registered++;
                } catch (Exception e) {
                    // REGISTER 타임아웃, 파싱 실패 등 -> 이 연결은 버리고 계속 accept (Worker 카운트 X)
                    System.err.println("[acceptWorkers] 연결 거부/등록 실패: " + e);
                    e.printStackTrace();
                    if (socket != null) {
                        try {
                            socket.close();
                        } catch (IOException ignored) {
                        }
                    }
                }
            }
            // 필요한 4개를 다 받았으니 더 이상 새 접속을 받지 않는다.
            try {
                serverSocket.close();
            } catch (IOException ignored) {
            }
        }, "AcceptThread");
        acceptThread.start();
    }

    /**
     * 핵심 동적 분배 루프.
     *
     * 재할당 대기 작업을 먼저 꺼내 큐가 가장 여유로운 Worker에게 보낸다.
     * 재할당 작업은 직전에 실패시킨 Worker를 제외하고 고르며, TASK에 retry=true로 표시해 보낸다.
     */
    private void runDispatchLoop() throws InterruptedException {
        while (!kvStore.isAllDone()) {
            KVStore.PendingKey pending = kvStore.nextPendingKey();
            if (pending == null) {
                // 아직 결과를 기다리는 중이라 지금 당장 보낼 새 작업이 없다.
                // 아래 sleep은 "실시간 처리 지연 시뮬레이션"이 아니라, 그냥 CPU를 100% 쓰는
                // busy-wait을 피하기 위한 것 뿐이다 (System Clock에는 영향 없음).
                Thread.sleep(20);
                continue;
            }

            int excludeWorkerId = pending.isRetry ? kvStore.lastFailedWorkerOf(pending.key) : -1;

            int workerId = scheduler.pickWorkerForDispatch(excludeWorkerId);
            if (workerId == -1) {
                // 보낼 수 있는 Worker가 없다 (전부 가득 찼거나, 제외한 Worker만 여유가 있음)
                // -> 원래 있던 큐로 돌려놓고 잠시 대기.
                kvStore.returnUndispatched(pending);
                Thread.sleep(20);
                continue;
            }

            ClientHandler handler = workers.get(workerId);
            int value = kvStore.valueOf(pending.key);
            int index = kvStore.indexOf(pending.key);

            // 통신 지연 1초를 "가상"으로 반영 (실제로 1초 기다리지 않는다)
            double t = clock.advance(Constants.NETWORK_DELAY);
            // 전송 전에 기록해야, Worker가 곧바로 거절 응답을 보내도 원래 큐를 알 수 있다.
            kvStore.markDispatched(pending);
            scheduler.onDispatched(workerId);
            handler.sendTask(pending.key, index, value, pending.isRetry, t);

            String label = kvLabel(index, pending.key);
            if (pending.isRetry) {
                String excluded = (excludeWorkerId == -1) ? "" : " (excluded Worker" + excludeWorkerId + ")";
                log.log(t, "DISTRIB", "SUCCESS", label + " reassigned to Worker"
                        + workerId + excluded + ".");
            } else {
                log.log(t, "DISTRIB", "INFO", "Dispatching " + label + " -> Worker" + workerId);
            }
        }
    }

    // 진행률 로그 주기. 동적 분배가 실제로 진행되고 있음을 로그에서 보여주기 위해
    // 완료 개수가 이 배수에 도달할 때마다 남긴다.
    private static final int PROGRESS_LOG_INTERVAL = 500;

    /** 로그 표기용: "KV[0001] (Key=a3f7)" 형태로 순번 인덱스와 hex key를 함께 보여준다. */
    private static String kvLabel(int index, String key) {
        return "KV[" + String.format("%04d", index) + "] (Key=" + key + ")";
    }

    /** ClientHandler가 RESULT 메시지를 받으면 호출하는 콜백. */
    public synchronized void onWorkerResult(int workerId, String key, String status) {
        String label = kvLabel(kvStore.indexOf(key), key);
        if ("SUCCESS".equals(status)) {
            kvStore.markSuccess(key);
            log.log(clock.get(), "RESULT", "SUCCESS",
                    label + " stored by Worker" + workerId + ". value=" + kvStore.valueOf(key));
            logProgressIfNeeded();
        } else if ("REJECTED".equals(status)) {
            // 큐 초과로 거절됨 -> 장애 재할당 횟수에 넣지 않고 원래 큐로 되돌린다.
            kvStore.requeueAfterReject(key);
            int n = queueRejectCount.incrementAndGet();
            log.log(clock.get(), "DISTRIB", "WARN",
                    label + " rejected by Worker" + workerId + " (queue overflow). Requeued (#" + n + ").");
        } else {
            kvStore.requeueAfterFail(key, workerId);
            int n = reassignCount.incrementAndGet();
            log.log(clock.get(), "RESULT", "FAIL",
                    label + " FAILED by Worker" + workerId + " (20% rule). Requeued for reassignment (#" + n + ").");
        }
    }

    /** ClientHandler가 Worker 종료 직전 STATS 메시지를 받으면 호출하는 콜백. */
    public void onWorkerStats(int workerId, Message msg) {
        workerFinalStats.put(workerId, new WorkerFinalStat(
                msg.getInt("received"), msg.getInt("success"), msg.getInt("fail"),
                msg.getDouble("avgWait", 0), msg.getInt("p2pSent"), msg.getInt("p2pReceived"),
                msg.getInt("p2pEvents"), msg.getInt("retryReceived"), msg.getInt("rejected"),
                msg.getDouble("totalTime", 0)));
        statsReceived.countDown();
    }

    private void logProgressIfNeeded() {
        int done = kvStore.doneCount();
        if (done % PROGRESS_LOG_INTERVAL == 0) {
            double t = clock.get();
            double percent = (done * 100.0) / Constants.TOTAL_KV_PAIRS;
            log.log(t, "DISTRIB", "INFO", String.format(
                    "Progress: %d / %d KV pairs stored (%.1f%%)", done, Constants.TOTAL_KV_PAIRS, percent));
            log.log(t, "DISTRIB", "INFO", scheduler.describeQueues());
        }
    }

    public void onWorkerDisconnected(int workerId) {
        log.log(clock.get(), "DISCONNECT", "WARN", "Worker" + workerId + " connection closed.");
    }

    private void shutdownAll() {
        double t = clock.advance(Constants.NETWORK_DELAY);
        for (ClientHandler h : workers.values()) {
            h.sendShutdown(t);
        }

        // 각 Worker가 SHUTDOWN을 받고 종료하기 직전 STATS를 보고할 때까지 잠시 대기한다.
        // (전부 이미 처리 완료된 상태라 실제로는 순식간에 도착한다. 타임아웃은 안전장치일 뿐.)
        try {
            statsReceived.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        double finalT = clock.get();

        // Worker별 통계를 먼저 합산해서 요약 -> Worker별 순서로 기록한다.
        int sumSuccess = 0, sumFail = 0, sumP2PTasks = 0, sumP2PEvents = 0, sumRetry = 0, sumRejected = 0;
        List<String> perWorkerLines = new ArrayList<>();
        List<Integer> missingStats = new ArrayList<>();
        for (int workerId = 1; workerId <= Constants.NUM_WORKERS; workerId++) {
            WorkerFinalStat s = workerFinalStats.get(workerId);
            if (s == null) {
                missingStats.add(workerId);
                continue;
            }
            sumSuccess += s.success;
            sumFail += s.fail;
            sumP2PTasks += s.p2pSent;
            sumP2PEvents += s.p2pEvents;
            sumRetry += s.retryReceived;
            sumRejected += s.rejected;
            perWorkerLines.add(String.format(
                    "Worker%d - received=%d (accepted=%d) success=%d fail=%d retryReceived=%d rejected=%d "
                            + "avgWait=%.2fs p2pSendEvents=%d p2pSent=%d p2pReceived=%d totalTime=%.2fs",
                    workerId, s.received, s.received - s.rejected, s.success, s.fail, s.retryReceived, s.rejected,
                    s.avgWait, s.p2pEvents, s.p2pSent, s.p2pReceived, s.totalTime));
        }

        // 완료된 KV 저장소 전체(5,000쌍)를 최종 로그에 남긴다.
        // STAT 요약보다 먼저 찍어서, 로그 파일 맨 끝은 한눈에 보기 좋은 통계 요약 + TERMINATE로 끝나게 한다.
        Map<String, Integer> finalStore = kvStore.snapshotStore();
        log.log(finalT, "KVSTORE", "INFO", "=== FINAL KV STORE DUMP (" + finalStore.size() + " pairs) ===");
        for (Map.Entry<String, Integer> e : finalStore.entrySet()) {
            log.log(finalT, "KVSTORE", "INFO",
                    kvLabel(kvStore.indexOf(e.getKey()), e.getKey()) + " = " + e.getValue());
        }

        log.log(finalT, "STAT", "INFO", "=== FINAL STATISTICS ===");
        log.log(finalT, "STAT", "INFO", "Total KV pairs processed     : " + kvStore.doneCount() + " / " + Constants.TOTAL_KV_PAIRS);
        log.log(finalT, "STAT", "INFO", "Total SUCCESS                : " + sumSuccess);
        log.log(finalT, "STAT", "INFO", "Total FAIL (then retried)    : " + sumFail);
        log.log(finalT, "STAT", "INFO", "Fault reassignments          : " + reassignCount.get() + " 건");
        log.log(finalT, "STAT", "INFO", "Priority tasks re-dispatched : " + sumRetry + " 건");
        log.log(finalT, "STAT", "INFO", "Queue overflow rejects       : " + queueRejectCount.get() + " 건 (worker report: " + sumRejected + ")");
        log.log(finalT, "STAT", "INFO", "P2P send events              : " + sumP2PEvents + " 회");
        log.log(finalT, "STAT", "INFO", "P2P tasks transferred        : " + sumP2PTasks + " 건");
        log.log(finalT, "STAT", "INFO", String.format("Total execution time         : %.2f sec", finalT));
        log.log(finalT, "STAT", "INFO", "--- Per-Worker Stats ---");
        for (String line : perWorkerLines) {
            log.log(finalT, "STAT", "INFO", "  " + line);
        }
        for (int workerId : missingStats) {
            log.log(finalT, "STAT", "WARN", "Worker" + workerId + " - no STATS report received before shutdown.");
        }

        log.log(finalT, "TERMINATE", "SUCCESS", "Graceful shutdown. All workers disconnected.");
        log.close();
    }
}

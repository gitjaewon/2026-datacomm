package kvstore.master;

import kvstore.common.Constants;
import kvstore.common.FileLogger;
import kvstore.common.VirtualClock;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Master Node.
 *
 * ※ 필수 구현 조건: 이 프로그램은 반드시 AWS/GCP 등 "물리적 외부 서버"에서 실행되어야 한다
 *    (로컬 PC에서 돌리면 0점 처리 대상이니 주의!).
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
    private final AtomicInteger nextWorkerId = new AtomicInteger(1);
    private final CountDownLatch allWorkersConnected = new CountDownLatch(Constants.NUM_WORKERS);

    public VirtualClock getClock() {
        return clock;
    }

    public WorkloadScheduler getScheduler() {
        return scheduler;
    }

    /**
     * Worker가 QUEUE 메시지로 자기 큐 크기를 보고해올 때마다 호출됨.
     * 강의자료 4장(로깅) 예시 로그: "[9.00] MASTER | DISTRIB | WARN | Worker1 queue full (10/10). Pausing dispatch."
     * 를 그대로 재현하기 위해, 큐가 최대치(10)에 도달한 순간을 여기서 감지해서 남긴다.
     */
    public void onWorkerQueueUpdate(int workerId, int size) {
        scheduler.updateQueueSize(workerId, size);
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
        log.log(clock.get(), "INIT", "INFO", "System Clock started.");

        log.log(clock.get(), "INIT", "INFO",
                "Generating 5,000 KV pairs... Key=hex4(unique), Value=rand(1~100)");
        kvStore.generateAll();
        log.log(clock.advance(0.02), "INIT", "SUCCESS",
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
            for (int i = 0; i < Constants.NUM_WORKERS; i++) {
                try {
                    Socket socket = serverSocket.accept();
                    int workerId = nextWorkerId.getAndIncrement();
                    scheduler.registerWorker(workerId);

                    ClientHandler handler = new ClientHandler(workerId, socket, this);
                    workers.put(workerId, handler);
                    handler.start();

                    log.log(clock.advance(0.01), "CONNECT", "SUCCESS",
                            "Worker" + workerId + " connected. Ready Queue initialized (0/10).");
                    allWorkersConnected.countDown();
                } catch (IOException e) {
                    e.printStackTrace();
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
     * TODO(학습 포인트): 지금은 "보낼 작업이 있으면 WorkloadScheduler가 골라준 최소 큐 Worker에게
     * 계속 보낸다"는 단순 구조다. 실제 채점 포인트는 WorkloadScheduler.pickWorkerForDispatch()의
     * 알고리즘이니, 필요하면 그 쪽을 고도화하면 된다 (이 메서드 자체는 크게 안 건드려도 된다).
     */
    private void runDispatchLoop() throws InterruptedException {
        while (!kvStore.isAllDone()) {
            String key = kvStore.nextPendingKey();
            if (key == null) {
                // 아직 결과를 기다리는 중이라 지금 당장 보낼 새 작업이 없다.
                // 아래 sleep은 "실시간 처리 지연 시뮬레이션"이 아니라, 그냥 CPU를 100% 쓰는
                // busy-wait을 피하기 위한 것 뿐이다 (System Clock에는 영향 없음).
                Thread.sleep(20);
                continue;
            }

            int workerId = scheduler.pickWorkerForDispatch();
            if (workerId == -1) {
                // 모든 Worker 큐가 가득 찼다 -> 이 key는 최우선 큐로 돌려놓고 잠시 대기.
                kvStore.requeueAsPriority(key);
                Thread.sleep(20);
                continue;
            }

            ClientHandler handler = workers.get(workerId);
            int value = kvStore.valueOf(key);

            // 통신 지연 1초를 "가상"으로 반영 (실제로 1초 기다리지 않는다)
            double t = clock.advance(Constants.NETWORK_DELAY);
            handler.sendTask(key, value, false, t);
            scheduler.onDispatchedOptimistically(workerId);

            log.log(t, "DISTRIB", "INFO", "Dispatching KV[" + key + "] -> Worker" + workerId);
        }
    }

    /** ClientHandler가 RESULT 메시지를 받으면 호출하는 콜백. */
    public synchronized void onWorkerResult(int workerId, String key, String status) {
        if ("SUCCESS".equals(status)) {
            kvStore.markSuccess(key);
            log.log(clock.get(), "RESULT", "SUCCESS",
                    "KV[" + key + "] stored by Worker" + workerId + ". value=" + kvStore.valueOf(key));
        } else {
            kvStore.requeueAsPriority(key);
            log.log(clock.get(), "RESULT", "FAIL",
                    "KV[" + key + "] FAILED by Worker" + workerId + " (20% rule). Requeing...");
        }
    }

    public void onWorkerDisconnected(int workerId) {
        log.log(clock.get(), "DISCONNECT", "WARN", "Worker" + workerId + " connection closed.");
    }

    private void shutdownAll() {
        double t = clock.get();
        for (ClientHandler h : workers.values()) {
            h.sendShutdown(t);
        }

        log.log(t, "STAT", "INFO", "=== FINAL STATISTICS ===");
        log.log(t, "STAT", "INFO", "Total KV pairs processed : " + kvStore.doneCount());
        log.log(t, "STAT", "INFO", "Total execution time     : " + t + " sec");
        // TODO: Worker별 처리/성공/실패 통계, P2P 이벤트 총합, 장애 재할당 총합 등
        //   3장의 6개 지표를 여기서 다 합쳐서 출력하고 싶다면, RESULT 처리 시점에
        //   Map<Integer, WorkerStat> 같은 걸 만들어 같이 누적해두면 된다.

        log.log(clock.advance(0.05), "TERMINATE", "SUCCESS", "Graceful shutdown. All workers disconnected.");
        log.close();
    }
}

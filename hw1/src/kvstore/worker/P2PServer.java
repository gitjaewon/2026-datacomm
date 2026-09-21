package kvstore.worker;

import kvstore.common.Constants;
import kvstore.common.FileLogger;
import kvstore.common.Message;
import kvstore.common.VirtualClock;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 다른 Worker가 보내는 P2P 요청(큐 상태 조회 P2P_QUERY, 작업 이전 P2P_TRANSFER)을
 * 받아서 그 자리에서 바로 응답해주는 리스너.
 *
 * ReadyQueue는 이미 synchronized로 thread-safe하게 만들어져 있으므로,
 * 이 Thread가 WorkerThread의 메인 루프와 동시에 접근해도 안전하다.
 *
 * (참고: MasterLink는 "읽기 전용 배달부"였지만, P2P 요청은 그 자리에서 즉시 응답해야
 *  하는 request-response 형태라 여기서는 읽기+쓰기를 한 번에 처리하는 구조로 만들었다.)
 */
public class P2PServer extends Thread {

    private final int port;
    private final ReadyQueue readyQueue;
    private final FileLogger log;
    private final VirtualClock clock;
    private final AtomicInteger p2pReceivedCounter; // WorkerThread와 공유하는 통계 카운터
    private final AtomicInteger masterTaskReceived; // Master에게서 받은 TASK 수 (QUEUE 보고용)
    private final MasterLink masterLink; // P2P로 받은 직후 새 큐 크기를 Master에 바로 보고하기 위함

    private volatile boolean running = true;
    private ServerSocket serverSocket;

    public P2PServer(int port, ReadyQueue readyQueue, FileLogger log, VirtualClock clock,
                      AtomicInteger p2pReceivedCounter, AtomicInteger masterTaskReceived, MasterLink masterLink) {
        this.port = port;
        this.readyQueue = readyQueue;
        this.log = log;
        this.clock = clock;
        this.p2pReceivedCounter = p2pReceivedCounter;
        this.masterTaskReceived = masterTaskReceived;
        this.masterLink = masterLink;
        setDaemon(true);
        setName("P2PServer-" + port);
    }

    @Override
    public void run() {
        try {
            serverSocket = new ServerSocket(port);
            while (running) {
                Socket socket = serverSocket.accept();
                handleOneRequest(socket);
            }
        } catch (IOException e) {
            // shutdown()에서 serverSocket을 닫으면 정상적으로 여기로 빠져나온다.
        }
    }

    private void handleOneRequest(Socket socket) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

            String line = in.readLine();
            if (line == null) {
                return;
            }
            Message req = Message.parse(line);
            // P2P 메시지도 다른 통신과 동일하게 clock을 실어 보내고 sync한다 (VirtualClock 설계 의도).
            clock.sync(req.getDouble("clock", 0));

            if ("P2P_QUERY".equals(req.getType())) {
                // 상대 Worker가 "네 큐 크기가 몇이야?" 라고 물어봄 -> 바로 답해준다.
                Message res = new Message("P2P_STATUS");
                res.set("size", String.valueOf(readyQueue.size()));
                res.set("clock", String.valueOf(clock.get()));
                out.println(res.toLine());

            } else if ("P2P_TRANSFER".equals(req.getType())) {
                // 상대 Worker가 작업 몇 개를 나에게 떠넘김 -> 앞에서부터 담다가 하나라도 못 담으면 멈추고,
                // 담은 개수만큼 ACK. 보낸 쪽은 "앞에서부터 ACK 개수만큼 받아졌다"고 보고 나머지를
                // 되돌리므로, 중간에 건너뛰고 뒤의 작업을 받으면 작업 유실/중복이 생긴다.
                String[] keys = req.get("keys").split(";");
                String[] indexes = req.get("indexes").split(";");
                String[] values = req.get("values").split(";");
                int accepted = 0;
                StringBuilder acceptedKeys = new StringBuilder();
                for (int i = 0; i < keys.length; i++) {
                    boolean ok = readyQueue.offer(new Task(keys[i], Integer.parseInt(indexes[i]),
                            Integer.parseInt(values[i]), false, clock.get()));
                    if (ok) {
                        accepted++;
                        if (acceptedKeys.length() > 0) {
                            acceptedKeys.append(", ");
                        }
                        acceptedKeys.append(Task.label(Integer.parseInt(indexes[i]), keys[i]));
                    } else {
                        break;
                    }
                }
                p2pReceivedCounter.addAndGet(accepted);

                // 한 건도 못 받았으면 이전이 일어난 게 아니므로 로그를 남기지 않는다.
                if (accepted > 0) {
                    int newSize = readyQueue.size();
                    log.log(clock.get(), "LB", "SUCCESS",
                            "Received " + accepted + " tasks via P2P from Worker" + req.get("fromId") + ": "
                                    + acceptedKeys + ". Queue: " + newSize + "/10");

                    // 한 개씩 들어온 직후의 크기마다 WARN. 위 LB 로그 뒤에 찍어야 순서가 맞는다.
                    for (int k = 1; k <= accepted; k++) {
                        logQueueWarnIfNeeded(newSize - accepted + k);
                    }
                }

                // 큐가 늘어난 걸 다음 TASK/RESULT 보고 때까지 묵혀두면 Master가 여전히 한가하다고
                // 착각해서 계속 일을 더 밀어넣을 수 있다. P2P로 받은 직후 바로 새 큐 크기를 보고한다.
                if (accepted > 0) {
                    Message queueMsg = new Message("QUEUE");
                    queueMsg.set("recv", String.valueOf(masterTaskReceived.get()));
                    queueMsg.set("size", String.valueOf(readyQueue.size()));
                    queueMsg.set("clock", String.valueOf(clock.get()));
                    masterLink.send(queueMsg);
                }

                Message ack = new Message("P2P_ACK");
                ack.set("count", String.valueOf(accepted));
                ack.set("clock", String.valueOf(clock.get()));
                out.println(ack.toLine());
            }
        } catch (Exception e) {
            // 요청 하나 처리 실패는 전체 Worker를 죽이지 않고 무시한다.
        }
    }

    /** 큐가 70%를 초과한 상태에서 작업이 들고날 때마다 WARN을 기록. */
    private void logQueueWarnIfNeeded(int size) {
        if (size > Constants.QUEUE_MAX * Constants.QUEUE_WARN_RATIO) {
            log.log(clock.get(), "QUEUE", "WARN", "Queue over 70% (" + size + "/10) after P2P receive.");
        }
    }

    public void shutdown() {
        running = false;
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException ignored) {
        }
    }
}

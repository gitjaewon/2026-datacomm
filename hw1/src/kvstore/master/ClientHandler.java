package kvstore.master;

import kvstore.common.Message;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * Master 쪽에서 Worker 1개와의 소켓 연결을 전담하는 Thread.
 *
 * 역할:
 *  - Worker로부터 오는 QUEUE(큐 크기 보고) / RESULT(작업 성공·실패 결과) 메시지를
 *    계속 읽어들여서 Master의 공용 자료구조(KVStore, WorkloadScheduler)에 반영한다.
 *  - Master.runDispatchLoop()가 이 클래스의 sendTask()를 호출해서 실제로 작업을 내려보낸다.
 *
 * 즉, "소켓에서 한 줄 읽기"라는 blocking 작업을 Worker 개수(4개)만큼의 독립된 Thread가
 * 각각 맡아서 처리하는 구조다.
 */
public class ClientHandler extends Thread {

    private final int workerId;
    private final Master master; // KVStore/Scheduler/Logger에 접근하기 위한 콜백 참조
    private final BufferedReader in;
    private final PrintWriter out;

    public ClientHandler(int workerId, Socket socket, Master master) throws IOException {
        this.workerId = workerId;
        this.master = master;
        this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.out = new PrintWriter(socket.getOutputStream(), true);
        setName("ClientHandler-Worker" + workerId);
    }

    public int getWorkerId() {
        return workerId;
    }

    @Override
    public void run() {
        try {
            String line;
            while ((line = in.readLine()) != null) {
                Message msg = Message.parse(line);
                master.getClock().sync(msg.getDouble("clock", 0));

                switch (msg.getType()) {
                    case "QUEUE":
                        master.onWorkerQueueUpdate(workerId, msg.getInt("size"));
                        break;
                    case "RESULT":
                        master.onWorkerResult(workerId, msg.get("key"), msg.get("status"));
                        break;
                    case "STATS":
                        master.onWorkerStats(workerId, msg);
                        break;
                    default:
                        // TODO: 필요하면 다른 메시지 타입도 여기서 처리하면 된다.
                        break;
                }
            }
        } catch (IOException e) {
            // 소켓이 끊어짐 (Worker 프로세스 종료 등)
            // TODO(심화/선택): Worker 자체가 죽는 상황까지 고려한다면, 여기서
            //   그 Worker가 들고 있던 미완료 작업을 다른 Worker에게 재분배하는 로직을 추가할 수 있다.
            //   (지금은 개별 작업 단위 20% 실패 재시도만 필수 요구사항이라, 이 부분은 선택 구현이다)
        } finally {
            master.onWorkerDisconnected(workerId);
        }
    }

    /** Master의 Dispatcher(runDispatchLoop)가 호출: 이 Worker에게 작업을 하나 전송한다. */
    public void sendTask(String key, int value, boolean isRetry, double clock) {
        Message msg = new Message("TASK");
        msg.set("key", key);
        msg.set("value", String.valueOf(value));
        msg.set("retry", String.valueOf(isRetry));
        msg.set("clock", String.valueOf(clock));
        out.println(msg.toLine());
    }

    public void sendShutdown(double clock) {
        Message msg = new Message("SHUTDOWN");
        msg.set("clock", String.valueOf(clock));
        out.println(msg.toLine());
    }
}

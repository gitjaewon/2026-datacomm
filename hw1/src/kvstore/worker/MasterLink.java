package kvstore.worker;

import kvstore.common.Message;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;

/**
 * Master와의 소켓 연결에서 "읽기(blocking)"만 전담하는 보조 Thread.
 *
 * 왜 별도 Thread가 필요한가?
 *  - BufferedReader.readLine()은 데이터가 올 때까지 멈춰있는(blocking) 호출이다.
 *  - 만약 WorkerThread의 메인 루프에서 직접 readLine()을 부르면, 그동안 큐 처리도
 *    P2P 체크도 전부 멈춰버린다.
 *  - 그래서 "소켓에서 한 줄 읽어서 inbox에 넣어주기만 하는" 아주 단순한 배달부 Thread를
 *    따로 두고, 실제 판단(큐에 넣을지, 몇 초 후 처리할지 등)은 WorkerThread.run()이 전담한다.
 *
 * 즉 이 클래스는 로직 판단을 전혀 하지 않는 순수 I/O 담당이고,
 * "Worker Node = 독립적인 Thread"의 실제 주체는 WorkerThread다.
 */
public class MasterLink extends Thread {

    private final Socket socket;
    private final PrintWriter out;
    private final BufferedReader reader;
    private final BlockingQueue<Message> inbox;

    public MasterLink(Socket socket, BlockingQueue<Message> inbox) throws IOException {
        this.socket = socket;
        this.inbox = inbox;
        this.out = new PrintWriter(socket.getOutputStream(), true);
        this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        setDaemon(true);
        setName("MasterLink");
    }

    @Override
    public void run() {
        try {
            String line;
            while ((line = reader.readLine()) != null) {
                inbox.put(Message.parse(line));
            }
        } catch (Exception e) {
            // Master와 연결이 끊어짐 (정상 종료 or 오류) -> 조용히 종료
        }
    }

    public synchronized void send(Message msg) {
        out.println(msg.toLine());
    }

    /**
     * SHUTDOWN을 받고 Worker가 종료할 때 반드시 호출해야 한다.
     * 소켓을 닫아야 Master 쪽 ClientHandler.readLine()이 null을 반환하고 정상 종료되며,
     * 이 클래스 자신도 readLine()에서 예외를 받고 while 루프를 빠져나온다.
     * (이걸 안 하면 Master/Worker의 JVM 프로세스가 끝나지 않고 계속 떠 있게 된다!)
     */
    public void close() {
        try {
            socket.close();
        } catch (IOException ignored) {
        }
    }
}

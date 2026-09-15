package kvstore.common;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * 각 노드(Master, Worker1~4)가 자기 로그 파일에 이벤트를 기록하기 위한 도우미 클래스.
 *
 * 공통 로그 형식 (과제 명세 4장): [clock] NODE | EVENT | STATUS | message
 *  - STATUS는 반드시 INFO / SUCCESS / FAIL / WARN 중 하나만 사용할 것.
 *  - EVENT는 자유 형식이지만, 실제로 쓴 EVENT 종류는 전부 AllDefinedLogs.txt에 정리해서 제출해야 한다.
 *
 * 여러 Thread(마스터 처리 스레드, P2P 스레드 등)가 동시에 log()를 호출할 수 있으므로
 * synchronized로 보호해서 로그 줄이 서로 섞이지 않게 한다.
 */
public class FileLogger {

    private final String nodeName;
    private final PrintWriter writer;

    public FileLogger(String filePath, String nodeName) {
        this.nodeName = nodeName;
        try {
            // append=false : 실행할 때마다 새로 덮어쓴다 (과거 실행 로그가 섞이지 않도록)
            this.writer = new PrintWriter(new FileWriter(filePath, false), true);
        } catch (IOException e) {
            throw new RuntimeException("로그 파일을 열 수 없습니다: " + filePath, e);
        }
    }

    public synchronized void log(double clock, String event, String status, String message) {
        String line = String.format("[%.2f] %s | %-9s | %-7s | %s", clock, nodeName, event, status, message);
        writer.println(line);
        System.out.println(line); // 실행 중 콘솔에서도 바로 확인할 수 있게 같이 출력 (불필요하면 지워도 됨)
    }

    public synchronized void close() {
        writer.close();
    }
}

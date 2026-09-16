package kvstore.common;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Master <-> Worker, Worker <-> Worker 사이에서 주고받는 모든 메시지의 공통 포맷.
 *
 * 한 줄짜리 텍스트로 직렬화한다: TYPE|key1=value1|key2=value2|...
 * 예) TASK|key=a3f7|value=42|retry=false|clock=12.34
 *
 * ※ 헷갈리지 않게 구분할 것:
 *   - 이 Message의 TYPE(TASK, RESULT, QUEUE, P2P_QUERY ...)은 "소켓으로 주고받는 네트워크 프로토콜" 용어.
 *   - 4장 로그 형식에 나오는 EVENT(INIT, DISTRIB, RECV, PROC ...)는 "로그 파일에 남기는" 용어.
 *   서로 관련은 있지만 다른 목적의 값이라 이름이 겹치지 않게 설계했다.
 *
 * 사용 중인 메시지 타입 목록 (필요하면 자유롭게 추가/변경 가능):
 *   [Master -> Worker]
 *     TASK      : key, value, retry(true/false), clock
 *                 (retry=true면 실패 후 재할당된 작업. Worker는 큐 맨 앞에 넣는다)
 *     SHUTDOWN  : clock
 *   [Worker -> Master]
 *     REGISTER  : workerId               (접속 직후 1회, 자기 workerId를 알림)
 *     RESULT    : key, status(SUCCESS/FAIL/REJECTED), clock
 *                 (FAIL = 20% 규칙으로 처리 실패, REJECTED = 큐가 가득 차서 받지 못함)
 *     QUEUE     : size, clock            (자신의 현재 큐 크기를 보고)
 *     STATS     : received, success, fail, avgWait, p2pSent, p2pReceived,
 *                 p2pEvents, retryReceived, rejected, totalTime, clock
 *                 (SHUTDOWN을 받고 종료하기 직전, Master가 최종 STAT 로그에 노드별 통계를
 *                  남길 수 있도록 자신의 최종 통계를 한 번 보고한다)
 *   [Worker <-> Worker, P2P]
 *     P2P_QUERY   : fromId, clock
 *     P2P_STATUS  : size, clock           (P2P_QUERY에 대한 응답)
 *     P2P_TRANSFER: keys(세미콜론 구분), values(세미콜론 구분), clock
 *     P2P_ACK     : count, clock          (P2P_TRANSFER에 대한 응답. 앞에서부터 받은 작업 수)
 */
public class Message {

    private final String type;
    private final Map<String, String> fields = new LinkedHashMap<>();

    public Message(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public void set(String key, String value) {
        fields.put(key, value);
    }

    public String get(String key) {
        return fields.get(key);
    }

    public int getInt(String key) {
        return Integer.parseInt(fields.get(key));
    }

    public double getDouble(String key, double defaultValue) {
        String v = fields.get(key);
        return (v == null) ? defaultValue : Double.parseDouble(v);
    }

    public String toLine() {
        StringBuilder sb = new StringBuilder(type);
        for (Map.Entry<String, String> e : fields.entrySet()) {
            sb.append("|").append(e.getKey()).append("=").append(e.getValue());
        }
        return sb.toString();
    }

    public static Message parse(String line) {
        String[] parts = line.split("\\|");
        Message msg = new Message(parts[0]);
        for (int i = 1; i < parts.length; i++) {
            String[] kv = parts[i].split("=", 2);
            if (kv.length == 2) {
                msg.set(kv[0], kv[1]);
            }
        }
        return msg;
    }
}

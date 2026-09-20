package kvstore.worker;

/** Ready Queue 안에서 대기하는 작업 하나를 표현하는 간단한 데이터 클래스. */
public class Task {

    public final String key;
    public final int index; // Master가 부여한 순번 ID (1~5000), 로그 표기용
    public final int value;
    public final boolean isRetry; // Master로부터 "재할당(Priority)" 받은 작업인지
    public final double enqueueClock; // 이 작업이 큐에 들어간 시점의 System Clock (평균 대기시간 계산용)

    public Task(String key, int index, int value, boolean isRetry, double enqueueClock) {
        this.key = key;
        this.index = index;
        this.value = value;
        this.isRetry = isRetry;
        this.enqueueClock = enqueueClock;
    }

    /** 로그 표기용: "KV[0001] (Key=a3f7)" 형태로 순번 인덱스와 hex key를 함께 보여준다. */
    public static String label(int index, String key) {
        return "KV[" + String.format("%04d", index) + "] (Key=" + key + ")";
    }
}

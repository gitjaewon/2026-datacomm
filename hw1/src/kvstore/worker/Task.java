package kvstore.worker;

/** Ready Queue 안에서 대기하는 작업 하나를 표현하는 간단한 데이터 클래스. */
public class Task {

    public final String key;
    public final int value;
    public final boolean isRetry; // Master로부터 "재할당(Priority)" 받은 작업인지
    public final boolean fromP2P; // 다른 Worker로부터 P2P로 넘겨받은 작업인지
    public final double enqueueClock; // 이 작업이 큐에 들어간 시점의 System Clock (평균 대기시간 계산용)

    public Task(String key, int value, boolean isRetry, boolean fromP2P, double enqueueClock) {
        this.key = key;
        this.value = value;
        this.isRetry = isRetry;
        this.fromP2P = fromP2P;
        this.enqueueClock = enqueueClock;
    }
}

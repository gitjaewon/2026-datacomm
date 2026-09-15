package kvstore.common;

/**
 * 과제 명세서(HW1description)에 나온 숫자들을 한 곳에 모아둔 상수 모음.
 * 나중에 값이 바뀌더라도 여기 한 군데만 고치면 되도록 하기 위함.
 */
public class Constants {

    // 0장: 생성할 (Key, Value) 쌍 개수와 Value 범위
    public static final int TOTAL_KV_PAIRS = 5000;
    public static final int VALUE_MIN = 1;
    public static final int VALUE_MAX = 100;

    // Worker Node 개수 (Master 1개 + Worker 4개 구조)
    public static final int NUM_WORKERS = 4;

    // Ready Queue 관련 (최대 10개, 70% 초과 시 WARN, 초과 시 FAIL)
    public static final int QUEUE_MAX = 10;
    public static final double QUEUE_WARN_RATIO = 0.7;

    // Worker 작업 처리 시뮬레이션 (실제 sleep이 아니라 VirtualClock에 더하는 값)
    public static final int PROC_TIME_MIN = 1; // sec
    public static final int PROC_TIME_MAX = 3; // sec
    public static final double SUCCESS_RATE = 0.8; // 80% 성공 / 20% 실패

    // 노드 간 통신 지연 (가상값, 1초 고정)
    public static final double NETWORK_DELAY = 1.0;

    // P2P 부하 분산 관련
    public static final double P2P_WAIT_THRESHOLD = 15.0; // sec, 이 이상이면 이전 요청
    public static final double P2P_AVG_PROC_TIME = 2.0;   // 예상 대기시간 계산에 쓰이는 평균 처리시간
    public static final int P2P_CHECK_INTERVAL_MIN = 1;   // sec, 부하 상태 점검 주기 (1~3초 랜덤)
    public static final int P2P_CHECK_INTERVAL_MAX = 3;   // sec
}

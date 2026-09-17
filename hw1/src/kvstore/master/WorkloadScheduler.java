package kvstore.master;

import kvstore.common.Constants;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Master가 각 Worker의 큐 상태를 기억하고, 다음 작업을 "누구에게 보낼지" 결정하는 클래스.
 *
 * ===== 최소 큐 우선 (Least Queue First) =====
 *   - 현재 큐에 남은 작업이 가장 적은(가장 여유로운) Worker에게 우선 할당한다.
 *   - 모든 Worker의 큐가 가득 찼으면(10/10) -1을 반환해서, 호출한 쪽이 잠깐 대기하도록 한다.
 */
public class WorkloadScheduler {

    // workerId -> 현재 Master가 알고 있는 큐 크기 (0~10)
    private final Map<Integer, Integer> queueSizes = new ConcurrentHashMap<>();

    public void registerWorker(int workerId) {
        queueSizes.put(workerId, 0);
    }

    /** Worker가 QUEUE 메시지로 보고해온 실제 큐 크기로 갱신한다. */
    public void updateQueueSize(int workerId, int size) {
        queueSizes.put(workerId, size);
    }

    /**
     * Master가 작업을 보내자마자 "낙관적으로" 미리 +1 해두는 값.
     * (Worker가 QUEUE 메시지로 실제 값을 보고해오기 전까지, Master가 같은 Worker에게
     *  연달아 몰아서 보내는 것을 막기 위한 임시 보정)
     */
    public void onDispatchedOptimistically(int workerId) {
        queueSizes.merge(workerId, 1, Integer::sum);
    }

    /** 진행률 로그용: 현재 Master가 파악하고 있는 각 Worker의 큐 크기를 한 줄로 요약한다. */
    public String describeQueues() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Integer, Integer> e : new TreeMap<>(queueSizes).entrySet()) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append("Worker").append(e.getKey()).append(": queue=").append(e.getValue());
        }
        return sb.toString();
    }

    /**
     * 큐가 가장 여유로운 Worker의 id를 반환. 보낼 수 있는 Worker가 없으면 -1.
     * excludeWorkerId는 후보에서 뺀다 (-1이면 제외 없음). 재할당 시 실패시킨 Worker를 빼는 데 쓴다.
     */
    public int pickWorkerForDispatch(int excludeWorkerId) {
        int bestWorker = -1;
        int bestSize = Integer.MAX_VALUE;
        for (Map.Entry<Integer, Integer> e : queueSizes.entrySet()) {
            if (e.getKey() == excludeWorkerId) {
                continue;
            }
            int size = e.getValue();
            if (size < Constants.QUEUE_MAX && size < bestSize) {
                bestSize = size;
                bestWorker = e.getKey();
            }
        }
        return bestWorker;
    }
}

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
 *   - 큐 크기는 "Worker가 보고한 값 + 아직 도착하지 않은 작업 수"로 판단해서,
 *     보고가 늦게 와도 이미 보낸 작업을 빠뜨리지 않는다 (큐 초과 전송 방지).
 *   - 모든 Worker의 큐가 가득 찼으면(10/10) -1을 반환해서, 호출한 쪽이 잠깐 대기하도록 한다.
 */
public class WorkloadScheduler {

    // workerId -> Worker가 마지막으로 보고한 큐 크기
    private final Map<Integer, Integer> reportedSizes = new ConcurrentHashMap<>();
    // workerId -> 그 보고 시점까지 Worker가 Master에게서 받은 TASK 누적 수
    private final Map<Integer, Integer> recvAtReport = new ConcurrentHashMap<>();
    // workerId -> Master가 그 Worker에게 보낸 TASK 누적 수
    private final Map<Integer, Integer> sentCounts = new ConcurrentHashMap<>();

    public void registerWorker(int workerId) {
        reportedSizes.put(workerId, 0);
        recvAtReport.put(workerId, 0);
        sentCounts.put(workerId, 0);
    }

    /** Worker가 QUEUE 메시지로 보고해온 큐 크기와, 그 시점까지 받은 TASK 수로 갱신한다. */
    public synchronized void updateQueueSize(int workerId, int size, int recv) {
        reportedSizes.put(workerId, size);
        recvAtReport.put(workerId, recv);
    }

    /** Master가 작업을 보낼 때마다 호출. 보낸 누적 수를 +1 한다. */
    public synchronized void onDispatched(int workerId) {
        sentCounts.merge(workerId, 1, Integer::sum);
    }

    /** 보고된 큐 크기 + 아직 도착하지 않은 작업 수(보낸 수 - Worker가 받은 수) */
    private int estimatedSize(int workerId) {
        return reportedSizes.get(workerId) + sentCounts.get(workerId) - recvAtReport.get(workerId);
    }

    /** 진행률 로그용: 현재 Master가 파악하고 있는 각 Worker의 큐 크기를 한 줄로 요약한다. */
    public synchronized String describeQueues() {
        StringBuilder sb = new StringBuilder();
        for (int workerId : new TreeMap<>(reportedSizes).keySet()) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append("Worker").append(workerId).append(": queue=").append(estimatedSize(workerId));
        }
        return sb.toString();
    }

    /**
     * 큐가 가장 여유로운 Worker의 id를 반환. 보낼 수 있는 Worker가 없으면 -1.
     * excludeWorkerId는 후보에서 뺀다 (-1이면 제외 없음). 재할당 시 실패시킨 Worker를 빼는 데 쓴다.
     */
    public synchronized int pickWorkerForDispatch(int excludeWorkerId) {
        int bestWorker = -1;
        int bestSize = Integer.MAX_VALUE;
        for (int workerId : reportedSizes.keySet()) {
            if (workerId == excludeWorkerId) {
                continue;
            }
            int size = estimatedSize(workerId);
            if (size < Constants.QUEUE_MAX && size < bestSize) {
                bestSize = size;
                bestWorker = workerId;
            }
        }
        return bestWorker;
    }
}

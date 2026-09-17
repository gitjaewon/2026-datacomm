package kvstore.master;

import kvstore.common.Constants;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Master가 들고 있는 5,000개 (Key, Value) 데이터와, 그 처리 상태를 관리하는 클래스.
 *
 * 상태는 3단계로 나뉜다:
 *   1) pendingQueue   : 아직 한 번도 어떤 Worker에게도 보내지 않은 작업
 *   2) (분배된 상태)   : 어떤 Worker의 큐 안에 들어가 있는 중 (결과가 돌아올 때까지 기다리며,
 *                        재할당 작업이었는지와 마지막으로 실패시킨 Worker만 따로 기록한다)
 *   3) priorityQueue  : 실패해서 재시도를 기다리는 작업 (Fault Tolerance, 최우선 처리)
 *   4) store          : 최종적으로 SUCCESS 처리되어 "저장 완료"된 결과
 */
public class KVStore {

    private final Map<String, Integer> allPairs = new HashMap<>();          // 전체 5000개 원본 (key -> value)
    private final Queue<String> pendingQueue = new ConcurrentLinkedQueue<>(); // 아직 분배 안 된 작업
    private final Queue<String> priorityQueue = new ConcurrentLinkedQueue<>(); // 실패 후 재시도 대기
    private final Map<String, Integer> store = new ConcurrentHashMap<>();     // 성공적으로 저장 완료된 결과

    private final Map<String, Boolean> dispatchedAsRetry = new ConcurrentHashMap<>(); // 분배 중인 작업이 재할당 작업인지
    private final Map<String, Integer> lastFailedWorker = new ConcurrentHashMap<>();  // 작업을 마지막으로 실패시킨 Worker

    private final Random random = new Random();

    /** 분배할 작업 한 건. isRetry=true면 priorityQueue에서 꺼낸 재할당 작업이다. */
    public static class PendingKey {
        public final String key;
        public final boolean isRetry;

        PendingKey(String key, boolean isRetry) {
            this.key = key;
            this.isRetry = isRetry;
        }
    }

    /** 시작 시 5,000개의 (Key, Value) 생성. Key는 unique 16진수 4자리, Value는 1~100 무작위 정수. */
    public synchronized void generateAll() {
        Set<String> usedKeys = new HashSet<>();
        while (usedKeys.size() < Constants.TOTAL_KV_PAIRS) {
            String key = String.format("%04x", random.nextInt(0x10000)); // 예: a3f7
            if (usedKeys.add(key)) {
                int value = Constants.VALUE_MIN + random.nextInt(Constants.VALUE_MAX - Constants.VALUE_MIN + 1);
                allPairs.put(key, value);
                pendingQueue.add(key);
            }
        }
    }

    public int valueOf(String key) {
        return allPairs.get(key);
    }

    /**
     * 다음에 분배할 작업을 하나 꺼낸다.
     * 실패 재시도 작업(priorityQueue)을 항상 먼저 꺼내고, 어느 큐에서 꺼냈는지 isRetry로 함께 돌려준다.
     */
    public PendingKey nextPendingKey() {
        String k = priorityQueue.poll();
        if (k != null) {
            return new PendingKey(k, true);
        }
        k = pendingQueue.poll();
        return (k == null) ? null : new PendingKey(k, false);
    }

    /** Worker에게 보내기 직전에 호출. 거절돼서 돌아올 때 원래 큐를 알 수 있도록 기록한다. */
    public void markDispatched(PendingKey pk) {
        dispatchedAsRetry.put(pk.key, pk.isRetry);
    }

    /** 보낼 Worker가 없어 못 보낸 작업을 원래 큐로 되돌린다. */
    public void returnUndispatched(PendingKey pk) {
        if (pk.isRetry) {
            priorityQueue.add(pk.key);
        } else {
            pendingQueue.add(pk.key);
        }
    }

    public void markSuccess(String key) {
        store.put(key, allPairs.get(key));
        dispatchedAsRetry.remove(key);
        lastFailedWorker.remove(key);
    }

    /**
     * 20% 규칙으로 실패한 작업을 최우선 큐에 다시 등록한다 (성공할 때까지 반복 재시도).
     * 다음 재할당 때 제외할 수 있도록 실패시킨 Worker를 기록한다.
     */
    public void requeueAfterFail(String key, int failedWorkerId) {
        lastFailedWorker.put(key, failedWorkerId);
        dispatchedAsRetry.remove(key);
        priorityQueue.add(key);
    }

    /**
     * Worker 큐가 가득 차서 거절된 작업을 되돌린다.
     * 장애가 아니므로 실패 기록은 남기지 않고, 신규 작업은 pendingQueue로, 재할당 작업은 priorityQueue로 보낸다.
     */
    public void requeueAfterReject(String key) {
        Boolean wasRetry = dispatchedAsRetry.remove(key);
        if (Boolean.TRUE.equals(wasRetry)) {
            priorityQueue.add(key);
        } else {
            pendingQueue.add(key);
        }
    }

    /** 이 작업을 마지막으로 실패시킨 Worker id. 실패한 적이 없으면 -1. */
    public int lastFailedWorkerOf(String key) {
        return lastFailedWorker.getOrDefault(key, -1);
    }

    public int doneCount() {
        return store.size();
    }

    public boolean isAllDone() {
        return store.size() >= Constants.TOTAL_KV_PAIRS;
    }

    /** 최종 출력용: 완료된 KV 저장소 전체를 key 정렬된 스냅샷으로 반환. */
    public synchronized Map<String, Integer> snapshotStore() {
        return new TreeMap<>(store);
    }
}

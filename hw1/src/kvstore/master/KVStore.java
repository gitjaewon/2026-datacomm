package kvstore.master;

import kvstore.common.Constants;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Master가 들고 있는 5,000개 (Key, Value) 데이터와, 그 처리 상태를 관리하는 클래스.
 *
 * 상태는 3단계로 나뉜다:
 *   1) pendingQueue   : 아직 한 번도 어떤 Worker에게도 보내지 않은 작업
 *   2) (분배된 상태)   : 어떤 Worker의 큐 안에 들어가 있는 중 (여기선 따로 추적 안 하고,
 *                        결과가 SUCCESS/FAIL로 돌아올 때까지 그냥 기다린다)
 *   3) priorityQueue  : 실패해서 재시도를 기다리는 작업 (Fault Tolerance, 최우선 처리)
 *   4) store          : 최종적으로 SUCCESS 처리되어 "저장 완료"된 결과
 */
public class KVStore {

    private final Map<String, Integer> allPairs = new HashMap<>();          // 전체 5000개 원본 (key -> value)
    private final Queue<String> pendingQueue = new ConcurrentLinkedQueue<>(); // 아직 분배 안 된 작업
    private final Queue<String> priorityQueue = new ConcurrentLinkedQueue<>(); // 실패 후 재시도 대기
    private final Map<String, Integer> store = new ConcurrentHashMap<>();     // 성공적으로 저장 완료된 결과

    private final Random random = new Random();

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
     * 다음에 분배할 key를 하나 꺼낸다.
     * 실패 재시도 작업(priorityQueue)을 항상 먼저 처리해서 "최우선 큐"라는 요구사항을 만족시킨다.
     */
    public String nextPendingKey() {
        String k = priorityQueue.poll();
        if (k != null) {
            return k;
        }
        return pendingQueue.poll();
    }

    public void markSuccess(String key) {
        store.put(key, allPairs.get(key));
    }

    /** 실패한 작업을 최우선 큐에 다시 등록한다 (성공할 때까지 반복 재시도). */
    public void requeueAsPriority(String key) {
        priorityQueue.add(key);
    }

    public int doneCount() {
        return store.size();
    }

    public boolean isAllDone() {
        return store.size() >= Constants.TOTAL_KV_PAIRS;
    }
}

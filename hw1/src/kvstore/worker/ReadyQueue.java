package kvstore.worker;

import kvstore.common.Constants;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Worker의 대기 작업 버퍼. 최대 10개.
 *
 * WorkerThread(메인 처리 루프), P2PServer(다른 Worker의 요청 처리 스레드)가
 * 동시에 이 큐를 건드릴 수 있으므로 모든 메서드를 synchronized로 감싼다.
 */
public class ReadyQueue {

    private final LinkedList<Task> queue = new LinkedList<>();

    /** 큐에 넣는다. 이미 꽉 찼으면(10개) false를 반환 -> 호출한 쪽에서 FAIL 처리해야 한다. */
    public synchronized boolean offer(Task task) {
        if (queue.size() >= Constants.QUEUE_MAX) {
            return false;
        }
        queue.addLast(task);
        return true;
    }

    /** 큐 맨 앞의 작업을 꺼낸다. 비어있으면 null. */
    public synchronized Task poll() {
        return queue.poll();
    }

    public synchronized int size() {
        return queue.size();
    }

    /**
     * P2P 부하 분산을 위해 큐 "뒤쪽(가장 늦게 처리될 예정인)" 작업 몇 개를 꺼내온다.
     * 뒤쪽 작업을 넘기는 이유: 앞쪽(곧 처리될) 작업까지 넘기면 오히려 지연이 더 생길 수 있어서다.
     */
    public synchronized List<Task> pollFromTail(int count) {
        List<Task> result = new ArrayList<>();
        for (int i = 0; i < count && !queue.isEmpty(); i++) {
            result.add(queue.removeLast());
        }
        return result;
    }
}

package kvstore.worker;

import kvstore.common.Constants;

/**
 * 조원의 로컬 PC에서 실행하는 진입점(main). 4개의 Worker Thread를 띄운다.
 *
 * 실행 예) java kvstore.worker.WorkerLauncher <MasterIP> <MasterPort>
 *
 * 기본 가정: 4개의 Worker가 전부 이 PC(localhost) 안에서 Thread로 동작하고,
 * P2P 통신도 localhost의 서로 다른 포트(6001~6004)로 주고받는다.
 */
public class WorkerLauncher {

    private static final int BASE_P2P_PORT = 6000;

    public static void main(String[] args) throws InterruptedException {
        if (args.length < 2) {
            System.out.println("사용법: java kvstore.worker.WorkerLauncher <MasterIP> <MasterPort>");
            return;
        }

        String masterHost = args[0];
        int masterPort = Integer.parseInt(args[1]);
        int numWorkers = Constants.NUM_WORKERS;

        WorkerThread[] threads = new WorkerThread[numWorkers];

        for (int i = 1; i <= numWorkers; i++) {
            int myPort = BASE_P2P_PORT + i;

            int[] peerPorts = new int[numWorkers - 1];
            int idx = 0;
            for (int j = 1; j <= numWorkers; j++) {
                if (j != i) {
                    peerPorts[idx++] = BASE_P2P_PORT + j;
                }
            }

            threads[i - 1] = new WorkerThread(i, masterHost, masterPort, myPort, peerPorts, "localhost");
        }

        for (WorkerThread t : threads) {
            t.start();
        }
        for (WorkerThread t : threads) {
            t.join();
        }

        System.out.println("모든 Worker 종료 완료.");
    }
}

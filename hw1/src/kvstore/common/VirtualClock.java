package kvstore.common;

/**
 * 실제 시간(real time)이 아니라 "가상의 누적 시간"을 관리하는 클래스.
 *
 * 왜 필요한가?
 *  - 과제 명세 0-1의 System Clock 설명: Worker 처리시간(1~3초)이나 노드 간 통신 지연(1초)은
 *    실제로 Thread.sleep()을 하는 게 아니라, 이 시계 값에 "더하기만" 해서 흉내낸다.
 *  - 그래야 프로그램이 5,000개를 실시간으로 몇 시간씩 기다리지 않고 순식간에 처리할 수 있다.
 *
 * 주의할 점 (설계 배경 설명):
 *  - Master와 각 Worker는 서로 다른 프로세스(Master는 클라우드 서버, Worker는 로컬 PC)라서
 *    메모리를 공유할 수 없다. 즉 "전역 시계 하나"를 물리적으로 공유하는 건 불가능하다.
 *  - 대신 메시지를 주고받을 때마다 각자의 clock 값을 실어 보내고, 받는 쪽은 sync()를 호출해서
 *    "내 시계 vs 상대가 보낸 시계 값 중 더 큰 쪽"을 채택한다.
 *    (분산시스템 수업에서 배우는 Lamport Logical Clock과 같은 아이디어)
 *  - 이렇게 하면 완벽하게 하나의 시계는 아니어도, 크게 어긋나지 않게 "느슨하게 동기화"된다.
 */
public class VirtualClock {

    private double time = 0.0;

    /** delta(초)만큼 가상 시간을 흘려보내고, 그 결과 시각을 반환한다. */
    public synchronized double advance(double delta) {
        time += delta;
        return time;
    }

    /** 상대방이 보내온 clock 값과 내 시계 중 더 큰 값을 채택한다. */
    public synchronized double sync(double incoming) {
        if (incoming > time) {
            time = incoming;
        }
        return time;
    }

    public synchronized double get() {
        return time;
    }
}

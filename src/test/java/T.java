import org.junit.Test;

public class T {
    @Test
    public void test() throws InterruptedException {
        Thread taskThread = new Thread(taskThatFinishesEarlyOnInterruption());
        taskThread.start();
        Thread.sleep(3_000);     // requirement 4
        taskThread.interrupt();
    }

    private static Runnable taskThatFinishesEarlyOnInterruption() {
        return new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 10; i++) {
                    System.out.print(i);      // requirement 1
                    try {
                        Thread.sleep(1_000);  // requirement 2
                    } catch (InterruptedException e) {
                        System.out.println("aaaaaaaaaaa" + Thread.currentThread().isInterrupted());
                        break;                // requirement 7
                    }
                }
            }
        };
    }

}

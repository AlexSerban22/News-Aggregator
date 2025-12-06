public class MyThread implements Runnable {
    public int threadId;

    public MyThread(int threadId) {
        this.threadId = threadId;
    }

    @Override
    public void run() {
        while (true) {
            // luăm un path din coadă
            String filePath = Tema1.articlesQueue.poll();

            // dacă e null, coada e goală => thread-ul poate ieși
            if (filePath == null) {
                break;
            }

            processFile(filePath);
        }
    }

    private void processFile(String filePath) {
        // aici citim JSON-ul și parsăm articolele
    }
}
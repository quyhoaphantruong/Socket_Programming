import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLongArray;

public class ProgressTracker {
    private static ProgressTracker instance = new ProgressTracker();
    private ConcurrentMap<String, FileProgress> progressMap = new ConcurrentHashMap<>();

    private ProgressTracker() {
    }

    public static ProgressTracker getInstance() {
        return instance;
    }

    public void initializeFileProgress(String fileName, int numChunks) {
        progressMap.putIfAbsent(fileName, new FileProgress(numChunks, fileName));
    }

    public void updateProgress(String fileName, int chunkIndex, long bytesDownloaded, long totalBytes) {
        FileProgress fileProgress = progressMap.get(fileName);
        if (fileProgress != null) {
            fileProgress.updateChunkProgress(chunkIndex, bytesDownloaded, totalBytes);
            displayProgress();
        }
    }

    private synchronized void displayProgress() {
        // Display progress for each file
        for (FileProgress fileProgress : progressMap.values()) {
            fileProgress.displayProgress();
        }
    }

    private static class FileProgress {
        private int numChunks;
        private AtomicLongArray totalBytes;
        private AtomicLongArray bytesDownloaded;
        private String fileName;

        public FileProgress(int numChunks, String fileName) {
            this.numChunks = numChunks;
            this.fileName = fileName;
            totalBytes = new AtomicLongArray(numChunks);
            bytesDownloaded = new AtomicLongArray(numChunks);
        }

        public void updateChunkProgress(int chunkIndex, long bytesDownloaded, long totalBytes) {
            this.bytesDownloaded.set(chunkIndex, bytesDownloaded);
            this.totalBytes.set(chunkIndex, totalBytes);
        }

        public void displayProgress() {
            for (int i = 0; i < numChunks; i++) {
                long total = totalBytes.get(i);
                long downloaded = bytesDownloaded.get(i);
                double progress = (total == 0) ? 0 : (double) downloaded / total * 100;
                System.out.printf("Downloading %s part %d .... %.0f%%%n", fileName, i + 1, progress);
            }
            System.out.println();
        }
    }
}

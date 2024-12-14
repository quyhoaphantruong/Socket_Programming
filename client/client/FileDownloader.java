import java.io.*;
import java.util.concurrent.*;

public class FileDownloader implements Runnable {
    private String fileName;
    private String serverIp;
    private int port;
    private ConcurrentHashMap<String, Long> fileToSize;
    private ExecutorService downloadPool;

    public FileDownloader(String fileName, String serverIp, int port, 
                          ConcurrentHashMap<String, Long> fileToSize, ExecutorService downloadPool) {
        this.fileName = fileName;
        this.serverIp = serverIp;
        this.port = port;
        this.fileToSize = fileToSize;
        this.downloadPool = downloadPool; 
    }

    @Override
    public void run() {
        try {
            long fileSize = fileToSize.get(fileName);
            final int NUM_CHUNKS = 5;
            long chunkSize = fileSize / NUM_CHUNKS;
            ProgressTracker progressTracker = ProgressTracker.getInstance();

            // Initialize file progress
            progressTracker.initializeFileProgress(fileName, NUM_CHUNKS);
            CountDownLatch latch = new CountDownLatch(NUM_CHUNKS);

            for (int i = 0; i < NUM_CHUNKS; i++) {
                long start = i * chunkSize;
                long size = (i == NUM_CHUNKS - 1) ? (fileSize - start) : chunkSize;
                ChunkDownloader chunkDownloader = new ChunkDownloader(fileName, start, size, i, 
                        serverIp, port, progressTracker, latch);
                downloadPool.submit(chunkDownloader);
            }

            latch.await();
            assembleChunks(NUM_CHUNKS);

            System.out.println("Download complete for file: " + fileName);
        } catch (Exception e) {
            System.err.println("Error downloading file " + fileName + ": " + e.getMessage());
        }
    }

    private void assembleChunks(int numChunks) {
        try (FileOutputStream fos = new FileOutputStream(fileName)) {
            System.out.println("assemble chunks " + numChunks + " " + fileName);
            for (int i = 0; i < numChunks; i++) {
                String chunkFileName = fileName + ".part" + i;
                try (FileInputStream fis = new FileInputStream(chunkFileName)) {
                    byte[] buffer = new byte[8192];
                    int bytesRead;
                    while ((bytesRead = fis.read(buffer)) != -1) {
                        fos.write(buffer, 0, bytesRead);
                    }
                }
                // Delete chunk file after merging
                File chunkFile = new File(chunkFileName);
                if (!chunkFile.delete()) {
                    System.err.println("Failed to delete chunk file: " + chunkFileName);
                }
            }
        } catch (IOException e) {
            System.err.println("Error assembling chunks: " + e.getMessage());
        }
    }
}

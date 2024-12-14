import java.io.*;
import java.net.*;
import java.util.concurrent.CountDownLatch;

public class ChunkDownloader implements Runnable {
    private String fileName;
    private long start;
    private long size;
    private int chunkIndex;
    private String serverIp;
    private int port;
    private ProgressTracker progressTracker;
    private CountDownLatch latch;  

    public ChunkDownloader(String fileName, long start, long size, int chunkIndex, 
                           String serverIp, int port, ProgressTracker progressTracker,
                           CountDownLatch latch) {
        this.fileName = fileName;
        this.start = start;
        this.size = size;
        this.chunkIndex = chunkIndex;
        this.serverIp = serverIp;
        this.port = port;
        this.progressTracker = progressTracker;
        this.latch = latch;
    }

    @Override
    public void run() {
        try (Socket socket = new Socket(serverIp, port)) {
            BufferedInputStream bis = new BufferedInputStream(socket.getInputStream());
            BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(fileName + ".part" + chunkIndex));

            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            DataInputStream dis = new DataInputStream(bis);

            // Send request to server for chunk data
            dos.writeUTF("GET_CHUNK " + fileName + " " + start + " " + size);
            dos.flush();

            // Read server response
            String response = dis.readUTF();
            if (!response.equals("OK")) {
                System.err.println("Server response: " + response);
                bos.close();
                return;
            }

            byte[] buffer = new byte[8192];
            long bytesToRead = size;
            long totalBytesRead = 0;
            int i = 0;

            while (bytesToRead > 0) {
                int bytesRead = bis.read(buffer, 0, (int) Math.min(buffer.length, bytesToRead));
                if (bytesRead == -1) break;
                bos.write(buffer, 0, bytesRead);
                bytesToRead -= bytesRead;
                totalBytesRead += bytesRead;
                i += 1;

                // Update progress
                if (i % 10 == 0)
                    progressTracker.updateProgress(fileName, chunkIndex, totalBytesRead, size);
            }
            bos.flush(); 
            progressTracker.updateProgress(fileName, chunkIndex, totalBytesRead, size);
            bos.close(); 
        } catch (IOException e) {
            System.err.println("Error downloading chunk " + chunkIndex + " of file " + fileName + ": " + e.getMessage());
        } finally {
            latch.countDown();  
        }
    }
}

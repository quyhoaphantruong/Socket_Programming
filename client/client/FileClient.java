import java.io.*;
import java.net.*;
import java.util.concurrent.*;

public class FileClient {
    private static final String SERVER_IP = "192.168.100.108";
    private static final int PORT = 12345;
    private static final long CHECK_INTERVAL_MS = 5000;

    private volatile boolean running = true;
    private BlockingQueue<String> filesToDownload = new LinkedBlockingQueue<>();
    private ConcurrentHashMap<String, Long> fileToSize = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        new FileClient().start();
    }

    public void start() {
        displayAvailableFiles();
        
        InputFileWatcher inputWatcher = new InputFileWatcher("input.txt", filesToDownload);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        ExecutorService downloadExecutor = Executors.newSingleThreadExecutor();
        ExecutorService chunkExecutor = Executors.newFixedThreadPool(5);

        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down client...");
            running = false;
            scheduler.shutdown();
            downloadExecutor.shutdown();
        }));

        // Schedule the input watcher to check for new files every 5 seconds
        scheduler.scheduleAtFixedRate(() -> {
            if (running) {
                inputWatcher.checkForNewFiles();
                String fileName;
                while ((fileName = filesToDownload.poll()) != null) {
                    downloadExecutor.submit(new FileDownloader(fileName, SERVER_IP, PORT, fileToSize, chunkExecutor));
                }
            }
        }, 0, CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS);

        // Keep the main thread alive
        while (running) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                running = false;
            }
        }

        scheduler.shutdown();
    }

    private void displayAvailableFiles() {
        try (Socket socket = new Socket(SERVER_IP, PORT);
             DataInputStream dis = new DataInputStream(socket.getInputStream());
             DataOutputStream dos = new DataOutputStream(socket.getOutputStream())) {

            dos.writeUTF("LIST_FILES");
            dos.flush();

            String fileList = dis.readUTF();
            saveFileSize(fileList);
            System.out.println("Available files:");
            System.out.println(fileList);

        } catch (IOException e) {
            System.err.println("Error retrieving file list: " + e.getMessage());
        }
    }

    private void saveFileSize(String fileList) {
        String[] files = fileList.split("\n");
        for (String file : files) {
            String[] parts = file.trim().split(" ");
            if (parts.length == 2) {
                try {
                    fileToSize.put(parts[0], Long.parseLong(parts[1]));
                } catch (NumberFormatException e) {
                    System.err.println("Invalid file size format for file: " + parts[0]);
                }
            } else {
                System.err.println("Skipping malformed file entry: " + file);
            }
        }
    }
}

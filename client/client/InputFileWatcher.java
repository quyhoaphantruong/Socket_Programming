import java.io.*;
import java.util.concurrent.BlockingQueue; 

public class InputFileWatcher {
    private String inputFilePath;
    private BlockingQueue<String> filesToDownload;
    private long lastPositionRead;

    public InputFileWatcher(String inputFilePath, BlockingQueue<String> filesToDownload) {
        this.inputFilePath = inputFilePath;
        this.filesToDownload = filesToDownload;
        this.lastPositionRead = 0;
    }

    public void checkForNewFiles() {
        File inputFile = new File(inputFilePath);

        if (inputFile.exists()) {
            try (RandomAccessFile reader = new RandomAccessFile(inputFile, "r")) {
                reader.seek(lastPositionRead);

                String fileName;
                while ((fileName = reader.readLine()) != null) {
                    fileName = fileName.trim();
                    filesToDownload.offer(fileName);
                }
                lastPositionRead = reader.getFilePointer();
            } catch (IOException e) {
                System.err.println("Error reading input.txt: " + e.getMessage());
            }
        } else {
            System.out.println("file not exist");
        }
    }
}

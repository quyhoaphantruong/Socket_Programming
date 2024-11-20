import java.io.*;
import java.util.*;

public class FileListProvider {
    private Map<String, Long> fileList;

    public FileListProvider(String fileListPath) {
        fileList = new HashMap<>();
        loadFileList(fileListPath);
    }

    private void loadFileList(String fileListPath) {
        try (BufferedReader reader = new BufferedReader(new FileReader(fileListPath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String fileName = line.trim();
                if (!fileName.isEmpty()) {
                    File file = new File(fileName);
                    if (file.exists() && file.isFile()) {
                        long fileSize = file.length();
                        fileList.put(fileName, fileSize);
                    } else {
                        System.err.println("File not found or is not a regular file: " + fileName);
                    }
                }
            }
        } catch (Exception e) {
        }
    }

    public Map<String, Long> getFileList() {
        return fileList;
    }

    public boolean fileExists(String fileName) {
        return fileList.containsKey(fileName);
    }

    public long getFileSize(String fileName) {
        return fileList.get(fileName);
    }
}

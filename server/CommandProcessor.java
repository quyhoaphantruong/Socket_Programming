import java.io.*;
import java.util.*;

public class CommandProcessor {
    private DataInputStream dis;
    private DataOutputStream dos;
    private FileListProvider fileListProvider;

    public CommandProcessor(DataInputStream dis, DataOutputStream dos, FileListProvider fileListProvider) {
        this.dis = dis;
        this.dos = dos;
        this.fileListProvider = fileListProvider;
    }

    public void processCommand(String command) {
        if (command.equals("LIST_FILES")) {
            sendFileList();
        } else if (command.startsWith("GET_CHUNK")) {
            sendFileChunk(command);
        } else if (command.startsWith("GET_FILE_SIZE")) {
            sendFileSize(command);
        } else {
            sendError("Unknown command");
        }
    }

    private void sendFileList() {
        Map<String, Long> fileList = fileListProvider.getFileList();
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Long> entry : fileList.entrySet()) {
            sb.append(entry.getKey()).append(" ").append(entry.getValue()).append("\n");
        }
        writeMessage(sb.toString());
    }

    private void sendFileChunk(String command) {
        String[] tokens = command.split(" ", 4);
        if (tokens.length != 4) {
            sendError("Invalid GET_CHUNK command");
            return;
        }

        String fileName = tokens[1];
        long start = Long.parseLong(tokens[2]);
        long size = Long.parseLong(tokens[3]);

        if (!fileListProvider.fileExists(fileName)) {
            sendError("File not found");
            return;
        }

        long fileSize = fileListProvider.getFileSize(fileName);
        if (start < 0 || start >= fileSize) {
            sendError("Invalid start position");
            return;
        }

        if (start + size > fileSize) {
            size = fileSize - start;
        }

        ChunkSender chunkSender = new ChunkSender(dos);
        chunkSender.sendChunk(fileName, start, size);
    }

    private void sendFileSize(String command) {
        String[] tokens = command.split(" ", 2);
        if (tokens.length != 2) {
            sendError("Invalid GET_FILE_SIZE command");
            return;
        }

        String fileName = tokens[1];
        if (!fileListProvider.fileExists(fileName)) {
            sendError("File not found");
            return;
        }

        long fileSize = fileListProvider.getFileSize(fileName);
        writeMessage("OK");
        writeLong(fileSize);
    }

    private void sendError(String errorMessage) {
        writeMessage("ERROR " + errorMessage);
    }

    private void writeMessage(String message) {
        try {
            dos.writeUTF(message);
            dos.flush();
        } catch (IOException e) {
        }
    }

    private void writeLong(long value) {
        try {
            dos.writeLong(value);
            dos.flush();
        } catch (IOException e) {
        }
    }
}

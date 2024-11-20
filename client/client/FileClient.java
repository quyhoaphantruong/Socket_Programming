package client.client;
import java.io.*;
import java.net.*;
import java.util.*;

/**
 * The ClientHandler class processes requests from a connected client.
 */
class ClientHandler implements Runnable {

    private Socket clientSocket;
    private Map<String, Long> fileList;
    private DataInputStream dis;
    private DataOutputStream dos;

    public ClientHandler(Socket socket, Map<String, Long> fileList) {
        this.clientSocket = socket;
        this.fileList = fileList;
    }

    @Override
    public void run() {
        try {
            dis = new DataInputStream(clientSocket.getInputStream());
            dos = new DataOutputStream(clientSocket.getOutputStream());

            while (true) {
                String command = dis.readUTF();
                System.out.println("Received command: " + command);

                if (command.equals("LIST_FILES")) {
                    handleListFiles();
                } else if (command.startsWith("GET_CHUNK")) {
                    handleGetChunk(command);
                } else if (command.equals("QUIT")) {
                    System.out.println("Client requested to quit.");
                    break;
                } else {
                    dos.writeUTF("ERROR Unknown command");
                }
            }
        } catch (IOException e) {
            System.out.println("Client disconnected: " + clientSocket.getRemoteSocketAddress());
        } finally {
            closeConnections();
        }
    }

    /**
     * Handles the LIST_FILES command by sending the list of available files to the client.
     */
    private void handleListFiles() throws IOException {
        // Convert fileList to a string representation
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Long> entry : fileList.entrySet()) {
            sb.append(entry.getKey()).append(" ").append(entry.getValue()).append("\n");
        }

        dos.writeUTF(sb.toString());
        dos.flush();
        System.out.println("Sent file list to client.");
    }

    /**
     * Handles the GET_CHUNK command by sending the requested file chunk to the client.
     *
     * @param command The command string containing file name and chunk details.
     */
    private void handleGetChunk(String command) throws IOException {
        // Command format: GET_CHUNK filename start size
        String[] tokens = command.split(" ", 4);
        if (tokens.length != 4) {
            dos.writeUTF("ERROR Invalid GET_CHUNK command");
            return;
        }

        String filename = tokens[1];
        long start;
        long size;

        try {
            start = Long.parseLong(tokens[2]);
            size = Long.parseLong(tokens[3]);
        } catch (NumberFormatException e) {
            dos.writeUTF("ERROR Invalid start or size");
            return;
        }

        if (!fileList.containsKey(filename)) {
            dos.writeUTF("ERROR File not found");
            return;
        }

        File file = new File(filename);
        if (!file.exists()) {
            dos.writeUTF("ERROR File not found on server");
            return;
        }

        long fileSize = file.length();
        if (start < 0 || start >= fileSize) {
            dos.writeUTF("ERROR Invalid start position");
            return;
        }

        if (start + size > fileSize) {
            size = fileSize - start; // Adjust size if it exceeds file length
        }

        dos.writeUTF("OK"); // Acknowledge that the request is valid
        dos.flush();

        // Send the file chunk to the client
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            raf.seek(start);

            byte[] buffer = new byte[4096];
            long bytesToSend = size;

            while (bytesToSend > 0) {
                int bytesRead = raf.read(buffer, 0, (int) Math.min(buffer.length, bytesToSend));
                if (bytesRead == -1) break;

                dos.write(buffer, 0, bytesRead);
                bytesToSend -= bytesRead;
            }

            dos.flush();
            System.out.println("Sent chunk of file '" + filename + "' to client.");
        } catch (IOException e) {
            dos.writeUTF("ERROR Sending chunk");
            System.err.println("Error sending file chunk: " + e.getMessage());
        }
    }

    /**
     * Closes the connections and streams associated with the client.
     */
    private void closeConnections() {
        try {
            if (dis != null) dis.close();
            if (dos != null) dos.close();
            if (clientSocket != null && !clientSocket.isClosed()) clientSocket.close();
            System.out.println("Closed connection with client.");
        } catch (IOException e) {
            System.err.println("Error closing connections: " + e.getMessage());
        }
    }
}

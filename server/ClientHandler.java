import java.io.*;
import java.net.*;

public class ClientHandler implements Runnable {
    private Socket clientSocket;
    private FileListProvider fileListProvider;
    private DataInputStream dis;
    private DataOutputStream dos;

    public ClientHandler(Socket clientSocket, FileListProvider fileListProvider) {
        this.clientSocket = clientSocket;
        this.fileListProvider = fileListProvider;
    }

    @Override
    public void run() {
        initializeStreams();
        CommandProcessor commandProcessor = new CommandProcessor(dis, dos, fileListProvider);

        while (true) {
            String command = readCommand();
            if (command == null || command.equals("QUIT")) {
                break;
            }
            commandProcessor.processCommand(command);
        }

        closeConnections();
    }

    private void initializeStreams() {
        try {
            dis = new DataInputStream(clientSocket.getInputStream());
            dos = new DataOutputStream(clientSocket.getOutputStream());
        } catch (IOException e) {
        }
    }

    private String readCommand() {
        try {
            return dis.readUTF();
        } catch (IOException e) {
            return null;
        }
    }

    private void closeConnections() {
        try {
            dis.close();
            dos.close();
            clientSocket.close();
        } catch (IOException e) {
        }
    }
}

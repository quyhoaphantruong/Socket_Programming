import java.net.*;
import java.util.concurrent.*;

import client.client.ClientHandler;

public class FileServer {
    private static final int PORT = 12345;

    public static void main(String[] args) {
        new FileServer().start();
    }

    public void start() {
        FileListProvider fileListProvider = new FileListProvider("inputs.txt");
        ExecutorService clientHandlingPool = Executors.newCachedThreadPool();

        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("Server started on port " + PORT);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                ClientHandler clientHandler = new ClientHandler(clientSocket, fileListProvider);
                clientHandlingPool.submit(clientHandler);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            clientHandlingPool.shutdown();
        }
    }
}

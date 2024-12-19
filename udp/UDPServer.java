import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.zip.CRC32;

public class UDPServer {
    // Constants
    private static final int CHUNK_SIZE = 1250; // bytes
    private static final int MAX_BUFFER_SIZE = 4096; // bytes
    private static final int NUM_WORKERS = 30;

    // Message Types
    private static final byte MESSAGE_LS_TYPE = 1;
    private static final byte MESSAGE_PORT_TYPE = 2;
    private static final byte MESSAGE_LS_RESPONSE_TYPE = 3;
    private static final byte MESSAGE_GET_FILE_TYPE = 4;
    private static final byte MESSAGE_PORT_RANGE_TYPE = 5; // Not used in current implementation
    private static final byte MESSAGE_SYN_PORT_TYPE = 6;
    private static final byte MESSAGE_DATA_CHUNK_TYPE = 7;
    private static final byte MESSAGE_ACK_TYPE = 8;
    private static final byte MESSAGE_GET_ERROR_TYPE = 10;
    private static final byte MESSAGE_FIN_TYPE = 11;
    private static final byte MESSAGE_RETRANSMIT_TYPE = 12;

    // Server configuration
    private static final String SERVER_HOST = "0.0.0.0";
    private static final int SERVER_PORT = 12345;
    private static final String INPUT_FILE = "input.txt"; // Server's input file listing available files

    // Synchronization lock for printing
    private static final Object printLock = new Object();

    // Available files map
    private final Map<String, Long> availableFiles = new ConcurrentHashMap<>();

    // ExecutorService for handling client requests
    private final ExecutorService clientExecutor = Executors.newCachedThreadPool();

    // ExecutorService for worker threads
    private final ExecutorService workerExecutor = Executors.newFixedThreadPool(NUM_WORKERS);

    // Server socket
    private DatagramSocket serverSocket;

    public static void main(String[] args) {
        UDPServer server = new UDPServer();
        server.startUDPServer();
    }

    /**
     * Starts the UDP server.
     */
    public void startUDPServer() {
        try {
            serverSocket = new DatagramSocket(SERVER_PORT, InetAddress.getByName(SERVER_HOST));
            log("UDP Server listening on " + SERVER_HOST + ":" + SERVER_PORT);

            loadAvailableFiles();

            // Main server loop
            while (true) {
                byte[] receiveBuffer = new byte[MAX_BUFFER_SIZE];
                DatagramPacket receivePacket = new DatagramPacket(receiveBuffer, receiveBuffer.length);
                serverSocket.receive(receivePacket);

                // Handle each client request in a separate thread
                clientExecutor.execute(new ClientHandler(receivePacket));
            }

        } catch (SocketException e) {
            log("SocketException: " + e.getMessage());
        } catch (IOException e) {
            log("IOException: " + e.getMessage());
        } finally {
            shutdownServer();
        }
    }

    /**
     * Loads available files from INPUT_FILE into availableFiles map.
     */
    private void loadAvailableFiles() {
        File inputFile = new File(INPUT_FILE);
        if (!inputFile.exists()) {
            log("'" + INPUT_FILE + "' not found. No files are available for download.");
            return;
        }

        try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                String[] parts = line.split("\\s+");
                if (parts.length != 2) {
                    log("Invalid line in input file: " + line);
                    continue;
                }
                String filename = parts[0];
                long size = Long.parseLong(parts[1]);
                availableFiles.put(filename, size);
            }
            if (availableFiles.isEmpty()) {
                log("'" + INPUT_FILE + "' is empty. No files are available for download.");
            } else {
                log("Loaded available files:");
                for (Map.Entry<String, Long> entry : availableFiles.entrySet()) {
                    log(" - " + entry.getKey() + " (" + entry.getValue() + " bytes)");
                }
            }
        } catch (IOException | NumberFormatException e) {
            log("Error reading '" + INPUT_FILE + "': " + e.getMessage());
        }
    }

    private void shutdownServer() {
        if (serverSocket != null && !serverSocket.isClosed()) {
            serverSocket.close();
        }
        clientExecutor.shutdown();
        workerExecutor.shutdown();
        log("Server has been shut down.");
    }

    private static void log(String message) {
        synchronized (printLock) {
            System.out.println("[" + new Date() + "] " + message);
        }
    }

    private class ClientHandler implements Runnable {
        private final DatagramPacket packet;

        public ClientHandler(DatagramPacket packet) {
            this.packet = packet;
        }

        @Override
        public void run() {
            byte[] data = Arrays.copyOf(packet.getData(), packet.getLength());
            InetAddress clientAddress = packet.getAddress();
            int clientPort = packet.getPort();

            if (data.length < 1) {
                log("Received malformed message from " + clientAddress + ":" + clientPort + ". Ignoring.");
                return;
            }

            byte messageType = data[0];

            switch (messageType) {
                case MESSAGE_LS_TYPE:
                    log("Received LS request from " + clientAddress + ":" + clientPort);
                    handleLSRequest(clientAddress, clientPort);
                    break;

                case MESSAGE_GET_FILE_TYPE:
                    log("Received GET request from " + clientAddress + ":" + clientPort);
                    handleGetRequest(data, clientAddress, clientPort);
                    break;

                default:
                    log("Received unknown message type (" + messageType + ") from " + clientAddress + ":" + clientPort);
                    sendErrorMessage("ERROR: Unknown command.", clientAddress, clientPort);
                    break;
            }
        }

        private void handleLSRequest(InetAddress clientAddress, int clientPort) {
            if (availableFiles.isEmpty()) {
                sendErrorMessage("ERROR: No files available for download.", clientAddress, clientPort);
                return;
            }

            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, Long> entry : availableFiles.entrySet()) {
                sb.append(entry.getKey()).append(" ").append(entry.getValue()).append("\n");
            }
            byte[] payload = sb.toString().trim().getBytes(StandardCharsets.UTF_8);

            ByteBuffer buffer = ByteBuffer.allocate(1 + payload.length);
            buffer.put(MESSAGE_LS_RESPONSE_TYPE);
            buffer.put(payload);

            sendMessage(buffer.array(), clientAddress, clientPort);
            log("Sent LS_RESPONSE to " + clientAddress + ":" + clientPort + " with available files.");
        }

        private void handleGetRequest(byte[] data, InetAddress clientAddress, int clientPort) {
            if (data.length < 5) {
                log("Received malformed GET request from " + clientAddress + ":" + clientPort + ". Ignoring.");
                sendErrorMessage("ERROR: Malformed GET request.", clientAddress, clientPort);
                return;
            }

            ByteBuffer buffer = ByteBuffer.wrap(data);
            buffer.get(); // Skip message type
            int filenameLength = buffer.getInt();

            if (data.length < 5 + filenameLength) {
                log("Received incomplete GET request from " + clientAddress + ":" + clientPort + ". Ignoring.");
                sendErrorMessage("ERROR: Incomplete GET request.", clientAddress, clientPort);
                return;
            }

            byte[] filenameBytes = new byte[filenameLength];
            buffer.get(filenameBytes);
            String filename = new String(filenameBytes, StandardCharsets.UTF_8);

            if (!availableFiles.containsKey(filename)) {
                sendErrorMessage("ERROR: File '" + filename + "' not found.", clientAddress, clientPort);
                log("Sent error to " + clientAddress + ":" + clientPort + ": File '" + filename + "' not found.");
                return;
            }

            long fileSize = availableFiles.get(filename);
            int totalChunks = (int) Math.ceil((double) fileSize / CHUNK_SIZE);

            // Split into distinct ranges for worker threads
            int chunksPerWorker = (int) Math.ceil((double) totalChunks / NUM_WORKERS);
            int startChunk = 0;
            List<WorkerThread> workers = new ArrayList<>();

            for (int i = 0; i < NUM_WORKERS; i++) {
                int endChunk = startChunk + chunksPerWorker - 1;
                if (endChunk >= totalChunks) {
                    endChunk = totalChunks - 1;
                }
                if (startChunk > endChunk) {
                    break; // All chunks assigned
                }
                // log("Assigning chunks " + startChunk + " to " + endChunk + " to worker " + (i + 1));
                WorkerThread worker = new WorkerThread(filename, clientAddress, clientPort, startChunk, endChunk, fileSize);
                workers.add(worker);
                workerExecutor.execute(worker);
                startChunk = endChunk + 1;
                if (startChunk >= totalChunks) {
                    break;
                }
            }

            log("Sent total chunks to " + clientAddress + ":" + clientPort + ": " + totalChunks);
        }

        private void sendErrorMessage(String errorMessage, InetAddress clientAddress, int clientPort) {
            byte[] errorBytes = errorMessage.getBytes(StandardCharsets.UTF_8);
            ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + errorBytes.length);
            buffer.put(MESSAGE_GET_ERROR_TYPE);
            buffer.putInt(errorBytes.length);
            buffer.put(errorBytes);
            sendMessage(buffer.array(), clientAddress, clientPort);
        }

        private void sendMessage(byte[] message, InetAddress clientAddress, int clientPort) {
            DatagramPacket sendPacket = new DatagramPacket(message, message.length, clientAddress, clientPort);
            try {
                serverSocket.send(sendPacket);
            } catch (IOException e) {
                log("Failed to send message to " + clientAddress + ":" + clientPort + " - " + e.getMessage());
            }
        }
    }

    /**
     * WorkerThread class to handle sending file chunks to the client.
     */
    private class WorkerThread implements Runnable {
        private final String filename;
        private final InetAddress clientAddress;
        private int clientPort;
        private final int startChunk;
        private final int endChunk;
        private final long fileSize;

        public WorkerThread(String filename, InetAddress clientAddress, int clientPort, int startChunk, int endChunk, long fileSize) {
            this.filename = filename;
            this.clientAddress = clientAddress;
            this.clientPort = clientPort;
            this.startChunk = startChunk;
            this.endChunk = endChunk;
            this.fileSize = fileSize;
        }

        @Override
        public void run() {
            DatagramSocket workerSocket = null;
            try {
                workerSocket = new DatagramSocket();
                int availablePort = workerSocket.getLocalPort();

                // Prepare PORT message: [2][port (H)][start_chunk (I)][end_chunk (I)]
                ByteBuffer portBuffer = ByteBuffer.allocate(1 + 2 + 4 + 4);
                portBuffer.order(ByteOrder.BIG_ENDIAN);

                portBuffer.put(MESSAGE_PORT_TYPE);
                portBuffer.putShort((short) availablePort);
                portBuffer.putInt(startChunk);
                portBuffer.putInt(endChunk);
                byte[] portMessage = portBuffer.array();

                sendMessage(serverSocket, portMessage, clientAddress, clientPort);

                // Set timeout for receiving SYN
                workerSocket.setSoTimeout(5000); // 5 seconds

                // Wait for SYN
                byte[] synBuffer = new byte[1024]; // [6][port (H)][start_chunk (I)][end_chunk (I)]
                DatagramPacket synPacket = new DatagramPacket(synBuffer, synBuffer.length);
                workerSocket.receive(synPacket);

                // Validate SYN message
                ByteBuffer synByteBuffer = ByteBuffer.wrap(synPacket.getData(), 0, synPacket.getLength());
                byte synMessageType = synByteBuffer.get();
                if (synMessageType != MESSAGE_SYN_PORT_TYPE) {
                    log("Invalid message type (" + synMessageType + ") for SYN from " + clientAddress + ":" + clientPort + ". Stopping worker thread " + startChunk + "-" + endChunk);
                    return;
                }
                int synPort = synByteBuffer.getShort() & 0xFFFF;
                int synStartChunk = synByteBuffer.getInt();
                int synEndChunk = synByteBuffer.getInt();

                if (synPort != availablePort || synStartChunk != startChunk || synEndChunk != endChunk) {
                    log("Invalid SYN parameters from " + clientAddress + ":" + clientPort + ". Expected port " + availablePort + ", chunks " + startChunk + "-" + endChunk + ". Got port " + synPort + ", chunks " + synStartChunk + "-" + synEndChunk + ". Stopping worker thread.");
                    return;
                }
                // log("Received SYN message from client port: " + synPort + ", startChunk: " + synStartChunk + ", endChunk: " + synEndChunk);
                this.clientPort = synPacket.getPort();
                // Proceed with sending file chunks
                sendFileChunks(workerSocket);

            } catch (SocketTimeoutException e) {
                log("Timeout waiting for SYN from client " + clientAddress + ":" + clientPort + ". Stopping worker thread " + startChunk + "-" + endChunk + ".");
            } catch (IOException e) {
                log("IOException in worker thread for " + startChunk + "-" + endChunk + ": " + e.getMessage());
            } finally {
                if (workerSocket != null && !workerSocket.isClosed()) {
                    workerSocket.close();
                }
                log("Completed sending chunks " + startChunk + "-" + endChunk + " to " + clientAddress + ":" + clientPort);
            }
        }

        private void sendFileChunks(DatagramSocket workerSocket) {
            try (RandomAccessFile file = new RandomAccessFile(filename, "r")) {
                int currentChunk = startChunk;

                while (currentChunk <= endChunk) {
                    // Calculate the actual chunk size
                    long offset = (long) currentChunk * CHUNK_SIZE;
                    file.seek(offset);
                    int bytesToRead = CHUNK_SIZE;
                    if (offset + CHUNK_SIZE > fileSize) {
                        bytesToRead = (int) (fileSize - offset);
                    }
                    byte[] data = new byte[bytesToRead];
                    int readBytes = file.read(data);
                    if (readBytes == -1) {
                        break; // EOF
                    }

                    // Compute CRC32
                    CRC32 crc32 = new CRC32();
                    crc32.update(data);
                    long crcValue = crc32.getValue();

                    // Prepare DATA_CHUNK message: [7][chunk_id (I)][crc32 (I)][data]
                    ByteBuffer dataBuffer = ByteBuffer.allocate(1 + 4 + 4 + data.length);
                    dataBuffer.put(MESSAGE_DATA_CHUNK_TYPE);
                    dataBuffer.putInt(currentChunk);
                    dataBuffer.putInt((int) crcValue);
                    dataBuffer.put(data);
                    byte[] dataChunkMessage = dataBuffer.array();

                    // Send DATA_CHUNK
                    sendMessage(workerSocket, dataChunkMessage, clientAddress, clientPort);

                    // Wait for ACK or RETRANSMIT
                    try {
                        workerSocket.setSoTimeout(5000); // 5 seconds
                        byte[] ackBuffer = new byte[1024]; // [8][chunk_id (I)] or [12][chunk_id (I)]
                        DatagramPacket ackPacket = new DatagramPacket(ackBuffer, ackBuffer.length);
                        workerSocket.receive(ackPacket);

                        ByteBuffer ackByteBuffer = ByteBuffer.wrap(ackPacket.getData(), 0, ackPacket.getLength());
                        byte ackType = ackByteBuffer.get();
                        int ackChunkId = ackByteBuffer.getInt();
                        if (ackType == MESSAGE_ACK_TYPE) {
                            if (ackChunkId != currentChunk) {
                                log("Incorrect ACK number " + ackChunkId + " from " + clientAddress + ":" + clientPort + ". Expected " + currentChunk + ". Resending chunk.");
                                file.seek((long) currentChunk * CHUNK_SIZE);
                            } else {
                                currentChunk++;
                            }
                        } else if (ackType == MESSAGE_RETRANSMIT_TYPE) {
                            if (ackChunkId >= startChunk && ackChunkId <= endChunk) {
                                log("Received RETRANSMIT request for chunk " + ackChunkId + " from " + clientAddress + ":" + clientPort + ". Resending.");
                                currentChunk = ackChunkId;
                                file.seek((long) currentChunk * CHUNK_SIZE);
                                Thread.sleep(1000); // Wait before resending
                            } else {
                                log("Received RETRANSMIT request for out-of-range chunk " + ackChunkId + " from " + clientAddress + ":" + clientPort + ". Ignoring.");
                            }
                        } else {
                            log("Invalid ACK message type (" + ackType + ") from " + clientAddress + ":" + clientPort + ". Resending chunk " + currentChunk);
                            file.seek((long) currentChunk * CHUNK_SIZE);
                        }

                    } catch (SocketTimeoutException e) {
                        log("Timeout waiting for ACK for chunk " + currentChunk + " from " + clientAddress + ":" + clientPort + ". Resending chunk.");
                        file.seek((long) currentChunk * CHUNK_SIZE);
                    } catch (IOException | InterruptedException e) {
                        log("Exception while waiting for ACK: " + e.getMessage());
                        file.seek((long) currentChunk * CHUNK_SIZE);
                    }
                }

            } catch (FileNotFoundException e) {
                log("File not found: " + filename);
            } catch (IOException e) {
                log("IOException while sending file chunks: " + e.getMessage());
            }
        }

        private void sendMessage(DatagramSocket socket, byte[] message, InetAddress address, int port) throws IOException {
            DatagramPacket sendPacket = new DatagramPacket(message, message.length, address, port);
            socket.send(sendPacket);
        }
    }
}

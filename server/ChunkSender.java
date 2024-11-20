import java.io.*;

public class ChunkSender {
    private DataOutputStream dos;

    public ChunkSender(DataOutputStream dos) {
        this.dos = dos;
    }

    public void sendChunk(String fileName, long start, long size) {
        writeMessage("OK");

        try (RandomAccessFile raf = new RandomAccessFile(fileName, "r")) {
            raf.seek(start);
            byte[] buffer = new byte[8192];
            long bytesToSend = size;

            while (bytesToSend > 0) {
                int bytesRead = raf.read(buffer, 0, (int) Math.min(buffer.length, bytesToSend));
                if (bytesRead == -1) break;
                dos.write(buffer, 0, bytesRead);
                bytesToSend -= bytesRead;
            }

            dos.flush();
        } catch (IOException e) {
        }
    }

    private void writeMessage(String message) {
        try {
            dos.writeUTF(message);
            dos.flush();
        } catch (IOException e) {
        }
    }
}

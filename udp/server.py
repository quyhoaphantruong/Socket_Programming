import socket
import os
import threading
import struct
import math
import zlib

# Constants
CHUNK_SIZE = 1200  # bytes
ACK_FORMAT = '!I'  # Sequence Number (I)
HEADER_FORMAT = '!B'  # Message Type (B)
MAX_BUFFER_SIZE = 4096  # bytes
NUM_WORKERS = 50

# Message Types
MESSAGE_LS_TYPE = 1
MESSAGE_PORT_TYPE = 2
MESSAGE_LS_RESPONSE_TYPE = 3
MESSAGE_GET_FILE_TYPE = 4
MESSAGE_PORT_RANGE_TYPE = 5  # Not used in current implementation
MESSAGE_SYN_PORT_TYPE = 6
MESSAGE_DATA_CHUNK_TYPE = 7
MESSAGE_ACK_TYPE = 8
MESSAGE_ACK_RECEIVED_TYPE = 9  # Not used in current implementation
MESSAGE_GET_ERROR_TYPE = 10
MESSAGE_FIN_TYPE = 11
MESSAGE_RETRANSMIT_TYPE = 12  # Not used in current implementation

# Server configuration
SERVER_HOST = '0.0.0.0'
SERVER_PORT = 12345
INPUT_FILE = 'input.txt'  # Server's input file listing available files

print_lock = threading.Lock()

def send_message(sock, message, client_address):
    sock.sendto(message, client_address)

def get_available_port():
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind(('', 0))
        available_port = s.getsockname()[1]
        return available_port

def read_available_files():
    """
    Read the 'input.txt' file and return its content as a single string.
    Each line should follow the format: <filename>, <size_in_bytes>
    """
    if not os.path.exists(INPUT_FILE):
        with print_lock:
            print(f"'{INPUT_FILE}' not found. No files are available for download.")
        return ""

    try:
        with open(INPUT_FILE, 'r') as f:
            content = f.read().strip()
            if not content:
                with print_lock:
                    print(f"'{INPUT_FILE}' is empty. No files are available for download.")
                return ""
            return content
    except Exception as e:
        with print_lock:
            print(f"Error reading '{INPUT_FILE}': {e}")
        return ""

def handle_chunk_range_for_file(filename, client_addr, start_chunk, end_chunk, server_socket, available_files):
    worker_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    worker_socket.bind(('', 0))  # Bind to any available port
    available_port = worker_socket.getsockname()[1]

    # Prepare PORT message: [2][port (H)][start_chunk (I)][end_chunk (I)]
    message_type = MESSAGE_PORT_TYPE
    try:
        message = struct.pack('!BHII', message_type, available_port, start_chunk, end_chunk)
    except struct.error as e:
        with print_lock:
            print(f"Error packing PORT message: {e}. Skipping worker thread {start_chunk}-{end_chunk}")
        worker_socket.close()
        return
    send_message(sock=server_socket, message=message, client_address=client_addr)

    # Set a timeout for receiving SYN
    worker_socket.settimeout(5.0)  # 5 seconds timeout

    try:
        # Wait for client to reply with SYN
        syn_data, new_client_addr = worker_socket.recvfrom(1024)
        if len(syn_data) < 11:
            with print_lock:
                print(f"Malformed SYN message from {client_addr}. Stopping worker thread {start_chunk}-{end_chunk}")
            return
        # Unpack SYN message
        syn_message_type = struct.unpack('!B', syn_data[:1])[0]
        if syn_message_type != MESSAGE_SYN_PORT_TYPE:
            with print_lock:
                print(f"Invalid message type {syn_message_type} for SYN from {client_addr}. Stopping worker thread {start_chunk}-{end_chunk}")
            return
        # Unpack the rest
        syn_port, syn_start_chunk, syn_end_chunk = struct.unpack('!HII', syn_data[1:11])
        if syn_port != available_port or syn_start_chunk != start_chunk or syn_end_chunk != end_chunk:
            with print_lock:
                print(f"Invalid SYN parameters from {client_addr}. Expected port {available_port}, chunks {start_chunk}-{end_chunk}. Got port {syn_port}, chunks {syn_start_chunk}-{syn_end_chunk}. Stopping worker thread.")
            return
    except socket.timeout:
        with print_lock:
            print(f"Timeout waiting for SYN from client {client_addr}. Stopping worker thread {start_chunk}-{end_chunk}.")
        return
    except struct.error:
        with print_lock:
            print(f"Malformed SYN message from {client_addr}. Stopping worker thread {start_chunk}-{end_chunk}.")
        return

    # Proceed with sending file chunks
    try:
        file_size = available_files.get(filename, 0)
        total_chunks = math.ceil(file_size / CHUNK_SIZE)
        with open(filename, 'rb') as f:
            f.seek(start_chunk * CHUNK_SIZE)
            bytes_remaining = (end_chunk - start_chunk + 1) * CHUNK_SIZE
            chunk_id = start_chunk

            while bytes_remaining > 0 and chunk_id < total_chunks:
                data = f.read(CHUNK_SIZE)
                if not data:
                    break
                bytes_to_read = len(data)

                # Compute CRC32 of the data
                crc32 = zlib.crc32(data) & 0xffffffff  # Ensure unsigned

                # Prepare the DATA_CHUNK message: [7][chunk_id (I)][crc32 (I)][data]
                data_chunk_message_type = MESSAGE_DATA_CHUNK_TYPE
                try:
                    message = struct.pack('!BII', data_chunk_message_type, chunk_id, crc32) + data
                except struct.error as e:
                    with print_lock:
                        print(f"Error packing DATA_CHUNK message: {e}. Resending chunk {chunk_id}")
                    f.seek(-bytes_to_read, os.SEEK_CUR)  # Re-read the chunk
                    continue

                send_message(sock=worker_socket, message=message, client_address=new_client_addr)

                # Wait for ACK from client
                try:
                    worker_socket.settimeout(5.0)  # 5 seconds timeout for ACK
                    ack_data, _ = worker_socket.recvfrom(1024)
                    if len(ack_data) != struct.calcsize('!BI'):
                        with print_lock:
                            print(f"Malformed ACK received from {client_addr}. Resending chunk {chunk_id}")
                        f.seek(-bytes_to_read, os.SEEK_CUR)  # Re-read the chunk
                        continue

                    # Unpack ACK message
                    ack_message_type = struct.unpack('!B', ack_data[:1])[0]
                    if ack_message_type != MESSAGE_ACK_TYPE:
                        with print_lock:
                            print(f"Invalid ACK message type {ack_message_type} from {client_addr}. Resending chunk {chunk_id}")
                        f.seek(-bytes_to_read, os.SEEK_CUR)  # Re-read the chunk
                        continue

                    ack_num = struct.unpack('!I', ack_data[1:5])[0]

                    if ack_num != chunk_id:
                        with print_lock:
                            print(f"Incorrect ACK number {ack_num} from {client_addr}. Expected {chunk_id}. Resending chunk.")
                        f.seek(-bytes_to_read, os.SEEK_CUR)  # Re-read the chunk
                    else:
                        chunk_id += 1
                        bytes_remaining -= bytes_to_read
                except socket.timeout:
                    with print_lock:
                        print(f"Timeout waiting for ACK for chunk {chunk_id} from {client_addr}. Resending chunk.")
                    f.seek(-bytes_to_read, os.SEEK_CUR)  # Re-read the chunk
    except Exception as e:
        with print_lock:
            print(f"Error in worker thread for {start_chunk}-{end_chunk}: {e}")

    worker_socket.close()
    with print_lock:
        print(f"Completed sending chunks {start_chunk}-{end_chunk} to {client_addr}")

def handle_client_get_request(file_name, client_address, server_socket, available_files):
    """
    Handle the initial GET request from the client.
    Tell the client which port to connect to handle chunk ranges.
    """
    if file_name not in available_files:
        # Send GET_ERROR message: [10][error_message_length (I)][error_message_bytes]
        message_type = MESSAGE_GET_ERROR_TYPE
        error_message = f"ERROR: File '{file_name}' not found.".encode('utf-8')
        message = struct.pack('!BI', message_type, len(error_message)) + error_message
        send_message(server_socket, message, client_address)
        with print_lock:
            print(f"Sent error to {client_address}: {error_message.decode()}")
        return

    file_size = available_files[file_name]
    total_chunks = math.ceil(file_size / CHUNK_SIZE)

    # Split into distinct range to assign to worker threads
    chunks_per_worker = math.ceil(total_chunks / NUM_WORKERS)
    start_chunk = 0
    worker_threads = []

    for i in range(NUM_WORKERS):
        end_chunk = start_chunk + chunks_per_worker - 1
        if end_chunk >= total_chunks:
            end_chunk = total_chunks - 1
        with print_lock:
            print(f"Assigning chunks {start_chunk} to {end_chunk} to worker {i+1}")
        # Start a worker thread
        t = threading.Thread(target=handle_chunk_range_for_file, 
                             args=(file_name, client_address, start_chunk, end_chunk, server_socket, available_files))
        worker_threads.append(t)
        start_chunk = end_chunk + 1
        if start_chunk >= total_chunks:
            break  # All chunks assigned

    for thread in worker_threads:
        thread.start()

    with print_lock:
        print(f"Sent total chunks to {client_address}: {total_chunks}")

def get_available_files():
    files = read_available_files()
    result = {}    
    for file in files.split('\n'):
        filename, size = file.split(' ')
        result[filename] = int(size)
    return result
def handle_client_ls_files(client_address, server_socket):
    """
    Send the content of 'input.txt' to the client as a string.
    Prepare LS_RESPONSE message: [3][payload as string bytes]
    """
    available_files = read_available_files()  # Now returns string
    if not available_files:
        # Optionally, send an empty response or a specific message indicating no files
        message_type = MESSAGE_GET_ERROR_TYPE
        error_message = "ERROR: No files available for download.".encode('utf-8')
        message = struct.pack('!BI', message_type, len(error_message)) + error_message
        send_message(sock=server_socket, message=message, client_address=client_address)
        with print_lock:
            print(f"Sent error to {client_address}: {error_message.decode()}")
        return

    message_type = MESSAGE_LS_RESPONSE_TYPE
    message = struct.pack('!B', message_type) + available_files.encode('utf-8')
    send_message(sock=server_socket, message=message, client_address=client_address)
    with print_lock:
        print(f"Sent LS_RESPONSE to {client_address} with available files.")


def handle_client_handler(server_socket, data, client_address, available_files):
    """
    Handle incoming messages from a client.
    Determine the message type and process accordingly.
    """
    if len(data) < 1:
        # Invalid message
        with print_lock:
            print(f"Received malformed message from {client_address}. Ignoring.")
        return

    # Unpack message type
    message_type = struct.unpack('!B', data[:1])[0]
    
    if message_type == MESSAGE_LS_TYPE:
        with print_lock:
            print(f"Received LS request from {client_address}")
        handle_client_ls_files(client_address, server_socket)
    elif message_type == MESSAGE_GET_FILE_TYPE:
        # Unpack filename
        if len(data) < 5:
            with print_lock:
                print(f"Received malformed GET request from {client_address}. Ignoring.")
            return
        filename_length = struct.unpack('!I', data[1:5])[0]
        if len(data) < 5 + filename_length:
            with print_lock:
                print(f"Received incomplete GET request from {client_address}. Ignoring.")
            return
        filename = data[5:5+filename_length].decode('utf-8')
        with print_lock:
            print(f"Received GET request for '{filename}' from {client_address}")
        handle_client_get_request(filename, client_address, server_socket, available_files)
    else:
        # Unknown or unsupported message type
        # Send GET_ERROR message: [10][error_message_length (I)][error_message_bytes]
        error_message = "ERROR: Unknown command.".encode('utf-8')
        message_type = MESSAGE_GET_ERROR_TYPE
        message = struct.pack('!BI', message_type, len(error_message)) + error_message
        send_message(server_socket, message, client_address)
        with print_lock:
            print(f"Sent error to {client_address}: {error_message.decode()}")

def start_udp_server():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_socket.bind((SERVER_HOST, SERVER_PORT))
    with print_lock:
        print(f"UDP Server listening on {SERVER_HOST}:{SERVER_PORT}")

    available_files = get_available_files()

    running = True

    def server_loop():
        while running:     
            data, client_address = server_socket.recvfrom(1024)
            print(data)
            handle_client_handler(server_socket=server_socket, data=data, 
                                      client_address=client_address, available_files=available_files)

    server_thread = threading.Thread(target=server_loop, daemon=True)
    server_thread.start()

    try:
        while True:
            server_thread.join(timeout=1.0)
    except KeyboardInterrupt:
        with print_lock:
            print("\nServer is shutting down gracefully...")
        running = False
        server_socket.close()
        server_thread.join()
        with print_lock:
            print("Server has been shut down.")

if __name__ == "__main__":
    start_udp_server()

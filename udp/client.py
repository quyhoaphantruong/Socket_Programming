import socket
import threading
import struct
import os
import sys
import time
import zlib
import queue

# Constants
CHUNK_SIZE = 1200 # bytes
MAX_BUFFER_SIZE = 4096
NUM_WORKERS = 50

# Message Types
MESSAGE_LS_TYPE = 1
MESSAGE_PORT_TYPE = 2
MESSAGE_LS_RESPONSE_TYPE = 3
MESSAGE_GET_FILE_TYPE = 4
MESSAGE_PORT_RANGE_TYPE = 5
MESSAGE_SYN_PORT_TYPE = 6
MESSAGE_DATA_CHUNK_TYPE = 7
MESSAGE_ACK_TYPE = 8
MESSAGE_ACK_RECEIVED_TYPE = 9
MESSAGE_GET_ERROR_TYPE = 10
MESSAGE_FIN_TYPE = 11
MESSAGE_RETRANSMIT_TYPE = 12

# Client configuration
MONITOR_INPUT_FILE = 'input.txt'

print_lock = threading.Lock()
thread_file_size_local = threading.local()
current_downloaded_chunk = 0
downloaded_files = set()

def send_message(sock, message, server_address):
    sock.sendto(message, server_address)

def list_files(sock, server_address):
    """
    Send LS request to the server and retrieve the list of available files.
    """
    # Prepare LS message: [1]
    message_type = MESSAGE_LS_TYPE
    message = struct.pack('!B', message_type)
    send_message(sock, message, server_address)
    try:
        sock.settimeout(5.0)
        data, _ = sock.recvfrom(MAX_BUFFER_SIZE)
        if len(data) < 5:
            with print_lock:
                print("Malformed LS_RESPONSE received from server.")
            return ""
        # Unpack LS_RESPONSE: [3][filename_bytes]
        response_message_type = struct.unpack('!B', data[:1])[0]

        if response_message_type != MESSAGE_LS_RESPONSE_TYPE:
            with print_lock:
                print("Received unexpected message type during LS.")
            return ""
        files = data[1:].decode()
        if not files:
            print("No files available on the server.")
            return ""
        
        print("Available files on the server:")
        downloaded_files = {}

        for file in files.split('\n'):
            print(file)
            filename, size = file.split(' ')
            downloaded_files[filename] = int(size)
        return downloaded_files
    except socket.timeout:
        print("Failed to receive file list from the server.")
        return ""

def get_file(sock, server_address, filename):
    """
    Send GET request to the server for the specified filename.
    """
    # Prepare GET_FILE message: [4][filename_length (I)][filename_bytes]
    message_type = MESSAGE_GET_FILE_TYPE
    filename_bytes = filename.encode('utf-8')
    filename_length = len(filename_bytes)
    try:
        message = struct.pack('!BI', message_type, filename_length) + filename_bytes
    except struct.error as e:
        with print_lock:
            print(f"Error packing GET_FILE message: {e}")
        return
    send_message(sock, message, server_address)
    with print_lock:
        print(f"Sent GET request for '{filename}' to the server.")

def handle_worker_connection(server_ip, server_port, start_chunk, end_chunk, filename):
    """
    Handle downloading a specific chunk range from the server.
    """
    temp_filename = f"{filename}.{start_chunk}_{end_chunk}.part"
    server_worker_address = (server_ip, server_port)
    chunk_id = start_chunk
    global current_downloaded_chunk

    # Create a new UDP socket for this worker
    worker_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    worker_socket.bind(('', 0))  # Bind to any available port
    thread_file_size_local.num_downloaded_chunk = 0

    try:
        # Prepare SYN message: [6][port (H)][start_chunk (I)][end_chunk (I)]
        message_type = MESSAGE_SYN_PORT_TYPE
        syn_message = struct.pack('!BHII', message_type, server_port, start_chunk, end_chunk)
        send_message(worker_socket, syn_message, server_worker_address)
        # print(f"[Thread {start_chunk}-{end_chunk}] Sent SYN to {server_ip}:{server_port}")

        # Open the temp file for writing
        with open(temp_filename, 'wb') as temp_file:
            while chunk_id <= end_chunk:
                try:
                    worker_socket.settimeout(5.0)  # 5 seconds timeout for data
                    data, addr = worker_socket.recvfrom(MAX_BUFFER_SIZE)
                    if not data:
                        continue
                    if len(data) < 9:
                        with print_lock:
                            print(f"[Thread {start_chunk}-{end_chunk}] Received malformed packet. Ignoring.")
                        continue

                    # Unpack DATA_CHUNK message: [7][chunk_id (I)][crc32 (I)][data]
                    message_type = struct.unpack('!B', data[:1])[0]
                    if message_type != MESSAGE_DATA_CHUNK_TYPE:
                        with print_lock:
                            print(f"[Thread {start_chunk}-{end_chunk}] Received unexpected message type {message_type}. Ignoring.")
                        continue
                    received_chunk_id, received_crc32 = struct.unpack('!II', data[1:9])
                    chunk_data = data[9:]

                    if received_chunk_id != chunk_id:
                        with print_lock:
                            print(f"[Thread {start_chunk}-{end_chunk}] Received out-of-order chunk. Expected {chunk_id}, got {received_chunk_id}. Ignoring.")
                        continue

                    computed_crc32 = zlib.crc32(chunk_data) & 0xffffffff  # Ensure unsigned

                    if computed_crc32 != received_crc32:
                        with print_lock:
                            print(f"[Thread {start_chunk}-{end_chunk}] CRC mismatch for chunk {chunk_id}. Expected {received_crc32}, got {computed_crc32}. Not sending ACK.")
                        # Do not send ACK, server will resend
                        continue

                    temp_file.write(chunk_data)

                    # Prepare ACK message: [8][chunk_id (I)]
                    ack_message_type = MESSAGE_ACK_TYPE
                    try:
                        ack_message = struct.pack('!BI', ack_message_type, chunk_id)
                    except struct.error as e:
                        with print_lock:
                            print(f"[Thread {start_chunk}-{end_chunk}] Error packing ACK message: {e}. Not sending ACK.")
                        continue
                    send_message(worker_socket, ack_message, server_worker_address)
                    # with print_lock:
                    #     print(f"[Thread {start_chunk}-{end_chunk}] Received and ACKed chunk {chunk_id}.")
                    chunk_id += 1
                    thread_file_size_local.num_downloaded_chunk += 1
                    if thread_file_size_local.num_downloaded_chunk % 10 == 0:
                        with print_lock:
                            current_downloaded_chunk += thread_file_size_local.num_downloaded_chunk
                        thread_file_size_local.num_downloaded_chunk = 0
                except socket.timeout:
                    with print_lock:
                        print(f"[Thread {start_chunk}-{end_chunk}] Timeout waiting for chunk {chunk_id}. Resending SYN.")
                    # Optionally, resend SYN or implement retry mechanisms here
                    send_message(worker_socket, syn_message, server_worker_address)
                    chunk_id = start_chunk
                    with print_lock:
                        current_downloaded_chunk -= (end_chunk - start_chunk + 1)
                except Exception as e:
                    with print_lock:
                        print(f"[Thread {start_chunk}-{end_chunk}] Error: {e}")
                    break
    except struct.error as e:
        with print_lock:
            print(f"[Thread {start_chunk}-{end_chunk}] Error packing SYN message: {e}")
    finally:
        if thread_file_size_local.num_downloaded_chunk:
            with print_lock:
                current_downloaded_chunk += thread_file_size_local.num_downloaded_chunk
        worker_socket.close()

def assemble_file(filename):
    """
    Assemble all temp part files into the final file.
    """
    # Collect all temp files and sort them based on start_chunk
    temp_files = sorted([f for f in os.listdir('.') if f.startswith(filename) and f.endswith('.part')],
                        key=lambda x: int(x.split('_')[1].split('.')[0]))
    try:
        with open(filename, 'wb') as final_file:
            for temp_file in temp_files:
                with open(temp_file, 'rb') as tf:
                    final_file.write(tf.read())
        with print_lock:
            print(f"Successfully assembled the file '{filename}'.")
        # Optionally, remove temp files
        for temp_file in temp_files:
            os.remove(temp_file)
        with print_lock:
            print("Temporary files removed.")
    except Exception as e:
        with print_lock:
            print(f"Error assembling file '{filename}': {e}")

def receive_ports(sock, num_ports, timeout=10):
    """
    Receive PORT messages from the server.
    Returns a list of tuples: (port, start_chunk, end_chunk)
    """
    ports_info = []
    sock.settimeout(timeout)
    start_time = time.time()
    while len(ports_info) < num_ports:
        try:
            data, addr = sock.recvfrom(MAX_BUFFER_SIZE)
            if len(data) < 11:
                with print_lock:
                    print(f"Received malformed PORT message from {addr}. Ignoring.")
                continue
            # Unpack PORT message: [2][port (H)][start_chunk (I)][end_chunk (I)]
            message_type = struct.unpack('!B', data[:1])[0]
            if message_type != MESSAGE_PORT_TYPE:
                with print_lock:
                    print(f"Received unexpected message type {message_type} instead of PORT from {addr}. Ignoring.")
                continue
            port, start_chunk, end_chunk = struct.unpack('!HII', data[1:11])
            ports_info.append((port, start_chunk, end_chunk))
            # with print_lock:
            #     print(f"Received PORT message: Port={port}, Chunks={start_chunk}-{end_chunk}")
        except socket.timeout:
            with print_lock:
                print("Timeout while waiting for PORT messages.")
            break
        except struct.error:
            with print_lock:
                print("Received malformed PORT message. Ignoring.")
        # Check for overall timeout
        if time.time() - start_time > timeout:
            break
    return ports_info

def monitor_input_file(server_available_files, download_queue: queue, server_address):
    """
    Monitor the local 'input.txt' file every 5 seconds for new download requests.
    Add valid download requests to the download_queue.
    """
    last_position = 0
    while True:
        if not os.path.exists(MONITOR_INPUT_FILE):
            time.sleep(5)
            continue
        with open(MONITOR_INPUT_FILE, 'r') as f:
            lines = f.read()
            for line in lines.split('\n'):
                if not line: continue
                filename = line
                if filename in server_available_files and filename not in downloaded_files:
                    download_queue.put(filename)
        time.sleep(5)

def consumer_thread(server_ip, server_port, download_queue):
    """
    Consumer thread that processes the download_queue and downloads files sequentially.
    """
    while True:
        filename = download_queue.get()
        if filename is None:
            break
        with print_lock:
            print(f"Starting download for '{filename}'")
        # Initiate GET request
        main_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        main_socket.bind(('', 0))  # Bind to any available port
        downloaded_files.add(filename)
        try:
            get_file(main_socket, (server_ip, server_port), filename)
            # Receive PORT messages
            ports_info = receive_ports(main_socket, NUM_WORKERS, timeout=10)
            if not ports_info:
                with print_lock:
                    print(f"No PORT messages received for '{filename}'. Cannot proceed with download.")
                continue

            # Spawn threads to download each chunk range
            threads = []
            total_chunks = 0
            global current_downloaded_chunk
            current_downloaded_chunk = 0
            for port, start_chunk, end_chunk in ports_info:
                total_chunks += (end_chunk - start_chunk + 1)
                thread = threading.Thread(target=handle_worker_connection, args=(server_ip, port, start_chunk, end_chunk, filename))
                thread.start()
                threads.append(thread)

            # Start monitor progress thread
            monitor_progress_thread = threading.Thread(target=monitor_progress, 
                                                       args=(filename, total_chunks),
                                                       daemon=True)
            monitor_progress_thread.start()

            for thread in threads:
                thread.join()
            
            with print_lock:
                print(f"All chunk downloads completed for '{filename}'.")

            assemble_file(filename)
        finally:
            main_socket.close()
            print(f"Download completed for '{filename}'.")
        download_queue.task_done()

def monitor_progress(filename, total_chunks):
    print(f"start monitor thread {filename}")
    global current_downloaded_chunk
    while True:
        with print_lock:
            current_progress = round(current_downloaded_chunk * 100 / total_chunks, 2)
            print(f"Progress for downloading {filename}: {current_progress}%")
        if current_progress == 100:
            return
        time.sleep(2)

def main():
    if len(sys.argv) != 3:
        print("Usage: python client.py <SERVER_IP> <SERVER_PORT>")
        sys.exit(1)

    server_ip = sys.argv[1]
    server_port = int(sys.argv[2])
    server_address = (server_ip, server_port)

    main_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    main_socket.bind(('', 0))
    with print_lock:
        print(f"Client socket bound to {main_socket.getsockname()}")

    try:
        # List available files
        # server_available_files is a map <filename, size in byte>
        server_available_files = list_files(main_socket, server_address)
        if not server_available_files:
            with print_lock:
                print("No files available on the server. Exiting.")
            return

        download_queue = queue.Queue()

        # Start monitor thread
        monitor_thread_obj = threading.Thread(target=monitor_input_file, args=(server_available_files, download_queue, server_address), daemon=True)
        monitor_thread_obj.start()

        # Start consumer thread
        consumer_thread_obj = threading.Thread(target=consumer_thread, args=(server_ip, server_port, download_queue), daemon=True)
        consumer_thread_obj.start()

        # Keep the main thread alive
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nClient is shutting down gracefully...")
    finally:
        main_socket.close()
        with print_lock:
            print("Client socket closed.")

if __name__ == "__main__":
    main()
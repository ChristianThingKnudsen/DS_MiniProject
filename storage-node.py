import os
import random
import string
import sys
import socket
import pickle
from time import time

import zmq
from utils import random_string
import messages_pb2
import reedsolomon 

MAX_CHUNKS_PER_FILE = 10


def write_file(file_data, file_name=None):
    """
    Write the given data to a local file with the given filename

    :param file_data: A bytes object that stores the file contents
    :param file_name: The file name. If not given, a random string is generated
    :return: The file name of the newly written file, or None if there was an error
    """

    if not file_name:
        # Generate random filename
        file_name = ''.join([random.SystemRandom().choice(string.ascii_letters + string.digits) for n in range(9)])
        # Add '.bin' extension
        file_name += ".bin"

    try:
        # Open filename for writing binary content ('wb')
        # note: when a file is opened using the 'with' statment,
        # it is closed automatically when the scope ends
        with open('./' + file_name, 'wb') as f:
            f.write(file_data)
    except EnvironmentError as e:
        print("Error writing file: {}".format(e))
        return None

    return file_name


def retrieve_file(file_name):
    try:
        with open(file_name, "rb") as file:
            print("Found chunk %s, sending it back" % file_name)
            file_to_return = file.read()
            print('File read')

    except FileNotFoundError:
        print(f'File not found with name: {file_name}')
        pass

    return file_to_return


# Read the folder name where chunks should be stored from the first program argument
# (or use the current folder if none was given)
data_folder = sys.argv[1] if len(sys.argv) > 1 else "./"
if data_folder != "./":
    # Try to create the folder
    try:
        os.mkdir('./' + data_folder)
    except FileExistsError as _:
        # OK, the folder exists
        pass

context = zmq.Context()
# Socket to receive Store Chunk messages from the controller
# Ask the user to input the last segment of the server IP address
# server_address = input("Server address: 192.168.0.___ ") # Incomment in order to make it dynamic
server_address = str(100) # Added in order to simplify

# Socket to receive Store Chunk messages from the controller
pull_address = "tcp://192.168.0." + server_address + ":5557"
# pull_address = "tcp://localhost:5557"
receiver = context.socket(zmq.PULL)
receiver.connect(pull_address)
print("Listening on %s" % pull_address)

# Socket to send results to the controller
sender = context.socket(zmq.PUSH)
sender.connect("tcp://192.168.0." + server_address + ":5558")
# sender.connect("tcp://localhost:5558")

# Socket to receive Get Chunk messages from the controller
subscriber = context.socket(zmq.SUB)
subscriber.connect("tcp://192.168.0." + server_address + ":5559")
# subscriber.connect("tcp://localhost:5559")

# HDFS sockets:
hdfs_receive_socket = context.socket(zmq.REP)
hdfs_receive_socket.bind("tcp://*:5540")

hdfs_retrieve_socket = context.socket(zmq.REP)
hdfs_retrieve_socket.bind("tcp://*:5541")

encode_socket = context.socket(zmq.REP)
encode_socket.bind("tcp://*:5542")

decode_socket = context.socket(zmq.REP)
decode_socket.bind("tcp://*:5543")

ecrs_receive_socket = context.socket(zmq.REP)
ecrs_receive_socket.bind("tcp://*:5544")

# Receive every message (empty subscription)
subscriber.setsockopt(zmq.SUBSCRIBE, b'')

# Use a Poller to monitor two sockets at the same time
poller = zmq.Poller()
poller.register(receiver, zmq.POLLIN)
poller.register(subscriber, zmq.POLLIN)
poller.register(hdfs_receive_socket, zmq.POLLIN)
poller.register(hdfs_retrieve_socket, zmq.POLLIN)
poller.register(encode_socket, zmq.POLLIN)
poller.register(decode_socket, zmq.POLLIN)
poller.register(ecrs_receive_socket, zmq.POLLIN)

print("Data folder: %s" % data_folder)

local_ip = (([ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")] or [
    [(s.connect(("8.8.8.8", 53)), s.getsockname()[0], s.close()) for s in
     [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]]) + ["no IP found"])[0]
print(f"Running on: {local_ip}")

while True:
    try:
        # Poll all sockets
        socks = dict(poller.poll())
    except KeyboardInterrupt:
        break

    # At this point one or multiple sockets have received a message
    if receiver in socks:
        # Incoming message on the PULL
        # Incoming message on the 'receiver' socket where we get tasks to store a chunk
        msg = receiver.recv_multipart()
        # Parse the Protobuf message from the first frame
        task = messages_pb2.storedata_request()
        task.ParseFromString(msg[0])

        # The data starts with the second frame, iterate and store all frames
        for i in range(0, len(msg) - 1):
            data = msg[1 + i]
            print('Chunk to save: %s, size: %d bytes' % (task.filename + "." + str(i), len(data)))
            # Store the chunk with the given filename
            chunk_local_path = data_folder + '/' + task.filename + "." + str(i)
            write_file(data, chunk_local_path)
            print("Chunk saved to %s" % chunk_local_path)

        sender.send_pyobj({'filename': task.filename, 'ip': local_ip})

    if subscriber in socks:
        # Incoming message on the 'subscriber' socket where we get retrieve requests
        msg = subscriber.recv()
        # Parse the Protobuf message from the first frame
        task = messages_pb2.getdata_request()
        task.ParseFromString(msg)

        filename = task.filename
        print("File data request: %s" % filename)
        # Try to load all fragments with this name
        # send response only if found

        # First frame is the filename
        frames = [bytes(filename, 'utf-8')]
        # Subsequent frames will contain the chunks' data
        for i in range(0, MAX_CHUNKS_PER_FILE):
            try:
                with open(data_folder + '/' + filename + "." + str(i), "rb") as in_file:
                    print("Found chunk %s, sending it back" % filename)
                    # Add chunk as a new frame
                    frames.append(in_file.read())
            except FileNotFoundError:
                # This is OK here
                break
            # Only send a result if at least one chunk was found
        if len(frames) > 1:
            sender.send_multipart(frames)

    if hdfs_receive_socket in socks:
        t1 = time()
        message = hdfs_receive_socket.recv_multipart()

        # Parse the Protobuf message from the first frame
        task = messages_pb2.storedata_delegating_request()
        task.ParseFromString(message[0])

        # The data is the second frame
        data = message[1]

        local_path = data_folder + '/' + task.filename
        write_file(data, local_path)
        print("File saved to %s" % local_path)
        hdfs_receive_socket.send(b"Done")

        nodes_left = task.replica_locations
        print(f"storage node, nodes left: {nodes_left}")
        if len(nodes_left) > 0:
            next_node = nodes_left.pop(0)

            # Create Connection to node and send
            socket = context.socket(zmq.REQ)
            socket.connect(next_node + ':5540')

            # Create new proto_buf-msg
            new_task = messages_pb2.storedata_delegating_request()
            new_task.filename = task.filename
            new_task.replica_locations.extend(nodes_left)

            encoded_pb_file = new_task.SerializeToString()

            try:
                socket.send_multipart([
                    encoded_pb_file,
                    data
                ])
                f = open("HDFS_StorageNode_"+task.filename.split(".")[0] + ".csv", "a")
                f.write(str(time() - t1) + "\n")
                f.close()
                result = socket.recv()
                print(result)

            except zmq.error.ZMQError as e:
                print(e)
        else:
            f = open("HDFS_StorageNode_"+task.filename.split(".")[0] + ".csv", "a")
            f.write(str(time() - t1) + "\n")
            f.close()

    if hdfs_retrieve_socket in socks:
        message = hdfs_retrieve_socket.recv()

        task = messages_pb2.getdata_request()
        task.ParseFromString(message)

        print(f'Retrieving: {task.filename}')
        file = retrieve_file(task.filename)
        print(f'Length of message: {len(bytes(file))}')
        hdfs_retrieve_socket.send(bytes(file))

    if ecrs_receive_socket in socks:
        msg = ecrs_receive_socket.recv_multipart()
        # Parse the Protobuf message from the first frame
        task = messages_pb2.storedata_request()
        task.ParseFromString(msg[0])

        for i in range(0, len(msg) - 1):
            data = msg[1 + i]
            print('Chunk to save: %s, size: %d bytes' % (task.filename + "." + str(i), len(data)))
            # Store the chunk with the given filename
            chunk_local_path = data_folder + '/' + task.filename + "." + str(i)
            write_file(data, chunk_local_path)
            print("Chunk saved to %s" % chunk_local_path)

        ecrs_receive_socket.send_pyobj({'filename': task.filename, 'ip': local_ip})

    if encode_socket in socks:
        t1 = time() 
        msg = encode_socket.recv_pyobj()

        fragment_names = []
        for _ in range(4):
            fragment_names.append(random_string(8))

        encode_socket.send_pyobj({
            "names": fragment_names
        })

        encoded_fragments = reedsolomon.encode_file(msg['data'], int(msg['max_erasures']), msg['filename'], fragment_names)
        nodes = msg['nodes']

        sockets = []
        for i in range(len(encoded_fragments) - 1):
            sock = context.socket(zmq.REQ)
            print(f'Accessing: {nodes[i]}')
            sock.connect(nodes[i] + ':5544')

            sockets.append(sock)
            fragment = encoded_fragments[i]
            task = messages_pb2.storedata_request()
            task.filename = fragment['name']

            sock.send_multipart([
                task.SerializeToString(),
                fragment['data']
            ])

        data = encoded_fragments[-1]['data']
        # Store the chunk with the given filename
        chunk_local_path = data_folder + '/' + encoded_fragments[-1]['name'] + "." + str(0)
        write_file(data, chunk_local_path)
        print("Chunk saved to %s" % chunk_local_path)

        for socket in sockets:
            resp = socket.recv_pyobj()
            res_filename = resp['filename']
            res_ip = resp['ip']
            print(f'File {res_filename} stored on {res_ip}')

        f = open("EC_StorageNode_encode_"+str(msg['max_erasures'])+"l_"+msg['filename'].split(".")[0]+".csv", "a")
        f.write(str(time() - t1) + "\n")
        f.close()

    if decode_socket in socks:
        t1 = time() 
        print("Decode_socket")
        msg = decode_socket.recv_pyobj() 

        symbols = msg['data']
        file_size = msg['size']
        filename = msg['filename']
        data = reedsolomon.decode_file(symbols, filename)
        decode_socket.send_multipart([
            data[:file_size]
        ])
        f = open("EC_StorageNode_decode_"+str(len(symbols))+"l_"+filename.split(".")[0]+".csv", "a")
        f.write(str(time() - t1) + "\n")
        f.close()

    pass

import math
import random
import string
import zmq

import messages_pb2

def random_string(length=8):
    """
    Returns a random alphanumeric string of the given length.
    Only lowercase ascii letters and numbers are used.

    :param length: Length of the requested random string
    :return: The random generated string
    """
    return ''.join([random.SystemRandom().choice(string.ascii_letters + string.digits) for n in range(length)])


def store_file(file_data, send_task_socket, response_socket, n_stripes, n_replicas):
    if n_stripes == 1:
        filenames, nodes = __store_file__(file_data, send_task_socket, response_socket, n_stripes, n_replicas)

        storage_details = {"filenames": filenames, "nodes": nodes}

    else:
        filenames_part1, filenames_part2, nodes_part1, nodes_part2 = __store_file__(file_data, send_task_socket,
                                                                                    response_socket,
                                                                                    n_stripes,
                                                                                    n_replicas)
        storage_details = {
            "part1_filenames": filenames_part1,
            "part1_nodes": nodes_part1,
            "part2_filenames": filenames_part2,
            "part2_nodes": nodes_part2
        }

    return storage_details


def __store_file__(file_data, send_task_socket, response_socket, n_stripes, n_replicas):
    """
    Implements storing a file with RAID 1 using 4 storage nodes.

    :param file_data: A bytearray that holds the file contents
    :param send_task_socket: A ZMQ PUSH socket to the storage nodes
    :param response_socket: A ZMQ PULL socket where the storage nodes respond.
    :return: A list of the random generated chunk names, e.g. (c1,c2), (c3,c4)
    """

    if n_stripes == 1:
        filenames = []
        nodes = []
        for i in range(n_replicas):
            pb_file = messages_pb2.storedata_request()
            pb_file.filename = random_string(8)
            encoded_pb_file = pb_file.SerializeToString()

            send_task_socket.send_multipart([encoded_pb_file, file_data])

        # Wait until we receive k responses from the workers
        for task_nbr in range(n_replicas):
            resp = response_socket.recv_pyobj()
            filename = resp['filename']
            node_ip = resp['ip']

            filenames.append(filename)
            nodes.append(node_ip)

            print(f'Stored: {filename} on node {node_ip}')

        return filenames, nodes

    elif n_stripes == 2:
        size = len(file_data)

        # RAID 1: cut the file in half and store both halves 2x
        file_data_1 = file_data[:math.ceil(size / 2.0)]
        file_data_2 = file_data[math.ceil(size / 2.0):]

        file_data_1_names = []
        file_data_2_names = []

        for i in range(n_replicas):
            file_data_1_names.append(random_string(8))
            file_data_2_names.append(random_string(8))

        print("Filenames for part 1: %s" % file_data_1_names)
        print("Filenames for part 2: %s" % file_data_2_names)

        # Send 2 'store data' Protobuf requests with the first half and chunk names
        for name in file_data_1_names:
            task = messages_pb2.storedata_request()
            task.filename = name
            send_task_socket.send_multipart([
                task.SerializeToString(),
                file_data_1
            ])

        # Send 2 'store data' Protobuf requests with the second half and chunk names
        for name in file_data_2_names:
            task = messages_pb2.storedata_request()
            task.filename = name
            send_task_socket.send_multipart([
                task.SerializeToString(),
                file_data_2
            ])

        filenames_part1 = []
        filenames_part2 = []
        nodes_part1 = []
        nodes_part2 = []
        # Wait until we receive all responses from the workers
        for task_nbr in range(2 * n_replicas):
            resp = response_socket.recv_pyobj()
            filename = resp['filename']
            node_ip = resp['ip']

            if filename in file_data_1_names:
                filenames_part1.append(filename)
                nodes_part1.append(node_ip)
            else:
                filenames_part2.append(filename)
                nodes_part2.append(node_ip)

            print(f'Stored: {filename} on node {node_ip}')

        # Return the chunk names of each replica
        return filenames_part1, filenames_part2, nodes_part1, nodes_part2


#
def get_file_from_filename(networking, filenames, nodes, data_req_socket, response_socket, request_heartbeat_socket):
    if len(filenames) > 1:
        random_index = random.randint(0, len(filenames) - 1)
    else:
        random_index = 0

    # Select one filename
    filename = filenames.pop(random_index)
    node = nodes.pop(random_index)

    is_alive = networking.check_heartbeat(node, request_heartbeat_socket, response_socket)

    if is_alive:
        # Request filename
        task = messages_pb2.getdata_request()
        task.filename = filename
        data_req_socket.send(
            task.SerializeToString()
        )

        result = response_socket.recv_multipart()
        # First frame: file name (string)
        filename_received = result[0].decode('utf-8')
        # Second frame: data
        file_data = result[1]

        print("File received successfully")

        return file_data

    else:
        return get_file_from_filename(networking, filenames, nodes, data_req_socket, response_socket,
                                      request_heartbeat_socket)


def get_file_from_parts(networking, part1_filenames, part2_filenames, part1_nodes, part2_nodes, data_req_socket,
                        response_socket, request_heartbeat_socket):
    """
    Implements retrieving a file that is stored with RAID 1 using 4 storage nodes.

    :param part1_filenames: List of chunk names that store the first half
    :param part2_filenames: List of chunk names that store the second half
    :param data_req_socket: A ZMQ SUB socket to request chunks from the storage nodes
    :param response_socket: A ZMQ PULL socket where the storage nodes respond.
    :return: The original file contents
    """
    # Select one chunk of each half
    random_part1_index = random.randint(0, len(part1_filenames) - 1)
    random_part2_index = random.randint(0, len(part2_filenames) - 1)

    part1_filename = part1_filenames.pop(random_part1_index)
    part1_node = part1_nodes.pop(random_part1_index)

    part2_filename = part2_filenames.pop(random_part2_index)
    part2_node = part2_nodes.pop(random_part2_index)

    part1_node_is_alive = False
    part2_node_is_alive = False

    part1_node_is_alive = networking.check_heartbeat(part1_node, request_heartbeat_socket, response_socket)
    part2_node_is_alive = networking.check_heartbeat(part2_node, request_heartbeat_socket, response_socket)

    while not (part1_node_is_alive and part2_node_is_alive):
        if not part1_node_is_alive:
            if len(part1_filenames) > 1:
                random_part1_index = random.randint(0, len(part1_filenames) - 1)
            else:
                random_part1_index = 0
            part1_filename = part1_filenames.pop(random_part1_index)
            part1_node = part1_nodes.pop(random_part1_index)
            part1_node_is_alive = networking.check_heartbeat(part1_node, request_heartbeat_socket, response_socket)
        if not part2_node_is_alive:
            if len(part1_filenames) > 1:
                random_part2_index = random.randint(0, len(part2_filenames) - 1)
            else:
                random_part2_index = 0
            part2_filename = part2_filenames.pop(random_part2_index)
            part2_node = part2_nodes.pop(random_part2_index)
            part2_node_is_alive = networking.check_heartbeat(part2_node, request_heartbeat_socket, response_socket)

    print(f'Fetching part 1: {part1_filename}')
    # Request both chunks in parallel
    task1 = messages_pb2.getdata_request()
    task1.filename = part1_filename
    data_req_socket.send(
        task1.SerializeToString()
    )
    print(f'Fetching part 2: {part2_filename}')
    task2 = messages_pb2.getdata_request()
    task2.filename = part2_filename
    data_req_socket.send(
        task2.SerializeToString()
    )

    # Receive both chunks and insert them to
    file_data_parts = [None, None]
    for _ in range(2):
        result = response_socket.recv_multipart()
        # First frame: file name (string)
        filename_received = result[0].decode('utf-8')
        print("Received %s" % filename_received)

        # Second frame: data
        chunk_data = result[1]

        if filename_received == part1_filename:
            # The first part was received
            file_data_parts[0] = chunk_data
        else:
            # The second part was received
            file_data_parts[1] = chunk_data

    print("Both chunks received successfully")

    # Combine the parts and return
    file_data = file_data_parts[0] + file_data_parts[1]
    return file_data
#

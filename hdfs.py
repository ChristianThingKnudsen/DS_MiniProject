import zmq
import messages_pb2
import random
from time import time


def store_file(networking, context, filename, n_replicas, file_data):
    peer_nodes = networking.get_n_peer_node_address(n_replicas)
    print(f"HDFS pper nodes: {peer_nodes}")
    first_node = peer_nodes.pop(0)

    # Create Protobuf file
    pb_file = messages_pb2.storedata_delegating_request()
    pb_file.filename = filename
    pb_file.replica_locations.extend(peer_nodes)

    encoded_pb_file = pb_file.SerializeToString()

    # Create Connection to node and send
    socket = context.socket(zmq.REQ)
    socket.connect(first_node + ':5540')
    try:
        socket.send_multipart([
            encoded_pb_file,
            file_data
        ])
        result = socket.recv()
    except zmq.error.ZMQError as e:
        print(e)

    return {"nodes": [first_node] + peer_nodes}


def get_file(networking, context, filename, nodes, request_heartbeat_socket, response_socket):
    print(f"len nodes: {len(nodes)}")
    if len(nodes) > 1:
        random_index = random.randint(0, len(nodes) - 1)
    else:
        random_index = 0
    print(f"random index: ${random_index}")
    node = nodes.pop(random_index)
    node = node[6:]
    print(f'Checking: {node}')

    pb_file = messages_pb2.getdata_request()
    pb_file.filename = filename

    # is_alive = networking.check_heartbeat(node, request_heartbeat_socket, response_socket) # TODO: Incomment later

    if True: # is_alive # TODO: Removed by us 
        socket = context.socket(zmq.REQ)
        socket.connect('tcp://' + node + ':5541')
        try:
            encoded_pb_file = pb_file.SerializeToString()
            socket.send(bytes(encoded_pb_file))
            result = socket.recv()
            return result
        except zmq.error.ZMQError as e:
            print(e)
            pass

    return get_file(networking, context, filename, nodes, request_heartbeat_socket, response_socket)

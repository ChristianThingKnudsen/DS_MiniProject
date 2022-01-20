import zmq
import messages_pb2
import random


def store_file(networking, context, filename, n_replicas, file_data): # Storing 
    peer_nodes = networking.get_peer_nodes_addresses(n_replicas)
    first_node = peer_nodes.pop(0)
    # Protobuf
    pb_file = messages_pb2.storedata_delegating_request()
    pb_file.filename = filename
    pb_file.replica_locations.extend(peer_nodes)
    encoded_pb_file = pb_file.SerializeToString()
    # Connection
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


def get_file(networking, context, filename, nodes, response_socket): # Getting
    if len(nodes) > 1: # If there is more than one node left, do it random
        rnd = random.randint(0, len(nodes) - 1)
    else:
        rnd = 0
    node = nodes.pop(rnd)
    node = node[6:]
    print(f'Checking node: {node}') # Raw ip
    # Protobuf
    pb_file = messages_pb2.getdata_request()
    pb_file.filename = filename
    # Connection
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

    return get_file(networking, context, filename, nodes, response_socket)
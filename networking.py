import random
import zmq
import messages_pb2


class Networking:

    def __init__(self, peers):
        self.peers = peers

    def get_n_peer_node_address(self, n):
        nodes_left = self.peers[:]
        peer_nodes = []
        for i in range(n):
            if len(nodes_left) > 2:
                index = random.randint(0, len(nodes_left) - 1)
            else:
                index = 0
            peer_nodes.append(nodes_left.pop(index))

        return peer_nodes

import random


class Networking:

    def __init__(self, peers): # Constructor
        self.peers = peers

    def get_peer_nodes_addresses(self, n): # Method for getting all peer nodes adresses left
        nodes_left = self.peers[:] # Peer nodes left 
        peer_nodes = [] # peer nodes array
        for i in range(n):
            if len(nodes_left) > 2: # Pick rabdom if nodes left is greater than 2
                index = random.randint(0, len(nodes_left) - 1)
            else: # Take first index if the peer array consists of 2 node addresses 
                index = 0
            peer_nodes.append(nodes_left.pop(index)) # Remove (pop) index 

        return peer_nodes

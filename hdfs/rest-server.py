from flask import Flask, make_response, g, request, send_file
import base64
# from networking import Networking
from database import Database
import zmq
# import messages_pb2
import json
import io
import random

from utils import write_file 
# from database import get_db

app = Flask(__name__)

# ip_nodes = []
# for i in range(config.NUMBER_OF_NODES):
#     address = input(f'Node {i} address: 192.168.1.___ ')
#     ip_nodes.append('tcp://192.168.1.' + address)

# networking = Networking(ip_nodes)
database = Database()

# context = zmq.Context()

@app.route('/',  methods=['GET'])
def hello_world():
    return make_response({"message":"Hello world"})


@app.route('/files', methods=['POST'])
def add_files():
    payload = request.get_json()
    filename = payload.get('filename')
    content_type = payload.get('content_type')
    from base64 import b64decode
    file_data = b64decode(payload.get('contents_b64'))
    size = len(file_data)

    storage_details = write_file(file_data) #{"nodes": [first_node] + peer_nodes}
    # Insert the File record in the DB
    db = database.get_db()
    cursor = db.execute(
        "INSERT INTO `file`(`filename`, `size`, `content_type`, `storage_mode`, `storage_details`) VALUES (?,?,?,?,?)",
        # (filename, size, content_type, 'HDFS', json.dumps(storage_details))
        (filename, size, content_type, 'HDFS', storage_details)
    )
    db.commit()

    # Return the ID of the new file record with HTTP 201 (Created) status code
    return make_response({"id": cursor.lastrowid}, 201)


# @app.route('/files',  methods=['POST'])
# def add_files():
#     payload = request.get_json()
#     filename = payload.get('filename')
#     n_replicas = payload.get('n_replicas')
#     file_data = base64.b64decode(payload.get('contents_b64'))
#     size = len(file_data)
#     content_type = payload.get('content_type')

#     peer_nodes = networking.get_n_peer_node_address(n_replicas)
#     first_node = peer_nodes.pop(0)

#     # Create Protobuf file
#     pb_file = messages_pb2.storedata_delegating_request()
#     pb_file.filename = filename
#     pb_file.replica_locations.extend(peer_nodes)

#     encoded_pb_file = pb_file.SerializeToString()

#     # Create Connection to node and send
#     socket = context.socket(zmq.REQ)
#     socket.connect(first_node + ':5540')

#     try:
#         socket.send_multipart([
#             encoded_pb_file,
#             file_data
#         ])
#         result = socket.recv()
#         print(result)
#     except zmq.error.ZMQError as e:
#         print(e)

#     storage_details = {"nodes": [first_node] + peer_nodes}

#     # Insert the File record in the DB
#     db = database.get_db()
#     cursor = db.execute(
#         "INSERT INTO `file`(`filename`, `size`, `content_type`, `storage_mode`, `storage_details`) VALUES (?,?,?,?,?)",
#         (filename, size, content_type, 'HDFS', json.dumps(storage_details))
#     )
#     db.commit()

#     # Return the ID of the new file record with HTTP 201 (Created) status code
#     return make_response({"id": cursor.lastrowid}, 201)

# @app.route('/files/<file_name>', methods=['GET'])
# def download_file(file_name):
#     db_conn = Database.get_db()
#     cursor = db_conn.execute("SELECT * FROM `file` WHERE `filename`=?", [file_name])
#     if not cursor:
#         return make_response({"message": "Error connecting to the database"}, 500)

#     f = cursor.fetchone()
#     if not f:
#         return make_response({"message": "File {} not found".format(file_name)}, 404)

#     # Convert to a Python dictionary
#     f = dict(f)

#     pb_file = messages_pb2.getdata_request()
#     pb_file.filename = f['filename']

#     storage_details = json.loads(f['storage_details'])

#     nodes = storage_details['nodes']

#     # Select one node
#     node = nodes[random.randint(0, len(nodes) - 1)]

#     socket = context.socket(zmq.REQ)
#     socket.connect(node + ':5541')
#     try:
#         encoded_pb_file = pb_file.SerializeToString()
#         socket.send(bytes(encoded_pb_file))
#         result = socket.recv()
#     except zmq.error.ZMQError as e:
#         print(e)
#         return make_response({"message": "ZMQ error"}, 400)

#     return send_file(io.BytesIO(result), mimetype=f['content_type'])


## From week 4 

@app.route('/files/<int:file_id>',  methods=['GET'])
def download_file(file_id):
    db = database.get_db()
    cursor = db.execute("SELECT * FROM `file` WHERE `id`=?", [file_id])
    if not cursor: 
        return make_response({"message": "Error connecting to the database"}, 500)
    f = cursor.fetchone()
    # Convert to a Python dictionary
    f = dict(f)
    print("File requested: {}".format(f))
    return send_file(f['storage_details'], mimetype=f['content_type'])


app.run(host="0.0.0.0", port=5000)

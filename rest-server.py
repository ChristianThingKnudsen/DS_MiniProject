import base64
import io
import json
import time
from networking import Networking
import zmq
import random
from flask import Flask, make_response, request, send_file
import copy
import hdfs
import messages_pb2 
import raid1 
import reedsolomon  
from database import Database

database = Database()

# For HDFS the name node need to know what storage nodes is available.
ip_nodes = []
address = 100 # Added by us in order to simplify
for i in range(4):
    # address = input(f'Node {i} address: 192.168.0.___ ') # Incomment for dynmic approach
    ip_nodes.append('tcp://192.168.0.' + str(address))
    address +=1
    

networking = Networking(ip_nodes)

context = zmq.Context()

# Socket to send tasks to Storage Nodes
send_task_socket = context.socket(zmq.PUSH)
send_task_socket.bind("tcp://*:5557")

# Socket to receive messages from Storage Nodes
response_socket = context.socket(zmq.PULL)
response_socket.bind("tcp://*:5558")

# Publisher socket for data request broadcasts
data_req_socket = context.socket(zmq.PUB)
data_req_socket.bind("tcp://*:5559")

# Socket to send tasks to Storage Nodes
request_heartbeat_socket = context.socket(zmq.PUB)
request_heartbeat_socket.bind("tcp://*:5560")

# Wait for all workers to start and connect.
time.sleep(1)
print("Listening to ZMQ messages on tcp://*:5558")

# Instantiate the Flask app (must be before the endpoint functions)
app = Flask(__name__)
# Close the DB connection after serving the request
app.teardown_appcontext(database.close_db)

@app.route('/')
def hello():
    return make_response({'message': 'Hello from rest-server!'})

@app.route('/files', methods=['GET'])
def list_files():
    db = database.get_db()
    cursor = db.execute("SELECT * FROM `file`")
    if not cursor:
        return make_response({"message": "Error connecting to the database"}, 500)

    files = cursor.fetchall()
    # Convert files from sqlite3.Row object (which is not JSON-encodable) to
    # a standard Python dictionary simply by casting
    files = [dict(file) for file in files]

    return make_response({"files": files})


@app.route('/files', methods=['POST'])
def add_files():
    payload = request.form
    files = request.files

    storage_type = payload.get('storage', 'HDFS')

    if not files or not files.get('file'):
        return make_response("File missing!", 400)

    # Reference to the file under 'file' key
    file = files.get('file')

    # The sender encodes a the file name and type together with the file contents
    filename = file.filename
    content_type = file.mimetype

    # Load the file contents into a bytearray and measure its size
    data = bytearray(file.read())
    size = len(data)
    print("File received: %s, size: %d bytes, type: %s" % (filename, size, content_type))

    if storage_type == "RAID1":
        print("RAID1")
        n_replicas = int(payload.get('n_replicas')) 
        n_stripes = int(payload.get('n_stripes'))
        if 0 < n_stripes < 3:
            storage_details = raid1.store_file(data, send_task_socket, response_socket, n_stripes, n_replicas) 
        else:
            return make_response({"message": "Number of stripes not supported"}, 404)

    elif storage_type == "HDFS":
        n_replicas = int(payload.get('n_replicas'))
        storage_details = hdfs.store_file(networking, context, filename, n_replicas, data)

    elif storage_type == "EC_RS": 
        max_erasures = int(payload.get('max_erasures', 1))
        type = payload.get('type')
        print("Max erasures: %d" % max_erasures)

        if type == 'a':
            fragment_names = reedsolomon.store_file(data, max_erasures, send_task_socket, response_socket, filename)

        elif type == 'b':

            nodes = networking.get_n_peer_node_address(4)

            encode_socket = context.socket(zmq.REQ)
            encode_socket.connect(nodes[-1] + ':5542')

            print(f"before encode: {time.time()}")
            encode_socket.send_pyobj({
                "data": data,
                "filename": filename,
                "nodes": nodes,
                "max_erasures":max_erasures
            })

            print(f"after encode: {time.time()}")

            result = encode_socket.recv_pyobj()
            fragment_names = result['names']

        storage_details = {
            "coded_fragments": fragment_names,
            "max_erasures": max_erasures,
            "type": type
        }
    else:
        return make_response({"message": "Invalid storage mode"}, 404)

    # Insert the File record in the DB
    db = database.get_db()
    cursor = db.execute(
        "INSERT INTO `file`(`filename`, `size`, `content_type`, `storage_type`, `storage_details`) VALUES (?,?,?,?,?)",
        (filename, size, content_type, storage_type, json.dumps(storage_details))
    )
    db.commit()

    # Return the ID of the new file record with HTTP 201 (Created) status code
    return make_response({"id": cursor.lastrowid}, 201)


@app.route('/files/<file_name>', methods=['GET'])
def download_file(file_name):
    db_conn = Database.get_db()
    cursor = db_conn.execute("SELECT * FROM `file` WHERE `filename`=?", [file_name])
    if not cursor:
        return make_response({"message": "Error connecting to the database"}, 500)

    f = cursor.fetchone()
    if not f:
        return make_response({"message": "File {} not found".format(file_name)}, 404)

    # Convert to a Python dictionary
    file_meta = dict(f)

    storage_details = json.loads(file_meta['storage_details'])
    if file_meta['storage_type'] == "RAID1":
        print("RAID1")
        if 'filenames' in storage_details: 
            filenames = storage_details['filenames']
            nodes = storage_details['nodes']

            file_data = raid1.get_file_from_filename(
                networking,
                filenames,
                nodes,
                data_req_socket,
                response_socket,
                request_heartbeat_socket
            )

        elif 'part1_filenames' in storage_details:
            part1_filenames = storage_details['part1_filenames']
            part2_filenames = storage_details['part2_filenames']
            part1_nodes = storage_details['part1_nodes']
            part2_nodes = storage_details['part2_nodes']

            file_data = raid1.get_file_from_parts(
                networking,
                part1_filenames,
                part2_filenames,
                part1_nodes,
                part2_nodes,
                data_req_socket,
                response_socket,
                request_heartbeat_socket
            )
    elif file_meta['storage_type'] == 'HDFS':
        nodes = storage_details['nodes']
        filename = file_meta['filename']
        file_data = hdfs.get_file(networking, context, filename, nodes, request_heartbeat_socket, response_socket)

    elif file_meta['storage_type'] == 'EC_RS':
        print("EC_RS")
        coded_fragments = storage_details['coded_fragments'] 
        max_erasures = storage_details['max_erasures']
        type = storage_details['type']

        if type == 'a':
            file_data = reedsolomon.get_file(
                coded_fragments,
                max_erasures,
                f['size'],
                data_req_socket,
                response_socket,
                file_name
            )

        elif type == 'b':
            fragnames = copy.deepcopy(coded_fragments)
            for i in range(max_erasures):
                fragnames.remove(random.choice(fragnames))

            # Request the coded fragments in parallel
            for name in fragnames:
                task = messages_pb2.getdata_request()
                task.filename = name
                data_req_socket.send(
                    task.SerializeToString()
                )

            # Receive all chunks and insert them into the symbols array
            symbols = []
            for _ in range(len(fragnames)):
                result = response_socket.recv_multipart()
                # In this case we don't care about the received name, just use the
                # data from the second frame
                print(result[0])
                symbols.append({
                    "chunkname": result[0].decode('utf-8'),
                    "data": bytearray(result[1])
                })

            node = networking.get_n_peer_node_address(1)

            decode_socket = context.socket(zmq.REQ)
            decode_socket.connect(node[0] + ':5543')

            decode_socket.send_pyobj({"data": symbols, "size": file_meta["size"],"filename":file_name})

            result = decode_socket.recv_multipart()

            file_data = result[0]
            print(f'decoded data rest_server: {file_data}')

    else:
        return make_response({"message": "Invalid storage details"}, 400)
    print(f'Length of file_data {len(file_data)}')
    return send_file(io.BytesIO(file_data), mimetype=file_meta['content_type'])


app.run(host="0.0.0.0", port=5000)

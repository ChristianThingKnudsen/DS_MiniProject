import kodo
import math
import random
import copy  # for deepcopy
from utils import random_string
import messages_pb2
import json
from time import time

STORAGE_NODES_NUM = 4

RS_CAUCHY_COEFFS = [
    bytearray([253, 126, 255, 127]),
    bytearray([126, 253, 127, 255]),
    bytearray([255, 127, 253, 126]),
    bytearray([127, 255, 126, 253])
]


def encode_file(file_data, max_erasures, filename, random_names):
    t1 = time()
    # Make sure we can realize max_erasures with 4 storage nodes
    assert (max_erasures >= 0)
    assert (max_erasures < STORAGE_NODES_NUM)

    # How many coded fragments (=symbols) will be required to reconstruct the encoded data.
    symbols = STORAGE_NODES_NUM - max_erasures
    # The size of one coded fragment (total size/number of symbols, rounded up)
    symbol_size = math.ceil(len(file_data) / symbols)
    # Kodo RLNC encoder using 2^8 finite field
    encoder = kodo.RLNCEncoder(kodo.field.binary8, symbols, symbol_size)
    encoder.set_symbols_storage(file_data)

    encoded_fragments = []
    # Generate one coded fragment for each Storage Node
    for i in range(STORAGE_NODES_NUM):
        # Select the next Reed Solomon coefficient vector
        coefficients = RS_CAUCHY_COEFFS[i]
        # Generate a coded fragment with these coefficients
        # (trim the coeffs to the actual length we need)
        symbol = encoder.produce_symbol(coefficients[:symbols])
        # Generate a random name for it and save
        name = random_names[i]
        # encoded_fragments.append(name)

        encoded_fragments.append({
            "name": name,
            "data": coefficients[:symbols] + bytearray(symbol)
        })
    f = open("EC_encoding_"+str(max_erasures)+"l_"+filename.split(".")[0]+".csv", "a")
    f.write(str(time() - t1) + "\n")
    f.close()
    return encoded_fragments


def store_file(file_data, max_erasures, send_task_socket, response_socket, filename):
    """
    Store a file using Reed Solomon erasure coding, protecting it against 'max_erasures'
    unavailable storage nodes.
    The erasure coding part codes are the customized version of the 'encode_decode_using_coefficients'
    example of kodo-python, where you can find a detailed description of each step.

    :param filename:
    :param file_data: The file contents to be stored as a Python bytearray
    :param max_erasures: How many storage node failures should the data survive
    :param send_task_socket: A ZMQ PUSH socket to the storage nodes
    :param response_socket: A ZMQ PULL socket where the storage nodes respond
    :return: A list of the coded fragment names, e.g. (c1,c2,c3,c4)
    """
    fragment_names = []
    for _ in range(4):
        fragment_names.append(random_string(8))

    encoded_fragments = encode_file(file_data, max_erasures, filename, fragment_names)

    for i in range(len(encoded_fragments)):
        fragment = encoded_fragments[i]
        task = messages_pb2.storedata_request()
        task.filename = fragment['name']

        send_task_socket.send_multipart([
            task.SerializeToString(),
            fragment['data']
        ])

    for task_nbr in encoded_fragments:
        resp = response_socket.recv_pyobj()

    return fragment_names


def decode_file(symbols, filename):
    t1 = time()
    """
    Decode a file using Reed Solomon decoder and the provided coded symbols.
    The number of symbols must be the same as STORAGE_NODES_NUM - max_erasures.

    :param symbols: coded symbols that contain both the coefficients and symbol data
    :return: the decoded file data
    """

    # Reconstruct the original data with a decoder
    symbols_num = len(symbols)
    symbol_size = len(symbols[0]['data']) - symbols_num  # subtract the coefficients' size
    decoder = kodo.RLNCDecoder(kodo.field.binary8, symbols_num, symbol_size)
    data_out = bytearray(decoder.block_size())
    decoder.set_symbols_storage(data_out)

    for symbol in symbols:
        # Separate the coefficients from the symbol data
        coefficients = symbol['data'][:symbols_num]
        symbol_data = symbol['data'][symbols_num:]
        # Feed it to the decoder
        decoder.consume_symbol(symbol_data, coefficients)

    # Make sure the decoder successfully reconstructed the file
    assert (decoder.is_complete())
    print("File decoded successfully")

    f = open("EC_decode_"+str(symbols_num)+"l_"+filename.split(".")[0]+".csv", "a")
    f.write(str(time() - t1) + "\n")
    f.close()
    return data_out


#


def get_file(coded_fragments, max_erasures, file_size,
             data_req_socket, response_socket, filename):
    """
    Implements retrieving a file that is stored with Reed Solomon erasure coding

    :param filename:
    :param coded_fragments: Names of the coded fragments
    :param max_erasures: Max erasures setting that was used when storing the file
    :param file_size: The original data size. 
    :param data_req_socket: A ZMQ SUB socket to request chunks from the storage nodes
    :param response_socket: A ZMQ PULL socket where the storage nodes respond.
    :return: A list of the random generated chunk names, e.g. (c1,c2), (c3,c4)
    """

    # We need 4-max_erasures fragments to reconstruct the file, select this many 
    # by randomly removing 'max_erasures' elements from the given chunk names. 
    fragnames = copy.deepcopy(coded_fragments)

    print(f"frags: {fragnames}")
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
        symbols.append({
            "chunkname": result[0].decode('utf-8'),
            "data": bytearray(result[1])
        })
    print("All coded fragments received successfully")

    # Reconstruct the original file data
    file_data = decode_file(symbols, filename)

    return file_data[:file_size]
#

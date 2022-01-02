import random
import string
import os

def random_string(length=8): # Generating a random string. Inspired by lab 6
    """
    Returns a random alphanumeric string of the given length. 
    Only lowercase ascii letters and numbers are used.

    :param length: Length of the requested random string 
    :return: The random generated string
    """
    return ''.join([random.SystemRandom().choice(string.ascii_letters + string.digits) for n in range(length)])
#

def write_file(data, filename=None): # Generic method for writing a file. Inspired by lab 6
    """
    Write the given data to a local file with the given filename

    :param data: A bytes object that stores the file contents
    :param filename: The file name. If not given, a random string is generated
    :return: The file name of the newly written file, or None if there was an error
    """
    if not filename:
        filename = random_string(8)
        filename += ".bin"
    
    try:
        with open('./'+filename, 'wb') as f:
            f.write(data)
    except EnvironmentError as e:
        print("Error writing file: {}".format(e))
        return None
    
    return filename
#

def is_raspberry_pi(): # Check if it is a raspberry pi. Inspired by lab 6 
    """
    Returns True if the current platform is a Raspberry Pi, otherwise False.
    """
    return os.uname().nodename == 'raspberrypi'
#
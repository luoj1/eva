import os
import time
from psutil import process_iter
from signal import SIGTERM
import re
import itertools

def shell(command):
    print(command)
    os.system(command)

def stop_eva_server():
    for proc in process_iter():
        if proc.name() == "eva_server":
            proc.send_signal(SIGTERM)


def connect_to_server():
    from eva.server.db_api import connect
    #%pip install nest_asyncio --quiet
    import nest_asyncio
    nest_asyncio.apply()

    # Connect client with server
    connection = connect(host = '127.0.0.1', port = 5432) 
    cursor = connection.cursor()

    return cursor

# Launch server
connect_to_server()
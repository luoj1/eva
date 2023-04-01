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

def launch_eva_server():
    # Stop EVA server if it is running
    stop_eva_server()

    os.environ['GPU_DEVICES'] = '0'

    # Start EVA server
    shell("nohup eva_server >> eva.log 2>&1 &")

    last_few_lines_count = 3
    try:
        with open('eva.log', 'r') as f:
            for lines in itertools.zip_longest(*[f]*last_few_lines_count):
                print(lines)
    except FileNotFoundError:
        pass

    # Wait for server to start
    time.sleep(10)

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
launch_eva_server()
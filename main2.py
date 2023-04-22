import faulthandler
#from flask import cli

faulthandler.enable()
import threading

threading.current_thread().name = "frigate"

from frigate.app import FrigateApp

#cli.show_server_banner = lambda *x: None
'''
def connect_to_server():
    from eva.server.db_api import connect
    import nest_asyncio
    nest_asyncio.apply()

    # Connect client with server
    connection = connect(host = '127.0.0.1', port = 5432) 
    cursor = connection.cursor()
    return cursor
'''
if __name__ == "__main__":
    frigate_app = FrigateApp()
    frigate_app.start()


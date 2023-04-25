# coding=utf-8
# Copyright 2018-2022 EVA
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import asyncio
from typing import Iterator, Optional

from eva.binder.statement_binder import StatementBinder
from eva.binder.statement_binder_context import StatementBinderContext
from eva.executor.plan_executor import PlanExecutor
from eva.models.server.response import Response, ResponseStatus
from eva.models.storage.batch import Batch
from eva.optimizer.plan_generator import PlanGenerator
from eva.optimizer.statement_to_opr_convertor import StatementToPlanConvertor
from eva.parser.parser import Parser
from eva.utils.logging_manager import logger
from eva.utils.stats import Timer

import subprocess as sp
import time
import psutil
import shutil
from collections import defaultdict
import datetime
from pathlib import Path

import logging
import threading
import os
import signal
import queue
import multiprocessing as mp
from multiprocessing.queues import Queue
from logging import handlers
from typing import Optional
from types import FrameType
from setproctitle import setproctitle
from typing import Deque, Optional
from types import FrameType
from collections import deque


def listener_configurer() -> None:
    root = logging.getLogger()
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "[%(asctime)s] %(name)-30s %(levelname)-8s: %(message)s", "%Y-%m-%d %H:%M:%S"
    )
    console_handler.setFormatter(formatter)
    root.addHandler(console_handler)
    root.setLevel(logging.INFO)


def root_configurer(queue: Queue) -> None:
    h = handlers.QueueHandler(queue)
    root = logging.getLogger()
    root.addHandler(h)
    root.setLevel(logging.INFO)


def log_process(log_queue: Queue) -> None:
    threading.current_thread().name = f"logger"
    setproctitle("frigate.logger")
    listener_configurer()

    stop_event = mp.Event()

    def receiveSignal(signalNumber: int, frame: Optional[FrameType]) -> None:
        stop_event.set()

    signal.signal(signal.SIGTERM, receiveSignal)
    signal.signal(signal.SIGINT, receiveSignal)

    while True:
        try:
            record = log_queue.get(timeout=1)
        except (queue.Empty, KeyboardInterrupt):
            if stop_event.is_set():
                break
            continue
        logger = logging.getLogger(record.name)
        logger.handle(record)


# based on https://codereview.stackexchange.com/a/17959
class LogPipe(threading.Thread):
    def __init__(self, log_name: str):
        """Setup the object with a logger and start the thread"""
        threading.Thread.__init__(self)
        self.daemon = False
        self.logger = logging.getLogger(log_name)
        self.level = logging.ERROR
        self.deque: Deque[str] = deque(maxlen=100)
        self.fdRead, self.fdWrite = os.pipe()
        self.pipeReader = os.fdopen(self.fdRead)
        self.start()

    def cleanup_log(self, log: str) -> str:
        """Cleanup the log line to remove sensitive info and string tokens."""
        #log = clean_camera_user_pass(log).strip("\n")
        return log

    def fileno(self) -> int:
        """Return the write file descriptor of the pipe"""
        return self.fdWrite

    def run(self) -> None:
        """Run the thread, logging everything."""
        for line in iter(self.pipeReader.readline, ""):
            self.deque.append(self.cleanup_log(line))

        self.pipeReader.close()

    def dump(self) -> None:
        while len(self.deque) > 0:
            self.logger.log(self.level, self.deque.popleft())

    def close(self) -> None:
        """Close the write end of the pipe."""
        os.close(self.fdWrite)

####
def execute_query(query, report_time: bool = False, **kwargs) -> Iterator[Batch]:
    """
    Execute the query and return a result generator.
    """
    query_compile_time = Timer()
    plan_generator = kwargs.pop("plan_generator", PlanGenerator())
    with query_compile_time:
        stmt = Parser().parse(query)[0]
        StatementBinder(StatementBinderContext()).bind(stmt)
        l_plan = StatementToPlanConvertor().visit(stmt)
        p_plan = plan_generator.build(l_plan)
        output = PlanExecutor(p_plan).execute_plan()

    query_compile_time.log_elapsed_time("Query Compile Time")
    return output


def execute_query_fetch_all(query, **kwargs) -> Optional[Batch]:
    """
    Execute the query and fetch all results into one Batch object.
    """
    output = execute_query(query, report_time=True, **kwargs)
    if output:
        batch_list = list(output)
        return Batch.concat(batch_list, copy=False)

def webcam_execute_query_fetch_all(query, **kwargs) -> Optional[Batch]:
    """
    Execute the query and fetch all results into one Batch object.
    """
    # setup camera
    logpipe = LogPipe(
        "ffmpeg.cam1"
    )
    '''
    ffmpeg_cmd = ['ffmpeg', '-hide_banner', '-loglevel', 'warning', '-threads', '1', 
     '-user_agent', 'FFmpeg Frigate/0.1x', '-avoid_negative_ts', 'make_zero', 
     '-fflags', '+genpts+discardcorrupt', '-rtsp_transport', 'tcp', 
     '-timeout', '5000000', '-use_wallclock_as_timestamps', '1', 
     '-i', 'rtsp://localhost:8554/stream', '-f', 'segment', '-segment_time', '5', 
     '-segment_format', 'mp4', '-reset_timestamps', '1', '-strftime', '1', 
     '-c', 'copy', '-an', '/media/james/p0/eva_tmp/cache/camera_1-%Y%m%d%H%M%S.mp4', '-r', '5', '-s', '1280x720', '-threads', '1', 
     '-f', 'rawvideo', '-pix_fmt', 'yuv420p', 'pipe:']
    '''
    ffmpeg_cmd = ['ffmpeg', '-i', 'rtsp://localhost:8558/stream',
     '-map', '0', '-c', 'copy', '-f', 'segment', '-segment_time', '5', '-reset_timestamps',
      '1', '-use_wallclock_as_timestamps', '1', '-strftime', '1',
      '-an', '/media/james/p0/eva_tmp/cache/camera_1-%Y%m%d%H%M%S.mp4'
    ]
    process = sp.Popen(
        ffmpeg_cmd,
        stdout=sp.DEVNULL,
        stderr=logpipe,
        stdin=sp.DEVNULL,
        start_new_session=True,
    )
    print('ffmep_cmd exec. Status: ', process.returncode)

    start = time.time()   
    elapsed = 0
    while elapsed < 10:
        #scan file
        CACHE_DIR = '/media/james/p0/eva_tmp/cache'
        PERM_DIR =  '/media/james/p0/eva_tmp/perm'
        cache_files = sorted(
            [
                d
                for d in os.listdir(CACHE_DIR)
                if os.path.isfile(os.path.join(CACHE_DIR, d))
                and d.endswith(".mp4")
                and not d.startswith("clip_")
            ]
        )

        files_in_use = []
        for process in psutil.process_iter():
            try:
                if process.name() != "ffmpeg":
                    continue
                flist = process.open_files()
                if flist:
                    for nt in flist:
                        if nt.path.startswith(CACHE_DIR):
                            files_in_use.append(nt.path.split("/")[-1])
            except:
                continue

        for f in cache_files:
        # Skip files currently in use
            if f in files_in_use:
                continue

            cache_path = os.path.join(CACHE_DIR, f)
            perm_path = os.path.join(PERM_DIR, f)
            shutil.copyfile(cache_path, perm_path)

            basename = os.path.splitext(f)[0]
            camera, date = basename.rsplit("-", maxsplit=1)
            start_time = datetime.datetime.strptime(date, "%Y%m%d%H%M%S")
            # do load
            basename_san = basename.split('-')
            basename_san = basename_san[0]+"_"+basename_san[1]

            load_query = 'LOAD VIDEO \"' + perm_path + '\" INTO ' + basename_san + ';'
            print('load',load_query )
            try:
                output = execute_query(load_query, report_time=False, **kwargs)
                if output:
                    batch_list = list(output)
                    print('load webcam: ',batch_list)
            except Exception as e:
                print('load webcam stopped by bad mp4 when process running')
                error_msg = str(e)
                logger.warn(error_msg)
                #return
            os.remove(cache_path)
        
        elapsed = time.time() - start
        time.sleep(1)  
    process.kill()
    print('ffmep_cmd stop ')

    # scan files
    CACHE_DIR = '/media/james/p0/eva_tmp/cache'
    PERM_DIR =  '/media/james/p0/eva_tmp/perm'
    cache_files = sorted(
        [
            d
            for d in os.listdir(CACHE_DIR)
            if os.path.isfile(os.path.join(CACHE_DIR, d))
            and d.endswith(".mp4")
            and not d.startswith("clip_")
        ]
    )

    files_in_use = []
    for process in psutil.process_iter():
        try:
            if process.name() != "ffmpeg":
                continue
            flist = process.open_files()
            if flist:
                for nt in flist:
                    if nt.path.startswith(CACHE_DIR):
                        files_in_use.append(nt.path.split("/")[-1])
        except:
            continue

    # group recordings by camera
    # grouped_recordings = defaultdict(list)
    for f in cache_files:
        # Skip files currently in use
        if f in files_in_use:
            continue

        cache_path = os.path.join(CACHE_DIR, f)
        perm_path = os.path.join(PERM_DIR, f)
        shutil.copyfile(cache_path, perm_path)

        basename = os.path.splitext(f)[0]
        camera, date = basename.rsplit("-", maxsplit=1)
        start_time = datetime.datetime.strptime(date, "%Y%m%d%H%M%S")
        # do load
        basename_san = basename.split('-')
        basename_san = basename_san[0]+"_"+basename_san[1]
        load_query = 'LOAD VIDEO \"' + perm_path + '\" INTO ' + basename_san + ';'
        print('load',load_query )
        try:
            output = execute_query(load_query, report_time=False, **kwargs)
            if output:
                batch_list = list(output)
                print('load webcam: ',batch_list)
        except Exception as e:
            print('load webcam stopped by corrupted mp4')
            error_msg = str(e)
            logger.warn(error_msg)
            os.remove(perm_path)

        os.remove(cache_path)
        '''
        grouped_recordings[camera].append(
            {
                "cache_path": cache_path,
                "start_time": start_time,
            }
        )
        '''


    # convert load webcam query to a number of load video
    '''
    output = execute_query(query, report_time=True, **kwargs)
    if output:
        batch_list = list(output)
        return Batch.concat(batch_list, copy=False)
    '''


@asyncio.coroutine
def handle_request(client_writer, request_message):
    """
    Reads a request from a client and processes it

    If user inputs 'quit' stops the event loop
    otherwise just echoes user input
    """
    logger.debug("Receive request: --|" + str(request_message) + "|--")
    print("Receive request: --|" + str(request_message) + "|--")
    error = False
    error_msg = None
    query_runtime = Timer()
    with query_runtime:
        try:
            if 'LOAD WEBCAM' in request_message:
                output_batch = webcam_execute_query_fetch_all(request_message)
            else:
                output_batch = execute_query_fetch_all(request_message)
        except Exception as e:
            error_msg = str(e)
            logger.warn(error_msg)
            error = True

    if not error:
        response = Response(
            status=ResponseStatus.SUCCESS,
            batch=output_batch,
            query_time=query_runtime.total_elapsed_time,
        )
    else:
        response = Response(
            status=ResponseStatus.FAIL,
            batch=None,
            error=error_msg,
        )

    query_runtime.log_elapsed_time("Query Response Time")

    logger.debug(response)

    response_data = Response.serialize(response)

    client_writer.write(b"%d\n" % len(response_data))
    client_writer.write(response_data)

    return response

# Copyright 2020 Uber Technologies, Inc.
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

import json
import logging
import multiprocessing as mp
import os
import sys
import signal
import threading
import time
import uuid
from os.path import expanduser

import docker
import psutil
import requests
import subprocess

import fiber
import fiber.core as core
import fiber.config as config
from fiber.core import ProcessStatus
from fiber.util import find_ip_by_net_interface, find_listen_address

logger = logging.getLogger('fiber')


STATUS_MAP = {
    "restarting": ProcessStatus.INITIAL,
    "created": ProcessStatus.INITIAL,
    "running": ProcessStatus.STARTED,
    "paused": ProcessStatus.STARTED,
    "exited": ProcessStatus.STOPPED,
}

HOME_DIR = expanduser("~")

def timeout(seconds=0, minutes=0, hours=0):
    """
    Add a signal-based timeout to any block of code.
    If multiple time units are specified, they will be added together to determine time limit.
    Usage:
    with timeout(seconds=5):
        my_slow_function(...)
    Args:
        - seconds: The time limit, in seconds.
        - minutes: The time limit, in minutes.
        - hours: The time limit, in hours.
    """

    limit = seconds + 60 * minutes + 3600 * hours

    def handler(signum, frame):  # pylint: disable=W0613
        raise TimeoutError("timed out after {} seconds".format(limit))

    try:
        signal.signal(signal.SIGALRM, handler)
        signal.setitimer(signal.ITIMER_REAL, limit)
        yield
    finally:
        signal.alarm(0)
        
def _can_connect(host, port, s):
    try:
        print("testing connection to host %s", host)
        s.connect((host, port))
        s.close()
        print("can connect to host %s", host)
        return True
    except socket.error:
        print("can't connect to host %s", host)
        return False
        
def _wait_for_worker_nodes_to_start_sshd(hosts, interval=1, timeout_in_seconds=180):
    with timeout(seconds=timeout_in_seconds):
        while hosts:
            print("hosts that aren't SSHable yet: %s", str(hosts))
            for host in hosts:
                ssh_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                if _can_connect(host, 22, ssh_socket):
                    hosts.remove(host)
                    print(f"can connect to host: {host}") 
            time.sleep(interval)
            

            
class Backend(core.Backend):
    name = "sagemaker"

    def __init__(self):
        pass

    def create_job(self, job_spec):
        proc = subprocess.Popen(job_spec.command)
        print(job_spec)
        job = core.Job(proc, proc.pid)
        job.host = 'localhost'

        return job

    def get_job_status(self, job):
        proc = job.data

        if proc.poll() is not None:
            # subprocess has terminated
            return ProcessStatus.STOPPED

        return ProcessStatus.STARTED

    def wait_for_job(self, job, timeout):
        proc = job.data

        if timeout == 0:
            return proc.poll()

        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            return None

        return proc.returncode

    def terminate_job(self, job):
        proc = job.data

        proc.terminate()

    def get_listen_addr(self):
        import socket
        with open('/opt/ml/input/config/resourceconfig.json') as f:
            sagemaker_config = json.load(f)

        print(sagemaker_config)
        #ifce = sagemaker_config["network_interface_name"]
        ifce = 'eth0'
        ip = sagemaker_config["current_host"]

        _wait_for_worker_nodes_to_start_sshd(['algo-1','algo-2'], interval=1, timeout_in_seconds=180)
        
        return ip, 0, ifce

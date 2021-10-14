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


import logging
import multiprocessing as mp
import os
import sys
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


class Backend(core.Backend):
    name = "sagemaker"

    def __init__(self):
        pass

    def create_job(self, job_spec):
        proc = subprocess.Popen(job_spec.command)
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
        ip, ifce = find_listen_address()
        
        ifce = "eth0"
        ip = find_ip_by_net_interface(ifce)
        print(f"IP addrss: {ip}")

        if ip is None:
            raise mp.ProcessError(
                "Can't find a usable IPv4 address to listen. ifce_name: {}, "
                "ifces: {}".format(ifce, psutil.net_if_addrs()))
        # use 0 to bind to a random free port number
        return ip, 0, ifce

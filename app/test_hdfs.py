"""
Python script that tests writing data into HDFS.

Referneces:
- https://stackoverflow.com/questions/47926758/python-write-to-hdfs-file
"""

import json
import socket

from typing import Dict, List, Tuple

import requests
from urllib3.util import connection

from hdfs import InsecureClient

NAMENODE_HOST: str = 'localhost'
NAMENODE_PORT: int = 9870
NAMENODE_URL: str = f'http://{NAMENODE_HOST}:{NAMENODE_PORT}'
HDFS_URL: str = f'{NAMENODE_URL}/webhdfs/v1/'
USER: str = 'martin'

records: List[Dict] = [
    {'name': 'foo', 'weight': 1},
    {'name': 'bar', 'weight': 2},
]

params: Dict = {
    'op': 'CREATE',
    'overwrite': True,
}

_orig_create_connection = connection.create_connection


def patched_create_connection(address: Tuple, *args, **kwargs) -> socket.socket:
    """
    We have to override the DNS entries because hitting one container using
    the Python requests library with a 307 response redirect from the Namenode
    indicating the hostname of the Datanode to send the write request results in
    Python trying to hit a container by Container ID instead of by hostname, which
    should be localhost. In addition, we don't want to update the DNS configuration
    of the host since this is just a test, so we mock the DNS resolver of the urllib3
    library.
    """
    hostname, port = address
    if hostname != 'localhost':
        print(f'WARNING: Replacing {hostname} with localhost.')
        hostname: str = 'localhost'
    return _orig_create_connection((hostname, port), *args, **kwargs)

connection.create_connection = patched_create_connection

response: requests.Response = requests.put(f'{HDFS_URL}user/{USER}/a.txt', params=params, data=records)
print(response.status_code, response.reason)
print(response.text)

assert response.status_code == 201

client: InsecureClient = InsecureClient(NAMENODE_URL, user=USER)

with client.write('hello.txt', overwrite=True, encoding='utf-8') as writer:
    writer.write('Hello World, HDFS!')

with client.write('test2.json', overwrite=True, encoding='utf-8') as writer:
    json.dump(records, writer)

for file_ in client.list(f'/user/{USER}/'):
    print(file_)

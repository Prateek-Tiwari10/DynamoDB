import grpc
import pytest

from dynamo import dynamo_pb2_grpc
from dynamo.server import create_server
from dynamo.storage import Storage


class Node:
    def __init__(self, folder: str) -> None:
        self.folder = folder
        self.storage = None
        self.server = None
        self.channel = None
        self.stub = None
        self.address = ""

    def start(self) -> None:
        self.storage = Storage(self.folder)
        self.server, port = create_server(self.storage, "test-node", 0)
        self.server.start()
        self.address = f"localhost:{port}"
        self.channel = grpc.insecure_channel(self.address)
        self.stub = dynamo_pb2_grpc.KVStoreStub(self.channel)

    def stop(self) -> None:
        self.channel.close()
        self.server.stop(grace=None).wait()
        self.storage.close()

    def restart(self) -> None:
        self.stop()
        self.start()


@pytest.fixture
def node(tmp_path):
    running = Node(str(tmp_path / "node"))
    running.start()
    yield running
    running.stop()

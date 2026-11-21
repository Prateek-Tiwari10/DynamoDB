"""Tests for dynamo/config.py: loading, validation, frozen object, log line format."""

import dataclasses
import logging
import os
import re

import pytest

from dynamo.config import LOG_DATE_FORMAT, LOG_FORMAT, ClusterConfig, load_config, setup_logging
from dynamo.reqid import REQUEST_ID, RequestIdFilter

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

NODES = """\
nodes:
  node1: localhost:50051
  node2: localhost:50052
  node3: localhost:50053
"""


def write(tmp_path, text: str) -> str:
    path = tmp_path / "cluster.yaml"
    path.write_text(text)
    return str(path)


@pytest.mark.parametrize("name, host", [("cluster.yaml", "node{}:50051"),
                                        ("cluster.local.yaml", "localhost:5005{}")])
def test_repo_config_files(name, host):
    config = load_config(os.path.join(REPO_ROOT, name))
    assert config.nodes == {f"node{i}": host.format(i) for i in range(1, 6)}
    assert config == ClusterConfig(nodes=config.nodes)        # every other value is the default


def test_defaults_when_only_nodes_given(tmp_path):
    config = load_config(write(tmp_path, NODES))
    assert (config.n, config.r, config.w, config.snapshot_every) == (3, 2, 2, 1000)
    assert config.sloppy_quorum is True


@pytest.mark.parametrize("extra, message", [
    ("r: 4\n", "r=4"),
    ("w: 0\n", "w=0"),
    ("n: 4\n", "n=4"),
    ("suspect_after_ms: 10000\n", "suspect_after_ms"),
    ("anti_entropy_ms: 0\n", "anti_entropy_ms"),
    ("snapshot_evry: 5\n", "unknown setting"),
])
def test_config_validation(tmp_path, extra, message):
    with pytest.raises(ValueError, match=message):
        load_config(write(tmp_path, NODES + extra))


@pytest.mark.parametrize("text, message", [
    ("n: 3\n", "'nodes' is missing"),
    ("nodes: {}\n", "at least one node"),
    ("nodes:\n  node1: localhost\n", "host:port"),
    ("- just a list\n", "mapping"),
])
def test_config_bad_shape(tmp_path, text, message):
    with pytest.raises(ValueError, match=message):
        load_config(write(tmp_path, text))


def test_config_frozen(tmp_path):
    config = load_config(write(tmp_path, NODES))
    with pytest.raises(dataclasses.FrozenInstanceError):
        config.n = 5


def format_line(request_id: str | None) -> str:
    formatter = logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT)
    record = logging.LogRecord("dynamo.coordinator", logging.INFO, "/x/coordinator.py", 1,
                               "put %s acks 2/2", ("cart:42",), None)
    token = REQUEST_ID.set(request_id) if request_id else None
    try:
        RequestIdFilter("node1").filter(record)
    finally:
        if token:
            REQUEST_ID.reset(token)
    return formatter.format(record)


def test_log_format():
    line = format_line("a3f9c2d1e5b7")
    assert re.fullmatch(r"\d\d:\d\d:\d\d\.\d{3} INFO \[node1\] \[req=a3f9c2d1\] "
                        r"coordinator: put cart:42 acks 2/2", line)


def test_log_format_outside_request():
    assert "[node1] [req=-] coordinator:" in format_line(None)


def test_setup_logging_without_request_filter(monkeypatch, capsys):
    monkeypatch.setenv("LOG_LEVEL", "debug")
    setup_logging()
    setup_logging()                                  # safe to call twice
    try:
        logging.getLogger("dynamo.test").debug("hello")
        err = capsys.readouterr().err
        assert err.count("hello") == 1
        assert "DEBUG [-] [req=-] test_config: hello" in err
    finally:
        logging.basicConfig(handlers=[logging.NullHandler()], force=True)

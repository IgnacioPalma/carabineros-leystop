import httpx
import pytest

from leystop.client import Blocked, Client

CAPTCHA = "<html><title>Validation request</title>Please type the text you see... captcha</html>"


def _client(responses):
    calls = []

    def handler(request):
        calls.append(request.url.path)
        if request.url.path == "/estadistica/registros":
            return httpx.Response(200, text="<html>ok</html>")
        return responses.pop(0)

    c = Client(min_interval=0, not_found_pause=0, transport=httpx.MockTransport(handler))
    return c, calls


def test_transient_404_is_retried():
    c, calls = _client([httpx.Response(404, text="not found"), httpx.Response(200, json=[1])])
    assert c.get_json("/api/x") == [1]
    assert calls.count("/api/x") == 2


def test_persistent_404_blocks():
    c, _ = _client([httpx.Response(404, text="nf") for _ in range(4)])
    with pytest.raises(Blocked):
        c.get_json("/api/x")


def test_captcha_blocks_immediately():
    c, calls = _client([httpx.Response(200, text=CAPTCHA)])
    with pytest.raises(Blocked):
        c.get_json("/api/x")
    assert calls.count("/api/x") == 1

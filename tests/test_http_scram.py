from functools import partial
import logging
import pytest
import requests
import scramp
from unittest.mock import patch, MagicMock

from conftest import PhonyConnection, mock_pool_manager

from hop.auth import Auth
from hop.http_scram import SCRAMAuth

logger = logging.getLogger("hop")

# Replicate the example SCRAM exchange from RFC 5802
test_mechanism = "SCRAM-SHA-1"
test_auth = Auth("user", "pencil", method=test_mechanism)
test_nonce = "fyko+d2lbbFgONRv9qkxdawL"
test_sid = "AAAABBBBCCCCDDDD"
test_client_first = "SCRAM-SHA-1 data=biwsbj11c2VyLHI9ZnlrbytkMmxiYkZnT05Sdjlxa3hkYXdM"
test_server_first_data = "cj1meWtvK2QybGJiRmdPTlJ2OXFreGRhd0wzcmZjTkhZSlkxWlZ2V1ZzN" \
                         "2oscz1RU1hDUitRNnNlazhiZjkyLGk9NDA5Ng=="
test_server_first = f"SCRAM-SHA-1 sid={test_sid},data={test_server_first_data}"
test_client_final_data = "Yz1iaXdzLHI9ZnlrbytkMmxiYkZnT05Sdjlxa3hkYXdMM3JmY05IWUpZ" \
                         "MVpWdldWczdqLHA9djBYOHYzQnoyVDBDSkdiSlF5RjBYK0hJNFRzPQ=="
test_client_final = f"SCRAM-SHA-1 sid={test_sid},data={test_client_final_data}"
test_server_final = f"sid={test_sid},data=dj1ybUY5cHFWOFM3c3VBb1pXamE0ZEpSa0ZzS1E9"


def test_SCRAMAuth_generate_client_first():
    with patch("secrets.token_urlsafe", MagicMock(return_value=test_nonce)):
        a = SCRAMAuth(test_auth)
        cf = a._generate_client_first()
        assert cf == test_client_first


def request_copy(self):
    new_request = MagicMock()
    new_request.headers = self.headers
    new_request.url = self.url
    return new_request


def test_SCRAMAuth_handle_first():
    with patch("secrets.token_urlsafe", MagicMock(return_value=test_nonce)):
        # matching mechanism
        a = SCRAMAuth(test_auth)
        test_resp = MagicMock()
        test_resp.headers = {"www-authenticate": test_mechanism}
        test_resp.request.url = "http://example.com"
        test_resp.request.headers = {}
        test_resp.request.copy = partial(request_copy, test_resp.request)
        result = a._handle_first(test_resp)
        assert "Authorization" in result.request.headers
        assert result.request.headers["Authorization"] == test_client_first

        # non-matching mechanism
        test_resp = MagicMock()
        test_resp.headers = {"www-authenticate": "UNEXPECTED-MECHANISM"}
        test_resp.request.url = "http://example.com"
        test_resp.request.headers = {}
        test_resp.request.copy = partial(request_copy, test_resp.request)
        result = a._handle_first(test_resp)
        assert "Authorization" not in result.request.headers


def test_SCRAMAuth_handle_final():
    with patch("secrets.token_urlsafe", MagicMock(return_value=test_nonce)):
        # matching mechanism
        a = SCRAMAuth(test_auth)
        test_resp = MagicMock()
        test_resp.headers = {"www-authenticate": test_server_first}
        test_resp.request.url = "http://example.com"
        test_resp.request.headers = {}
        test_resp.request.copy = partial(request_copy, test_resp.request)
        a._generate_client_first()
        result = a._handle_final(test_resp)
        assert "Authorization" in result.request.headers
        assert result.request.headers["Authorization"] == test_client_final

        # non-matching mechanism
        test_server_first_wrong_mech = f"UNEXPECTED-MECHANISM sid={test_sid}," \
            f"data={test_server_first_data}"
        a = SCRAMAuth(test_auth)
        test_resp = MagicMock()
        test_resp.headers = {"www-authenticate": test_server_first_wrong_mech}
        test_resp.request.url = "http://example.com"
        test_resp.request.headers = {}
        test_resp.request.copy = partial(request_copy, test_resp.request)
        a._generate_client_first()
        result = a._handle_final(test_resp)
        assert "Authorization" not in result.request.headers

        # missing sid
        test_server_first_no_sid = f"SCRAM-SHA-1 data={test_server_first_data}"
        a = SCRAMAuth(test_auth)
        test_resp = MagicMock()
        test_resp.headers = {"www-authenticate": test_server_first_no_sid}
        test_resp.request.url = "http://example.com"
        test_resp.request.headers = {}
        test_resp.request.copy = partial(request_copy, test_resp.request)
        a._generate_client_first()
        with pytest.raises(RuntimeError) as err:
            result = a._handle_final(test_resp)
            assert "Authorization" not in result.request.headers

        # missing data
        test_server_first_no_data = f"SCRAM-SHA-1 sid={test_sid}"
        a = SCRAMAuth(test_auth)
        test_resp = MagicMock()
        test_resp.headers = {"www-authenticate": test_server_first_no_data}
        test_resp.request.url = "http://example.com"
        test_resp.request.headers = {}
        test_resp.request.copy = partial(request_copy, test_resp.request)
        a._generate_client_first()
        with pytest.raises(RuntimeError) as err:
            result = a._handle_final(test_resp)
            assert "Authorization" not in result.request.headers


def test_SCRAMAuth_check_server_final():
    with patch("secrets.token_urlsafe", MagicMock(return_value=test_nonce)):
        a = SCRAMAuth(test_auth)
        sfirst = MagicMock()
        sfirst.headers = {"www-authenticate": test_server_first}
        sfirst.request.url = "http://example.com"
        sfirst.request.headers = {}
        sfirst.request.copy = partial(request_copy, sfirst.request)
        a._generate_client_first()
        a._handle_final(sfirst)
        sfinal = MagicMock()
        sfinal.headers = {"authentication-info": test_server_final}
        sfinal.request.url = "http://example.com"
        sfinal.request.headers = {}
        sfinal.request.copy = partial(request_copy, sfinal.request)
        a._check_server_final(sfinal)

        # missing sid
        test_server_final_no_sid = "data=dj1ybUY5cHFWOFM3c3VBb1pXamE0ZEpSa0ZzS1E9"
        a = SCRAMAuth(test_auth)
        sfirst = MagicMock()
        sfirst.headers = {"www-authenticate": test_server_first}
        sfirst.request.url = "http://example.com"
        sfirst.request.headers = {}
        sfirst.request.copy = partial(request_copy, sfirst.request)
        a._generate_client_first()
        a._handle_final(sfirst)
        sfinal = MagicMock()
        sfinal.headers = {"authentication-info": test_server_final_no_sid}
        sfinal.request.url = "http://example.com"
        sfinal.request.headers = {}
        sfinal.request.copy = partial(request_copy, sfinal.request)
        with pytest.raises(RuntimeError):
            a._check_server_final(sfinal)

        # wrong sid
        test_server_final_wrong_sid = "sid=XYZ,data=dj1ybUY5cHFWOFM3c3VBb1pXamE0ZEpSa0ZzS1E9"
        a = SCRAMAuth(test_auth)
        sfirst = MagicMock()
        sfirst.headers = {"www-authenticate": test_server_first}
        sfirst.request.url = "http://example.com"
        sfirst.request.headers = {}
        sfirst.request.copy = partial(request_copy, sfirst.request)
        a._generate_client_first()
        a._handle_final(sfirst)
        sfinal = MagicMock()
        sfinal.headers = {"authentication-info": test_server_final_wrong_sid}
        sfinal.request.url = "http://example.com"
        sfinal.request.headers = {}
        sfinal.request.copy = partial(request_copy, sfinal.request)
        with pytest.raises(RuntimeError):
            a._check_server_final(sfinal)

        # missing data
        test_server_final_no_data = f"sid={test_sid}"
        a = SCRAMAuth(test_auth)
        sfirst = MagicMock()
        sfirst.headers = {"www-authenticate": test_server_first}
        sfirst.request.url = "http://example.com"
        sfirst.request.headers = {}
        sfirst.request.copy = partial(request_copy, sfirst.request)
        a._generate_client_first()
        a._handle_final(sfirst)
        sfinal = MagicMock()
        sfinal.headers = {"authentication-info": test_server_final_no_data}
        sfinal.request.url = "http://example.com"
        sfinal.request.headers = {}
        sfinal.request.copy = partial(request_copy, sfinal.request)
        with pytest.raises(RuntimeError):
            a._check_server_final(sfinal)

        # wrong data
        test_server_final_wrong_data = f"sid={test_sid},data=dj1iYWRkYXRh"
        a = SCRAMAuth(test_auth)
        sfirst = MagicMock()
        sfirst.headers = {"www-authenticate": test_server_first}
        sfirst.request.url = "http://example.com"
        sfirst.request.headers = {}
        sfirst.request.copy = partial(request_copy, sfirst.request)
        a._generate_client_first()
        a._handle_final(sfirst)
        sfinal = MagicMock()
        sfinal.headers = {"authentication-info": test_server_final_wrong_data}
        sfinal.request.url = "http://example.com"
        sfinal.request.headers = {}
        sfinal.request.copy = partial(request_copy, sfinal.request)
        with pytest.raises(scramp.core.ScramException):
            a._check_server_final(sfinal)


def test_SCRAMAuth_integration():
    conn = PhonyConnection([MagicMock(status=401,
                                      headers={"WWW-Authenticate":
                                               f'{test_mechanism} realm="realm@example.com"'}
                                      ),
                            MagicMock(status=401,
                                      headers={"WWW-Authenticate": test_server_first}
                                      ),
                            MagicMock(status=200,
                                      headers={"Authentication-Info": test_server_final}
                                      )
                            ])
    PM = mock_pool_manager(conn)
    with patch("requests.adapters.PoolManager", PM), \
            patch("secrets.token_urlsafe", MagicMock(return_value=test_nonce)):
        a = SCRAMAuth(test_auth)
        resp = requests.get("http://example.com", auth=a)
        print(resp)
        assert conn.counter == 3
        assert "Authorization" not in conn.requests[0]["headers"]
        assert "Authorization" in conn.requests[1]["headers"]
        assert conn.requests[1]["headers"]["Authorization"] == test_client_first
        assert "Authorization" in conn.requests[2]["headers"]
        assert conn.requests[2]["headers"]["Authorization"] == test_client_final


def test_SCRAMAuth_integration_shortcut():
    conn = PhonyConnection([MagicMock(status=401,
                                      headers={"WWW-Authenticate": test_server_first}
                                      ),
                            MagicMock(status=200,
                                      headers={"Authentication-Info": test_server_final}
                                      )
                            ])
    PM = mock_pool_manager(conn)
    with patch("requests.adapters.PoolManager", PM), \
            patch("secrets.token_urlsafe", MagicMock(return_value=test_nonce)):
        a = SCRAMAuth(test_auth, shortcut=True)
        body = "A" * 1024
        resp = requests.post("http://example.com", auth=a, data=body)
        assert conn.counter == 2
        assert "Authorization" in conn.requests[0]["headers"]
        assert conn.requests[0]["headers"]["Authorization"] == test_client_first
        assert conn.requests[0]["body"] is None, "No body should be sent during auth handshake"
        assert "Authorization" in conn.requests[1]["headers"]
        assert conn.requests[1]["headers"]["Authorization"] == test_client_final
        assert conn.requests[1]["body"] == body, "After handshake completes, body should be sent"


def test_SCRAMAuth_integration_auth_unnecessary():
    conn = PhonyConnection([MagicMock(status=200,
                                      headers={},
                                      ),
                            ])
    PM = mock_pool_manager(conn)
    with patch("requests.adapters.PoolManager", PM), \
            patch("secrets.token_urlsafe", MagicMock(return_value=test_nonce)):
        a = SCRAMAuth(test_auth)
        resp = requests.get("http://example.com", auth=a)
        assert conn.counter == 1
        assert "Authorization" not in conn.requests[0]["headers"]


def test_SCRAMAuth_integration_bad_server_final():
    conn = PhonyConnection([MagicMock(status=401,
                                      headers={"WWW-Authenticate":
                                               f'{test_mechanism} realm="realm@example.com"'}
                                      ),
                            MagicMock(status=401,
                                      headers={"WWW-Authenticate": test_server_first}
                                      ),
                            MagicMock(status=200,
                                      headers={"Authentication-Info":
                                               f"sid={test_sid},data=dj1CYWRTZXJ2ZXJTaWduYXR1cmU="}
                                      )
                            ])
    PM = mock_pool_manager(conn)
    with patch("requests.adapters.PoolManager", PM), \
            patch("secrets.token_urlsafe", MagicMock(return_value=test_nonce)):
        a = SCRAMAuth(test_auth)
        with pytest.raises(scramp.ScramException):
            resp = requests.get("http://example.com", auth=a)


def test_SCRAMAuth_integration_server_fail():
    conn = PhonyConnection([MagicMock(status=401,
                                      headers={"WWW-Authenticate":
                                               f'{test_mechanism} realm="realm@example.com"'}
                                      ),
                            MagicMock(status=500)
                            ])
    PM = mock_pool_manager(conn)
    with patch("requests.adapters.PoolManager", PM), \
            patch("secrets.token_urlsafe", MagicMock(return_value=test_nonce)):
        a = SCRAMAuth(test_auth)
        resp = requests.get("http://example.com", auth=a)
        assert resp.status_code == 500
        assert conn.counter == 2
        assert "Authorization" not in conn.requests[0]["headers"]
        assert "Authorization" in conn.requests[1]["headers"]
        assert conn.requests[1]["headers"]["Authorization"] == test_client_first

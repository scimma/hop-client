import base64
import logging
import re
import requests
from scramp import ScramClient
import secrets
import threading


logger = logging.getLogger("hop")


class SCRAMAuth(requests.auth.AuthBase):
    def __init__(self, credential, shortcut: bool = False, check_final=True):
        """
        Args:
            credential: The set of SCRAM credentials to supply when connecting
            shortcut: Do not wait for a challenge from the server, and skip directly to sending the
                      authentication 'response', which in this case is the SCRAM
                      'client-first-message'. This saves a pointless network round-trip when
                      authentication is needed and the client can be certain that this is the
                      necessary scheme.
            check_final: Perform the final check of the server signature required by RFC 5802. The
                         value of this is unclear when communication is over HTTPS and the server
                         has already been authenticated by the TLS.
        """
        self._thread_local = threading.local()
        # these are immutable, so we do not bother making them thread-local
        self.username = credential.username
        self.password = credential.password
        self.mechanism = credential.mechanism.upper()
        self.shortcut = shortcut
        self.check_final = check_final

    def init_per_thread_state(self):
        if not hasattr(self._thread_local, "init"):
            self._thread_local.init = True
            self._thread_local.num_calls = 0
            self._thread_local.saved_body = None

    def _redo_request(self, r: requests.Response, auth_header: str, final: bool = False, **kwargs):
        # Consume content and release the original connection
        # to allow our new request to reuse the same one.
        r.content
        r.close()
        prep = r.request.copy()
        requests.cookies.extract_cookies_to_jar(prep._cookies, r.request, r.raw)
        prep.prepare_cookies(prep._cookies)
        if final and self.shortcut and self._thread_local.saved_body is not None:
            prep.prepare_body(self._thread_local.saved_body, None)

        prep.headers['Authorization'] = auth_header
        prep.register_hook("response", self.process)
        _r = r.connection.send(prep, **kwargs)
        _r.history.append(r)
        _r.request = prep
        return _r

    def _generate_client_first(self):
        """Perform the client-side preparation for the first round of the SCRAM exchange.

            Returns: The encoded data for the Authorization header
        """
        self._thread_local.nonce = secrets.token_urlsafe()
        logger.debug(f" client nonce: {self._thread_local.nonce}")
        self._thread_local.sclient = ScramClient([self.mechanism],
                                                 self.username, self.password,
                                                 c_nonce=self._thread_local.nonce)
        cfirst = self._thread_local.sclient.get_client_first()
        logger.debug(f" client first: {cfirst}")
        cfirst = cfirst.encode("utf-8")
        return f"{self.mechanism} data={base64.b64encode(cfirst).decode('utf-8')}"

    def _handle_first(self, r: requests.Response, **kwargs):
        # Need to examine which auth mechanisms the server declares it accepts to find out
        # if the one we can do is on the list
        mechanisms = requests.utils.parse_list_header(r.headers.get("www-authenticate", ""))
        matching_mech = False
        for mechanism in mechanisms:
            if mechanism.upper() == self.mechanism or \
                    mechanism.upper().startswith(self.mechanism + " "):
                matching_mech = True
                break
        if not matching_mech:
            self._thread_local.num_calls = 0
            return r
        # At this point we know our mechanism is allowed, so we begin the SCRAM exchange

        self._thread_local.num_calls = 1
        return self._redo_request(r, auth_header=self._generate_client_first(), **kwargs)

    def _handle_final(self, r: requests.Response, **kwargs):
        # To contiue the handshake, the server should have set the www-authenticate header to
        # the mechanism we are using, followed by the data we need to use.
        # Check for this, and isolate the data to parse.
        logger.debug(f"Authenticate header sent by server: {r.headers.get('www-authenticate')}")
        m = re.fullmatch(f"{self.mechanism} (.+)", r.headers.get("www-authenticate"),
                         flags=re.IGNORECASE)
        if not m:
            self._thread_local.num_calls = 0
            return r
        auth_data = requests.utils.parse_dict_header(m.group(1))
        logger.debug("auth_data:", auth_data)
        # Next, make sure that both of the fields we need were actually sent in the dictionary
        if auth_data.get("sid", None) is None:
            self._thread_local.num_calls = 0
            raise RuntimeError("Missing sid in SCRAM server first: " + m.group(1))
        if auth_data.get("data", None) is None:
            self._thread_local.num_calls = 0
            raise RuntimeError("Missing data in SCRAM server first: " + m.group(1))

        self._thread_local.sid = auth_data.get("sid")
        sfirst = auth_data.get("data")
        logger.debug(f" sid: {self._thread_local.sid}")
        sfirst = base64.b64decode(sfirst).decode("utf-8")
        logger.debug(f" server first: {sfirst}")
        self._thread_local.sclient.set_server_first(sfirst)
        cfinal = self._thread_local.sclient.get_client_final()
        logger.debug(f" client final: {cfinal}")
        cfinal = base64.b64encode(cfinal.encode("utf-8")).decode('utf-8')
        self._thread_local.num_calls = 2
        return self._redo_request(r, auth_header=f"{self.mechanism} "
                                  f"sid={self._thread_local.sid},data={cfinal}", final=True)

    def _check_server_final(self, r: requests.Response, **kwargs):
        # The standard says that we MUST authenticate the server by checking the
        # ServerSignature, and treat it as an error if they do not match.
        logger.debug(f" authentication-info: {r.headers.get('authentication-info', None)}")
        raw_auth_data = r.headers.get("authentication-info", "")
        auth_data = requests.utils.parse_dict_header(raw_auth_data)
        # Next, make sure that both of the fields we need were actually sent in the dictionary
        if auth_data.get("sid", None) is None:
            self._thread_local.num_calls = 0
            raise RuntimeError("Missing sid in SCRAM server final: " + raw_auth_data)
        if auth_data.get("data", None) is None:
            self._thread_local.num_calls = 0
            raise RuntimeError("Missing data in SCRAM server final: " + raw_auth_data)
        if auth_data.get("sid") != self._thread_local.sid:
            self._thread_local.num_calls = 0
            raise RuntimeError("sid mismatch in server final www-authenticate header")
        sfinal = auth_data.get("data")
        self._thread_local.sclient.set_server_final(base64.b64decode(sfinal).decode("utf-8"))

    def process(self, r: requests.Response, **kwargs):
        if self._thread_local.num_calls < 2 and "www-authenticate" not in r.headers:
            self._thread_local.num_calls = 0
            return r
        if self._thread_local.num_calls >= 2:
            self._check_server_final(r)
            # prevent infinite looping if something goes wrong
            r.request.deregister_hook("response", self.process)

        if r.status_code == 401:
            if self._thread_local.num_calls == 0:
                return self._handle_first(r, **kwargs)
            elif self._thread_local.num_calls == 1:
                return self._handle_final(r, **kwargs)
        return r

    def __call__(self, r):
        self.init_per_thread_state()

        r.register_hook("response", self.process)
        # This is a bit hacky and not fully general:
        # If we are using the shortcut of assuming that we must do SCRAM authentication,
        # we assume that the request will have to be repeated, so we remove the body initially and
        # squirrel it away to be re-attached to the final request at the end of the SCRAM handshake.
        # This has the advantage of not sending the potentially large body data repeatedly.
        if self.shortcut:
            self._thread_local.saved_body = r.body
            r.prepare_body(b"", None)
            r.headers["Content-Length"] = 0
            r.headers['Authorization'] = self._generate_client_first()
            self._thread_local.num_calls = 1  # skip state ahead

        return r

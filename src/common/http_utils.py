import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def build_session() -> requests.Session:
    """
    HTTP session with retry strategy:
    - retries 5 times
    - exponential backoff
    - retries on rate limiting and transient server errors
    """
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s

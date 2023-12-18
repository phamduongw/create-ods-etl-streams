import os
import base64

CURRENT_FILE_PATH = os.path.abspath(__file__)
ENV_FILE_PATH = os.path.join(os.path.dirname(CURRENT_FILE_PATH), ".env")


def read_env_file():
    with open(ENV_FILE_PATH, "r") as file:
        for line in file:
            if line.strip() and not line.startswith("#"):
                key, value = line.strip().split("=", 1)
                os.environ[key] = value


def getBase64Credentials(username, password):
    credentials = "{}:{}".format(username, password)
    credentials_bytes = credentials.encode("utf-8")

    base64_credentials = base64.b64encode(credentials_bytes).decode("utf-8")

    return base64_credentials

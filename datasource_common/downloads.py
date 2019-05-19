import requests


CHUNK_SIZE = 65536


def download_file(url, path):
    r = requests.get(url, stream=True)
    with open(path, 'wb') as f:
        for chunk in r.iter_content(CHUNK_SIZE):
            f.write(chunk)

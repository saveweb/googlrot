import urllib.parse as urlparse

ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
ADDITIONAL_CHARS = "/"
PATH_OK_CHARS = ALPHABET + ADDITIONAL_CHARS


class BasicGooGlURL(str):
    def __new__(cls, url: str):
        assert ".goo.gl/" not in url, "Invalid URL (.goo.gl/ found)"

        if "goo.gl/" not in url:
            raise ValueError("Invalid URL (goo.gl/ not found)")

        # remove any before goo.gl/
        assert url.count("goo.gl/") == 1, "Invalid URL (goo.gl/ found multiple times)"
        url_parts = url.split("goo.gl/")
        assert len(url_parts) == 2, "Invalid URL (goo.gl/ found multiple times)"
        url = f"https://goo.gl/{url_parts[1]}"

        prased = urlparse.urlparse(url)
        assert 3 <= len(prased.path) <= 255, "Invalid URL (path length)"
        # remove any after goo.gl/{OK_CHARS}
        truncated_path = cls.truncate(prased.path)
        for c in truncated_path:
            assert c in PATH_OK_CHARS, "Invalid URL (path characters)"


        stdurl = f"https://{prased.hostname}{truncated_path}"
        assert stdurl.startswith("https://goo.gl/"), "Invalid URL (not a goo.gl URL)"
        return super().__new__(cls, stdurl)
    
    @staticmethod
    def truncate(path: str):
        chars = []
        for c in path:
            if c in PATH_OK_CHARS:
                chars.append(c)
            else:
                break
        return "".join(chars)


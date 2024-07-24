import pytest

from googlrot.url_type import BasicGooGlURL


@pytest.mark.parametrize("url", argvalues=[
    # real examples on GitHub
    "https://goo.gl/nBuQ4W",
    "https://goo.gl/nBuQ4W?si=1"
    "https://goo.gl/nBuQ4W#213"
    "https://goo.gl/eRQNAk",
    "http://goo.gl/fui2MH",
    "goo.gl/FFDUK5",
    "form-http://goo.gl/forms/6bujSMhkgM",
    "https://goo.gl/photos/GAwQYdcDJV8iNE1MA",
    "https://maps.app.goo.gl/peCJ6ZWJU8mSgWrSA",
    "https://goo.gl/forms/0cMMy02srh",
    "DEMO-https://goo.gl/X2t15y",
])
def test_stdurl(url):
    print(BasicGooGlURL(url))
    assert True

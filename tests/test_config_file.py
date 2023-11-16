import configparser


def test_pyconf():
    config = configparser.ConfigParser()
    try:
        result = config.read('pyconf.ini')
        assert len(result) > 0
    except Exception:
        assert False

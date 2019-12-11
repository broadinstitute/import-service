import logging, os, sys
from app import create_app


# Google's suggested integration with Stackdriver logging produces duplicate logs in Stackdriver.
# This does it right. See https://stackoverflow.com/a/58655297/2941784
if "GAE_APPLICATION" in os.environ:
    from google.cloud.logging.handlers import AppEngineHandler  # type: ignore
    import google.cloud.logging as glogging  # type: ignore
    client = glogging.Client()
    formatter = logging.Formatter("%(module)s.%(funcName)s: %(message)s")
    handler = AppEngineHandler(client, stream=sys.stderr)
    handler.setFormatter(formatter)
    handler.setLevel(logging.INFO)
    root = logging.getLogger()
    root.addHandler(handler)
    root.setLevel(logging.INFO)
else:
    # For local runs, use normal Python logging (so we don't send all our test logs to Stackdriver!)
    logging.basicConfig(format="%(module)s.%(funcName)s: %(message)s", level=logging.INFO)


app = create_app()

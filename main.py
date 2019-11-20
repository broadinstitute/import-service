from app import create_app


# Stackdriver logging picks up native Python logs, but ignores the formatting and only shows the message.
# Being able to use this formatter would save us some typing. But since it doesn't work on GCF, I've left it
# commented out for now.
# I've got a StackOverflow question out about this: https://stackoverflow.com/q/58955720/2941784
#
# import logging
# logging.basicConfig(format="%(module)s.%(funcName)s: %(message)s", level=logging.INFO)


app = create_app()


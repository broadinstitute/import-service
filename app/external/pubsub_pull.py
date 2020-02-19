from app.external import pubsub

def loop():
    """Main loop for pulling messages from PubSub when in PULL_MODE."""
    try:
        response = pubsub.pull_self(1)
        pass
    except:
        pass
    finally:
        pass

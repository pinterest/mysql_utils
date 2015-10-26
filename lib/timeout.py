import signal
from contextlib import contextmanager


@contextmanager
def timeout(seconds):
    """ Wrapper for signals handling a timeout for being
    used as a decorator. """
    def timeout_handler(signum, frame):
        pass

    original_handler = signal.signal(signal.SIGALRM, timeout_handler)

    try:
        signal.alarm(seconds)
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, original_handler)

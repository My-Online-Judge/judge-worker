import os
import signal

import pytest

from shutdown import Shutdown


def test_request_sets_the_flag():
    s = Shutdown()
    assert s.requested is False
    s.request()
    assert s.requested is True


@pytest.mark.parametrize("sig", [signal.SIGTERM, signal.SIGINT])
def test_install_turns_sigterm_and_sigint_into_the_flag(sig):
    s = Shutdown()
    old = {sg: signal.getsignal(sg) for sg in (signal.SIGTERM, signal.SIGINT)}
    try:
        s.install()
        os.kill(os.getpid(), sig)
        assert s.requested is True
    finally:
        for sg, handler in old.items():
            signal.signal(sg, handler)

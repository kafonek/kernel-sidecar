from importlib.metadata import version

import kernel_sidecar


def test_version():
    assert kernel_sidecar.__version__ == version('kernel-sidecar')
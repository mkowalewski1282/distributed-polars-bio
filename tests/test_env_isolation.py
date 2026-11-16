"""
Variables that control the dist_ops client (remote mode, results directory) and are
exported in the shell must not change how the tests behave: the standalone-mode tests
read files from ballista_genomics/output/ and assume standalone mode. Cluster tests
that run in separate processes set these variables explicitly on every call.
"""

import os

CLIENT_ENV_VARS = ("BALLISTA_SCHEDULER_URL", "DIST_OUTPUT_DIR", "BIO_TARGET_PARTITIONS")


def test_client_env_vars_do_not_leak_into_tests():
    leaked = [name for name in CLIENT_ENV_VARS if name in os.environ]
    assert not leaked, f"client variables visible in the test: {leaked}"

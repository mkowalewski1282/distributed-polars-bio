"""
Zmienne sterujące klientem dist_ops (tryb zdalny, katalog wyników) wyeksportowane
w powłoce nie mogą zmieniać zachowania testów: testy trybu standalone czytają
pliki z ballista_genomics/output/ i zakładają tryb standalone. Testy klastra
z osobnych procesów ustawiają te zmienne jawnie przy każdym wywołaniu.
"""

import os

CLIENT_ENV_VARS = ("BALLISTA_SCHEDULER_URL", "DIST_OUTPUT_DIR")


def test_client_env_vars_do_not_leak_into_tests():
    leaked = [name for name in CLIENT_ENV_VARS if name in os.environ]
    assert not leaked, f"zmienne klienta widoczne w teście: {leaked}"

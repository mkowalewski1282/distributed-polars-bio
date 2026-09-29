"""Wspólna konfiguracja testów."""

import pytest

#: Zmienne sterujące klientem dist_ops: tryb zdalny i katalog wyników. Testy
#: trybu standalone muszą działać niezależnie od tego, co jest wyeksportowane
#: w powłoce — inaczej mogłyby cicho liczyć zdalnie albo sprawdzać stare wyniki
#: z ballista_genomics/output/. Testy klastra z osobnych procesów ustawiają te
#: zmienne jawnie przy każdym wywołaniu klienta.
CLIENT_ENV_VARS = ("BALLISTA_SCHEDULER_URL", "DIST_OUTPUT_DIR")


@pytest.fixture(autouse=True)
def _isolate_client_env(monkeypatch):
    for name in CLIENT_ENV_VARS:
        monkeypatch.delenv(name, raising=False)

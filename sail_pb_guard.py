"""
Serializacja dostępu do globalnego kontekstu polars-bio z wielu partycji.

Dlaczego to jest potrzebne (znalezisko Fazy H, krok 5): polars-bio trzyma
JEDEN globalny, mutowalny kontekst DataFusion. Gdy silnik rozproszony wykonuje
funkcję użytkownika na kilku partycjach WSPÓŁBIEŻNIE w obrębie jednego procesu,
wywołania `pb.*()` nadpisują sobie nawzajem zarejestrowane tabele (`s1`/`s2`)
— i część grup dostaje pusty albo błędny wynik, cicho.

Empirycznie zweryfikowane, że to NIE jest błąd Saila: ten sam kształt zapytania
(LATERAL + UDTF nad wieloma partycjami) z funkcją czysto pythonową daje 3/3
poprawnych wyników, a z wywołaniem polars-bio 1/3.

Dlaczego lock musi mieszkać w OSOBNYM, IMPORTOWALNYM module, a nie w domknięciu
UDTF-a: PySpark serializuje UDTF-y przez cloudpickle. Obiekt `threading.Lock`
nie jest picklowalny („cannot pickle '_thread.lock' object"), więc lock zapisany
w domknięciu w ogóle nie przejdzie rejestracji. Natomiast moduł zaimportowany
po nazwie cloudpickle serializuje przez REFERENCJĘ (`import sail_pb_guard`),
więc każdy worker sięga po ten sam obiekt z `sys.modules`.

Ograniczenie: to działa w obrębie JEDNEGO procesu. Przy prawdziwej dystrybucji
wieloprocesowej każdy proces miałby własny lock i własny kontekst polars-bio —
co akurat jest poprawne, bo wtedy nie ma współdzielonego stanu do zepsucia.
"""

import threading

# Tworzony RAZ, przy imporcie modułu — a import w danym procesie zachodzi raz,
# więc nie ma wyścigu o samo utworzenie locka.
PB_LOCK = threading.Lock()


def merge(df, **kwargs):
    import polars_bio as pb

    with PB_LOCK:
        return pb.merge(df, **kwargs)


def overlap(df1, df2, **kwargs):
    import polars_bio as pb

    with PB_LOCK:
        return pb.overlap(df1, df2, **kwargs)


def nearest(df1, df2, **kwargs):
    import polars_bio as pb

    with PB_LOCK:
        return pb.nearest(df1, df2, **kwargs)


def coverage(df1, df2, **kwargs):
    import polars_bio as pb

    with PB_LOCK:
        return pb.coverage(df1, df2, **kwargs)


def subtract(df1, df2, **kwargs):
    import polars_bio as pb

    with PB_LOCK:
        return pb.subtract(df1, df2, **kwargs)

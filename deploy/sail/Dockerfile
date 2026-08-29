# Obraz z Sailem (pysail) + polars-bio + naszymi UDTF-ami.
#
# Uzywany w trybie KubernetesCluster: ten sam obraz sluzy za sterownik (driver)
# i za workery — Sail sam decyduje, w jakiej roli sie uruchomic, na podstawie
# zmiennych srodowiskowych SAIL_*.

FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --no-cache-dir \
        "pysail==0.5.3" \
        "polars-bio" \
        "pyspark[connect]" \
        pandas polars

# Nasze skrypty. sail_pb_guard.py jest KONIECZNY: serializuje dostep do
# globalnego kontekstu polars-bio miedzy partycjami dzialajacymi wspolbieznie
# w jednym procesie (patrz Faza H krok 5 — bez tego czesc grup gubi wyniki).
COPY sail_pb_guard.py /app/
COPY sail_overlap_udtf.py sail_merge_udtf.py sail_nearest_udtf.py \
     sail_coverage_subtract_udtf.py /app/

# /app musi byc na sciezce importu, zeby worker mogl `import sail_pb_guard`.
ENV PYTHONPATH=/app

CMD ["python", "-u", "sail_merge_udtf.py"]

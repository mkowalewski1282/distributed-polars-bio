# Obraz z klastrem Ballista + naszymi kodekami i zvendorowanym crate'em bio.
#
# Budowany dwuetapowo: etap `builder` kompiluje Rusta (ciezki, ~2 GB zaleznosci),
# etap koncowy zawiera juz tylko binarki i dane. Bez tego obraz mialby kilka GB.
#
# UWAGA (istotne przy budowaniu na maszynie z malym RAM): kompilacja
# DataFusion+Ballista wymaga duzo pamieci. Na maszynie 3.5 GB budowanie MUSI isc
# z CARGO_BUILD_JOBS=1, inaczej dochodzi do OOM. W chmurze (e2-standard-4, 16 GB)
# ten problem nie wystepuje i mozna zdjac to ograniczenie dla szybkosci.

FROM rust:1-bookworm AS builder

WORKDIR /build

# Najpierw same manifesty — warstwa z zaleznosciami bedzie cache'owana i nie
# przebuduje sie przy kazdej zmianie kodu.
COPY ballista_genomics/Cargo.toml ballista_genomics/Cargo.lock ./
COPY ballista_genomics/vendor ./vendor

RUN mkdir -p src/bin \
    && echo 'fn main() {}' > src/main.rs \
    && echo '' > src/lib.rs \
    && cargo build --release 2>/dev/null || true

COPY ballista_genomics/src ./src
COPY ballista_genomics/data ./data

ARG CARGO_JOBS=0
RUN if [ "$CARGO_JOBS" = "0" ]; then cargo build --release; \
    else CARGO_BUILD_JOBS="$CARGO_JOBS" cargo build --release; fi

# ---------------------------------------------------------------------------

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /build/target/release/ballista_genomics /usr/local/bin/
COPY --from=builder /build/target/release/dist_ops /usr/local/bin/
COPY --from=builder /build/data /app/data

# Katalog na wyniki i zrzuty EXPLAIN ANALYZE (montowany jako wolumen albo
# synchronizowany do GCS przez `gsutil rsync` po zakonczeniu zadania).
RUN mkdir -p /app/output

ENTRYPOINT ["dist_ops"]
CMD ["merge"]

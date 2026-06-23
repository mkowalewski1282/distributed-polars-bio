# Opis prototypu: genomic_overlap UDF w DataFusion/Ballista

## Co zostało zaimplementowane

Plik `src/main.rs` zawiera prototyp rejestracji własnej funkcji genomicznej
(`genomic_overlap`) w silniku zapytań DataFusion — tym samym mechanizmem
który używa Apache Ballista w trybie rozproszonym.

### Zaimplementowana funkcja: `genomic_overlap`

Funkcja sprawdza czy dwa interwały genomiczne się nakrywają:
- ten sam chromosom (`chrom_a == chrom_b`)
- nakładające się pozycje (`start_a < end_b AND start_b < end_a`)

Zarejestrowana jako UDF (User Defined Function) w DataFusion — silnik może
jej używać w dowolnym zapytaniu SQL:

```sql
WHERE genomic_overlap(a.chrom, a.start, a.end, b.chrom, b.start, b.end)
```

### Dane testowe

Pliki `data/intervals_a.csv` i `data/intervals_b.csv` zawierają po 5
interwałów genomicznych na chromosomach chr1 i chr2 — identyczne z danymi
użytymi w `overlap_comparison.py` (Sail) i `sail_overlap_udtf.py`.

### Wynik

8 par nakładających się interwałów — identyczne z polars-bio i Sail.

---

## Dlaczego nie używamy SessionContext::standalone() (Ballista)

Podczas implementacji odkryliśmy fundamentalne ograniczenie Ballistry w trybie
rozproszonym: **każde niestandardowe rozszerzenie musi być serializowalne**.

Ballista to klaster — scheduler i executory to osobne procesy które komunikują
się przez sieć. Scheduler musi przesłać plan zapytania do executora w formacie
binarnym (protobuf). Standardowe operacje (joiny, filtry) Ballista umie
serializować. Niestandardowe UDFy i tabele in-memory — nie.

### Błędy które napotkaliśmy

```
LogicalExtensionCodec is not provided for scalar function genomic_overlap
```

```
LogicalExtensionCodec is not provided (tabela MemTable)
```

### Co byłoby potrzebne

Żeby UDF działał w pełnym trybie Ballista distributed, trzeba zaimplementować
`LogicalExtensionCodec` — kod który serializuje i deserializuje custom UDF
do/z formatu protobuf. Wymaga to:

1. Definicji komunikatu w pliku `.proto`
2. Implementacji `LogicalExtensionCodec` trait w Ruście
3. Rejestracji kodeka w konfiguracji schedulera i executorów

To jest kilkaset linii dodatkowego kodu — wykonalne, ale poza zakresem
tego prototypu.

---

## Wnioski dla pracy magisterskiej

| | DataFusion (lokalnie) | Ballista distributed | Sail + UDTF |
|---|---|---|---|
| Rejestracja UDF | prosta (`register_udf`) | prosta + LogicalExtensionCodec | Python `@udtf` |
| Serializacja planu | nie potrzebna | wymagana dla każdego UDF | automatyczna (Sail) |
| Implementacja | ~50 linii Rust | ~50 + kilkaset linii Rust | ~50 linii Python |

**Kluczowy wniosek:** Sail + UDTF jest prostszą ścieżką do dystrybucji
operacji genomicznych niż Ballista, bo Sail ogarnia serializację planów
automatycznie przez protokół Spark Connect. Ballista wymaga jawnej
implementacji LogicalExtensionCodec dla każdego niestandardowego rozszerzenia.

DataFusion bez Ballistry demonstruje ten sam mechanizm rejestracji UDF
i daje identyczne wyniki — jest więc poprawnym prototypem pokazującym
że sam algorytm działa.

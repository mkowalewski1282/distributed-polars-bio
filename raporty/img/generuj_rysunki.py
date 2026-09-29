"""
Ilustracje do raportu `podsumowanie_brainstorming_przystepne.md`.

Rysunki operacji NIE są rysowane „z głowy”: wyniki pochodzą z prawdziwych
wywołań polars-bio na małym przykładzie (A, B poniżej), a skrypt dodatkowo
sprawdza asercjami, że to, co rysuje (np. pokryte fragmenty w `coverage`),
zgadza się z liczbami zwróconymi przez bibliotekę.

Uruchomienie (z katalogu repozytorium):
  python3 raporty/img/generuj_rysunki.py           # liczy przez polars-bio (import trwa minuty)
  python3 raporty/img/generuj_rysunki.py --cache   # używa zapisanego wyniki_operacji.json

Paleta: pierwsze trzy sloty palety referencyjnej (niebieski, pomarańczowy,
morski) — zwalidowane pod kątem daltonizmu dla wszystkich par; morski ma
niski kontrast z tłem, więc każdy element ma też podpis tekstowy.
"""

import json
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import Rectangle  # noqa: E402

KATALOG = Path(__file__).resolve().parent
CACHE = KATALOG / "wyniki_operacji.json"

# Przykład: chr1, współrzędne 0-based, półotwarte [start, end).
A = [(5, 20), (15, 30), (40, 55), (70, 80)]
B = [(10, 25), (45, 50), (52, 65), (88, 95)]

KOLOR_A = "#2a78d6"
KOLOR_B = "#eb6834"
KOLOR_WYNIK = "#1baf7a"
TUSZ = "#0b0b0b"
TUSZ_2 = "#52514e"
SIATKA = "#e4e3df"
WSZYSTKO_TLO = "#ffffff"
POSWIATA_A = "#d5e4f7"  # ok. 10-20% niebieskiego na białym
SZARY = "#b9b8b3"

plt.rcParams.update({
    "font.family": "DejaVu Sans",
    "font.size": 9,
    "axes.edgecolor": SZARY,
    "axes.labelcolor": TUSZ_2,
    "xtick.color": TUSZ_2,
    "ytick.color": TUSZ_2,
    "figure.facecolor": WSZYSTKO_TLO,
    "axes.facecolor": WSZYSTKO_TLO,
    "savefig.facecolor": WSZYSTKO_TLO,
})


# ---------------------------------------------------------------------------
# Wyniki z polars-bio
# ---------------------------------------------------------------------------

def policz_przez_polars_bio():
    import polars as pl
    import polars_bio as pb

    def ramka(wiersze):
        df = pl.DataFrame({
            "chrom": ["chr1"] * len(wiersze),
            "start": [w[0] for w in wiersze],
            "end": [w[1] for w in wiersze],
        })
        df.config_meta.set(coordinate_system_zero_based=True)
        return df

    a, b = ramka(A), ramka(B)
    wynik = {
        "overlap": pb.overlap(a, b, output_type="polars.DataFrame"),
        "nearest": pb.nearest(a, b, output_type="polars.DataFrame"),
        "coverage": pb.coverage(a, b, output_type="polars.DataFrame"),
        "merge": pb.merge(a, output_type="polars.DataFrame"),
        "subtract": pb.subtract(a, b, output_type="polars.DataFrame"),
    }
    return {nazwa: df.sort(pl.all()).to_dicts() for nazwa, df in wynik.items()}


def wczytaj_wyniki():
    if "--cache" in sys.argv and CACHE.exists():
        return json.loads(CACHE.read_text())
    wyniki = policz_przez_polars_bio()
    CACHE.write_text(json.dumps(wyniki, indent=2))
    return wyniki


# ---------------------------------------------------------------------------
# Pomocnicze rysowanie
# ---------------------------------------------------------------------------

def tory(przedzialy):
    """Rozkłada nakładające się przedziały na osobne tory (jak w przeglądarce genomu)."""
    konce_torow, przydzial = [], []
    for s, e in przedzialy:
        for i, koniec in enumerate(konce_torow):
            if koniec <= s:
                konce_torow[i] = e
                przydzial.append(i)
                break
        else:
            konce_torow.append(e)
            przydzial.append(len(konce_torow) - 1)
    return przydzial, len(konce_torow)


def nowa_figura(liczba_wierszy, prawy_margines=30):
    wys = 0.9 + 0.36 * liczba_wierszy
    fig, ax = plt.subplots(figsize=(6.3, wys))
    # Stałe marginesy zamiast tight_layout: wszystkie rysunki operacji mają
    # identyczną skalę osi, więc da się je porównywać wzrokiem.
    fig.subplots_adjust(left=0.01, right=0.99, top=1 - 0.12 / wys, bottom=0.62 / wys)
    ax.set_xlim(-16, 100 + prawy_margines)
    ax.set_ylim(-liczba_wierszy + 0.35, 0.75)
    for x in range(0, 101, 10):
        ax.axvline(x, color=SIATKA, lw=0.8, zorder=0)
    ax.spines[["top", "right", "left"]].set_visible(False)
    ax.spines["bottom"].set_bounds(0, 100)
    ax.set_xticks(range(0, 101, 10))
    ax.set_yticks([])
    ax.set_xlabel("pozycja na chromosomie chr1", color=TUSZ_2)
    return fig, ax


def pasek(ax, s, e, y, kolor, h=0.5, dy=0.0, z=2):
    ax.add_patch(Rectangle((s, y - h / 2 + dy), e - s, h, color=kolor, lw=0, zorder=z))


def podpis_toru(ax, y, tekst, kolor_znacznika=None):
    if kolor_znacznika:
        ax.add_patch(Rectangle((-4.2, y - 0.12), 2.2, 0.24, color=kolor_znacznika, lw=0))
        ax.text(-5.5, y, tekst, ha="right", va="center", color=TUSZ, fontsize=9)
    else:
        ax.text(-2, y, tekst, ha="right", va="center", color=TUSZ, fontsize=9)


def podpis_przedzialu(ax, s, e, y):
    ax.text((s + e) / 2, y + 0.3, f"[{s}, {e})", ha="center", va="bottom",
            color=TUSZ_2, fontsize=7)


def separator(ax, y, tekst):
    ax.plot([0, 100], [y, y], color=SZARY, lw=0.8, zorder=1)
    ax.text(0, y + 0.08, tekst, ha="left", va="bottom", color=TUSZ, fontsize=8.5,
            fontweight="bold")


def wejscia(ax, y0, pokaz_b=True):
    """Rysuje tory A (i B); zwraca y następnego wolnego wiersza i pozycje torów A."""
    tor_a, n_a = tory(A)
    y_a = [y0 - t for t in tor_a]
    for (s, e), y in zip(A, y_a):
        pasek(ax, s, e, y, KOLOR_A)
        podpis_przedzialu(ax, s, e, y)
    podpis_toru(ax, y0 - (n_a - 1) / 2, "A", KOLOR_A)
    y = y0 - n_a
    if pokaz_b:
        for s, e in B:
            pasek(ax, s, e, y, KOLOR_B)
            podpis_przedzialu(ax, s, e, y)
        podpis_toru(ax, y, "B", KOLOR_B)
        y -= 1
    return y, y_a


def zapisz(fig, nazwa, dopasuj=False):
    if dopasuj:
        fig.tight_layout()
    for rozszerzenie in ("pdf", "png"):
        fig.savefig(KATALOG / f"{nazwa}.{rozszerzenie}", dpi=200)
    plt.close(fig)


# ---------------------------------------------------------------------------
# Rysunki operacji
# ---------------------------------------------------------------------------

def rys_overlap(w):
    pary = w["overlap"]
    fig, ax = nowa_figura(3 + 0.6 + len(pary))
    y, _ = wejscia(ax, 0)
    y -= 0.3
    separator(ax, y + 0.45, f"Wynik overlap(A, B): {len(pary)} wiersze — każdy to para (A, B)")
    for i, p in enumerate(pary, start=1):
        yy = y - i + 0.6
        pasek(ax, p["start_1"], p["end_1"], yy, KOLOR_A, h=0.22, dy=0.13)
        pasek(ax, p["start_2"], p["end_2"], yy, KOLOR_B, h=0.22, dy=-0.13)
        podpis_toru(ax, yy, f"wiersz {i}")
        ax.text(101, yy, f"A[{p['start_1']},{p['end_1']}) × B[{p['start_2']},{p['end_2']})",
                ha="left", va="center", color=TUSZ_2, fontsize=7.5)
    zapisz(fig, "op_overlap")


def rys_nearest(w):
    wiersze = w["nearest"]
    fig, ax = nowa_figura(3 + 0.6 + len(wiersze))
    y, _ = wejscia(ax, 0)
    y -= 0.3
    separator(ax, y + 0.45, "Wynik nearest(A, B): dla każdego A — najbliższy B i odległość")
    for i, r in enumerate(wiersze, start=1):
        yy = y - i + 0.6
        pasek(ax, r["start_1"], r["end_1"], yy, KOLOR_A, h=0.22, dy=0.13)
        pasek(ax, r["start_2"], r["end_2"], yy, KOLOR_B, h=0.22, dy=-0.13)
        podpis_toru(ax, yy, f"wiersz {i}")
        opis = f"odległość {r['distance']}"
        if r["distance"] > 0:
            # Przerwa między końcem B a początkiem A (lub odwrotnie).
            lewy = min(r["end_1"], r["end_2"])
            prawy = max(r["start_1"], r["start_2"])
            ax.annotate("", xy=(prawy, yy), xytext=(lewy, yy),
                        arrowprops=dict(arrowstyle="<->,head_length=0.25,head_width=0.12",
                                        color=TUSZ, lw=0.9, shrinkA=0, shrinkB=0))
        nakladajace = [b for b in B if b[0] < r["end_1"] and r["start_1"] < b[1]]
        if r["distance"] == 0 and len(nakladajace) > 1:
            opis += " (remis)"
        ax.text(101, yy, opis, ha="left", va="center", color=TUSZ_2, fontsize=7.5)
    zapisz(fig, "op_nearest")


def pokryte_fragmenty(s, e):
    """Suma (bez powtórzeń) fragmentów [s, e) pokrytych przez B."""
    kawalki = sorted((max(s, bs), min(e, be)) for bs, be in B if bs < e and s < be)
    scalone = []
    for ks, ke in kawalki:
        if scalone and ks <= scalone[-1][1]:
            scalone[-1] = (scalone[-1][0], max(scalone[-1][1], ke))
        else:
            scalone.append((ks, ke))
    return scalone


def rys_coverage(w):
    wiersze = w["coverage"]
    fig, ax = nowa_figura(3 + 0.6 + len(wiersze))
    y, _ = wejscia(ax, 0)
    y -= 0.3
    separator(ax, y + 0.45, "Wynik coverage(A, B): wiersze A + ile pozycji pokrywa B")
    for i, r in enumerate(wiersze, start=1):
        yy = y - i + 0.6
        s, e = r["start"], r["end"]
        pasek(ax, s, e, yy, POSWIATA_A, h=0.4)
        fragmenty = pokryte_fragmenty(s, e)
        # Kontrola spójności rysunku z biblioteką.
        assert sum(ke - ks for ks, ke in fragmenty) == r["coverage"], (r, fragmenty)
        for ks, ke in fragmenty:
            pasek(ax, ks, ke, yy, KOLOR_WYNIK, h=0.4, z=3)
        podpis_toru(ax, yy, f"wiersz {i}")
        ax.text(101, yy, f"coverage = {r['coverage']} z {e - s}", ha="left", va="center",
                color=TUSZ_2, fontsize=7.5)
    zapisz(fig, "op_coverage")


def rys_merge(w):
    wiersze = w["merge"]
    fig, ax = nowa_figura(2 + 0.6 + 1.8)
    y, _ = wejscia(ax, 0, pokaz_b=False)
    y -= 0.3
    separator(ax, y + 0.45, f"Wynik merge(A): {len(wiersze)} wiersze — sklejone przedziały")
    yy = y - 1 + 0.4
    for r in wiersze:
        pasek(ax, r["start"], r["end"], yy, KOLOR_WYNIK)
        ax.text((r["start"] + r["end"]) / 2, yy - 0.32,
                f"[{r['start']}, {r['end']})\nn_intervals = {r['n_intervals']}",
                ha="center", va="top", color=TUSZ_2, fontsize=7, linespacing=1.3)
    podpis_toru(ax, yy, "wynik", KOLOR_WYNIK)
    zapisz(fig, "op_merge")


def rys_subtract(w):
    fragmenty = [(r["start"], r["end"]) for r in w["subtract"]]
    fig, ax = nowa_figura(3 + 0.6 + len(A))
    y, _ = wejscia(ax, 0)
    y -= 0.3
    separator(ax, y + 0.45, "Wynik subtract(A, B): części A, których nie pokrywa B")
    przypisane = 0
    for i, (s, e) in enumerate(A, start=1):
        yy = y - i + 0.6
        pasek(ax, s, e, yy, "#ecebe7", h=0.4)
        wlasne = [f for f in fragmenty if s <= f[0] and f[1] <= e]
        przypisane += len(wlasne)
        for fs, fe in wlasne:
            pasek(ax, fs, fe, yy, KOLOR_WYNIK, h=0.4, z=3)
        podpis_toru(ax, yy, f"z A[{s},{e})")
        opis = ", ".join(f"[{fs},{fe})" for fs, fe in wlasne) or "nic nie zostaje"
        ax.text(101, yy, opis, ha="left", va="center", color=TUSZ_2, fontsize=7.5)
    assert przypisane == len(fragmenty), "każdy fragment musi należeć do jednego A"
    zapisz(fig, "op_subtract")


# ---------------------------------------------------------------------------
# Rysunki pojęciowe
# ---------------------------------------------------------------------------

# Ilustracyjny fragment chromosomu (litery wymyślone — prawdziwy chr1 ma ~249 mln liter
# i zaczyna się od ~10 tys. liter N). Ten sam fragment służy w przykładzie pacjenta.
WZORZEC = "ACGTTAGCCATG"


def komorki(ax, y, litery, kolory, kolory_tekstu):
    for x, (litera, kolor, kolor_tekstu) in enumerate(zip(litery, kolory, kolory_tekstu)):
        ax.add_patch(Rectangle((x + 0.06, y + 0.06), 0.88, 0.88, color=kolor, lw=0))
        ax.text(x + 0.5, y + 0.5, litera, ha="center", va="center", color=kolor_tekstu,
                fontsize=9, family="DejaVu Sans Mono")


def numery_pozycji(ax, y):
    for x in range(len(WZORZEC)):
        ax.text(x + 0.5, y, str(x), ha="center", va="top", color=TUSZ_2, fontsize=8)
    ax.text(-0.3, y, "pozycja:", ha="right", va="top", color=TUSZ_2, fontsize=8)


def rys_wspolrzedne():
    fig, ax = plt.subplots(figsize=(6.3, 1.8))
    s, e = 3, 7
    wewn = [s <= x < e for x in range(len(WZORZEC))]
    komorki(ax, 0, WZORZEC, [KOLOR_A if w else "#f1f0ed" for w in wewn],
            ["#ffffff" if w else TUSZ for w in wewn])
    numery_pozycji(ax, -0.25)
    ax.annotate("start = 3\n(wliczony)", xy=(3.5, 1.0), xytext=(3.5, 1.75),
                ha="center", va="bottom", color=TUSZ, fontsize=8,
                arrowprops=dict(arrowstyle="-|>", color=TUSZ, lw=1))
    ax.annotate("end = 7\n(już niewliczony)", xy=(7.5, 1.0), xytext=(7.5, 1.75),
                ha="center", va="bottom", color=TUSZ, fontsize=8,
                arrowprops=dict(arrowstyle="-|>", color=TUSZ, lw=1))
    ax.text(12.4, 0.5, f"przedział [3, 7)\ndługość = 7 − 3 = 4\nlitery: {WZORZEC[s:e]}",
            ha="left", va="center", color=TUSZ, fontsize=8.5)
    ax.set_xlim(-2.2, 17)
    ax.set_ylim(-0.8, 2.6)
    ax.axis("off")
    zapisz(fig, "wspolrzedne", dopasuj=True)


def podpis_wiersza(ax, y, tekst):
    ax.text(-0.3, y, tekst, ha="right", va="center", color=TUSZ, fontsize=8.5)


def rys_pacjent():
    """Katalog genów (tabela A) × warianty pacjenta (tabela B) — skąd biorą się tabele."""
    gen, wariant = (2, 9), (5, 6)
    pacjent = WZORZEC[:5] + "C" + WZORZEC[6:]
    assert pacjent != WZORZEC and pacjent[5] != WZORZEC[5]
    fig, ax = plt.subplots(figsize=(6.3, 2.6))
    komorki(ax, 3.0, WZORZEC, ["#f1f0ed"] * 12, [TUSZ] * 12)
    podpis_wiersza(ax, 3.5, "wzorzec (genom referencyjny)")
    rozne = [a != b for a, b in zip(WZORZEC, pacjent)]
    komorki(ax, 1.9, pacjent, [KOLOR_B if r else "#f1f0ed" for r in rozne],
            ["#ffffff" if r else TUSZ for r in rozne])
    podpis_wiersza(ax, 2.4, "DNA pacjenta")
    pasek(ax, *gen, 1.2, KOLOR_A, h=0.45)
    podpis_wiersza(ax, 1.2, "tabela A: gen X")
    ax.text((gen[0] + gen[1]) / 2, 1.2, "wiersz (chr1, 2, 9)", ha="center", va="center",
            color="#ffffff", fontsize=7.5)
    pasek(ax, *wariant, 0.55, KOLOR_B, h=0.45)
    podpis_wiersza(ax, 0.55, "tabela B: wariant pacjenta")
    ax.text(wariant[1] + 0.2, 0.55, "wiersz (chr1, 5, 6)", ha="left", va="center",
            color=TUSZ_2, fontsize=7.5)
    numery_pozycji(ax, 0.15)
    nakladaja = wariant[0] < gen[1] and gen[0] < wariant[1]
    assert nakladaja
    ax.text(12.3, 0.88, "overlap(A, B):\nwariant leży\nw genie X", ha="left", va="center",
            color=TUSZ, fontsize=8.5)
    ax.set_xlim(-6.2, 15.5)
    ax.set_ylim(-0.3, 4.1)
    ax.axis("off")
    zapisz(fig, "pacjent", dopasuj=True)


def rys_odczyty():
    """Katalog genów (tabela A) × odczyty sekwenatora (tabela B) — sens operacji coverage."""
    gen = (2, 9)
    odczyty = [(0, 4), (3, 7)]
    pokryte = set()
    for s, e in odczyty:
        pokryte |= set(range(s, e))
    w_genie = [x for x in range(*gen)]
    liczba = sum(x in pokryte for x in w_genie)
    assert liczba == 5
    fig, ax = plt.subplots(figsize=(6.3, 2.2))
    pasek(ax, *gen, 3.0, KOLOR_A, h=0.45)
    podpis_wiersza(ax, 3.0, "tabela A: gen X")
    for i, (s, e) in enumerate(odczyty):
        y = 2.35 - i * 0.6
        pasek(ax, s, e, y, KOLOR_B, h=0.45)
        podpis_wiersza(ax, y, f"tabela B: odczyt {i + 1}")
    for x in w_genie:
        kolor = KOLOR_WYNIK if x in pokryte else "#f1f0ed"
        ax.add_patch(Rectangle((x + 0.06, 0.45), 0.88, 0.5, color=kolor, lw=0))
    podpis_wiersza(ax, 0.7, "pokrycie genu X")
    numery_pozycji(ax, 0.3)
    ax.text(12.3, 0.7, f"coverage = {liczba} z {len(w_genie)}:\npozycje 7 i 8\nnieodczytane",
            ha="left", va="center", color=TUSZ, fontsize=8.5)
    ax.set_xlim(-6.2, 15.5)
    ax.set_ylim(-0.3, 3.4)
    ax.axis("off")
    zapisz(fig, "odczyty", dopasuj=True)


DLUGOSCI_GRCH38 = {  # pary zasad, GRCh38
    "1": 248_956_422, "2": 242_193_529, "3": 198_295_559, "4": 190_214_555,
    "5": 181_538_259, "6": 170_805_979, "7": 159_345_973, "8": 145_138_636,
    "9": 138_394_717, "10": 133_797_422, "11": 135_086_622, "12": 133_275_309,
    "13": 114_364_328, "14": 107_043_718, "15": 101_991_189, "16": 90_338_345,
    "17": 83_257_441, "18": 80_373_285, "19": 58_617_616, "20": 64_444_167,
    "21": 46_709_983, "22": 50_818_468, "X": 156_040_895, "Y": 57_227_415,
}


def rys_chromosomy():
    nazwy = list(DLUGOSCI_GRCH38)
    mb = [DLUGOSCI_GRCH38[n] / 1e6 for n in nazwy]
    fig, ax = plt.subplots(figsize=(6.3, 2.4))
    x = range(len(nazwy))
    ax.bar(x, mb, width=0.62, color=KOLOR_A, lw=0)
    ax.set_xticks(list(x))
    ax.set_xticklabels(nazwy, fontsize=7.5)
    ax.set_ylabel("długość [mln par zasad]")
    ax.set_xlabel("chromosom człowieka (GRCh38)")
    ax.yaxis.grid(True, color=SIATKA, lw=0.8)
    ax.set_axisbelow(True)
    ax.spines[["top", "right", "left"]].set_visible(False)
    ax.tick_params(axis="y", length=0)
    for n in ("1", "21"):
        i = nazwy.index(n)
        ax.text(i, mb[i] + 4, f"{mb[i]:.0f}", ha="center", va="bottom", color=TUSZ, fontsize=7.5)
    ax.set_ylim(0, 275)
    zapisz(fig, "chromosomy", dopasuj=True)


def pudelko(ax, x, y, w, h, kolor, tekst="", kolor_tekstu=TUSZ, rozmiar=7.5):
    ax.add_patch(Rectangle((x, y), w, h, color=kolor, lw=0))
    if tekst:
        ax.text(x + w / 2, y + h / 2, tekst, ha="center", va="center",
                color=kolor_tekstu, fontsize=rozmiar)


def strzalka(ax, x0, y0, x1, y1):
    ax.annotate("", xy=(x1, y1), xytext=(x0, y0),
                arrowprops=dict(arrowstyle="-|>", color=TUSZ_2, lw=0.9,
                                shrinkA=2, shrinkB=2))


def rys_shuffle():
    """Repartycjonowanie po chromosomie: kawałki danych trafiają tam, gdzie ich chromosom."""
    fig, ax = plt.subplots(figsize=(6.3, 3.0))
    zrodla = [["chr1", "chr2", "chr3"], ["chr2", "chr1", "chr3"], ["chr3", "chr2", "chr1"]]
    szer, przerwa, odstep_plikow = 0.5, 0.06, 0.45
    szer_pliku = 3 * szer + 2 * przerwa
    cele = {}
    for k, c in enumerate(["chr1", "chr2", "chr3"]):
        x = k * (szer_pliku + odstep_plikow)
        pudelko(ax, x, 0.2, szer_pliku, 0.6, KOLOR_A, f"executor {k + 1}\ntylko {c}",
                "#ffffff")
        cele[c] = x + szer_pliku / 2
    for i, kawalki in enumerate(zrodla):
        x0 = i * (szer_pliku + odstep_plikow)
        ax.text(x0 + szer_pliku / 2, 2.85, f"plik {i + 1}", ha="center", va="bottom",
                color=TUSZ_2, fontsize=7.5)
        for j, c in enumerate(kawalki):
            x = x0 + j * (szer + przerwa)
            pudelko(ax, x, 2.25, szer, 0.55, "#cde2fb", c)
            strzalka(ax, x + szer / 2, 2.25, cele[c], 0.8)
    prawa = 3 * szer_pliku + 2 * odstep_plikow
    ax.text(prawa + 0.15, 2.52, "przed: każdy plik ma\nwszystkiego po trochu",
            ha="left", va="center", color=TUSZ, fontsize=8)
    ax.text(prawa + 0.15, 0.5, "po shuffle: każdy\nchromosom w jednym\nmiejscu",
            ha="left", va="center", color=TUSZ, fontsize=8)
    ax.set_xlim(-0.05, prawa + 1.6)
    ax.set_ylim(0.1, 3.1)
    ax.axis("off")
    zapisz(fig, "shuffle", dopasuj=True)


def rys_broadcast():
    """Broadcast: mała tabela B w całości do każdego executora, duża A dzielona."""
    fig, ax = plt.subplots(figsize=(6.3, 2.6))
    pudelko(ax, 0.1, 1.9, 1.3, 0.7, KOLOR_B, "B (mała, cała)", "#ffffff")
    for k in range(3):
        pudelko(ax, 0.1, 1.15 - k * 0.5, 1.3, 0.4, KOLOR_A, f"A, część {k + 1}", "#ffffff")
    for k in range(3):
        yb = 2.2 - k * 0.95
        pudelko(ax, 3.4, yb, 1.1, 0.34, KOLOR_B, "kopia B", "#ffffff", 7)
        pudelko(ax, 3.4, yb + 0.36, 1.1, 0.34, KOLOR_A, f"A, część {k + 1}", "#ffffff", 7)
        ax.text(4.6, yb + 0.35, f"executor {k + 1}", ha="left", va="center",
                color=TUSZ, fontsize=8)
        strzalka(ax, 1.4, 2.25, 3.4, yb + 0.17)
        strzalka(ax, 1.4, 1.35 - k * 0.5, 3.4, yb + 0.53)
    ax.text(0.1, 3.05, "dane wejściowe", color=TUSZ, fontsize=8)
    ax.text(3.4, 3.05, "każdy executor dostaje CAŁE B i kawałek A", color=TUSZ, fontsize=8)
    ax.set_xlim(-0.1, 6.3)
    ax.set_ylim(0.1, 3.3)
    ax.axis("off")
    zapisz(fig, "broadcast", dopasuj=True)


def rys_rdzenie():
    fig, ax = plt.subplots(figsize=(6.3, 1.9))
    role = [
        ("system,\nscheduler,\nklienci", SZARY, TUSZ),
        ("węzeł 1", KOLOR_A, "#ffffff"),
        ("węzeł 2", KOLOR_A, "#ffffff"),
        ("węzeł 3", KOLOR_A, "#ffffff"),
        ("wolny", "#f1f0ed", TUSZ_2),
        ("wolny", "#f1f0ed", TUSZ_2),
    ]
    for k, (opis, kolor, kolor_tekstu) in enumerate(role):
        x = k * 1.05
        pudelko(ax, x, 0.55, 1.0, 0.8, kolor, opis, kolor_tekstu, 7.5)
        ax.text(x + 0.5, 1.45, f"rdzeń {k}", ha="center", va="bottom", color=TUSZ, fontsize=8)
        ax.text(x + 0.5, 0.45, f"wątki {2 * k}, {2 * k + 1}", ha="center", va="top",
                color=TUSZ_2, fontsize=7)
    ax.set_xlim(-0.05, 6.35)
    ax.set_ylim(0.0, 1.8)
    ax.axis("off")
    zapisz(fig, "rdzenie", dopasuj=True)


def main():
    wyniki = wczytaj_wyniki()
    rys_wspolrzedne()
    rys_pacjent()
    rys_odczyty()
    rys_overlap(wyniki)
    rys_nearest(wyniki)
    rys_coverage(wyniki)
    rys_merge(wyniki)
    rys_subtract(wyniki)
    rys_chromosomy()
    rys_shuffle()
    rys_broadcast()
    rys_rdzenie()
    print("Zapisano rysunki w", KATALOG)


if __name__ == "__main__":
    main()

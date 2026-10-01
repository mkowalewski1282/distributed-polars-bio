"""Narzędzie pomiarowe (specyfikacja metodyki, sekcja 8)."""

import os

# Matplotlib (importowany pośrednio przez polars-bio) przy domyślnym backendzie sprawdza
# serwer X ze zmiennej DISPLAY. Na tej maszynie DISPLAY wskazuje host Windows bez serwera X
# (/etc/bash.bashrc), a połączenie TCP czeka na timeout: import polars-bio trwał ~270 s
# zamiast ~1 s. Backend bez okien omija to sprawdzenie; jawnie ustawiony MPLBACKEND wygrywa.
os.environ.setdefault("MPLBACKEND", "Agg")

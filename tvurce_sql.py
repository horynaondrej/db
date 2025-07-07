#!/usr/bin/env python3
"""modul slouží pro popis datasetu a datových
typů jednotlivých sloupců. Obsahuje třídy
pro načítání konfigurace, zpracování dat
a generování výstupů."""

import logging
import csv
import os
import importlib.util
from decimal import Decimal, InvalidOperation
from typing import Any
from datetime import datetime
from collections import Counter

from uprava_nazvu import zjisti_nazvy_sloupcu
from master_config import SLOZKA

# vytvoř absolutní cestu k modulu config.py
cesta_k_modulu = os.path.join(os.path.dirname(__file__), SLOZKA, "config.py")

# vytvoř import spec
spec = importlib.util.spec_from_file_location("config", cesta_k_modulu)
config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(config)


class TvurceSQL:
    """Třída pro zpracování datového souboru a vytvoření
    sql příkazu create s datovými typy sloupců"""

    def __init__(self):
        self.cesta = os.path.dirname(__file__)
        self.vstup = os.path.join(self.cesta, SLOZKA, config.ZDROJ)
        self.oddelovac = config.ODDELOVAC
        self.tabulka = config.TABULKA
        self.uvozovky = None if config.UVOZOVKY == "None" else config.UVOZOVKY
        self.data = []
        self.hlavicka = []
        self.data_nazvy = []
        self.vystup = os.path.join(self.cesta, SLOZKA, config.HLAVICKA)
        self.typy = []
        self.deklarace = []  # obsahuje výsledné datové typy sloupců

    def nacti_data(self):
        """Načte obsah datového souboru a uloží jej do atributu content"""
        try:
            logging.info("Otevírám soubor %s", self.vstup)
            with open(self.vstup, "r+", newline="", encoding="utf8") as soubor:
                a = 0
                cteni = csv.reader(
                    soubor, delimiter=self.oddelovac, quotechar=self.uvozovky
                )
                for row in cteni:
                    self.data.append(row)
                    if a == 1000:
                        break
                    a += 1
                soubor.close()
                logging.info("Načtení dat dokončeno")
        except IOError:
            logging.info("Soubor s daty neexistuje")

    def rozdel_data(self):
        """rozdělí hlavičku od dat"""
        self.hlavicka = self.data.pop(0)

    def pretypuj_datum(self, date_str: str) -> datetime:
        """pokusí se převést textový řetězec na datetime
        pomocí známých formátů"""
        formats = [
            "%d.%m.%Y %H:%M:%S",  # 30.06.2025 14:30:00
            "%d.%m.%Y %H:%M",  # 30.06.2025 14:30
            "%d.%m.%Y",  # 30.06.2025
            "%d.%m.%y",  # 30.06.25
            "%d/%m/%Y %H:%M:%S",  # 30/06/2025 14:30:00
            "%d/%m/%Y %H:%M",  # 30/06/2025 14:30
            "%d/%m/%Y",  # 30/06/2025
            "%Y-%m-%d %H:%M:%S",  # 2025-06-30 14:30:00
            "%Y-%m-%d %H:%M",  # 2025-06-30 14:30
            "%Y-%m-%d",  # 2025-06-30
            "%d %b %Y %H:%M",  # 30 Jun 2025 14:30
            "%d %B %Y %H:%M",  # 30 June 2025 14:30
            "%d %b %Y",  # 30 Jun 2025
            "%d %B %Y",  # 30 June 2025
        ]

        for fmt in formats:
            try:
                return datetime.strptime(date_str.strip(), fmt)
            except ValueError:
                continue

        raise ValueError(f"Nepodařilo se rozpoznat formát data: '{date_str}'")

    def zjisti_typ_hodnoty(self, hodnota: Any) -> str:
        """funkce pro přetypování konkrétn hodnoty"""
        res = ""
        # když je hodnota 0 nebo null, tak ji nebude
        # brát do statistiky
        if hodnota in ("0", ""):
            res = "null"
        # když sloupec obsahuje více znaků tečka ".",
        # tak se přetypuje na datum
        elif "." in hodnota and len(hodnota.split(".")) == 3 and not ":" in hodnota:
            # přetypuj na datum
            try:
                self.pretypuj_datum(hodnota)
                res = "date"
            except ValueError:
                res = "varchar"
        elif ":" in hodnota:
            # přetypuj na timestamp protože má hodnota i čas
            try:
                self.pretypuj_datum(hodnota)
                res = "timestamp"
            except ValueError:
                res = "varchar"
        elif "." in hodnota or "," in hodnota:
            try:
                Decimal(hodnota.replace(",", "."))
                res = "decimal(18, 3)"
            except (ValueError, InvalidOperation):
                res = "varchar"
        else:
            try:
                int(hodnota)
                res = "integer"
            except ValueError:
                res = "varchar"
        return res

    def zjisti_typy_sloupcu(self):
        """přetypuj sloupec na duckdb typ"""

        if not self.data or not self.data[0]:
            print("Pole je prázdné.")
            return

        radky = len(self.data)
        sloupce = len(self.data[0])

        # inicializace prázdného pole
        self.typy = [["" for _ in range(sloupce)] for _ in range(radky)]

        for j in range(sloupce):  # po sloupcích
            for i in range(radky):  # po řádcích
                self.typy[i][j] = self.zjisti_typ_hodnoty(self.data[i][j])

    def vytvor_statistiku_datovych_typu(self):
        """vezmi pole s datovými typy a vytvoř
        předpis datového typu pro každý sloupec"""

        # transpozice dat (sloupce místo řádků)
        columns = list(zip(*self.typy))

        # výpis nejčastější hodnoty pro každý sloupec
        for _, col in enumerate(columns):

            # hodnota s největší četností
            most_common = Counter(col).most_common(1)[0]
            res = most_common[0]
            # když bude v jedné hodnotě číslo decimal, a jinde 0 nebo null,
            # tak bude typ decimal
            if any(x.startswith('decimal') for x in col):
                res = "decimal(18, 3)"
            # když je null, musí se ověřit, zda je některá hodnota varchar
            elif res == "null":
                if any(x.startswith('varchar') for x in col):
                    res = "varchar"
                else:
                    # až když není výskyt varchar, bude hodnota integer,
                    res = "integer"
            # jinak bude typ s největší četností
            else:
                pass
            self.deklarace.append(res)

    def uloz_data(self):
        """Uloží hlavičku do textového souboru."""
        try:
            with open(self.vystup, "w", encoding="utf8") as soubor:
                soubor.write(f"create or replace table {self.tabulka} (\n")
                tmp = list(zip(self.data_nazvy, self.deklarace))
                for i, zaznam in enumerate(tmp):
                    soubor.write(f"    {zaznam[0]} {zaznam[1]}")
                    if i == len(tmp) - 1:
                        soubor.write("\n")
                    else:
                        soubor.write(",\n")
                soubor.write(");")
            logging.info("Zapsání do souboru dokončeno")
        except IOError as e:
            logging.info("I/O error(%s): %s", e.errno, e.strerror)


def main():
    """Hlavní funkce skriptu, která inicializuje a spouští procesy."""
    fmt = "%(asctime)s: %(message)s"
    logging.basicConfig(format=fmt, level=logging.INFO, datefmt="%H:%M:%S")

    logging.info("Spuštění skriptu")

    tvurce_sql = TvurceSQL()
    tvurce_sql.nacti_data()
    tvurce_sql.rozdel_data()
    tvurce_sql.data_nazvy = zjisti_nazvy_sloupcu(tvurce_sql.hlavicka)
    tvurce_sql.zjisti_typy_sloupcu()
    tvurce_sql.vytvor_statistiku_datovych_typu()
    tvurce_sql.uloz_data()

    logging.info("Ukončení skriptu")


# Hlavní vlákno skriptu
if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""modul slouží pro přípravu tabulky
pro import do databáze duckdb
"""

import csv
import os
import logging
from datetime import datetime
import importlib.util

from typing import Any

from uprava_nazvu import zjisti_nazvy_sloupcu
from master_config import SLOZKA

# vytvoř absolutní cestu k modulu config.py
cesta_k_modulu = os.path.join(os.path.dirname(__file__), SLOZKA, "config.py")

# vytvoř import spec
spec = importlib.util.spec_from_file_location("config", cesta_k_modulu)
config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(config)


class OpravarDat:
    """Třída pro zpracování dat a práci s SQLite databází."""

    def __init__(self):
        self.cesta = os.path.dirname(__file__)
        self.con = None
        self.cur = None
        self.vstup = os.path.join(self.cesta, SLOZKA, config.ZDROJ)
        self.sql_create = os.path.join(self.cesta, SLOZKA, config.HLAVICKA)
        self.tabulka = config.TABULKA
        self.oddelovac = config.ODDELOVAC
        self.uvozovky = None if config.UVOZOVKY == "None" else config.UVOZOVKY
        self.data = []
        self.hlavicka = []
        self.hlavicka_opravena = []
        self.retezec = ""
        self.hodnoty = ""
        self.prikaz_create = ""
        self.sloupce_a_typy = []
        self.meritka = []
        self.datumy = []

    def nacti_data(self):
        """Načte data ze zdrojového CSV souboru."""
        try:
            with open(self.vstup, "r+", newline="", encoding="utf8") as soubor:
                cteni = csv.reader(
                    soubor, delimiter=self.oddelovac, quotechar=self.uvozovky
                )
                for row in cteni:
                    self.data.append(row)
                logging.info("Načtení dat dokončeno")
        except IOError as e:
            logging.info("Soubor s daty neexistuje %s", e)

    def zkontroluj_data(self):
        """zkontroluj délku a podobu dat"""
        logging.info("Řádků: %s, Sloupců: %s", len(self.data), len(self.data[0]))

        # kontrola počtu sloupců za řádek
        unikatni_pocet_sloupcu = {len(zaznam) for zaznam in self.data}
        logging.info("Kontrola sloupců, jen jedna hodnota: %s", unikatni_pocet_sloupcu)

    def nacti_prikaz_create(self):
        """je potřeba načíst i příkaz create,
        aby tento skript věděl, jaké sloupce jsou
        měřítka"""
        try:
            with open(self.sql_create, "r", encoding="utf8") as soubor:
                self.prikaz_create = soubor.read()
        except FileNotFoundError:
            logging.info("Soubor s SQL create neexistuje")

    def zpracuj_prikaz_create(self):
        """rozděl příkaz create na sloupce a jejich typy"""
        vsl = []
        rozdeleny = self.prikaz_create.split("\n")
        for i in rozdeleny[1:-1]:
            tmp = i.replace("    ", "")
            vsl.append(tmp.split(" "))
        self.sloupce_a_typy = vsl

    def rozdel_data(self):
        """rozdělí hlavičku od dat"""
        self.hlavicka = self.data.pop(0)

    def zjisti_sloupce_podle_typu(self, typy):
        """Zjistí indexy sloupců podle zadaných typů."""
        return [
            i
            for i, hod in enumerate(self.sloupce_a_typy)
            if any(hod[1].startswith(t) for t in typy)
        ]

    def vymen_oddelovace(self):
        """Nahradí čárky za tečky v definovaných sloupcích."""
        for i in self.meritka:
            for zaznam in self.data:
                zaznam[i] = zaznam[i].replace(",", ".")

    def oprav_datum(self, hodnota):
        """
        Převede hodnoty ve sloupci 'datum' na datetime.datetime.
        Podporuje 'DD.MM.YYYY' i 'DD.MM.YYYY HH:MM:SS'.
        """
        vsl = None
        if hodnota == "":
            return vsl
        if ":" in hodnota:
            tmp = datetime.strptime(hodnota, "%d.%m.%Y %H:%M:%S")
            vsl = tmp.strftime("%Y-%m-%d %H:%M:%S")
        else:
            # Pokud selže, zkusit pouze datum
            tmp = datetime.strptime(hodnota, "%d.%m.%Y")
            vsl = tmp.strftime("%Y-%m-%d") + " 00:00:00"
        return vsl

    def vymen_oravene_datum(self):
        """Nahradí formát datumu pro import do duckdb"""
        for i in self.datumy:
            for zaznam in self.data:
                zaznam[i] = self.oprav_datum(zaznam[i])

    def sjednot_data_a_hlavicku(self):
        """sloupci pro následné uložení hlavička a data"""
        self.data = [self.hlavicka_opravena] + self.data

    def uloz_data(
        self, vystup: str, data: list[Any], oddelovac: str, uvozovky: str = None
    ) -> None:
        """Metoda uloží 2D seznam do CSV souboru

        Args:
            hlavicka: list
            data: list
            vystup: str - cesta k souboru

        Return:
            None
        """
        if len(data) > 0:
            try:
                with open(vystup, "w", encoding="utf-8") as soubor:
                    w = csv.writer(
                        soubor,
                        delimiter=oddelovac,
                        quotechar=uvozovky,
                        lineterminator="\n",
                    )
                    for ity in data:
                        w.writerow(ity)
            except IOError:
                logging.info("Nezdařilo se zapsat do souboru")
        else:
            logging.info("Data pro zápis neexistují")


def main():
    """Hlavní funkce skriptu, která inicializuje a spouští procesy."""
    fmt = "%(asctime)s: %(message)s"
    logging.basicConfig(format=fmt, level=logging.INFO, datefmt="%H:%M:%S")

    logging.info("Spuštění skriptu")

    opravar_dat = OpravarDat()
    opravar_dat.nacti_data()
    opravar_dat.zkontroluj_data()
    opravar_dat.rozdel_data()
    opravar_dat.hlavicka_opravena = zjisti_nazvy_sloupcu(opravar_dat.hlavicka)
    opravar_dat.nacti_prikaz_create()
    opravar_dat.zpracuj_prikaz_create()
    opravar_dat.meritka = opravar_dat.zjisti_sloupce_podle_typu(["decimal"])
    opravar_dat.datumy = opravar_dat.zjisti_sloupce_podle_typu(["date", "timestamp"])
    opravar_dat.vymen_oddelovace()
    opravar_dat.vymen_oravene_datum()
    opravar_dat.sjednot_data_a_hlavicku()
    opravar_dat.uloz_data(
        os.path.join(opravar_dat.cesta, SLOZKA, "tmp.csv"), opravar_dat.data, ";", '"'
    )

    logging.info("Ukončení skriptu")


if __name__ == "__main__":
    main()

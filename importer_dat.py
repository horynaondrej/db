#!/usr/bin/env python3
"""modul slouží pro import dat do databáze
duckdb"""

import os
import logging
import importlib.util
import duckdb

from master_config import SLOZKA

# vytvoř absolutní cestu k modulu config.py
cesta_k_modulu = os.path.join(os.path.dirname(__file__), SLOZKA, "config.py")

# vytvoř import spec
spec = importlib.util.spec_from_file_location("config", cesta_k_modulu)
config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(config)


class ImporterDat:
    """odesílá sql příkazy pro smazání a vytvoření
    tabulky v databázi, importuje data"""

    def __init__(self):
        self.prikaz_create = ""
        self.tabulka = config.TABULKA
        self.con = None
        self.cesta = os.path.dirname(__file__)
        self.db = os.path.join(self.cesta, config.DB)
        self.sql_create = os.path.join(self.cesta, SLOZKA, config.HLAVICKA)
        self.tmp = os.path.join(self.cesta, SLOZKA, "tmp.csv")

    def pripoj_se_k_databazi(self):
        """vytvoř připoení k databázi"""
        self.con = duckdb.connect(self.db)

    def odpoj_se_od_databaze(self):
        """odpojí se od databáze"""
        if self.con:
            self.con.commit()
            self.con.close()

    def smaz_tabulku(self):
        """smaž tabulku"""
        self.con.execute(f"drop table if exists {self.tabulka}")

    def nacti_prikaz_create(self):
        """odešle přkaz create table do databáze"""
        try:
            with open(self.sql_create, "r", encoding="utf8") as soubor:
                self.prikaz_create = soubor.read()
        except FileNotFoundError:
            logging.info("Soubor s SQL create neexistuje")

    def vytvor_tabulku(self):
        """vytvoř tabulku"""
        if self.con:
            self.con.execute(self.prikaz_create)
        logging.info("Vytvoření tabulky proběhlo v pořádku")

    def nahraj_data(self) -> None:
        """nahraj csv data do tabulky"""
        logging.info("Začínám nahrávat a importovat data..")
        self.con.execute(
            f"""
            copy {self.tabulka}
            from '{self.tmp}'
                (delimiter ';',
                quote '"')
            """
        )
        logging.info("Data úspěšně importovaná")

    def odeber_docasne_soubory(self):
        """odeber již nepotřebné soubory (už byly nahrané
        do databáze)"""
        if os.path.exists(self.tmp):
            os.remove(self.tmp)
            logging.info("Soubor tmp.csv byl úspěšně smazán")
        else:
            logging.info("Soubor neexistuje")


def main():
    """Hlavní funkce skriptu, která inicializuje a spouští procesy."""
    fmt = "%(asctime)s: %(message)s"
    logging.basicConfig(format=fmt, level=logging.INFO, datefmt="%H:%M:%S")

    logging.info("Spuštění skriptu")

    importer_dat = ImporterDat()
    importer_dat.pripoj_se_k_databazi()
    importer_dat.smaz_tabulku()
    importer_dat.nacti_prikaz_create()
    importer_dat.vytvor_tabulku()
    importer_dat.nahraj_data()
    importer_dat.odpoj_se_od_databaze()
    importer_dat.odeber_docasne_soubory()

    logging.info("Ukončení skriptu")


if __name__ == "__main__":
    main()

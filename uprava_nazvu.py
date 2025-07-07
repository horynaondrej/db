#!/usr/bin/env python3

"""modul pro úpravu názvů"""

import re


def zjisti_nazvy_sloupcu(data):
    """upraví názvy sloupců v hlavičce souboru."""
    upravene = []
    kriticke = {"index": "ix", "key2": "neco"}
    for i in data:
        novy = i.lower()
        novy = re.sub(
            r"[áéíýóůúěščřž]",
            lambda x: x.group(0).translate(
                str.maketrans("áéíýóůúěščřž", "aeiyouuescrz")
            ),
            novy,
        )
        novy = (
            novy.replace(" ", "")
            .replace(".", "")
            .replace("-", "")
            .replace("(kc)", "")
            .replace("(h)", "")
            .replace("(", "")
            .replace(")", "")
            .replace("/", "")
            .replace("\\", "")
            .replace("[", "")
            .replace("]", "")
        )
        if novy in kriticke:
            novy = kriticke.get(novy)
        upravene.append(novy)
    return upravene

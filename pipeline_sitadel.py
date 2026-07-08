#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PermisLocal — Pipeline SITADEL → leads.json
============================================
Transforme les autorisations d'urbanisme (open data SITADEL, data.gouv.fr)
en leads géolocalisés pour artisans du bâtiment.

Usage :
    python3 pipeline_sitadel.py --dept 44 --mois 6        # données réelles
    python3 pipeline_sitadel.py --demo                     # jeu de démonstration

Dépendances : pip install requests pandas
Le géocodage utilise l'API Adresse de l'État (gratuite, sans clé).

Sortie : leads.json (à placer à côté de index.html)
"""

import argparse
import csv
import io
import json
import random
import re
import sys
import zipfile
from datetime import date, datetime, timedelta

# ---------------------------------------------------------------------------
# 1. RÉSOLUTION DE LA SOURCE (API data.gouv.fr)
# ---------------------------------------------------------------------------
# Jeux de données SDES/SITADEL publiés sur data.gouv.fr. Les slugs sont
# stables ; les URLs de fichiers changent à chaque millésime, donc on les
# résout dynamiquement via l'API.
DATASET_SLUGS = [
    # PC/DP créant des logements (le plus utile pour les artisans)
    "liste-des-autorisations-durbanisme-creees-logements",
    # locaux non résidentiels (utile pour le B2B : hangars, commerces...)
    "liste-des-autorisations-durbanisme-creees-locaux-non-residentiels",
]
DATA_GOUV_API = "https://www.data.gouv.fr/api/1/datasets/{slug}/"
BAN_BULK_URL = "https://api-adresse.data.gouv.fr/search/csv/"


def resolve_latest_resources(slug):
    """Retourne les URLs CSV/ZIP du dataset, la plus récente d'abord."""
    import requests
    r = requests.get(DATA_GOUV_API.format(slug=slug), timeout=60)
    r.raise_for_status()
    resources = r.json().get("resources", [])
    keep = [
        res for res in resources
        if res.get("format", "").lower() in ("csv", "zip")
        or res.get("url", "").lower().endswith((".csv", ".zip"))
    ]
    keep.sort(key=lambda res: res.get("last_modified") or "", reverse=True)
    return [(res["title"], res["url"]) for res in keep]


# ---------------------------------------------------------------------------
# 2. CLASSIFICATION MÉTIER
# ---------------------------------------------------------------------------
# On classe chaque autorisation vers les corps de métier concernés à partir
# de la nature du projet et des champs surfaces. Ajustable à volonté.
METIERS = {
    "piscine":        [r"piscine"],
    "maison_neuve":   [r"construction.*maison", r"maison individuelle", r"construction d'une habitation"],
    "extension":      [r"extension", r"agrandissement", r"surelevation", r"surélévation"],
    "garage_carport": [r"garage", r"carport", r"abri voiture"],
    "toiture":        [r"toiture", r"couverture", r"refection.*toit", r"réfection.*toit"],
    "solaire":        [r"photovolta", r"panneaux solaires"],
    "veranda_terrasse": [r"veranda", r"véranda", r"terrasse", r"pergola"],
    "cloture_portail": [r"cloture", r"clôture", r"portail", r"mur de cloture"],
    "abri_annexe":    [r"abri de jardin", r"annexe", r"dependance", r"dépendance"],
    "renovation":     [r"renovation", r"rénovation", r"rehabilitation", r"réhabilitation",
                       r"ravalement", r"changement.*menuiseries", r"isolation"],
}


def classify(nature_txt):
    nature = (nature_txt or "").lower()
    tags = [m for m, patterns in METIERS.items()
            if any(re.search(p, nature) for p in patterns)]
    return tags or ["autre"]


# ---------------------------------------------------------------------------
# 3. PIPELINE DONNÉES RÉELLES
# ---------------------------------------------------------------------------
# Champs SITADEL usuels (le script tolère les variantes de casse/millésime) :
#   NUM_DAU / Num_PC ......... n° d'autorisation
#   DATE_REELLE_AUTORISATION . date d'autorisation
#   COMM ..................... code commune INSEE (les 2 premiers chiffres ~ dept)
#   ADR_NUM/ADR_TYPEVOIE/ADR_LIBVOIE/ADR_LIEUDIT/ADR_LOCALITE/ADR_CODPOST
#   SUPERFICIE_TERRAIN, SURF_HAB_CREEE / I_PIECE...
#   NATURE_PROJET_DECLAREE / NATURE_PROJET
#   CAT_DEM (catégorie demandeur), DENOM_DEM + SIREN_DEM (personnes morales)
# RGPD : les noms des demandeurs particuliers ne figurent pas dans la
# diffusion ; le lead = l'adresse du projet (démarchage par courrier/dépliant).

def find_col(cols, *candidates):
    low = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in low:
            return low[cand.lower()]
    for cand in candidates:  # match partiel
        for c in cols:
            if cand.lower() in c.lower():
                return c
    return None


def run_real(dept, months, out_path):
    import pandas as pd
    import requests

    frames = []
    for slug in DATASET_SLUGS:
        try:
            resources = resolve_latest_resources(slug)
        except Exception as e:
            print(f"[!] API data.gouv indisponible pour {slug}: {e}")
            continue
        if not resources:
            print(f"[!] Aucune ressource CSV trouvée pour {slug}")
            continue
        title, url = resources[0]
        print(f"[+] Téléchargement : {title}\n    {url}")
        raw = requests.get(url, timeout=600).content
        if url.lower().endswith(".zip") or raw[:2] == b"PK":
            zf = zipfile.ZipFile(io.BytesIO(raw))
            name = next(n for n in zf.namelist() if n.lower().endswith(".csv"))
            raw = zf.read(name)
        df = pd.read_csv(io.BytesIO(raw), sep=None, engine="python",
                         dtype=str, encoding_errors="replace", on_bad_lines="skip")
        print(f"    {len(df):,} lignes, colonnes: {list(df.columns)[:12]}...")
        frames.append(df)

    if not frames:
        sys.exit("Aucune donnée récupérée. Vérifie ta connexion, ou lance --demo.")

    leads = []
    cutoff = (date.today() - timedelta(days=30 * months)).isoformat()
    for df in frames:
        cols = df.columns
        c_comm = find_col(cols, "COMM", "code_commune", "CODGEO")
        c_date = find_col(cols, "DATE_REELLE_AUTORISATION", "date_autorisation")
        c_nat = find_col(cols, "NATURE_PROJET_DECLAREE", "NATURE_PROJET", "nature")
        c_num = find_col(cols, "NUM_DAU", "Num_PC", "numero")
        c_sup = find_col(cols, "SUPERFICIE_TERRAIN", "superficie")
        c_shab = find_col(cols, "SURF_HAB_CREEE", "surface")
        c_catdem = find_col(cols, "CAT_DEM")
        c_denom = find_col(cols, "DENOM_DEM")
        c_siren = find_col(cols, "SIREN_DEM")
        adr_parts = [find_col(cols, p) for p in
                     ("ADR_NUM", "ADR_TYPEVOIE", "ADR_LIBVOIE", "ADR_LIEUDIT", "ADR_LOCALITE")]
        c_cp = find_col(cols, "ADR_CODPOST", "code_postal")

        sel = df
        if c_comm:
            sel = sel[sel[c_comm].astype(str).str.startswith(str(dept).zfill(2))]
        if c_date:
            sel = sel[sel[c_date].astype(str) >= cutoff]
        print(f"    → {len(sel):,} autorisations dept {dept} depuis {cutoff}")

        for _, row in sel.iterrows():
            adresse = " ".join(str(row[c]) for c in adr_parts
                               if c and str(row.get(c, "")) not in ("nan", "", "None"))
            leads.append({
                "id": str(row.get(c_num, "")) if c_num else "",
                "date": str(row.get(c_date, ""))[:10] if c_date else "",
                "nature": str(row.get(c_nat, "")) if c_nat else "",
                "metiers": classify(str(row.get(c_nat, "")) if c_nat else ""),
                "adresse": adresse.strip(),
                "cp": str(row.get(c_cp, "")) if c_cp else "",
                "commune_insee": str(row.get(c_comm, "")) if c_comm else "",
                "surface_terrain": str(row.get(c_sup, "")) if c_sup else "",
                "surface_creee": str(row.get(c_shab, "")) if c_shab else "",
                "demandeur_pro": str(row.get(c_denom, "")) if c_denom and str(row.get(c_denom, "")) not in ("nan", "") else None,
                "siren": str(row.get(c_siren, "")) if c_siren and str(row.get(c_siren, "")) not in ("nan", "") else None,
            })

    print(f"[+] {len(leads)} leads avant géocodage")
    leads = geocode_ban(leads)
    save(leads, out_path, demo=False, dept=str(dept).zfill(2))


def geocode_ban(leads):
    """Géocodage en masse via l'API Adresse (CSV bulk, gratuite)."""
    import requests
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["idx", "adresse", "citycode"])
    for i, l in enumerate(leads):
        w.writerow([i, l["adresse"] or "", l["commune_insee"] or ""])
    buf.seek(0)
    print("[+] Géocodage BAN (api-adresse.data.gouv.fr)...")
    r = requests.post(
        BAN_BULK_URL,
        files={"data": ("leads.csv", buf.read())},
        data={"columns": "adresse", "citycode": "citycode"},
        timeout=600,
    )
    r.raise_for_status()
    for row in csv.DictReader(io.StringIO(r.text)):
        i = int(row["idx"])
        lat, lon = row.get("latitude"), row.get("longitude")
        if lat and lon:
            leads[i]["lat"] = round(float(lat), 6)
            leads[i]["lon"] = round(float(lon), 6)
    geocoded = [l for l in leads if "lat" in l]
    print(f"[+] {len(geocoded)}/{len(leads)} leads géocodés")
    return geocoded


# ---------------------------------------------------------------------------
# 4. JEU DE DÉMONSTRATION (offline, réaliste, dept 44)
# ---------------------------------------------------------------------------
COMMUNES_44 = [
    ("Nantes", "44109", 47.2184, -1.5536), ("Saint-Nazaire", "44184", 47.2735, -2.2137),
    ("Rezé", "44143", 47.1917, -1.5693), ("Saint-Herblain", "44162", 47.2122, -1.6496),
    ("Orvault", "44114", 47.2717, -1.6222), ("Vertou", "44215", 47.1687, -1.4693),
    ("Carquefou", "44026", 47.2975, -1.4907), ("La Baule-Escoublac", "44055", 47.2861, -2.3922),
    ("Guérande", "44069", 47.3281, -2.4292), ("Pornic", "44131", 47.1156, -2.1056),
    ("Clisson", "44043", 47.0870, -1.2827), ("Ancenis-Saint-Géréon", "44003", 47.3667, -1.1767),
    ("Châteaubriant", "44036", 47.7178, -1.3757), ("Blain", "44015", 47.4764, -1.7633),
    ("Savenay", "44195", 47.3606, -1.9422), ("Treillières", "44209", 47.3306, -1.6206),
    ("Sainte-Luce-sur-Loire", "44172", 47.2506, -1.4854), ("Bouguenais", "44020", 47.1778, -1.6236),
    ("Pornichet", "44132", 47.2622, -2.3403), ("Machecoul-Saint-Même", "44087", 46.9936, -1.8236),
]
NATURES = [
    ("Construction d'une piscine enterrée", ["piscine"], 40, 0),
    ("Construction d'une maison individuelle", ["maison_neuve"], 550, 120),
    ("Extension d'une habitation existante", ["extension"], 0, 35),
    ("Surélévation de la toiture et création de combles habitables", ["extension", "toiture"], 0, 42),
    ("Construction d'un garage accolé", ["garage_carport"], 0, 24),
    ("Réfection complète de la toiture", ["toiture"], 0, 0),
    ("Installation de panneaux photovoltaïques en toiture", ["solaire"], 0, 0),
    ("Construction d'une véranda", ["veranda_terrasse"], 0, 18),
    ("Édification d'une clôture et d'un portail", ["cloture_portail"], 0, 0),
    ("Construction d'un abri de jardin", ["abri_annexe"], 0, 15),
    ("Rénovation avec changement des menuiseries et isolation par l'extérieur", ["renovation"], 0, 0),
    ("Extension avec création d'une terrasse couverte", ["extension", "veranda_terrasse"], 0, 28),
    ("Construction d'une piscine et d'un pool house", ["piscine", "abri_annexe"], 45, 12),
    ("Ravalement de façade", ["renovation"], 0, 0),
]
COMMUNES_62 = [
    ("Arras", "62041", 50.2910, 2.7775), ("Calais", "62193", 50.9513, 1.8587),
    ("Boulogne-sur-Mer", "62160", 50.7264, 1.6147), ("Lens", "62498", 50.4292, 2.8319),
    ("Liévin", "62510", 50.4228, 2.7708), ("Béthune", "62119", 50.5303, 2.6408),
    ("Saint-Omer", "62765", 50.7480, 2.2528), ("Berck", "62108", 50.4076, 1.5928),
    ("Le Touquet-Paris-Plage", "62826", 50.5211, 1.5909), ("Hénin-Beaumont", "62427", 50.4136, 2.9503),
    ("Bruay-la-Buissière", "62178", 50.4839, 2.5481), ("Outreau", "62643", 50.7053, 1.5942),
    ("Étaples", "62318", 50.5186, 1.6414), ("Carvin", "62215", 50.4931, 2.9581),
    ("Avion", "62065", 50.4103, 2.8322), ("Saint-Martin-Boulogne", "62758", 50.7269, 1.6367),
    ("Wimereux", "62893", 50.7692, 1.6106), ("Marck", "62548", 50.9497, 1.9506),
    ("Longuenesse", "62525", 50.7361, 2.2422), ("Auchel", "62048", 50.5061, 2.4736),
]
COMMUNES_PAR_DEPT = {"44": COMMUNES_44, "62": COMMUNES_62}
DEPT_NOMS = {"44": "Loire-Atlantique", "62": "Pas-de-Calais"}
VOIES = ["rue des Camélias", "avenue de la Libération", "impasse des Mésanges",
         "rue du Moulin", "boulevard des Océanides", "chemin de la Métairie",
         "rue des Frères Lumière", "allée des Tilleuls", "route de la Côte",
         "rue de la Vigne", "place de l'Église", "rue des Ajoncs"]
PROS = [None] * 8 + ["SARL ATLANTIQUE HABITAT", "SCI LES DUNES", "MAISONS OCEA", None, "SCCV LE CLOS DES CHENES"]


def run_demo(out_path, n=160, seed=None, dept="44"):
    dept = str(dept).zfill(2)
    communes = COMMUNES_PAR_DEPT.get(dept)
    if not communes:
        sys.exit(f"Pas de jeu de démo pour le dept {dept} (dispo : {list(COMMUNES_PAR_DEPT)}). "
                 "Ajoute ses communes dans COMMUNES_PAR_DEPT.")
    rng = random.Random(seed or int(dept))
    today = date.today()
    leads = []
    for i in range(n):
        commune, insee, clat, clon = rng.choice(communes)
        nature, metiers, sup_base, shab_base = rng.choice(NATURES)
        d = today - timedelta(days=rng.randint(3, 175))
        pro = rng.choice(PROS)
        leads.append({
            "id": f"PC 0{insee[2:]} 25 D{1000 + i}",
            "date": d.isoformat(),
            "nature": nature,
            "metiers": metiers,
            "adresse": f"{rng.randint(1, 120)} {rng.choice(VOIES)}, {commune}",
            "cp": f"{dept}{rng.randint(0, 9)}{rng.randint(0, 9)}0",
            "commune_insee": insee,
            "commune": commune,
            "surface_terrain": str(sup_base and rng.randint(300, 1200) or rng.randint(200, 900)),
            "surface_creee": str(shab_base and max(8, int(rng.gauss(shab_base, shab_base * 0.3))) or 0),
            "demandeur_pro": pro,
            "siren": f"{rng.randint(300, 899)} {rng.randint(100, 999)} {rng.randint(100, 999)}".replace(" ", "") if pro else None,
            "lat": round(clat + rng.gauss(0, 0.018), 6),
            "lon": round(clon + rng.gauss(0, 0.022), 6),
        })
    leads.sort(key=lambda l: l["date"], reverse=True)
    save(leads, out_path, demo=True, dept=dept)


def save(leads, out_path, demo, dept=None):
    payload = {
        "generated": datetime.now().isoformat(timespec="seconds"),
        "demo": demo,
        "dept": dept,
        "dept_nom": DEPT_NOMS.get(dept, ""),
        "count": len(leads),
        "leads": leads,
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=1)
    # leads.js : même contenu chargeable en double-cliquant index.html (pas de CORS)
    js_path = out_path.rsplit(".", 1)[0] + ".js"
    with open(js_path, "w", encoding="utf-8") as f:
        f.write("window.LEADS = ")
        json.dump(payload, f, ensure_ascii=False)
        f.write(";")
    print(f"[✓] {len(leads)} leads → {out_path} + {js_path}" + ("  (DONNÉES DE DÉMO)" if demo else ""))


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="SITADEL → leads.json")
    ap.add_argument("--dept", default="44", help="département (ex: 44)")
    ap.add_argument("--mois", type=int, default=6, help="ancienneté max en mois")
    ap.add_argument("--out", default="leads.json")
    ap.add_argument("--demo", action="store_true", help="génère un jeu de démonstration hors ligne")
    args = ap.parse_args()
    if args.demo:
        run_demo(args.out, dept=args.dept)
    else:
        run_real(args.dept, args.mois, args.out)

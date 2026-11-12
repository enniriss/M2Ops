"""Le rapport de qualité du jour : $GAZETTE_DATA_DIR/rapports/qualite-AAAA-MM-JJ.md (et .json).

Réécrit à chaque passage : il dit ce qu'a fait le dernier passage (entrés, gardés, rejetés par
règle et par source), puis le cumul de la journée, les cinq rejets les plus fréquents, les
règles qui écartent une grosse part d'une source (« trop gourmandes ? »), les contrôles, et la
liste des passages. Le fichier de la veille reste tel quel : c'est l'archive.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from gazette.silver.controles import Resultat, taux_par_source
from gazette.silver.regles import Config
from gazette.silver.traitement import Passage

DOSSIER_RAPPORTS = "rapports"
PART_GOURMANDE = 0.3  # une règle qui écarte 30 % d'une source mérite qu'on relise ses rejets
PARIS = ZoneInfo("Europe/Paris")


def chemin_rapport(racine: Path, partition: str, extension: str = "md") -> Path:
    return racine / DOSSIER_RAPPORTS / f"qualite-{partition}.{extension}"


def _heure(iso: str | None) -> str:
    return datetime.fromisoformat(iso).astimezone(PARIS).strftime("%H:%M") if iso else "–"


def donnees_du_jour(connexion, partition: str) -> dict:
    """Les chiffres de la journée, relus en base (tous les passages de la partition)."""
    sources = taux_par_source(connexion, partition)
    par_regle_source: dict[str, dict[str, int]] = {}
    for regle, source, nombre in connexion.execute(
        "SELECT regle, source, count(*) FROM rejets WHERE partition = ? GROUP BY regle, source",
        (partition,),
    ):
        par_regle_source.setdefault(regle, {})[source] = nombre

    top5 = []
    for regle, source, nombre, exemple in connexion.execute(
        "SELECT regle, source, count(*) AS n, min(titre) FROM rejets WHERE partition = ? "
        "GROUP BY regle, source ORDER BY n DESC, regle, source LIMIT 5",
        (partition,),
    ):
        entres = sources[source][0]
        top5.append({"regle": regle, "source": source, "rejetes": nombre,
                     "part_de_la_source": round(nombre / entres, 3), "exemple": exemple})  # fmt: skip

    gourmandes = [
        {"regle": regle, "source": source, "rejetes": n, "entres": sources[source][0],
         "part_de_la_source": round(n / sources[source][0], 3)}
        for regle, par_source in sorted(par_regle_source.items())
        for source, n in sorted(par_source.items())
        if n / sources[source][0] >= PART_GOURMANDE
    ]  # fmt: skip
    passages = [
        dict(ligne)
        for ligne in connexion.execute(
            "SELECT id, debut, fin, run_id, entres, gardes, rejetes FROM passages "
            "WHERE partition = ? ORDER BY id",
            (partition,),
        )
    ]
    entres = sum(e for e, _ in sources.values())
    rejetes = sum(r for _, r in sources.values())
    return {
        "entres": entres,
        "gardes": entres - rejetes,
        "rejetes": rejetes,
        "par_source": {s: {"entres": e, "rejetes": r, "gardes": e - r} for s, (e, r) in sources.items()},
        "par_regle_source": par_regle_source,
        "top5": top5,
        "gourmandes": gourmandes,
        "passages": passages,
    }


def _taux(rejetes: int, entres: int) -> str:
    return f"{rejetes / entres:.0%}" if entres else "–"


def en_markdown(
    partition: str, passage: Passage, jour: dict, controles: list[Resultat], config: Config
) -> str:
    regles = [r.id for r in config.regles]
    l = [
        f"# Rapport de qualité · {partition}",
        "",
        f"> Réécrit à chaque passage ; dernier : n° {passage.id}, "
        f"{datetime.now(PARIS):%d/%m/%Y %H:%M} (heure de Paris). "
        "Règles : `gazette/silver/regles.toml`, documentées dans `docs/qualite.md`.",
        "",
        f"## Dernier passage (n° {passage.id})",
        "",
        "| Fichiers bronze | Entrés | Gardés | Rejetés | Taux de rejet |",
        "|---:|---:|---:|---:|---:|",
        f"| {passage.fichiers} | {passage.entres} | {passage.gardes} | {passage.rejetes} "
        f"| {_taux(passage.rejetes, passage.entres)} |",
        "",
    ]
    if passage.entres:
        l += ["| Source | Entrés | Gardés | Rejetés | Par règle |", "|---|---:|---:|---:|---|"]
        for source, c in sorted(passage.par_source.items()):
            detail = ", ".join(f"{r} {c[r]}" for r in regles if c[r])
            l.append(f"| {source} | {c['entres']} | {c['gardes']} | {c['rejetes']} | {detail or '–'} |")
        l.append("")
    else:
        l += ["Aucun fichier bronze nouveau depuis le passage précédent.", ""]

    l += [
        "## La journée (tous les passages)",
        "",
        f"**{jour['entres']} entrés, {jour['gardes']} gardés, {jour['rejetes']} rejetés "
        f"({_taux(jour['rejetes'], jour['entres'])}).**",
        "",
        "### Rejetés par règle et par source",
        "",
    ]
    sources = list(jour["par_source"])
    regles_vues = [r for r in regles if r in jour["par_regle_source"]]
    if regles_vues:
        l.append("| Règle | " + " | ".join(sources) + " | Total |")
        l.append("|---|" + "---:|" * (len(sources) + 1))
        for regle in regles_vues:
            par_source = jour["par_regle_source"][regle]
            l.append(f"| {regle} | " + " | ".join(str(par_source.get(s, 0) or "·") for s in sources)
                     + f" | {sum(par_source.values())} |")  # fmt: skip
        l.append("| **Entrés** | " + " | ".join(str(jour["par_source"][s]["entres"]) for s in sources)
                 + f" | {jour['entres']} |")  # fmt: skip
        l.append("| **Taux de rejet** | " + " | ".join(
            _taux(jour["par_source"][s]["rejetes"], jour["par_source"][s]["entres"]) for s in sources)
            + f" | {_taux(jour['rejetes'], jour['entres'])} |")  # fmt: skip
    else:
        l.append("Aucun rejet.")
    l += ["", "### Les cinq rejets les plus fréquents", ""]
    if jour["top5"]:
        l += ["| # | Règle | Source | Rejetés | Part de la source | Exemple |", "|---:|---|---|---:|---:|---|"]
        for rang, t in enumerate(jour["top5"], 1):
            l.append(f"| {rang} | {t['regle']} | {t['source']} | {t['rejetes']} "
                     f"| {t['part_de_la_source']:.0%} | {t['exemple'] or ''} |")  # fmt: skip
    else:
        l.append("Aucun rejet.")
    l += ["", f"### Règles trop gourmandes ? (au moins {PART_GOURMANDE:.0%} d'une source)", ""]
    if jour["gourmandes"]:
        for g in jour["gourmandes"]:
            l.append(f"- ⚠️ **{g['regle']}** écarte {g['part_de_la_source']:.0%} de **{g['source']}** "
                     f"({g['rejetes']} sur {g['entres']}) : relire `SELECT titre, detail FROM rejets "
                     f"WHERE regle = '{g['regle']}' AND source = '{g['source']}'`.")  # fmt: skip
    else:
        l.append("Aucune : pas de règle qui écarte une grosse part d'une source.")
    l += ["", "## Contrôles", "", "| Contrôle | Verdict | Détail |", "|---|---|---|"]
    for c in controles:
        l.append(f"| {c.nom} | {'✅' if c.ok else '🔴'} | {c.message} |")
    l += ["", "## Passages du jour", "", "| N° | Début | Entrés | Gardés | Rejetés |", "|---:|---|---:|---:|---:|"]
    for p in jour["passages"]:
        l.append(f"| {p['id']} | {_heure(p['debut'])} | {p['entres']} | {p['gardes']} | {p['rejetes']} |")
    return "\n".join(l) + "\n"


def ecrire_rapport(
    racine: Path,
    partition: str,
    passage: Passage,
    jour: dict,
    controles: list[Resultat],
    config: Config,
) -> Path:
    """Écrit le .md et le .json du jour ; renvoie le chemin du .md."""
    md = chemin_rapport(racine, partition)
    md.parent.mkdir(parents=True, exist_ok=True)
    md.write_text(en_markdown(partition, passage, jour, controles, config), encoding="utf-8")
    contenu = {
        "partition": partition,
        "genere_le": datetime.now(timezone.utc).isoformat(),
        "dernier_passage": {
            "id": passage.id, "fichiers": passage.fichiers, "entres": passage.entres,
            "gardes": passage.gardes, "rejetes": passage.rejetes,
            "par_regle": dict(passage.par_regle),
            "par_source": {s: dict(c) for s, c in passage.par_source.items()},
        },
        "jour": jour,
        "controles": [{"nom": c.nom, "ok": c.ok, "message": c.message, "metriques": c.metriques}
                      for c in controles],
    }  # fmt: skip
    chemin_rapport(racine, partition, "json").write_text(
        json.dumps(contenu, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return md

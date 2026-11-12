"""Les règles d'exclusion : lues dans regles.toml, appliquées dans l'ordre, la première gagne.

Chaque rejet porte l'identifiant de sa règle (R01_publi…) pour que le rapport compte par règle,
et un détail lisible (le motif reconnu, l'article dont c'est le doublon…).

Les doublons (R10 à R12) se jugent par rapport à une `Memoire` : les articles déjà gardés
(ceux des dernières heures, relus en base, puis ceux du passage au fur et à mesure). Le premier
publié est gardé, les suivants sont rejetés avec l'id de celui qu'ils répètent (doublon_de).
"""

import re
import tomllib
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

from gazette.silver.normalisation import cle_titre, encodage_casse, jaccard, mots_titre

REGLES_TOML = Path(__file__).with_name("regles.toml")
TYPES = {
    "motifs", "titre_vide", "date_future", "encodage", "perime",
    "doublon_exact", "depeche_reprise", "doublon_approx",
}  # fmt: skip


@dataclass(frozen=True)
class Regle:
    id: str
    nom: str
    type: str
    lien: tuple[re.Pattern, ...] = ()
    titre: tuple[re.Pattern, ...] = ()
    texte: tuple[re.Pattern, ...] = ()
    rubriques: frozenset[str] = frozenset()


@dataclass(frozen=True)
class Config:
    regles: tuple[Regle, ...]
    seuils: dict
    controles: dict

    def regle(self, id_regle: str) -> Regle:
        return next(r for r in self.regles if r.id == id_regle)


@dataclass(frozen=True)
class Verdict:
    regle: str
    detail: str
    doublon_de: str | None = None


def _motifs(valeurs: list[str]) -> tuple[re.Pattern, ...]:
    return tuple(re.compile(v, re.IGNORECASE) for v in valeurs)


def charger_regles(chemin: Path = REGLES_TOML) -> Config:
    with open(chemin, "rb") as f:
        brut = tomllib.load(f)
    regles = []
    for r in brut["regles"]:
        if r["type"] not in TYPES:
            raise ValueError(f"{r['id']} : type de règle inconnu {r['type']!r}")
        regles.append(
            Regle(
                id=r["id"],
                nom=r["nom"],
                type=r["type"],
                lien=_motifs(r.get("lien", [])),
                titre=_motifs(r.get("titre", [])),
                texte=_motifs(r.get("texte", [])),
                rubriques=frozenset(cle_titre(x) for x in r.get("rubriques", [])),
            )
        )
    ids = [r.id for r in regles]
    if len(ids) != len(set(ids)):
        raise ValueError("Deux règles portent le même identifiant dans regles.toml")
    return Config(tuple(regles), brut["seuils"], brut["controles"])


@dataclass
class Memoire:
    """Les articles gardés auxquels on compare les suivants (doublons)."""

    par_lien: dict[str, str] = field(default_factory=dict)
    par_source_titre: dict[tuple[str, str], str] = field(default_factory=dict)
    par_titre: dict[str, tuple[str, str]] = field(default_factory=dict)  # clé → (id, source)
    mots: list[tuple[str, frozenset[str]]] = field(default_factory=list)

    def ajouter(self, article: dict) -> None:
        if article["lien"]:
            self.par_lien.setdefault(article["lien"], article["id"])
        cle = article["titre_cle"]
        if cle:
            self.par_source_titre.setdefault((article["source"], cle), article["id"])
            self.par_titre.setdefault(cle, (article["id"], article["source"]))
        self.mots.append((article["id"], mots_titre(article["titre"])))


def _date(valeur: str | None) -> datetime | None:
    return datetime.fromisoformat(valeur) if valeur else None


def _par_motifs(regle: Regle, article: dict) -> str | None:
    for motif in regle.lien:
        if motif.search(article["_lien_brut"] or article["lien"] or ""):
            return f"lien ~ {motif.pattern}"
    for motif in regle.titre:
        if motif.search(article["titre"]):
            return f"titre ~ {motif.pattern}"
    texte = f"{article['titre']} {article['extrait']}"
    for motif in regle.texte:
        if motif.search(texte):
            return f"texte ~ {motif.pattern}"
    if article["rubrique"] and cle_titre(article["rubrique"]) in regle.rubriques:
        return f"rubrique = {article['rubrique']}"
    return None


def _juger_regle(regle: Regle, article: dict, memoire: Memoire, seuils: dict) -> Verdict | None:
    publie, recu = _date(article["publie_le"]), _date(article["recupere_le"])

    if regle.type == "motifs":
        detail = _par_motifs(regle, article)
        return Verdict(regle.id, detail) if detail else None

    if regle.type == "titre_vide":
        if len(article["titre"]) < seuils["longueur_titre_min"]:
            return Verdict(regle.id, f"titre de {len(article['titre'])} caractères")
        return None

    if regle.type == "date_future":
        if publie and recu and publie > recu + timedelta(minutes=seuils["avance_toleree_min"]):
            return Verdict(regle.id, f"publié le {article['publie_le']}, reçu le {article['recupere_le']}")
        return None

    if regle.type == "encodage":
        if encodage_casse(article["titre"]) or encodage_casse(article["extrait"]):
            return Verdict(regle.id, "séquences cassées après réparation")
        return None

    if regle.type == "perime":
        if publie and recu and recu - publie > timedelta(hours=seuils["age_max_h"]):
            return Verdict(regle.id, f"publié {(recu - publie).days} j avant sa collecte")
        return None

    if regle.type == "doublon_exact":
        if article["lien"] and article["lien"] in memoire.par_lien:
            return Verdict(regle.id, "même lien", memoire.par_lien[article["lien"]])
        cle = (article["source"], article["titre_cle"])
        if article["titre_cle"] and cle in memoire.par_source_titre:
            return Verdict(regle.id, "même titre, même journal", memoire.par_source_titre[cle])
        return None

    if regle.type == "depeche_reprise":
        connu = memoire.par_titre.get(article["titre_cle"])
        if connu and connu[1] != article["source"]:
            return Verdict(regle.id, f"même titre que {connu[1]}", connu[0])
        return None

    if regle.type == "doublon_approx":
        mots = mots_titre(article["titre"])
        if len(mots) < seuils["mots_min_jaccard"]:
            return None
        meilleur = max(
            ((jaccard(mots, m), id_connu) for id_connu, m in memoire.mots
             if len(m) >= seuils["mots_min_jaccard"]),
            default=(0.0, None),
        )  # fmt: skip
        if meilleur[0] >= seuils["jaccard_min"]:
            return Verdict(regle.id, f"Jaccard {meilleur[0]:.2f}", meilleur[1])
        return None

    raise ValueError(f"type de règle inconnu : {regle.type}")


def juger(article: dict, memoire: Memoire, config: Config) -> Verdict | None:
    """La première règle qui reconnaît l'article, ou None s'il est gardé."""
    for regle in config.regles:
        verdict = _juger_regle(regle, article, memoire, config.seuils)
        if verdict:
            return verdict
    return None


def trier(
    articles: list[dict], memoire: Memoire, config: Config
) -> list[tuple[dict, Verdict | None]]:
    """Juge les articles du plus ancien au plus récent ; les gardés rejoignent la mémoire."""
    ordre = sorted(articles, key=lambda a: (a["publie_le"] or a["recupere_le"] or "", a["id"]))
    resultats = []
    for article in ordre:
        verdict = juger(article, memoire, config)
        if verdict is None:
            memoire.ajouter(article)
        resultats.append((article, verdict))
    return resultats

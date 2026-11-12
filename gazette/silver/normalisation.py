"""Normalisation : une fonction pure par transformation, chacune testée (tests/test_normalisation.py).

Elles ne lisent rien et n'écrivent rien : un texte (ou une date, un lien) entre, un texte sort.
`normaliser_article` les enchaîne sur un article brut et ne garde que ce qu'on a le droit de
republier (titre, extrait court, lien, date, source, rubrique) : pas l'auteur, pas le payload.
"""

import html
import re
import unicodedata
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

LONGUEUR_EXTRAIT = 300
# Séquences typiques d'un texte UTF-8 relu en cp1252 / latin-1 (« Ã© » pour « é »).
MOJIBAKE = re.compile(r"Ã.|â€|Â[\xa0-\xbf]", re.DOTALL)
PARAMETRES_DE_SUIVI = re.compile(r"^(utm_|xtor$|at_|fbclid$|gclid$|ns_)", re.IGNORECASE)
MOTS_VIDES = frozenset(
    "les des une pour par sur dans avec qui que est sont pas plus son ses aux leur leurs mais "
    "ont été cette ces elle ils elles nous vous tout tous fait après avant entre comme".split()
)


class _Texte(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.morceaux: list[str] = []

    def handle_data(self, data: str) -> None:
        self.morceaux.append(data)

    def handle_starttag(self, tag: str, attrs) -> None:
        if tag in {"br", "p", "div", "li"}:
            self.morceaux.append(" ")


def retirer_html(texte: str | None) -> str:
    """Les balises disparaissent, leur texte reste : « <p>Un <b>fait</b></p> » → « Un fait »."""
    if not texte:
        return ""
    if "<" not in texte:
        return texte
    lecteur = _Texte()
    lecteur.feed(texte)
    lecteur.close()
    return "".join(lecteur.morceaux).strip()


def decoder_entites(texte: str | None) -> str:
    """« &eacute; », « &#233; », et même « &amp;eacute; » (échappé deux fois) → « é »."""
    texte = texte or ""
    for _ in range(2):
        decode = html.unescape(texte)
        if decode == texte:
            break
        texte = decode
    return texte


def reparer_encodage(texte: str | None) -> str:
    """« Ã©tÃ© » → « été » : un texte UTF-8 relu en cp1252, qu'on relit dans le bon sens.

    Un texte sain n'est jamais touché : on ne répare que s'il contient des séquences cassées
    et que la relecture les fait disparaître.
    """
    texte = texte or ""
    for _ in range(2):  # parfois cassé deux fois
        if not MOJIBAKE.search(texte):
            break
        for encodage in ("cp1252", "latin-1"):
            try:
                repare = texte.encode(encodage).decode("utf-8")
            except (UnicodeEncodeError, UnicodeDecodeError):
                continue
            texte = repare
            break
        else:
            break
    return texte


def encodage_casse(texte: str | None) -> bool:
    """Vrai s'il reste des séquences cassées (ou le caractère de remplacement �)."""
    texte = texte or ""
    return "�" in texte or bool(MOJIBAKE.search(texte))


def normaliser_espaces(texte: str | None) -> str:
    """Espaces insécables, tabulations, retours à la ligne, espaces de largeur nulle → un espace."""
    texte = (texte or "").replace("​", "").replace("﻿", "")
    return re.sub(r"\s+", " ", texte).strip()


def date_utc(valeur: str | None) -> str | None:
    """Une date ISO 8601 ou RFC 822 (pubDate), ramenée en UTC ISO 8601 ; None si illisible.

    Une date sans fuseau est supposée déjà en UTC.
    """
    if not valeur:
        return None
    try:
        date = datetime.fromisoformat(valeur.replace("Z", "+00:00"))
    except ValueError:
        try:
            date = parsedate_to_datetime(valeur)
        except (TypeError, ValueError, IndexError):
            return None
    if date.tzinfo is None:
        date = date.replace(tzinfo=timezone.utc)
    return date.astimezone(timezone.utc).isoformat()


def borner_extrait(texte: str | None, longueur: int = LONGUEUR_EXTRAIT) -> str:
    """Un extrait court : coupé à `longueur` caractères, sur un espace, avec « … »."""
    texte = texte or ""
    if len(texte) <= longueur:
        return texte
    coupe = texte[: longueur - 1]
    if " " in coupe[longueur // 2 :]:
        coupe = coupe.rsplit(" ", 1)[0]
    return coupe.rstrip(" ,;:.-") + "…"


def normaliser_lien(lien: str | None) -> str | None:
    """Sans paramètres de suivi (utm_*, xtor…) ni ancre, domaine en minuscules."""
    if not lien:
        return None
    morceaux = urlsplit(lien.strip())
    requete = [(c, v) for c, v in parse_qsl(morceaux.query, keep_blank_values=True)
               if not PARAMETRES_DE_SUIVI.match(c)]  # fmt: skip
    return urlunsplit(
        (morceaux.scheme.lower(), morceaux.netloc.lower(), morceaux.path, urlencode(requete), "")
    )


def cle_titre(titre: str | None) -> str:
    """Le titre réduit pour comparer : minuscules, sans accents ni ponctuation.

    « L’Élysée : “Non” » et « l'elysee non » donnent la même clé.
    """
    sans_accents = unicodedata.normalize("NFKD", (titre or "").lower())
    sans_accents = "".join(c for c in sans_accents if not unicodedata.combining(c))
    return re.sub(r"[\W_]+", " ", sans_accents).strip()


def mots_titre(titre: str | None) -> frozenset[str]:
    """Les mots porteurs du titre (plus de deux lettres, hors mots vides), pour le Jaccard."""
    return frozenset(m for m in cle_titre(titre).split() if len(m) > 2 and m not in MOTS_VIDES)


def jaccard(a: frozenset[str], b: frozenset[str]) -> float:
    """Mots en commun / mots en tout : 1 pour deux titres identiques, 0 sans mot commun."""
    return len(a & b) / len(a | b) if a and b else 0.0


def nettoyer_texte(texte: str | None) -> str:
    """La chaîne complète pour un titre ou un extrait : encodage, HTML, entités, espaces."""
    return normaliser_espaces(decoder_entites(retirer_html(reparer_encodage(texte))))


def normaliser_article(brut: dict, fichier_brut: str) -> dict:
    """Un article brut (bronze) → la forme propre, et de quoi remonter au brut (traçabilité)."""
    titre = nettoyer_texte(brut.get("titre"))
    return {
        "id": brut["id"],
        "source": brut["source"],
        "flux": brut.get("flux"),
        "titre": titre,
        "extrait": borner_extrait(nettoyer_texte(brut.get("resume"))),
        "lien": normaliser_lien(brut.get("lien")),
        "publie_le": date_utc(brut.get("publie_le")),
        "recupere_le": date_utc(brut.get("recupere_le")),
        "rubrique": nettoyer_texte(brut.get("rubrique")) or None,
        "titre_cle": cle_titre(titre),
        "fichier_brut": fichier_brut,
        # Pour les règles seulement, jamais stockés : texte brut (encodage d'origine), lien complet.
        "_titre_brut": brut.get("titre") or "",
        "_resume_brut": brut.get("resume") or "",
        "_lien_brut": brut.get("lien") or "",
    }

"""Un test par fonction de normalisation (gazette/silver/normalisation.py)."""

from gazette.silver import normalisation as n


def test_retirer_html():
    assert n.retirer_html("<p>Un <b>fait</b> établi</p>") == "Un fait établi"
    assert n.retirer_html("ligne 1<br/>ligne 2").split() == ["ligne", "1", "ligne", "2"]
    assert n.retirer_html("3 < 5 et 5 > 3") == "3 < 5 et 5 > 3"  # pas une balise
    assert n.retirer_html(None) == ""


def test_decoder_entites():
    assert n.decoder_entites("&eacute;lu &amp; r&#233;form&eacute;") == "élu & réformé"
    assert n.decoder_entites("&amp;eacute;") == "é"  # échappé deux fois
    assert n.decoder_entites("rien à décoder") == "rien à décoder"


def test_reparer_encodage():
    casse = "Élection : la majorité change".encode("utf-8").decode("cp1252")
    assert casse.startswith("Ã")
    assert n.reparer_encodage(casse) == "Élection : la majorité change"
    assert n.reparer_encodage("São Paulo, déjà vu") == "São Paulo, déjà vu"  # texte sain intact
    assert n.reparer_encodage(None) == ""


def test_encodage_casse():
    assert n.encodage_casse("pr\ufffdsident")
    assert n.encodage_casse("prÃ©sident")
    assert not n.encodage_casse("président à l'Élysée")


def test_normaliser_espaces():
    assert n.normaliser_espaces("  Un\u00a0titre\u202f:\t\ntrop\u200b  espacé ") == "Un titre : trop espacé"
    assert n.normaliser_espaces(None) == ""


def test_date_utc():
    assert n.date_utc("Thu, 12 Nov 2026 07:30:00 +0100") == "2026-11-12T06:30:00+00:00"
    assert n.date_utc("2026-11-12T07:30:00+01:00") == "2026-11-12T06:30:00+00:00"
    assert n.date_utc("2026-11-12T06:30:00Z") == "2026-11-12T06:30:00+00:00"
    assert n.date_utc("2026-11-12T06:30:00") == "2026-11-12T06:30:00+00:00"  # sans fuseau : UTC
    assert n.date_utc("hier soir") is None
    assert n.date_utc(None) is None


def test_borner_extrait():
    court = "Un extrait court."
    assert n.borner_extrait(court) == court
    long = "mot " * 200
    borne = n.borner_extrait(long, 100)
    assert len(borne) <= 100 and borne.endswith("…") and not borne.endswith(" …")
    assert n.borner_extrait(None) == ""


def test_normaliser_lien():
    assert (
        n.normaliser_lien("https://WWW.Ouest-France.fr/a.html?utm_source=rss&utm_campaign=x&page=2")
        == "https://www.ouest-france.fr/a.html?page=2"
    )
    assert (
        n.normaliser_lien("https://www.franceinfo.fr/b.html#xtor=RSS-3-[lestitres]")
        == "https://www.franceinfo.fr/b.html"
    )
    assert n.normaliser_lien(None) is None


def test_cle_titre():
    assert n.cle_titre("L’Élysée : « Non », dit-il") == n.cle_titre("l'elysee non dit il")
    assert n.cle_titre(None) == ""


def test_mots_titre():
    assert n.mots_titre("Les élus de la Région votent le budget") == {"elus", "region", "votent", "budget"}


def test_jaccard():
    a, b = frozenset({"immeuble", "effondre", "york"}), frozenset({"immeuble", "effondre", "paris"})
    assert n.jaccard(a, a) == 1.0
    assert n.jaccard(a, b) == 0.5
    assert n.jaccard(a, frozenset()) == 0.0


def test_nettoyer_texte():
    assert n.nettoyer_texte(" <b>R&eacute;forme</b>\u00a0adoptée ") == "Réforme adoptée"


def test_normaliser_article_ne_garde_que_ce_qu_on_peut_republier():
    brut = {
        "id": "abc", "source": "le_monde", "flux": "une", "lien": "https://x.fr/a?utm_source=rss",
        "titre": "<i>Titre</i>", "resume": "x" * 1000, "publie_le": "2026-11-12T07:00:00+01:00",
        "recupere_le": "2026-11-12T08:00:00+00:00", "rubrique": None,
        "payload": {"author": "Jeanne Exemple"},
    }  # fmt: skip
    propre = n.normaliser_article(brut, "articles_bruts/date=2026-11-12/le_monde/une.json")
    assert propre["titre"] == "Titre"
    assert len(propre["extrait"]) <= n.LONGUEUR_EXTRAIT
    assert propre["lien"] == "https://x.fr/a"
    assert propre["publie_le"] == "2026-11-12T06:00:00+00:00"
    assert propre["fichier_brut"].endswith("une.json")
    assert "payload" not in propre and "Jeanne Exemple" not in str(propre)  # pas d'auteur

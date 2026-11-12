"""La couche silver : règles d'exclusion (sur la fixture), passage, contrôles, rapport, alerte."""

import json
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

import dagster as dg
import pytest

from gazette import alerte
from gazette.db import connecter
from gazette.definitions import defs, qualite_apres_collecte
from gazette.silver import controles, rapport, traitement
from gazette.silver.normalisation import normaliser_article
from gazette.silver.regles import Memoire, charger_regles, trier

FIXTURE = Path(__file__).parent / "fixtures" / "bronze_echantillon.json"
PARTITION = "2026-11-12"
MAINTENANT = datetime(2026, 11, 12, 8, 5, tzinfo=timezone.utc)
CONFIG = charger_regles()


def bruts() -> list[dict]:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def attendus() -> dict[str, str | None]:
    return {a["id"]: a["_attendu"] for a in bruts()}


def deposer_bronze(racine: Path, articles: list[dict], nom: str = "une_1") -> None:
    """Range les articles comme la collecte : un JSON par source dans la partition du jour."""
    par_source: dict[str, list[dict]] = {}
    for article in articles:
        par_source.setdefault(article["source"], []).append(article)
    for source, liste in par_source.items():
        dossier = racine / "articles_bruts" / f"date={PARTITION}" / source
        dossier.mkdir(parents=True, exist_ok=True)
        (dossier / f"{nom}.json").write_text(json.dumps(liste, ensure_ascii=False), encoding="utf-8")


def verdicts_fixture() -> dict[str, str | None]:
    articles = [normaliser_article(b, "fixture.json") for b in bruts()]
    return {a["id"]: (v.regle if v else None) for a, v in trier(articles, Memoire(), CONFIG)}


# ── les règles, une par une, sur la fixture ─────────────────────────────────────────────────


def test_regles_identifiees_et_uniques():
    ids = [r.id for r in CONFIG.regles]
    assert ids == sorted(ids) and len(ids) == 12
    assert all(i[:1] == "R" and i[1:3].isdigit() and "_" in i for i in ids)


@pytest.mark.parametrize("id_article,regle", sorted(attendus().items()))
def test_chaque_article_de_la_fixture_a_le_verdict_attendu(id_article, regle):
    assert verdicts_fixture()[id_article] == regle


def test_chaque_regle_est_couverte_par_la_fixture():
    assert {r for r in attendus().values() if r} == {r.id for r in CONFIG.regles}


def test_le_premier_publie_est_garde_les_suivants_pointent_vers_lui():
    articles = [normaliser_article(b, "fixture.json") for b in bruts()]
    doublons = {a["id"]: v.doublon_de for a, v in trier(articles, Memoire(), CONFIG) if v and v.doublon_de}
    assert doublons == {"r10": "propre-1", "r11": "propre-depeche", "r12": "propre-immeuble"}


def test_un_doublon_d_un_article_garde_au_passage_precedent_est_rejete():
    memoire = Memoire()
    memoire.ajouter({"id": "hier", "source": "le_monde", "lien": "https://x.fr/a",
                     "titre": "Un titre déjà vu au passage d'avant", "titre_cle": "un titre deja vu au passage d avant"})  # fmt: skip
    article = normaliser_article(
        {**bruts()[0], "id": "nouveau", "lien": "https://x.fr/a?utm_source=rss"}, "f.json"
    )
    [(_, verdict)] = trier([article], memoire, CONFIG)
    assert (verdict.regle, verdict.doublon_de) == ("R10_doublon_exact", "hier")


def test_regles_toml_invalide_refusee(tmp_path):
    (tmp_path / "r.toml").write_text(
        '[seuils]\n[controles]\n[[regles]]\nid = "R01_x"\nnom = "x"\ntype = "inconnu"\n',
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="type de règle inconnu"):
        charger_regles(tmp_path / "r.toml")


# ── un passage : articles_propres, rejets, traçabilité ──────────────────────────────────────


def test_passage_range_propres_et_rejets_avec_leur_regle(tmp_path):
    deposer_bronze(tmp_path, bruts())
    passage = traitement.passer(tmp_path, PARTITION, CONFIG, maintenant=MAINTENANT)

    nb_rejets = sum(1 for r in attendus().values() if r)
    assert (passage.entres, passage.rejetes) == (len(bruts()), nb_rejets)
    assert passage.gardes == len(bruts()) - nb_rejets
    with closing(connecter(tmp_path)) as connexion:
        rejets = dict(connexion.execute("SELECT id, regle FROM rejets").fetchall())
        propres = {i for (i,) in connexion.execute("SELECT id FROM articles_propres")}
        html = connexion.execute(
            "SELECT titre, extrait FROM articles_propres WHERE id = 'propre-html'"
        ).fetchone()
        encodage = connexion.execute(
            "SELECT titre FROM articles_propres WHERE id = 'propre-encodage'"
        ).fetchone()[0]
    assert rejets == {i: r for i, r in attendus().items() if r}
    assert propres == {i for i, r in attendus().items() if not r}
    assert tuple(html) == (
        "Les élus votent la réforme des retraites agricoles",
        "Le texte a été adopté à l'unanimité. Il entre en vigueur en janvier.",
    )
    assert encodage == "Élection municipale : la mairie de Toulouse change de majorité"


def test_rejouer_un_passage_ne_change_rien(tmp_path):
    deposer_bronze(tmp_path, bruts())
    traitement.passer(tmp_path, PARTITION, CONFIG, maintenant=MAINTENANT)
    second = traitement.passer(tmp_path, PARTITION, CONFIG, maintenant=MAINTENANT)
    assert (second.fichiers, second.entres) == (0, 0)
    with closing(connecter(tmp_path)) as connexion:
        total = connexion.execute(
            "SELECT (SELECT count(*) FROM articles_propres) + (SELECT count(*) FROM rejets)"
        ).fetchone()[0]
    assert total == len(bruts())


def test_d_un_article_propre_on_retrouve_l_item_brut_et_sa_source(tmp_path):
    deposer_bronze(tmp_path, bruts())
    traitement.passer(tmp_path, PARTITION, CONFIG, maintenant=MAINTENANT)
    with closing(connecter(tmp_path)) as connexion:
        propre = connexion.execute(
            "SELECT p.id, p.fichier_brut, s.nom FROM articles_propres p "
            "LEFT JOIN sources s ON s.slug = p.source WHERE p.id = 'propre-html'"
        ).fetchone()
    brut = json.loads((tmp_path / propre["fichier_brut"]).read_text(encoding="utf-8"))
    assert [b["titre"] for b in brut if b["id"] == propre["id"]] == [
        "Les &eacute;lus votent <b>la r&eacute;forme</b> des retraites agricoles"
    ]


def test_articles_propres_ne_stocke_ni_auteur_ni_texte_integral(tmp_path):
    deposer_bronze(tmp_path, bruts())
    traitement.passer(tmp_path, PARTITION, CONFIG, maintenant=MAINTENANT)
    with closing(connecter(tmp_path)) as connexion:
        colonnes = {c["name"] for c in connexion.execute("PRAGMA table_info(articles_propres)")}
        longueur = connexion.execute("SELECT max(length(extrait)) FROM articles_propres").fetchone()[0]
        contenu = str(connexion.execute("SELECT * FROM articles_propres").fetchall())
    assert not colonnes & {"payload", "auteur", "author", "contenu", "texte"}
    assert longueur <= 300
    assert "Jeanne Exemple" not in contenu


# ── les contrôles : des faits sur les données ───────────────────────────────────────────────


def resultats(tmp_path) -> dict[str, controles.Resultat]:
    with closing(connecter(tmp_path)) as connexion:
        return {r.nom: r for r in controles.controler(connexion, PARTITION, CONFIG)}


def test_controles_verts_apres_un_passage_normal(tmp_path):
    deposer_bronze(tmp_path, bruts())
    traitement.passer(tmp_path, PARTITION, CONFIG, maintenant=MAINTENANT)
    assert all(r.ok for r in resultats(tmp_path).values())


def test_controles_rouges_sur_des_donnees_fausses(tmp_path):
    """Un fait faux en base (écrit à la main, comme une règle cassée le ferait) est vu."""
    deposer_bronze(tmp_path, bruts())
    traitement.passer(tmp_path, PARTITION, CONFIG, maintenant=MAINTENANT)
    with closing(connecter(tmp_path)) as connexion:
        connexion.execute(
            "UPDATE articles_propres SET publie_le = '2027-01-01T00:00:00+00:00', titre = '' "
            "WHERE id = 'propre-1'"
        )
        connexion.execute("UPDATE articles_propres SET extrait = 'prÃ©sident' WHERE id = 'propre-html'")
        connexion.execute("UPDATE articles_propres SET lien = 'https://x.fr/meme' WHERE source = 'sud_ouest'")
    r = resultats(tmp_path)
    assert not r["aucune_date_future"].ok and not r["aucun_titre_vide"].ok
    assert not r["aucun_encodage_casse"].ok and not r["aucun_lien_en_double"].ok


def test_taux_de_rejet_d_une_source_au_dela_de_80_pourcent(tmp_path):
    publis = [
        {**bruts()[5], "id": f"publi-{i}", "lien": f"https://www.nicematin.com/guide-conso/p-{i}"}
        for i in range(9)
    ]
    deposer_bronze(tmp_path, [*publis, {**bruts()[4]}])  # nice_matin : 9 publis sur 10
    traitement.passer(tmp_path, PARTITION, CONFIG, maintenant=MAINTENANT)
    taux = resultats(tmp_path)["taux_rejet_par_source"]
    assert not taux.ok
    assert taux.metriques["taux_rejet_nice_matin"] == 0.9
    assert "nice_matin rejette 90%" in taux.message


# ── l'alerte ────────────────────────────────────────────────────────────────────────────────


def test_alerte_quand_un_controle_devient_faux_puis_redevient_vrai(tmp_path):
    vrai = controles.Resultat("aucun_titre_vide", True, "aucun titre vide")
    faux = controles.Resultat("aucun_titre_vide", False, "2 titres vides")
    with closing(connecter(tmp_path)) as connexion:
        assert alerte.changements(connexion, [vrai]) == []  # jamais vu, vrai : silence
        assert alerte.changements(connexion, [faux]) == [
            "🔴 Gazette · contrôle aucun_titre_vide faux : 2 titres vides"
        ]
        assert alerte.changements(connexion, [faux]) == []  # toujours faux : pas de répétition
        [message] = alerte.changements(connexion, [vrai])
        assert message.startswith("✅")


def test_charge_utile_discord_ou_slack():
    assert alerte.charge_utile("https://discord.com/api/webhooks/1/x", "m")["content"] == "m"
    assert alerte.charge_utile("https://hooks.slack.com/services/x", "m") == {"text": "m"}


def test_envoi_sans_webhook_ne_fait_rien(monkeypatch):
    monkeypatch.delenv(alerte.WEBHOOK_ENV, raising=False)
    assert alerte.envoyer("rien") is False


def test_envoi_vers_le_webhook(monkeypatch):
    envois = []

    class Reponse:
        def raise_for_status(self):
            pass

    monkeypatch.setattr(alerte.requests, "post", lambda url, json, timeout: envois.append((url, json)) or Reponse())
    assert alerte.envoyer("🔴 test", "https://discord.com/api/webhooks/1/x")
    assert envois[0][1]["content"] == "🔴 test"


# ── le rapport ──────────────────────────────────────────────────────────────────────────────


def test_rapport_du_jour_par_regle_par_source_et_top5(tmp_path):
    deposer_bronze(tmp_path, bruts())
    passage = traitement.passer(tmp_path, PARTITION, CONFIG, maintenant=MAINTENANT)
    with closing(connecter(tmp_path)) as connexion:
        jour = rapport.donnees_du_jour(connexion, PARTITION)
        res = controles.controler(connexion, PARTITION, CONFIG)
    chemin = rapport.ecrire_rapport(tmp_path, PARTITION, passage, jour, res, CONFIG)

    assert chemin == tmp_path / "rapports" / f"qualite-{PARTITION}.md"
    texte = chemin.read_text(encoding="utf-8")
    for attendu in ("Dernier passage", "Rejetés par règle et par source",
                    "Les cinq rejets les plus fréquents", "Règles trop gourmandes", "Contrôles"):  # fmt: skip
        assert attendu in texte
    assert all(r in texte for r in {r for r in attendus().values() if r})
    assert len(jour["top5"]) == 5
    assert jour["entres"] == len(bruts())
    contenu = json.loads(chemin.with_suffix(".json").read_text(encoding="utf-8"))
    assert contenu["dernier_passage"]["rejetes"] == passage.rejetes


def test_rapport_signale_une_regle_trop_gourmande(tmp_path):
    publis = [
        {**bruts()[5], "id": f"publi-{i}", "lien": f"https://www.nicematin.com/guide-conso/p-{i}"}
        for i in range(4)
    ]
    deposer_bronze(tmp_path, [*publis, bruts()[4]])
    traitement.passer(tmp_path, PARTITION, CONFIG, maintenant=MAINTENANT)
    with closing(connecter(tmp_path)) as connexion:
        jour = rapport.donnees_du_jour(connexion, PARTITION)
    assert jour["gourmandes"] == [{"regle": "R01_publi", "source": "nice_matin", "rejetes": 4,
                                   "entres": 5, "part_de_la_source": 0.8}]  # fmt: skip


# ── Dagster : le job qualité, après chaque collecte ─────────────────────────────────────────


def test_job_qualite_bout_en_bout(tmp_path, monkeypatch):
    monkeypatch.setenv("GAZETTE_DATA_DIR", str(tmp_path))
    monkeypatch.delenv(alerte.WEBHOOK_ENV, raising=False)
    deposer_bronze(tmp_path, bruts())
    from gazette.silver.assets import assets_silver

    resultat = dg.materialize(assets_silver, partition_key=PARTITION)
    assert resultat.success
    meta = resultat.asset_materializations_for_node("silver__articles_propres")[0].metadata
    assert meta["entres"].value == len(bruts())
    assert meta["rejets_R01_publi"].value == 2
    checks = {c.check_name: c.passed for c in resultat.get_asset_check_evaluations()}
    assert set(checks) == set(controles.NOMS_CONTROLES) and all(checks.values())
    assert (tmp_path / "rapports" / f"qualite-{PARTITION}.md").exists()


def test_qualite_lancee_apres_chaque_collecte():
    job = defs.resolve_job_def("gazette_qualite")
    assert set(job.graph.node_dict) == {"silver__articles_propres", "silver__rapport_qualite"}
    assert qualite_apres_collecte.default_status == dg.DefaultSensorStatus.RUNNING

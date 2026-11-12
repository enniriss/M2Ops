# Dictionnaire des données · La gazette

> Ce que la collecte (TD 3, bronze) et le tri (TD 4, silver) produisent, champ par champ, et le
> contrat qui va avec (fraîcheur, rétention, ce qu'on ne stocke pas, traçabilité : voir la fin).
> Tout est sous `$GAZETTE_DATA_DIR` (`/data` dans les conteneurs, `./data` sur la machine hôte).

```
data/
├── gazette.db                                   SQLite : sources, articles_propres, rejets, passages,
│                                                fichiers_traites, etat_controles (+ schema_migrations)
├── rapports/
│   ├── qualite-AAAA-MM-JJ.md                    rapport de qualité du jour, réécrit à chaque passage
│   └── qualite-AAAA-MM-JJ.json                  le même, pour une machine
└── articles_bruts/
    └── date=AAAA-MM-JJ/                         partition Dagster du jour (heure de Paris)
        └── <slug>/                              un dossier par journal
            ├── <flux>_<AAAAMMJJTHHMMSSffffffZ>.json   les articles nouveaux de ce passage
            └── <flux>_<AAAAMMJJTHHMMSSffffffZ>.xml    la réponse HTTP complète, telle quelle
```

## `articles_bruts/` · les articles bruts (fichiers JSON)

Un fichier par passage et par flux, **seulement s'il y a au moins un article nouveau**. Chaque fichier
est une liste JSON, un objet par article. Les fichiers ne sont jamais réécrits (ouverture en
mode `x` : écrire deux fois le même fichier lève une erreur).

| Champ | Type | Exemple | Description |
|---|---|---|---|
| `id` | texte (32 hex) | `"9f2c…e1"` | Identifiant de l'article : SHA-256 de `source` + `lien`, tronqué. Stable : le même article a toujours le même `id`. Aléatoire (UUID) pour un item sans lien ni guid, qu'on ne sait pas reconnaître. |
| `source` | texte | `"le_monde"` | Le journal (`slug` de `sources.csv` et de la table `sources`). |
| `flux` | texte | `"une"` | Le flux du journal qui a ramené l'article. |
| `lien` | texte · null | `"https://www.lemonde.fr/…"` | URL de l'article (`<link>` RSS, `<link href>` Atom). Avec `source`, c'est la **clé de dédoublonnage**. |
| `titre` | texte · null | `"Guerre en Ukraine : …"` | `<title>`, tel que publié. |
| `resume` | texte · null | `"Les émissaires américains…"` | Le chapô : `<description>` en RSS, `<summary>` ou `<content>` en Atom. Peut contenir du HTML. |
| `publie_le` | texte ISO 8601 UTC · null | `"2026-11-12T06:30:00+00:00"` | Date de publication : `<pubDate>` (RSS) ou `<published>` / `<updated>` (Atom), analysée par feedparser et ramenée en UTC. Si la date est illisible, la chaîne d'origine ; null si l'item n'en a pas. |
| `recupere_le` | texte ISO 8601 UTC | `"2026-11-12T08:41:03.512+00:00"` | Quand la collecte a reçu la réponse. |
| `rubrique` | texte · null | `"Politique"` | Première `<category>` de l'item. Absente chez la plupart des éditeurs (voir `qualite.md`). |
| `payload` | objet | `{ "id": "…", "tags": […], … }` | L'item complet tel que feedparser l'a lu, sans aucun nettoyage : rien n'est perdu pour silver. Les dates `*_parsed` y sont écrites en ISO 8601. |

**Dédoublonnage.** Avant d'écrire, le collecteur relit les clés déjà présentes dans tous les JSON de
la source, tous jours confondus. Clé : `lien`, à défaut le guid RSS (`payload.id`). Un article déjà
écrit n'est pas réécrit, même s'il revient le lendemain ou dans un autre flux du même journal.

## `sources` · l'état des flux (table SQLite)

Créée par la migration versionnée `migrations/001_sources.sql`, appliquée une seule fois
(`schema_migrations` garde la trace). Une ligne par flux de `docs/sources.csv`. Les lignes sont
recopiées au début de chaque run : le CSV reste le catalogue, la table porte l'état.

| Colonne | Type | Description |
|---|---|---|
| `id` | entier | Clé technique. |
| `slug` | texte | Le journal (`le_monde`…). Avec `flux`, unique. |
| `nom` | texte | Nom affiché (`Le Monde`). |
| `type` | `PQN` · `PQR` | Presse quotidienne nationale ou régionale. |
| `region` | texte | Zone couverte (`National` pour la PQN). |
| `flux` | texte | Nom du flux au sein du journal (`une`, `tout`…). |
| `url_flux` | texte, unique | URL du flux RSS. |
| `actif` | 0 · 1 | 0 = en sommeil : le flux n'est plus contacté. Passe à 0 après **3 échecs permanents de suite** (404, 410…). Retour à 1 : `python -m gazette.reactiver <slug>`. |
| `derniere_reussite` | ISO 8601 UTC · null | Dernière réponse 200 ou 304. |
| `echecs_consecutifs` | entier | Échecs permanents de suite ; remis à 0 à la première réussite. Les erreurs transitoires (5xx, timeout…) ne comptent pas : elles se règlent par les relances. |
| `derniere_erreur` | texte · null | Message de la dernière erreur permanente. |
| `etag` | texte · null | `ETag` de la dernière réponse 200, renvoyé en `If-None-Match`. |
| `last_modified` | texte · null | `Last-Modified` de la dernière réponse 200, renvoyé en `If-Modified-Since`. |

Lire l'état à la main :

```bash
docker compose exec code python -c "import sqlite3; [print(tuple(r)) for r in sqlite3.connect('/data/gazette.db').execute('select slug, flux, actif, echecs_consecutifs, derniere_reussite from sources')]"
```

## Ce que chaque run dit (métadonnées Dagster)

Une ligne de journal par source (`Le Monde : 16 reçus, 3 écrits, 13 ignorés, 0 flux non modifiés (304), 0 en échec, en 0.21 s`), et les mêmes compteurs en métadonnées de l'asset `bronze/<slug>`, visibles dans l'interface :

| Métadonnée | Description |
|---|---|
| `nb_recus` | Items présents dans les flux qui ont répondu 200. |
| `nb_ecrits` | Items nouveaux, écrits dans `articles_bruts/`. |
| `nb_ignores` | Items déjà connus (doublons) : `nb_recus - nb_ecrits`. |
| `nb_non_modifies` | Flux qui ont répondu 304 : rien de neuf, rien téléchargé. |
| `nb_flux_ok`, `nb_flux_en_echec`, `flux_en_echec` | Flux qui ont répondu, ou non, et pourquoi. |
| `flux_en_sommeil` | Flux à `actif = 0`. |
| `duree_s`, `octets` | Durée de la collecte de la source, volume téléchargé. |
| `fichiers` | Les JSON écrits. |

Deux *asset checks* par source : `au_moins_un_article` (la source a répondu, avec des items ou un 304)
et `sources_actives` (aucun flux en sommeil).

---

# Silver · les articles triés (TD 4)

Après chaque collecte, le job Dagster `gazette_qualite` lit les JSON bronze pas encore traités,
les normalise (`gazette/silver/normalisation.py`), leur applique les règles
(`gazette/silver/regles.toml`, documentées dans `qualite.md`) et range chaque article **une fois**,
soit dans `articles_propres`, soit dans `rejets`. Tables créées par `migrations/002_silver.sql`.

## `articles_propres` · ce que la gazette peut publier

| Colonne | Type | Description |
|---|---|---|
| `id` | texte, clé | **Le même id que l'article brut** (SHA-256 de source + lien, voir plus haut). |
| `source` | texte | Le journal (`slug`, table `sources`). |
| `flux` | texte | Le flux qui l'a ramené. |
| `titre` | texte | Sans HTML, entités décodées, encodage réparé, espaces normalisés. 15 caractères au moins. |
| `extrait` | texte | Le chapô nettoyé de même, **300 caractères au plus** (coupé sur un mot, avec « … »). |
| `lien` | texte · null | URL de l'article, sans paramètres de suivi (`utm_*`, `xtor`…) ni ancre. Unique (contrôle `aucun_lien_en_double`). |
| `publie_le` | ISO 8601 UTC · null | Date de publication, en UTC. Jamais dans le futur, jamais plus de 48 h avant la collecte. |
| `recupere_le` | ISO 8601 UTC | Quand la collecte l'a reçu. |
| `rubrique` | texte · null | Rubrique RSS nettoyée, si l'éditeur la donne. |
| `titre_cle` | texte | Titre réduit (minuscules, sans accents ni ponctuation) : la clé des doublons. |
| `partition` | AAAA-MM-JJ | Le jour de collecte (heure de Paris). |
| `fichier_brut` | texte | Le JSON bronze d'où il vient, relatif à `$GAZETTE_DATA_DIR`. |
| `passage_id` | entier | Le passage silver qui l'a rangé (table `passages`). |

## `rejets` · ce qui a été écarté, et par quelle règle

Mêmes `id`, `source`, `flux`, `titre`, `lien`, `publie_le`, `recupere_le`, `partition`,
`fichier_brut`, `passage_id` que ci-dessus, plus :

| Colonne | Description |
|---|---|
| `regle` | L'identifiant de la règle qui l'a écarté (`R01_publi` … `R12_doublon_approx`). |
| `detail` | Ce qui a déclenché la règle : le motif reconnu (`lien ~ /guide-conso/`), le score (`Jaccard 0.80`), l'âge… |
| `doublon_de` | Pour R10 à R12 : l'`id` de l'article gardé qu'il répète. |

## Les tables de suivi

| Table | Une ligne par | Ce qu'elle garde |
|---|---|---|
| `passages` | passage silver | `partition`, `debut`, `fin`, `run_id` (le run Dagster), `entres`, `gardes`, `rejetes`. |
| `fichiers_traites` | JSON bronze traité | `fichier`, `passage_id` : un fichier n'est jamais traité deux fois. |
| `etat_controles` | contrôle de qualité | Son dernier verdict (`ok`), `depuis` quand, `message` : on alerte quand il change. |

## Le rapport de qualité · `rapports/qualite-AAAA-MM-JJ.md`

Réécrit à chaque passage de la journée, figé le lendemain (c'est l'archive). Il dit : le dernier
passage (entrés, gardés, rejetés, par source et par règle), le cumul du jour (une matrice règle ×
source, avec le taux de rejet de chaque source), les cinq rejets les plus fréquents (règle, source,
part de la source, un exemple), les règles qui écartent au moins 30 % d'une source (« trop
gourmandes ? »), le verdict des contrôles et la liste des passages. Le `.json` contient les mêmes
chiffres. Un exemple réel est versionné dans `rapports/` à la racine du dépôt.

---

# Le contrat

## Fraîcheur

| Donnée | Mise à jour | Retard attendu |
|---|---|---|
| Bronze (`articles_bruts/`) | Toutes les 30 min (`*/30`, heure de Paris) | Un article publié est collecté en moins de 30 min (si l'éditeur le met dans son flux). |
| Silver (`articles_propres`, `rejets`) | Après chaque collecte (capteur `qualite_apres_collecte`), que la collecte ait réussi ou non | Quelques secondes après le bronze, donc moins de 30 min après la publication. |
| Rapport du jour | À chaque passage silver | Toujours à jour du dernier passage. |

Un retard se voit dans Dagster (heure de la dernière matérialisation de chaque asset) ; un run en
échec alerte le canal de l'équipe.

## Rétention

| Donnée | Durée | Ensuite |
|---|---|---|
| Brut (`articles_bruts/`, JSON et XML) | **90 jours** | Supprimé. C'est la seule copie du flux tel que reçu : 90 jours laissent le temps de rejouer silver après une correction de règle. |
| `articles_propres`, `rejets` | **1 an** | Supprimés. Un an suffit pour comparer les thèmes d'une année sur l'autre. |
| `passages`, `etat_controles`, rapports de qualité | **1 an** | Supprimés. |
| Éditions de la gazette (TD 5 et suivants) | **1 mois** sur le volume de données | Déplacées vers un **stockage froid** (archive compressée sur le disque de sauvegarde de la machine hôte), gardées 1 an, puis supprimées. |

La purge sera une tâche planifiée de l'orchestrateur ; d'ici là, ces durées sont l'engagement.

## Ce qu'on ne stocke pas

- **Seulement ce qu'on a le droit de republier** dans `articles_propres` : titre, extrait court
  (300 caractères au plus), lien, date, source (et la rubrique). Pas le texte intégral, pas les
  images, pas les commentaires (on ne lit que les flux RSS, jamais les pages).
- **Aucune donnée personnelle en silver.** Le brut contient, dans `payload`, ce que l'éditeur met
  dans son flux, dont parfois le **nom du journaliste** (`author`) : il n'est jamais recopié en
  silver, et il disparaît avec le brut au bout de 90 jours. Aucune donnée sur les lecteurs.
- **Pas de secret dans le dépôt** : le webhook d'alerte et les mots de passe sont dans `.env`,
  ignoré par git (`.env.example` donne la liste des variables).

## Traçabilité · d'un article propre au brut et à sa source

L'`id` d'un article propre est celui de l'article brut, et `fichier_brut` dit dans quel JSON le
trouver ; `source` renvoie à la table `sources` (et à `sources.csv`), `passage_id` au passage, et
`passages.run_id` au run Dagster qui l'a produit (journaux, durée, métadonnées).

```bash
# d'un article propre : sa source, son passage, son run Dagster
docker compose exec code python -c "import sqlite3; c = sqlite3.connect('/data/gazette.db'); print(c.execute('''
  SELECT p.titre, s.nom, s.url_flux, p.fichier_brut, x.run_id
  FROM articles_propres p JOIN sources s ON s.slug = p.source AND s.flux = p.flux
  JOIN passages x ON x.id = p.passage_id LIMIT 1''').fetchone())"
# puis l'item brut, tel que reçu (id et fichier_brut ci-dessus)
docker compose exec code python -c "import json, sys; print([a for a in json.load(open('/data/' + sys.argv[1])) if a['id'] == sys.argv[2]])" <fichier_brut> <id>
```

Dans l'autre sens, d'un rejet : `regle` et `detail` disent pourquoi, `doublon_de` dit quel article
gardé il répète.

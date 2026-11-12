# Dictionnaire des données · La gazette

> Ce que la collecte (TD 3) produit, champ par champ. Tout est sous `$GAZETTE_DATA_DIR`
> (`/data` dans les conteneurs, `./data` sur la machine hôte).

```
data/
├── gazette.db                                   base SQLite : table sources (+ schema_migrations)
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

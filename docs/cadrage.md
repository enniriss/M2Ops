# Note de cadrage · La gazette

> Projet M2Ops · M2 Data Engineering & IA · Efrei · 2026-2027

## Le besoin

Chaque matin, nous voulons savoir de quoi parle la presse française, nationale comme régionale, sans avoir à la lire nous-mêmes. Le pipeline récupère les titres et les extraits de 8 journaux. Il les range, les regroupe par thèmes et publie une « gazette » du jour. Il doit **tourner seul**, et nous devons pouvoir **prouver qu'il a tourné** : quand, combien d'articles, en combien de temps, quelles sources ont échoué.

## Périmètre

| Dedans | Dehors |
|---|---|
| 4 titres de PQN et 4 de PQR (voir `sources.csv`) | Le texte intégral des articles, les images, les commentaires |
| Titre, extrait, lien, date et rubrique de chaque article | Tout scraping de pages HTML |
| Une collecte automatique toutes les 30 min, une gazette par jour, le matin | Le temps réel (collecte à la minute, streaming) |
| Le regroupement des articles du jour par thèmes | L'analyse de sentiment, le résumé d'articles |
| La surveillance : métriques, alertes, journaux | Une interface d'administration ou des comptes utilisateurs |
| L'infrastructure décrite en code | Une application mobile |

## Contraintes

- **Coût nul :** aucun service payant, seulement des outils open source ou des offres gratuites.
- **Données autorisées :** uniquement ce que les éditeurs exposent aux machines (flux RSS), dans le respect de leurs conditions d'utilisation.
- **Hébergement :** tout tourne dans des conteneurs Docker sur une machine hôte allumée en permanence (*[machine à préciser]*).
- **Horaire :** la gazette doit être prête avant 8 h, heure de Paris *(hypothèse à confirmer)*.
- **Reproductibilité :** un `docker compose up` doit suffire pour remonter tout le pipeline à l'identique, sur n'importe quelle machine.

## Stack retenue et alternatives écartées

| Brique | Retenu | Écarté, et pourquoi |
|---|---|---|
| Récupérer | Flux **RSS** officiels lus avec Python `feedparser`. API GDELT en secours si un flux tombe | Scraping HTML : interdit par la consigne. NewsAPI : offre gratuite limitée au développement |
| Faire tourner | **Dagster** (open source) : chaque couche du médaillon est un *asset*. Un *schedule* lance la collecte toutes les 30 min, heure de Paris (fréquence raisonnable pour les éditeurs, avec requêtes conditionnelles ETag / Last-Modified). Le partitionnement par jour permet de rejouer une date précise | Airflow : plus lourd, pensé en tâches plutôt qu'en données produites. Cron ou GitHub Actions : pas d'interface, pas de relance fine ni de suivi des données. Dagster+ : version hébergée payante |
| Stocker | **Architecture médaillon**. Bronze : articles bruts en fichiers JSON (`articles_bruts/`) et réponse XML complète, immuables, sans doublon (voir `donnees.md`). Silver : articles nettoyés. Gold : thèmes du jour et indicateurs. État des sources (table `sources`, migrations versionnées dans `migrations/`), silver et gold dans **SQLite**, le tout sur un volume Docker | DuckDB + Parquet : pensé pour l'analytique à grande échelle, superflu à ce volume. Delta Lake / Iceberg : trop complexes pour quelques centaines d'articles par jour |
| Regrouper | **Clustering** : embeddings multilingues (sentence-transformers) + HDBSCAN, thèmes nommés par leurs mots-clés. TF-IDF + KMeans comme référence | TF-IDF seul : trop faible sur des textes courts (titres et chapôs), et il faut fixer le nombre de thèmes à l'avance |
| Surveiller | **Outils Dagster** : interface web (historique des runs, logs, données produites), métadonnées par asset (articles ingérés par source, temps d'ingestion, sources en échec), *asset checks* (ex. : au moins un article par source), *sensor* d'alerte en cas d'échec | Prometheus + Grafana : services de plus à héberger, superflus vu ce que Dagster fournit déjà |
| Infra en code | **Docker** : un `Dockerfile` pour le code du pipeline, un `docker-compose.yml` qui décrit tous les services (interface Dagster, démon, code, PostgreSQL pour l'historique de Dagster, volumes de données) | Terraform : aucune ressource cloud à créer, donc superflu. Kubernetes : trop lourd pour une seule machine |

### Stratégie en cas d'échec

1. **Isoler** : chaque source est traitée séparément. Si un flux échoue, les autres continuent et l'échec est enregistré dans les métadonnées du run.
2. **Réessayer** : une erreur transitoire (503, timeout) est réessayée avec une attente croissante (backoff de la session HTTP, puis *RetryPolicy* Dagster : 3 tentatives). Une erreur permanente (404, flux cassé) est comptée dans la table `sources` : 3 échecs de suite mettent le flux en sommeil, signalé par l'*asset check* `sources_actives` jusqu'à `python -m gazette.reactiver <slug>`.
3. **Se replier** : si un flux reste injoignable, on interroge GDELT filtré sur le domaine du journal.
4. **Alerter** : un *sensor* Dagster envoie une notification (email ou webhook Discord) dès qu'un run échoue. Un *asset check* signale toute source à 0 article.
5. **Rejouer** : depuis l'interface Dagster, on relance un run à partir de l'étape en échec, ou on rejoue une date précise grâce aux partitions journalières. Le dédoublonnage sur la clé (source, lien) garantit qu'aucun article n'est écrit deux fois.

## Risques

| Risque | Parade |
|---|---|
| Un flux RSS change d'URL ou disparaît | *Asset check* qui alerte si une source renvoie 0 article. Repli sur GDELT. Mise à jour de `sources.csv` |
| La machine hôte est éteinte ou redémarre : aucun run, et Dagster ne peut pas prévenir lui-même | Conteneurs en `restart: unless-stopped`. Signal « run terminé » envoyé à Healthchecks.io (gratuit), qui alerte s'il ne reçoit rien avant 8 h |
| Un volume Docker est perdu (données ou historique) | Volumes nommés, et sauvegarde régulière du dossier de données (bronze + `gazette.db`) |
| Le run échoue en cours de route | Voir « Stratégie en cas d'échec » |
| Les CGU d'un éditeur interdisent la réutilisation | Lecture des CGU pour chaque source (colonne `conditions`). On ne garde que titre, extrait et lien vers l'original |
| Les thèmes sont peu lisibles | Comparaison avec la référence TF-IDF + KMeans et relecture manuelle d'un échantillon |
| Le modèle d'embeddings est lourd (quelques centaines de Mo) | Téléchargé une seule fois et mis en cache dans l'image Docker |

## Rôles dans l'équipe

| Rôle | Responsable |
|---|---|
| Sources et collecte (RSS, GDELT, `sources.csv`) | *[Nom]* |
| Orchestration et infra en code (Dagster, Docker) | *[Nom]* |
| Stockage et modèle de données (médaillon, SQLite) | *[Nom]* |
| Regroupement thématique (clustering) | *[Nom]* |
| Surveillance et qualité (métadonnées Dagster, asset checks, alertes) | *[Nom]* |
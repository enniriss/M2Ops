# Qualité du brut · ce qui n'a rien à faire dans une gazette

> Relevé sur la collecte réelle du 12 novembre 2026 (vers 9 h 40, heure de Paris) : 295 articles,
> 8 journaux. À trier au TD 4 (couche silver) : le brut, lui, garde tout.

## Dix items à écarter (ou à traiter à part)

| # | Source | Titre (extrait) | Pourquoi il n'a rien à faire dans la gazette | Comment le repérer |
|---|---|---|---|---|
| 1 | Nice-Matin | « LEGO Père Noël : -30 % sur ce set à construire en famille… » | Contenu commercial (bon plan, liens d'affiliation), pas de l'information. | Chemin d'URL `/guide-conso/` ; `%`, `€` et « économie » dans le chapô. |
| 2 | La Dépêche | « La Dépêche du Midi lance son espace jeux : à vos neurones… » | Autopromotion du journal (belote, mots croisés, sudoku), et **publiée le 16/10/2025** : vieille d'un an. | `publie_le` très antérieur à `recupere_le` ; rubrique `Médias-Communication`. |
| 3 | La Dépêche | « CARTE. Prix des carburants : tarifs, stations et bornes… les moins chères » | Page de service mise à jour en continu, publiée le 17/09/2026 et remontée dans le flux. Pas une actualité du jour. | Préfixe `CARTE.` ; `publie_le` vieux de deux mois. |
| 4 | Sud Ouest | « Le dessin du jeudi 11 novembre : le programme de Jean-Luc Mélenchon… » | Un dessin de presse : aucun texte à regrouper par thème, le chapô est générique (« Retrouvez chaque jour l'actualité croquée… »). | Chemin `/le-dessin-du-jour/` ; chapô identique d'un jour à l'autre. |
| 5 | Le Figaro | « EN DIRECT - Présidentielle 2027 : « Il ne participera pas »… » | Un direct : même URL, titre et chapô réécrits toute la journée. Un seul article retenu (le premier vu), qui ne dit pas ce que le direct est devenu. | Préfixe `EN DIRECT` ; `/live/` ou `en-direct` dans l'URL. |
| 6 | Le Monde | « EN DIRECT, guerre en Ukraine : les émissaires américains… » | Même problème que le 5, et le même sujet revient chez La Dépêche (autre direct) et Franceinfo. | URL `/live/`. |
| 7 | La Dépêche | « REPLAY. Présidentielle 2027 : « Ils n'ont rien à proposer »… » | Un direct d'hier rebaptisé « replay » : l'URL (`direct-presidentielle-2027-eric-zemmour-…`) ne correspond plus au titre. Doublon de fond avec les directs du jour. | Préfixe `REPLAY.` ; slug d'URL et titre qui divergent. |
| 8 | La Dépêche | « Loto : il découvre plusieurs semaines après le tirage qu'il a gagné le jackpot… » | Fait divers « insolite » australien sous la rubrique `Loto - EuroMillions` : contenu d'audience, pas de l'actualité française. | Rubrique `Loto - EuroMillions`. |
| 9 | Sud Ouest | « Buveurs de bière : ces quatre habitudes quotidiennes qui dégradent la santé… » | Contenu intemporel « santé » à vocation de clic, republiable n'importe quel jour. | Formule « ces N … qui » dans le titre ; rubrique `/sante/`. |
| 10 | Sud Ouest | « Les géants peuvent-ils exister, la taille est-elle limitée ? Qu'en dit la physique ? » | Article de vulgarisation intemporel, et **chapô vide** : rien à regrouper par thème à part le titre. | `resume` vide. |

Pas d'horoscope, de programme TV ni de titre vide dans ce relevé-là. Les règles du TD 4 doivent
quand même les prévoir (titre vide ou null, rubriques
`Horoscope`, `Télévision`, `Jeux`).

## Ce que le relevé dit des sources, au-delà des dix items

- **`rubrique` absente chez 6 journaux sur 8.** Seuls La Dépêche et Le Figaro remplissent
  `<category>`. Ailleurs, la rubrique se lit dans le chemin de l'URL (`/loisirs/gastronomie/`,
  `/faits-divers/`, `/guide-conso/`) : à extraire en silver.
- **Paramètres de suivi dans les liens.** Ouest-France ajoute `?utm_source=…&utm_campaign=…`,
  Franceinfo `#xtor=RSS-3-[lestitres]`. Si l'éditeur les change, le même article aurait un autre
  `lien` et passerait le dédoublonnage : à normaliser en silver (et, à terme, dans la clé).
- **URL qui ne bouge plus avec le sujet.** Franceinfo publie les résultats des midterms 2026 sous
  `/monde/usa/midterms-2018/…` : l'URL ne permet pas de dater un article, seul `publie_le` le fait.
- **Dates anciennes dans les flux.** La Dépêche renvoie 100 items, dont certains de 2025 : filtrer
  sur `publie_le` (par exemple, moins de 48 h) avant de faire la gazette du jour.
- **Vidéos.** Sud Ouest et La Dépêche préfixent `VIDÉO.` / `VIDÉOS.` : le chapô est souvent
  court, à garder mais à pondérer pour le regroupement.

---

# Les règles d'exclusion (TD 4)

> Le code : `gazette/silver/regles.py`. Les motifs et les seuils : **`gazette/silver/regles.toml`**
> (on ajuste une règle sans toucher au code). Les tests : `tests/test_silver.py`, sur la fixture
> `tests/fixtures/bronze_echantillon.json` (au moins un article par règle, plus des pièges).

Chaque article passe d'abord par la **normalisation** (`gazette/silver/normalisation.py` : encodage
réparé, HTML retiré, entités décodées, espaces, date en UTC, lien sans paramètres de suivi, extrait
de 300 caractères au plus), puis par les règles **dans l'ordre** : la première qui le reconnaît le
rejette, et son identifiant est gardé dans la table `rejets` (colonne `regle`, avec un `detail` :
le motif reconnu, le score de ressemblance…). Un article qu'aucune règle ne reconnaît va dans
`articles_propres`.

Pour les doublons (R10 à R12), le **premier publié est gardé**, les suivants sont rejetés avec
`doublon_de` = l'id de celui qu'ils répètent. On compare au passage en cours et aux articles
gardés depuis 48 h.

| Règle | Ce qu'elle écarte | Pourquoi | Comment (regles.toml) | Exemple réel |
|---|---|---|---|---|
| `R01_publi` | Publi-reportages, contenus partenaires, bons plans | Contenu payé ou commercial, pas de l'information | Lien (`/guide-conso/`, `/partenaires/`, `brand-content`…), texte (« contenu partenaire », « publireportage », « code promo », « bon plan »…), rubriques | Nice-Matin, « LEGO Père Noël : -30 % sur ce set… » (`/guide-conso/`) |
| `R02_horoscope` | Horoscope, astrologie | Pas de l'actualité, le même texte chaque jour | Lien `/horoscope`, texte « horoscope », « astrologie », rubriques | (aucun le 12/11 ; fixture : « Horoscope du jour : ce que les astres réservent au Bélier ») |
| `R03_programme_tv` | Programmes TV | Contenu de service | Titre « programme TV », « que regarder ce soir »…, rubriques `Télévision`… Pas le mot « programme » seul : « le programme de Mélenchon » est de l'actualité | (fixture : « Programme TV : que regarder ce soir sur France 2 et TF1 ? ») |
| `R04_jeux` | Jeux, concours, loterie | Divertissement ou promotion | Lien `/jeux/`, `espace-jeux`, titre « quiz », « jeu-concours », « sudoku »…, rubrique `Loto - EuroMillions` | La Dépêche, « La Dépêche du Midi lance son espace jeux… » |
| `R05_titre_vide` | Titre vide ou de moins de 15 caractères | Illisible, impossible à regrouper | Seuil `longueur_titre_min` | (fixture : « Direct ») |
| `R06_date_future` | Publié plus de 10 min *après* sa réception | Date fausse ou fuseau mal lu : fausserait le classement par jour | Seuil `avance_toleree_min` | (fixture : publié le 13/11, reçu le 12/11) |
| `R07_encodage` | Encodage cassé qu'on n'a pas su réparer (`Ã©`, `�`) | Illisible, et deux titres identiques ne se reconnaissent plus | Réparation d'abord (`Ã©` → `é`), rejet seulement s'il en reste | (fixture : « Le gouvernement pr�sente… ») |
| `R08_hors_actu` | Cartes et pages de service, replays, dessin du jour | Mis à jour en continu ou sans texte : pas une actualité du jour | Titre commençant par `CARTE.`, `REPLAY.`, « Le dessin du jeudi… », lien `/le-dessin-du-jour/` | La Dépêche, « CARTE. Prix des carburants… » ; Sud Ouest, « Le dessin du jeudi 11 novembre… » |
| `R09_perime` | Publié plus de 48 h avant sa collecte | Article ancien remonté dans le flux | Seuil `age_max_h` | Franceinfo, « RESULTATS. Triomphe des démocrates aux midterms 2026… » (5 jours) |
| `R10_doublon_exact` | Même lien (sans `utm_*`/`xtor`), ou même titre dans le même journal | Le même article compté deux fois | Lien normalisé, titre réduit (minuscules, sans accents ni ponctuation) | (fixture : même article Le Monde dans deux flux, lien avec `?utm_source=rss`) |
| `R11_depeche_reprise` | Même titre, mot pour mot, chez un autre journal | Dépêche d'agence reprise à l'identique : un fait pèserait autant que le nombre de journaux qui le reprennent | Titre réduit identique, source différente | Ouest-France, « Le Royaume-Uni nomme un « émissaire pour la sécurité climatique »… » (déjà chez Nice-Matin) |
| `R12_doublon_approx` | Même titre à quelques mots près | Le même fait raconté deux fois | Jaccard sur les mots porteurs du titre ≥ `jaccard_min` (0,7), titres d'au moins 5 mots | Libération, « Un immeuble s'effondre à New York, au moins deux morts et 17 blessés » ≈ Sud Ouest (0,80) |

### Le seuil du doublon approximatif

Mesuré sur la collecte du 12/11 (319 articles) :

| `jaccard_min` | Doublons trouvés | Faux positifs (relus à la main) |
|---:|---:|---:|
| 0,6 | 9 | 0 |
| 0,7 (retenu) | 8 | 0 |
| 0,8 | 5 | 0 |

À 0,8, on rate « Avec la hausse du smic, 164 branches… » ≈ « Hausse du smic : 164 branches
professionnelles… » ou « Gérald Passedat quitte son mythique restaurant… » ≈ « Gérald Passedat,
chef triplement étoilé, vend son mythique restaurant… » (0,73). 0,6 ne fait gagner qu'un doublon :
on reste à 0,7 tant qu'on n'a pas relu plus de jours.

## Le premier passage, sur la collecte réelle du 12/11

Passage lancé par la collecte planifiée de 11 h dans la stack Docker : 342 entrés, 321 gardés,
**21 rejetés (6 %)** : R12 ×10, R08 ×5, R04 ×2, R09 ×2, R01 ×1, R11 ×1. Taux de rejet par
source : de 2 % (Libération) à 15 % (Franceinfo, qui reprend des sujets déjà publiés ailleurs).
Aucune règle n'écarte plus de 30 % d'une source ; les 5 contrôles sont vrais. Le rapport
complet : `rapports/qualite-2026-11-12.md`.

## Les contrôles de qualité

Ils ne testent pas le code (c'est la CI, sur la fixture) : ils vérifient des **faits sur les vraies
données**, en base, **après chaque passage** (asset Dagster `silver/rapport_qualite`, lancé après
chaque collecte). Chacun est un *asset check* : verdict et chiffres dans l'interface Dagster,
historisés run après run.

| Contrôle | Le fait vérifié | Métrique publiée |
|---|---|---|
| `aucune_date_future` | Aucun article propre du jour publié après sa réception (+10 min) | `nb_dates_futures` |
| `aucun_titre_vide` | Aucun article propre du jour au titre de moins de 15 caractères | `nb_titres_vides` |
| `taux_rejet_par_source` | Aucune source (≥ 10 articles dans la journée) ne voit plus de 80 % de ses articles rejetés | `taux_rejet_<source>` |
| `aucun_encodage_casse` | Aucun titre ni extrait propre avec des séquences cassées | `nb_encodages_casses` |
| `aucun_lien_en_double` | Aucun lien présent deux fois dans `articles_propres` | `nb_liens_en_double` |

En plus des checks, chaque passage publie ses chiffres en métadonnées de `silver/articles_propres`
(`entres`, `gardes`, `rejetes`, `taux_rejet`, et `rejets_R01_publi` … `rejets_R12_doublon_approx`) :
Dagster les trace d'un run à l'autre (onglet *Plots* de l'asset).

**Assertions maison plutôt qu'un outil dédié.** Great Expectations ou Soda apportent un format de
règles et des rapports, mais aussi une dépendance lourde, sa configuration et son propre stockage
des résultats. Ici, cinq requêtes SQL sur SQLite suffisent, et Dagster fournit déjà ce que ces
outils ajouteraient : affichage, historique, lien avec le run et les données, alerte.

**L'alerte.** Quand un contrôle **devient** faux (et quand il redevient vrai), un message part sur
le canal de l'équipe : webhook Discord ou Slack, dans `GAZETTE_ALERTE_WEBHOOK` (fichier `.env`).
On n'alerte pas à chaque passage tant qu'il reste faux (sinon un message toutes les 30 min, que plus
personne ne lit) : le dernier verdict est gardé dans `etat_controles`. Un run Dagster en échec alerte
aussi (capteur `alerte_run_en_echec`). Tester le canal :
`docker compose exec code python -m gazette.alerte "test"`.

**Elle a sonné, le 12/11/2026 vers 11 h 20**, sur le salon Discord de l'équipe, sur les vraies données
de la stack Docker : on a daté un article propre de Le Monde au 25/12/2026 (comme le ferait une date
mal lue qui passerait entre les règles), puis relancé `silver/rapport_qualite`.

1. 🔴 « Gazette · contrôle aucune_date_future faux : 1 article(s) propre(s) daté(s) dans le futur »
2. Date d'origine remise, contrôle relancé : ✅ « Gazette · contrôle aucune_date_future de nouveau vrai »

## Ajustements

On lit le rapport du jour (sections « Règles trop gourmandes ? » et « Les cinq rejets les plus
fréquents »), et on note ici ce qu'on change dans `regles.toml`, et pourquoi.

| Date | Règle | Changement | Pourquoi |
|---|---|---|---|
| 12/11/2026 | `R01_publi` | Retiré le motif « en partenariat avec » | Faux positif au premier passage : Nice-Matin, « C'est quoi ce projet mené dans la vallée de la Roya… », un vrai article sur un projet « en partenariat avec l'association Remontons la Roya ». |
| 12/11/2026 | `R03_programme_tv` | Motif sur « programme TV », pas sur « programme » seul | « Le programme de Jean-Luc Mélenchon publié en librairie » est de l'actualité (gardé dans la fixture). |
| 12/11/2026 | `R12_doublon_approx` | `jaccard_min` à 0,7 et non 0,8 | Voir « Le seuil du doublon approximatif » ci-dessus. |

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

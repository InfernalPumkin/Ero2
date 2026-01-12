# Ero2 — SAÉ Systèmes d’attente (moulinette)

Ce dépôt contient une simulation et une analyse d’une **infrastructure de correction automatique** (“moulinette”) vue comme un **système d’attente**.

Le sujet comporte deux scénarios :

- **Waterfall**
- **Channels & Dams**

## Sujet: Waterfall

L’idée du modèle Waterfall :

- **Étape 1 (exécution des tests)** : une file FIFO alimentée par des arrivées Poisson, servie par **K serveurs** (exécuteurs).
- **Étape 2 (envoi du résultat)** : une file FIFO servie par **1 serveur** (envoi front).
- Capacités finies optionnelles : `ks` (capacité système étape 1) et `kf` (capacité file étape 2).
- Option “back‑up” : au lieu de perdre un résultat quand la file 2 est pleine, on peut le sauvegarder et réessayer plus tard.

### Structure des fichiers

#### Code

- `waterfall_simpy.py`
  - Script principal de simulation **SimPy** du modèle Waterfall.
  - Génère des fichiers CSV (résumé + détails par job) dans `results/`.

#### Notebooks (analyse et graphes)

- `waterfall.ipynb`
  - Notebook “principal” : exécute des expériences, charge les CSV et produit les graphes/analyses (Q1/Q2/Q3 + synthèse).
  - Si vous lancez **Run All**, vous obtenez les tests et les graphes.

#### Données / résultats

- `results/`
  - Sorties brutes des simulations (CSV). Typiquement :
    - `*_summary.csv` : une ligne par run (métriques agrégées)
    - `*_jobs.csv` : une ligne par job complété (timestamps et durées)

### Installation / prérequis

- Python 3.10+ recommandé
- Packages : `simpy`, `matplotlib`, `pandas` (et éventuellement `numpy`)

Installation rapide :

```bash
python3 -m pip install --user simpy matplotlib pandas numpy
```

### Lancer l’analyse : exécuter le notebook

1) Ouvrir `waterfall.ipynb` dans VS Code (extension Jupyter) ou JupyterLab.

2) Faire **Run All**.

Le notebook :

- lance des simulations via `waterfall_simpy.py` (commandes `python3 ...`),
- écrit des CSV dans `results/`,
- lit les CSV,
- produit les graphes (taux de refus, pages blanches/drops, temps de séjour moyen, variance, etc.).

### Lancer une simulation en ligne de commande

Exemple :

```bash
python3 waterfall_simpy.py \
    --lam 2.0 --mu-exec 1.0 --mu-send 1.5 \
    --K 3 --ks 100 --kf 50 \
    --sim-time 10000 --runs 30 \
    --backup systematic --backup-p 0.5 \
    --seed 42 --out-dir results --out-prefix results_baseline
```

Deux fichiers sont produits :

- `results/results_baseline_summary.csv`
- `results/results_baseline_jobs.csv`

### Paramètres (script `waterfall_simpy.py`)

Tous les paramètres sont des arguments CLI.

#### Intensités / capacités

- `--lam` : taux d’arrivée $\lambda$ (arrivées Poisson). Unités = “par unité de temps” de votre simulation.
- `--mu-exec` : taux de service $\mu_{exec}$ des serveurs d’exécution (étape 1). Service ~ Exp($\mu_{exec}$).
- `--mu-send` : taux de service $\mu_{send}$ du serveur d’envoi (étape 2). Service ~ Exp($\mu_{send}$).
- `--K` : nombre de serveurs en parallèle à l’étape 1.

#### Tailles de file (capacités)

- `--ks` : capacité **totale** du système d’exécution (étape 1) = (en service + en attente).
  
  - `--ks 0` signifie **infini**.
  - Si plein : la soumission est refusée (`blocked_submit`).

- `--kf` : capacité de la file d’envoi (étape 2).
  
  - `--kf 0` signifie **infini**.
  - Si plein : comportement dépend de la politique de back‑up (voir ci-dessous).

#### Politique “back‑up” (gestion de file 2 pleine)

- `--backup` :
  
  - `none` : pas de sauvegarde → perte (`dropped`) = “page blanche” côté étudiant.
  - `systematic` : sauvegarde systématique en back‑up, ré-essaie plus tard.
  - `probabilistic` : sauvegarde avec probabilité `--backup-p`, sinon perte.

- `--backup-p` : probabilité de sauvegarde si `--backup probabilistic`.

- `--backup-retry-dt` : intervalle entre deux tentatives de réinsertion depuis le back‑up vers la file 2.

#### Durée / répétitions

- `--sim-time` : horizon de simulation (arrivées générées jusqu’à ce temps).
- `--grace` : temps de “grâce” après `sim-time` pour drainer les files (laisser l’envoi terminer des jobs).
- `--runs` : nombre de runs indépendants (pour moyenne ± écart‑type).
- `--seed` : graine de base (le script utilise `seed + run`).

#### Sorties

- `--out-dir` : dossier de sortie (par défaut `results`).
- `--out-prefix` : préfixe des fichiers CSV (`<prefix>_summary.csv`, `<prefix>_jobs.csv`).

### Métriques principales (dans `*_summary.csv`)

- `arrivals` : nombre d’arrivées (tags tentés).
- `blocked_submit` / `rate_blocked_submit` : refus à la soumission (système étape 1 plein).
- `blocked_forward` / `rate_blocked_forward` : tentatives bloquées à l’entrée de la file 2 (utile surtout quand on drop).
- `dropped` / `rate_dropped` : pertes = “pages blanches” (résultat non délivré).
- `completed` : jobs effectivement envoyés.
- `mean_time_in_system`, `var_time_in_system` : moyenne/variance du temps de séjour **sur jobs complétés**.
- `utilization_exec` : estimation d’utilisation de l’étape 1.
- `rest_in_backup`, `rest_in_forward`, `not_completed_yet` : backlog restant après la période de grâce.

## Sujet: Channels and Dams

Ce projet simule un système de file d'attente à deux populations (**ING** et **PREPA**) pour analyser les performances d'une "moulinette" de correction automatique. L'objectif est d'étudier l'impact de différentes stratégies de régulation (Barrage et Token Bucket) sur l'équité et le temps de séjour des étudiants.

### Comment lancer le code

Le script s'exécute avec Python 3. Assurez-vous d'avoir installé la bibliothèque simpy.

```bash
pip install simpy
```

### Paramètres de la simulation

Le script accepte de nombreux arguments en ligne de commande pour ajuster le scénario :

#### Populations (Flux d'entrée)

`--lam-ing` : Taux d'arrivée des ING (ex: 2.0 = 2 arrivées par seconde).
`--lam-prepa` : Taux d'arrivée des PREPA (plus faible).
`--mu-ing` : Vitesse de traitement des ING (1/mu = temps de service).
`--mu-prepa` : Vitesse de traitement des PREPA (souvent plus lent).
`--mu-send` : Vitesse du serveur d'envoi final.Système et Régulation
`--K` : Nombre de serveurs de traitement en parallèle (capacité de la moulinette).
`--mode` : Stratégie de contrôle :

- `baseline` : Pas de régulation (FIFO).
- `dam` : Barrage périodique (fermé pendant t-b, ouvert pendant t-o).
- `token` : Régulation par débit à l'aide d'un mécanisme de type token bucket.

#### Dam : Régulation par Barrage

`--t-b` : Définit le temps de blocage du barrage.
`--t-o` : Définit le temps d'ouverture du barrage.

#### Token : Régulation par débit (Token Bucket).

`--token-rate` : Vitesse de génération des jetons (débit autorisé pour les ING).
`--token-cap` : Capacité maximum du réservoir de jetons (tolérance aux rafales).

#### Administration

`--sim-time` : Durée totale de la simulation.
`--runs` : Nombre de répétitions pour obtenir des moyennes stables.
`--seed` : Graine aléatoire pour la reproductibilité des tests.

#### Paramètres par défaut

Voici les valeurs configurées par défaut dans le script :

| Paramètre      | Valeur   | Description                  |
| -------------- | -------- | ---------------------------- |
| `--lam-ing`    | 2.0      | Arrivées fréquentes (ING)    |
| `--lam-prepa`  | 0.2      | Arrivées rares (PREPA)       |
| `--mu-ing`     | 1.0      | Traitement rapide            |
| `--mu-prepa`   | 0.2      | Traitement lent              |
| `--mu-send`    | 2.0      | Envoi très rapide            |
| `--K`          | 3        | 3 serveurs de calcul         |
| `--mode`       | baseline | Mode sans rejet              |
| `--t-b`        | 10.0     | Barrage fermé 10 sec         |
| `--t-o`        | 5.0      | Barrage ouvert 5 sec         |
| `--token-rate` | 1.0      | 1 jeton généré par sec       |
| `--token-cap`  | 10       | Stock max de 10 jetons       |
| `--sim-time`   | 10000.0  | Durée de simulation          |
| `--runs`       | 30       | 30 répétitions               |
| `--out-dir`    | results  | Dossier de sortie            |
| `--out-prefix` | channels | Préfixe du fichier de sortie |

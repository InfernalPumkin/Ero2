# Ero2

## Waterfall

## Channels and Dams

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
`--mode` : Stratégie de contrôle :baseline : Pas de régulation (FIFO).dam : Barrage périodique (fermé pendant t-b, ouvert pendant t-b/2).

#### Dam : Régulation par Barrage

`--t-b` : Définit le temps de blocage (en secondes) en .

#### Token : Régulation par débit (Token Bucket).

`--t-b` : Temps de blocage du barrage.
`--token-rate` : Vitesse de génération des jetons (débit autorisé pour les ING).
`--token-cap` : Capacité maximum du réservoir de jetons (tolérance aux rafales).

#### Administration

`--sim-time` : Durée totale de la simulation.
`--runs` : Nombre de répétitions pour obtenir des moyennes stables.
`--seed` : Graine aléatoire pour la reproductibilité des tests.

#### Paramètres par défaut

Voici les valeurs configurées par défaut dans le script :

| Paramètre      | Valeur   | Description               |
| -------------- | -------- | ------------------------- |
| `--lam-ing`    | 2.0      | Arrivées fréquentes (ING) |
| `--lam-prepa`  | 0.2      | Arrivées rares (PREPA)    |
| `--mu-ing`     | 1.0      | Traitement rapide         |
| `--mu-prepa`   | 0.2      | Traitement lent           |
| `--mu-send`    | 2.0      | Envoi très rapide         |
| `--K`          | 3        | 3 serveurs de calcul      |
| `--mode`       | baseline | Mode sans rejet           |
| `--t-b`        | 10.0     | Barrage de 10 sec         |
| `--token-rate` | 1.0      | 1 jeton généré par sec    |
| `--token-cap`  | 10       | Stock max de 10 jetons    |
| `--sim-time`   | 10000.0  | Durée de simulation       |
| `--runs`       | 30       | 30 répétitions            |
| `--out-dir`    | results  | Dossier de sortie         |
| `--out-prefix` | channels | Fichier de sortie         |

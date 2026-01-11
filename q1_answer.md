# Q1 — Modélisation + simulations (files infinies)

## Schéma texte du modèle
- Arrivées (λ) → File exécution (M/M/K) avec K serveurs (μ_exec) → File envoi (M/M/1) avec 1 serveur (μ_send) → Sorties
- Files supposées infinies pour cette question (pas de bornes ks/kf).

## Hypothèses et justification rapide
- Arrivées Poisson (inter-arrivées ~ Exp(λ)) : standard pour modéliser des demandes indépendantes et aléatoires.
- Services exponentiels (μ_exec pour chaque serveur d'exécution, μ_send pour le serveur d'envoi) : hypothèse classique M/M/* qui permet des résultats analytiques et une simulation simple.
- Indépendance des temps de service et des arrivées, disciplines FIFO dans chaque file.

## À retenir en 30 secondes
- Pipeline à deux étages : exécution parallèle (K serveurs, taux μ_exec) puis envoi série (1 serveur, taux μ_send).
- Les files sont infinies ici : aucun rejet dû à la capacité.
- Objectif : observer les métriques de base (arrivées, temps d’attente/système, utilisation) sous ces hypothèses M/M/K puis M/M/1 en série.

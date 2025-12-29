#!/usr/bin/env python3
"""Simulation Waterfall (SimPy)

Exemple d'utilisation (bash) :
    python3 waterfall_simpy.py --lam 2.0 --mu-exec 1.0 --mu-send 1.5 --K 4 --ks 100 --kf 50 --sim-time 10000 --runs 30 --backup systematic --backup-p 0.5 --seed 42 --out-prefix results

Ce script simule le modèle Waterfall décrit :
 - file d'exécution (K serveurs) avec capacité ks (si ks=0 => infini)
 - file de renvoi (FIFO) avec serveur unique d'envoi, capacité kf (si kf=0 => infini)
 - politique si file pleine : drop ou back-up (systematic/probabilistic)

Sorties : résumé par run et fichier CSV (par défaut: <out-prefix>_summary.csv et <out-prefix>_jobs.csv)
"""

import simpy
import random
import argparse
import statistics
import csv
import os
import math
from typing import Optional


class Job:
    def __init__(self, id, arrival_time):
        self.id = id
        self.arrival = arrival_time
        self.service_time = None
        self.send_time = None
        self.from_backup = False


def job_process(env, job, exec_resource, forward_store, params, stats, backup_storage):
    """Gère le cycle de vie d'un job (un tag soumis).

    Étapes :
    1. Incrémente le compteur d'arrivées.
    2. Applique la capacité du système d'exécution `ks` (total = en service + en attente). Si pleine, le job est refusé immédiatement.
    3. Demande un des K serveurs d'exécution (peut bloquer en attente de disponibilité).
    4. Échantillonne et exécute le service (loi exponentielle de paramètre `mu_exec`) et enregistre la durée de service.
    5. Après le service, tente d'enfiler le résultat dans la file de renvoi (capacité `kf`) :
       - Si la file de renvoi est pleine, applique la politique configurée :
         * 'none' -> incrémente les compteurs de blocage et abandonne le job (page blanche)
         * 'systematic' -> sauvegarde le job dans le back‑up pour réessai ultérieur
         * 'probabilistic' -> sauvegarde avec probabilité `backup_p`, sinon abandon
    6. Met à jour les compteurs statistiques pour enqueues, abandons et back‑ups.

    Remarques :
    - Les capacités `ks` et `kf` sont appliquées par des tests dans le processus (pas via des limites natives de SimPy).
    - `backup_storage` est une simple liste Python utilisée comme FIFO lorsque le back‑up est activé.
    """
    stats['arrivals'] += 1
    # Vérifier la capacité du système d'exécution (ks : total = en service + en attente)
    if params['ks'] and (exec_resource.count + len(exec_resource.queue) >= params['ks']):
        # Refusé à la soumission
        stats['blocked_submit'] += 1
        return

    # Demande un serveur d'exécution
    req = exec_resource.request()
    arrival_req_time = env.now
    yield req
    start_service = env.now
    # durée de service
    service_time = random.expovariate(params['mu_exec']) if params['mu_exec'] > 0 else 0.0
    job.service_time = service_time
    stats['service_times'].append(service_time)
    yield env.timeout(service_time)
    # libération du serveur
    exec_resource.release(req)

    # Tentative d'enfilement du résultat dans la file de renvoi (Store)
    # Si la file de renvoi a une capacité finie, la vérifier
    if params['kf'] and (len(forward_store.items) >= params['kf']):
        # file pleine
        if params['backup'] == 'none':
            stats['blocked_forward'] += 1
            stats['dropped'] += 1
            return
        else:
            # décider de sauvegarder en back‑up
            if params['backup'] == 'systematic':
                # stocker en back‑up
                job.from_backup = True
                backup_storage.append(job)
                stats['backups_saved'] += 1
                return
            elif params['backup'] == 'probabilistic':
                if random.random() < params['backup_p']:
                    job.from_backup = True
                    backup_storage.append(job)
                    stats['backups_saved'] += 1
                    return
                else:
                    stats['blocked_forward'] += 1
                    stats['dropped'] += 1
                    return
            else:
                # politique inconnue -> abandon
                stats['blocked_forward'] += 1
                stats['dropped'] += 1
                return
    else:
        # mettre dans la file de renvoi
        yield forward_store.put(job)
        stats['enqueued_forward'] += 1


def sender_process(env, forward_store, params, stats, results_list):
    """Consomme en continu les jobs de la file de renvoi et les "envoie" vers le front.

    Comportement :
    - Attend un job dans `forward_store` (FIFO grâce à SimPy Store).
    - Simule une durée d'envoi échantillonnée selon Exp(mu_send) et attend cette durée.
    - Enregistre la durée d'envoi, la complétion et le temps de séjour (depart - arrival).
    - Ajoute une entrée détaillée pour chaque job dans `results_list` pour export.

    Ce processus tourne indéfiniment (ou jusqu'à l'arrêt de l'environnement) ; il modélise
    le serveur unique responsable de l'envoi des résultats vers le front.
    """
    while True:
        job = yield forward_store.get()
        # durée d'envoi
        send_time = random.expovariate(params['mu_send']) if params['mu_send'] > 0 else 0.0
        yield env.timeout(send_time)
        stats['send_times'].append(send_time)
        depart = env.now
        stats['completed'] += 1
        stats['times_in_system'].append(depart - job.arrival)
        results_list.append({
            'id': job.id,
            'arrival': job.arrival,
            'service_time': job.service_time,
            'send_time': send_time,
            'depart': depart,
            'from_backup': job.from_backup,
        })


def backup_dispatcher(env, forward_store, params, backup_storage, stats):
    """Réessaye périodiquement de déplacer les sauvegardes vers la file de renvoi.

    Lorsque la file de renvoi a de la capacité (kf est None ou taille actuelle < kf), prends
    le job le plus ancien dans `backup_storage` et place-le dans `forward_store` pour envoi.

    `retry_dt` contrôle la fréquence des réessais (valeurs petites => réessais fréquents).
    Chaque réessai réussi incrémente `backup_retries`.
    """
    # Déplacer depuis le back‑up vers la file de renvoi quand il y a de la place
    # L'intervalle de sommeil contrôle la fréquence des réessais
    retry_dt = params.get('backup_retry_dt', 0.1)
    while True:
        if backup_storage and (not params['kf'] or len(forward_store.items) < params['kf']):
            job = backup_storage.pop(0)
            job.from_backup = True
            yield forward_store.put(job)
            stats['backup_retries'] += 1
        else:
            yield env.timeout(retry_dt)


def arrival_generator(env, exec_resource, forward_store, params, stats, backup_storage):
    """Génère des arrivées selon un processus de Poisson (temps entre arrivées ~ Exp(lam)).

    Pour chaque arrivée :
    - incrémente un compteur d'identifiants et instancie un `Job` avec son timestamp d'arrivée
    - lance un `job_process` pour ce job (qui gère admission, exécution et renvoi)

    Le générateur tourne tant que `env.now < params['sim_time']`.
    """
    id_counter = 0
    lam = params['lam']
    while env.now < params['sim_time']:
        # échantillonne le temps entre deux arrivées
        ia = random.expovariate(lam) if lam > 0 else math.inf
        yield env.timeout(ia)
        id_counter += 1
        j = Job(id_counter, env.now)
        env.process(job_process(env, j, exec_resource, forward_store, params, stats, backup_storage))


def run_one(params, seed: Optional[int] = None):
    """Exécute une expérience de simulation indépendante.

    Étapes réalisées :
    - Optionnellement graine le RNG pour la reproductibilité.
    - Crée un environnement SimPy et les ressources :
        * `exec_resource` : Resource de capacité K (serveurs d'exécution parallèles)
        * `forward_store` : Store utilisé comme file FIFO de renvoi
        * `backup_storage` : liste Python utilisée pour stocker les back‑ups si activés
    - Initialise les conteneurs statistiques.
    - Démarre les processus : `arrival_generator`, `sender_process` et `backup_dispatcher` (si back‑up activé).
    - Lance l'environnement pendant `sim_time`, puis accorde une `grace_time` pour drainer les éléments restants.
    - Calcule les statistiques résumées (comptes, taux, moyenne/variance des temps) et les renvoie avec les détails par job.

    Retour :
    - summary (dict) : métriques agrégées pour le run
    - results (list of dict) : informations par job complété
    """
    if seed is not None:
        random.seed(seed)

    env = simpy.Environment()

    exec_resource = simpy.Resource(env, capacity=params['K'])
    forward_store = simpy.Store(env)
    backup_storage = []  # liste utilisée comme file (FIFO)

    stats = {
        'arrivals': 0,
        'blocked_submit': 0,
        'blocked_forward': 0,
        'dropped': 0,
        'backups_saved': 0,
        'backup_retries': 0,
        'enqueued_forward': 0,
        'completed': 0,
        'service_times': [],
        'send_times': [],
        'times_in_system': [],
    }

    results = []

    # démarrer les processus
    env.process(arrival_generator(env, exec_resource, forward_store, params, stats, backup_storage))
    env.process(sender_process(env, forward_store, params, stats, results))
    if params['backup'] != 'none':
        env.process(backup_dispatcher(env, forward_store, params, backup_storage, stats))

    env.run(until=params['sim_time'])

    # après sim_time, il peut rester des jobs dans forward_store ou en back‑up. Optionnellement, on peut drainer forward_store en laissant sender tourner pendant une période de grâce.
    # accorder une période de grâce
    grace = params.get('grace_time', 100.0)
    env.run(until=env.now + grace)

    # finaliser les statistiques
    summary = {}
    summary['arrivals'] = stats['arrivals']
    summary['blocked_submit'] = stats['blocked_submit']
    summary['blocked_forward'] = stats['blocked_forward']
    summary['dropped'] = stats['dropped']
    summary['backups_saved'] = stats['backups_saved']
    summary['backup_retries'] = stats['backup_retries']
    summary['enqueued_forward'] = stats['enqueued_forward']
    summary['completed'] = stats['completed']

    summary['mean_service_time'] = statistics.mean(stats['service_times']) if stats['service_times'] else 0.0
    summary['mean_send_time'] = statistics.mean(stats['send_times']) if stats['send_times'] else 0.0
    summary['mean_time_in_system'] = statistics.mean(stats['times_in_system']) if stats['times_in_system'] else 0.0
    summary['var_time_in_system'] = statistics.pvariance(stats['times_in_system']) if len(stats['times_in_system']) > 1 else 0.0

    # taux
    summary['rate_blocked_submit'] = summary['blocked_submit'] / summary['arrivals'] if summary['arrivals'] else 0.0
    summary['rate_blocked_forward'] = summary['blocked_forward'] / (summary['arrivals'] - summary['blocked_submit']) if (summary['arrivals'] - summary['blocked_submit']) else 0.0
    summary['rate_dropped'] = summary['dropped'] / summary['arrivals'] if summary['arrivals'] else 0.0

    # estimation d'utilisation des serveurs
    total_service = sum(stats['service_times'])
    summary['utilization_exec'] = total_service / (params['K'] * params['sim_time']) if params['K'] and params['sim_time'] else 0.0

    return summary, results


def run_experiments(args):
    """Exécute une série de runs indépendants et écrit les résultats en CSV.

    - Construit le dictionnaire `params` à partir des arguments CLI.
    - Boucle pour `args.runs` runs indépendants, chacun avec une graine différente (seed+run).
    - Collecte les métriques par run et les détails par job.
    - Écrit deux fichiers CSV dans `args.out_dir` :
        * `<out_prefix>_summary.csv` : une ligne par run (métriques agrégées)
        * `<out_prefix>_jobs.csv` : une ligne par job complété sur tous les runs
    """
    params = {
        'lam': args.lam,
        'mu_exec': args.mu_exec,
        'mu_send': args.mu_send,
        'K': args.K,
        'ks': args.ks if args.ks > 0 else None,
        'kf': args.kf if args.kf > 0 else None,
        'backup': args.backup,
        'backup_p': args.backup_p,
        'sim_time': args.sim_time,
        'grace_time': args.grace,
        'backup_retry_dt': args.backup_retry_dt,
    }

    os.makedirs(args.out_dir, exist_ok=True)
    summary_rows = []
    jobs_rows = []

    for run in range(1, args.runs + 1):
        seed = args.seed + run if args.seed is not None else None
        s, jobs = run_one(params, seed=seed)
        s['run'] = run
        s['seed'] = seed
        summary_rows.append(s)
        for j in jobs:
            j['run'] = run
            jobs_rows.append(j)
        print(f"Run {run}/{args.runs} : arrivées={s['arrivals']}, complétés={s['completed']}, abandonnés={s['dropped']}, sauvegardes={s['backups_saved']}, meanW={s['mean_time_in_system']:.4f}")

    # écrire les fichiers CSV
    summary_csv = os.path.join(args.out_dir, f"{args.out_prefix}_summary.csv")
    jobs_csv = os.path.join(args.out_dir, f"{args.out_prefix}_jobs.csv")

    with open(summary_csv, 'w', newline='') as f:
        fieldnames = list(summary_rows[0].keys()) if summary_rows else []
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in summary_rows:
            writer.writerow(row)

    if jobs_rows:
        with open(jobs_csv, 'w', newline='') as f:
            fieldnames = list(jobs_rows[0].keys())
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in jobs_rows:
                writer.writerow(row)

    print(f"Résultats enregistrés : {summary_csv}, {jobs_csv}")


def parse_args():
    """Parse les arguments en ligne de commande et retourne le namespace.

    L'interface CLI expose des paramètres pour contrôler les taux d'arrivée/service, les capacités,
    la politique de back‑up, la durée de simulation, le nombre de runs et les chemins de sortie.
    """
    p = argparse.ArgumentParser(description='Simulation Waterfall (SimPy)')
    p.add_argument('--lam', type=float, default=1.0, help='taux d\'arrivée lambda (par unité de temps)')
    p.add_argument('--mu-exec', type=float, default=1.0, help='taux de service des serveurs d\'exécution (mu)')
    p.add_argument('--mu-send', type=float, default=1.0, help='taux de service du serveur d\'envoi')
    p.add_argument('--K', type=int, default=2, help='nombre de serveurs d\'exécution parallèles')
    p.add_argument('--ks', type=int, default=0, help='capacité du système d\'exécution (0 = infini)')
    p.add_argument('--kf', type=int, default=0, help='capacité de la file de renvoi (0 = infini)')
    p.add_argument('--sim-time', type=float, default=1000.0, help='durée de simulation')
    p.add_argument('--grace', type=float, default=100.0, help='temps de grâce après sim-time pour drainer les files')
    p.add_argument('--runs', type=int, default=10, help='nombre de runs indépendants')
    p.add_argument('--seed', type=int, default=42, help='graine de base (seed+run utilisée si fournie)')
    p.add_argument('--backup', choices=['none', 'systematic', 'probabilistic'], default='none', help='politique de back‑up lorsque la file de renvoi est pleine')
    p.add_argument('--backup-p', type=float, default=0.5, help='probabilité de sauvegarde si politique probabiliste')
    p.add_argument('--backup-retry-dt', type=float, default=0.1, help='intervalle entre réessais pour le back‑up')
    p.add_argument('--out-dir', type=str, default='results', help='répertoire de sortie')
    p.add_argument('--out-prefix', type=str, default='waterfall', help='préfixe de nom de fichier de sortie')
    return p.parse_args()


def main():
    args = parse_args()
    run_experiments(args)


if __name__ == '__main__':
    main()

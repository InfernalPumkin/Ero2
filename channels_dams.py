#!/usr/bin/env python3
"""
Simulation Channels and Dams (SimPy) pour ERO2.
Basé sur waterfall_simpy.py mais adapté pour 2 populations + Dam + Alternative.
"""

import simpy
import random
import argparse
import statistics
import csv
import os
from typing import Optional

class Job:
    def __init__(self, id, arrival_time, pop_type):
        self.id = id
        self.arrival = arrival_time
        self.pop_type = pop_type  # 'ING' ou 'PREPA'
        self.service_time = 0.0
        self.send_time = 0.0

def dam_controller(env, params):
    """
    Contrôle l'état du 'dam' (barrage) pour la population ING.
    Cycle : Fermé pendant t_b, Ouvert pendant t_b / 2.
    """
    while True:
        # Phase de blocage (Fermé)
        params['dam_open'] = False
        yield env.timeout(params['t_b'])
        
        # Phase d'ouverture
        params['dam_open'] = True
        yield env.timeout(params['t_o'])

def token_bucket_refill(env, token_container, params):
    """
    Alternative Option C : Remplit le seau de jetons périodiquement.
    """
    rate = params['token_rate']  # jetons par unité de temps
    cap = params['token_cap']    # capacité max
    while True:
        yield env.timeout(1.0) # Ajout chaque unité de temps (ou ajuster selon besoins)
        current = token_container.level
        to_add = rate
        if current + to_add > cap:
            to_add = cap - current
        if to_add > 0:
            yield token_container.put(to_add)

def job_process(env, job, exec_resource, forward_store, params, stats, token_container=None):
    """
    Gère le cycle de vie d'un job.
    """
    # 1. Admission / Régulation
    if job.pop_type == 'ING':
        # Si Mode DAM activé
        if params['mode'] == 'dam':
            if not params['dam_open']:
                stats['ing_rejected_dam'] += 1
                return # Rejeté par le barrage
        
        # Si Mode TOKEN activé (Alternative)
        elif params['mode'] == 'token' and token_container is not None:
            # Tente de prendre 1 jeton
            if token_container.level < 1:
                stats['ing_rejected_token'] += 1
                return # Pas de jeton disponible
            else:
                yield token_container.get(1)

    stats['arrivals'][job.pop_type] += 1

    # 2. Exécution (Moulinette)
    req = exec_resource.request()
    yield req
    
    # Temps de service dépendant de la population
    if job.pop_type == 'ING':
        mu = params['mu_ing']
    else:
        mu = params['mu_prepa']
        
    service_time = random.expovariate(mu) if mu > 0 else 0.0
    job.service_time = service_time
    yield env.timeout(service_time)
    exec_resource.release(req)

    # 3. Envoi vers le front (identique Waterfall)
    # On suppose ici file infinie pour simplifier l'analyse "Channels", 
    # ou on garde la logique kf si nécessaire. Ici : simple put.
    yield forward_store.put(job)

def sender_process(env, forward_store, params, stats, results_list):
    """
    Traite la file de renvoi.
    """
    while True:
        job = yield forward_store.get()
        send_time = random.expovariate(params['mu_send']) if params['mu_send'] > 0 else 0.0
        yield env.timeout(send_time)
        
        depart = env.now
        total_time = depart - job.arrival
        
        stats['completed'][job.pop_type] += 1
        stats['times'][job.pop_type].append(total_time)
        
        results_list.append({
            'id': job.id,
            'type': job.pop_type,
            'arrival': job.arrival,
            'service_time': job.service_time,
            'depart': depart,
            'time_in_system': total_time
        })

def arrival_generator(env, pop_type, exec_resource, forward_store, params, stats, token_container):
    """
    Génère des arrivées pour une population donnée (ING ou PREPA).
    """
    lam = params[f'lam_{pop_type.lower()}']
    while env.now < params['sim_time']:
        ia = random.expovariate(lam) if lam > 0 else 1e9
        yield env.timeout(ia)
        
        j = Job(stats['id_counter'], env.now, pop_type)
        stats['id_counter'] += 1
        env.process(job_process(env, j, exec_resource, forward_store, params, stats, token_container))

def run_one(params, seed=None):
    if seed is not None:
        random.seed(seed)

    env = simpy.Environment()
    exec_resource = simpy.Resource(env, capacity=params['K'])
    forward_store = simpy.Store(env)
    
    # Pour l'option Token Bucket
    token_container = None
    if params['mode'] == 'token':
        token_container = simpy.Container(env, capacity=params['token_cap'], init=params['token_cap'])
        env.process(token_bucket_refill(env, token_container, params))

    # État partagé pour le Dam
    if params['mode'] == 'dam':
        params['dam_open'] = True # Départ ouvert
        env.process(dam_controller(env, params))
    else:
        params['dam_open'] = True # Toujours ouvert si pas en mode dam

    stats = {
        'id_counter': 0,
        'arrivals': {'ING': 0, 'PREPA': 0},
        'completed': {'ING': 0, 'PREPA': 0},
        'ing_rejected_dam': 0,
        'ing_rejected_token': 0,
        'times': {'ING': [], 'PREPA': []}
    }
    results = []

    # Lancement des processus
    env.process(arrival_generator(env, 'ING', exec_resource, forward_store, params, stats, token_container))
    env.process(arrival_generator(env, 'PREPA', exec_resource, forward_store, params, stats, token_container))
    env.process(sender_process(env, forward_store, params, stats, results))

    env.run(until=params['sim_time'])

    # Calculs finaux (Moyennes et Variances)
    summary = {}
    for pop in ['ING', 'PREPA']:
        times = stats['times'][pop]
        summary[f'count_{pop}'] = stats['completed'][pop]
        summary[f'mean_time_{pop}'] = statistics.mean(times) if times else 0.0
        summary[f'var_time_{pop}'] = statistics.pvariance(times) if len(times) > 1 else 0.0
        # p95
        if len(times) > 1:
            times.sort()
            idx = int(0.95 * len(times))
            summary[f'p95_time_{pop}'] = times[idx]
        else:
            summary[f'p95_time_{pop}'] = 0.0

    summary['ing_rejected'] = stats['ing_rejected_dam'] + stats['ing_rejected_token']
    return summary, results

def run_experiments(args):
    # Paramètres de simulation
    params = {
        'lam_ing': args.lam_ing,
        'lam_prepa': args.lam_prepa,
        'mu_ing': args.mu_ing,      # Service rate ING (rapide => grand mu)
        'mu_prepa': args.mu_prepa,  # Service rate PREPA (lent => petit mu)
        'mu_send': args.mu_send,
        'K': args.K,
        'sim_time': args.sim_time,
        'mode': args.mode,          # 'baseline', 'dam', 'token'
        't_b': args.t_b,            # Pour mode Dam (bloqué)
        't_o': args.t_o,            # Pour mode Dam (ouvert)
        'token_rate': args.token_rate, # Pour mode Token
        'token_cap': args.token_cap
    }

    os.makedirs(args.out_dir, exist_ok=True)
    summary_rows = []
    
    print(f"--- Démarrage Simulation (Mode: {args.mode}) ---")

    for run in range(1, args.runs + 1):
        seed = args.seed + run
        s, _ = run_one(params, seed=seed)
        s['run'] = run
        summary_rows.append(s)

    # Sauvegarde CSV
    fname = f"{args.out_prefix}_{args.mode}_summary.csv"
    out_path = os.path.join(args.out_dir, fname)
    
    if summary_rows:
        with open(out_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=summary_rows[0].keys())
            writer.writeheader()
            writer.writerows(summary_rows)
    
    print(f"Terminé. Résultats dans {out_path}")

if __name__ == '__main__':
    p = argparse.ArgumentParser()
    # Taux d'arrivée
    p.add_argument('--lam-ing', type=float, default=2.0, help="Lambda ING")
    p.add_argument('--lam-prepa', type=float, default=0.2, help="Lambda PREPA")
    # Taux de service (Attention: mu = 1/temps_moyen)
    p.add_argument('--mu-ing', type=float, default=1.0, help="Mu ING (rapide)")
    p.add_argument('--mu-prepa', type=float, default=0.2, help="Mu PREPA (lent)")
    p.add_argument('--mu-send', type=float, default=2.0)
    p.add_argument('--K', type=int, default=3)

    # Paramètres Dam / Token
    p.add_argument('--mode', choices=['baseline', 'dam', 'token'], default='baseline')
    p.add_argument('--t-b', type=float, default=10.0, help="Temps de blocage Dam")
    p.add_argument('--t-o', type=float, default=5.0, help="Temps d'ouverture du Dam (en sec)")
    p.add_argument('--token-rate', type=float, default=1.0, help="Jetons par sec")
    p.add_argument('--token-cap', type=int, default=10, help="Max jetons")

    p.add_argument('--sim-time', type=float, default=10000.0)
    p.add_argument('--runs', type=int, default=30)
    p.add_argument('--seed', type=int, default=42)
    p.add_argument('--out-dir', type=str, default='results')
    p.add_argument('--out-prefix', type=str, default='channels')

    args = p.parse_args()
    run_experiments(args)

"""
1. Scénario Baseline (Sans Dam) Simulez le problème : ING sature le système, PREPA attend trop.

Bash

python3 channels_dams.py --mode baseline --lam-ing 2.0 --mu-ing 2.0 --lam-prepa 0.2 --mu-prepa 0.1 --out-prefix step1
À vérifier dans le notebook : Regardez mean_time_PREPA vs mean_time_ING. PREPA devrait être très élevé.

2. Scénario avec Dam Activez le barrage pour bloquer les ING périodiquement.

Bash

python3 channels_dams.py --mode dam --t-b 10.0 --lam-ing 2.0 --mu-ing 2.0 --lam-prepa 0.2 --mu-prepa 0.1 --out-prefix step2
À vérifier dans le notebook : mean_time_PREPA devrait baisser (car le système respire), mais ing_rejected va augmenter (ou le temps d'attente ING si vous changez la logique de rejet en attente). Notez aussi la variance pour ING.

3. Scénario Alternative (Token Bucket) L'option C "plus réaliste".

Bash

python3 channels_dams.py --mode token --token-rate 1.5 --token-cap 20 --lam-ing 2.0 --mu-ing 2.0 --lam-prepa 0.2 --mu-prepa 0.1 --out-prefix step3
À vérifier dans le notebook : Comparez la variance (ou p95) avec le Dam. Le Token Bucket lisse généralement mieux le trafic que le "tout ou rien" du Dam.
"""

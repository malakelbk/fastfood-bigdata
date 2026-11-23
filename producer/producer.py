"""
producer.py - Générateur de commandes FastFood.ma (projet Big Data 2).

Deux modes :

  1) Fichier (historique pour les Parties 2 et 3) :
       python producer.py --mode file --n 50000
     -> écrit orders_history.jsonl (un événement JSON par ligne)

  2) Kafka (flux temps réel pour les Parties 4 et 5) :
       python producer.py --mode kafka --rate 20
     -> publie environ 20 événements par seconde dans le topic "orders"
       (clé du message = order_id, valeur = événement JSON en UTF-8)
       Ajoutez --dry-run pour afficher les événements sans Kafka.
"""

import argparse
import bisect
import heapq
import json
import os
import random
import signal
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

# --------------------------------------------------------------------------
# Paramètres de la simulation
# --------------------------------------------------------------------------
CITY_SHARE = {"Casablanca": 0.45, "Rabat": 0.22, "Marrakech": 0.18, "Tanger": 0.15}
CITY_CODE = {"Casablanca": "CAS", "Rabat": "RAB", "Marrakech": "MAR", "Tanger": "TAN"}
N_COURIERS = {"Casablanca": 50, "Rabat": 28, "Marrakech": 24, "Tanger": 18}
N_CUSTOMERS = 12000          # C-00001 ... C-12000

P_FAVORITE = 0.70            # commande passée dans l'un des restaurants habituels du client
P_UNKNOWN_RESTAURANT = 0.002 # restaurant absent du référentiel (arête orpheline)
UNKNOWN_RANGE = {"Casablanca": (901, 940), "Rabat": (941, 960), "Marrakech": (961, 980), "Tanger": (981, 999)}
P_CANCEL = 0.05              # annulation de base
CANCEL_CITY = {"Casablanca": 0.01, "Rabat": 0.0, "Marrakech": 0.0, "Tanger": 0.03}
CANCEL_PEAK = 0.03           # annulations supplémentaires aux heures de pointe
P_CANCEL_AFTER_PICKUP = 0.15 # part des annulations qui surviennent après le retrait
P_LATE = 0.05                # événement en retard
MAX_LATE_S = 15 * 60         # retard maximal : 15 minutes
P_DUP = 0.02                 # doublon

# prix unitaire médian (MAD) et temps de préparation (min, max) en minutes
CATEGORY = {"marocain": (55, (15, 35)), "pizza": (60, (12, 25)), "tacos": (45, (8, 18)),
            "burger": (55, (8, 18)), "poulet": (50, (10, 22)), "sandwich": (30, (5, 14)),
            "sushi": (85, (15, 32)), "asiatique": (65, (12, 26)), "healthy": (60, (8, 16)),
            "patisserie": (25, (5, 12))}
ITEMS = [1, 2, 3, 4, 5, 6]
ITEMS_W = [30, 30, 20, 10, 5, 5]

# Volume selon l'heure de event_time (UTC ; heure locale = UTC + 1) et le jour (lundi = 0)
HOURLY = [0.35, 0.20, 0.10, 0.06, 0.05, 0.05, 0.08, 0.15, 0.25, 0.35, 0.60, 1.60,
          2.40, 1.80, 0.90, 0.60, 0.60, 0.90, 1.70, 2.50, 2.60, 1.80, 1.10, 0.60]
DAILY = [1.00, 1.00, 1.00, 1.05, 1.15, 1.25, 1.20]

# Disponibilité des livreurs : blocs de travail entrecoupés de pauses
WORK_MIN = (25, 110)
SHORT_PAUSE_MIN = (8, 35)
LONG_OFF_MIN = (90, 360)
P_SHORT_PAUSE = 0.75

# Taux des comportements particuliers (part des nouvelles commandes)
P_SCN_A = 0.0050
P_SCN_B = 0.0045


def iso(ts):
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Picker:
    """Tirage pondéré rapide (poids cumulés précalculés)."""

    def __init__(self, items, weights):
        self.items = list(items)
        acc, self.cum = 0.0, []
        for w in weights:
            acc += w
            self.cum.append(acc)

    def pick(self, rng):
        return rng.choices(self.items, cum_weights=self.cum)[0]


class Schedule:
    """Planning d'un livreur : blocs de travail générés à la demande."""

    def __init__(self, rng, start):
        self.rng = rng
        self.starts, self.ends = [], []
        self.t = start - rng.uniform(0, LONG_OFF_MIN[1] * 60)

    def _extend(self, until):
        rng = self.rng
        while self.t <= until:
            s = self.t
            e = s + rng.uniform(*WORK_MIN) * 60
            self.starts.append(s)
            self.ends.append(e)
            pause = SHORT_PAUSE_MIN if rng.random() < P_SHORT_PAUSE else LONG_OFF_MIN
            self.t = e + rng.uniform(*pause) * 60

    def on_duty(self, t):
        self._extend(t)
        i = bisect.bisect_right(self.starts, t) - 1
        return i >= 0 and t < self.ends[i]


# --------------------------------------------------------------------------
# Population : clients, restaurants, livreurs
# --------------------------------------------------------------------------
class Population:
    def __init__(self, restaurants, seed, start_ts):
        rng = random.Random(seed)
        srng = random.Random(f"fastfood/{seed}")
        self.cities = list(CITY_SHARE)
        city_pick = Picker(self.cities, [CITY_SHARE[c] for c in self.cities])

        # restaurants
        self.rest = {r["restaurant_id"]: r for r in restaurants}
        self.rest_pop = {rid: rng.lognormvariate(0.0, 0.7) for rid in self.rest}
        self.rest_by_city = {c: [rid for rid, r in self.rest.items() if r["city"] == c] for c in self.cities}
        self.rest_pick = {c: Picker(rs, [self.rest_pop[r] for r in rs]) for c, rs in self.rest_by_city.items()}

        # clients
        self.customers = [f"C-{i:05d}" for i in range(1, N_CUSTOMERS + 1)]
        self.city = {u: city_pick.pick(rng) for u in self.customers}
        self.activity = {u: rng.lognormvariate(0.0, 0.9) for u in self.customers}
        self.dist = {u: min(12.0, max(0.4, rng.lognormvariate(1.05, 0.45))) for u in self.customers}
        self.favorites = {}
        for u in self.customers:
            fav = []
            for _ in range(rng.randint(3, 6)):
                r = self.rest_pick[self.city[u]].pick(rng)
                if r not in fav:
                    fav.append(r)
            self.favorites[u] = fav

        # livreurs
        self.couriers = {c: [f"L-{CITY_CODE[c]}-{i:02d}" for i in range(1, N_COURIERS[c] + 1)] for c in self.cities}
        all_couriers = [l for c in self.cities for l in self.couriers[c]]
        self.zeal = {l: rng.lognormvariate(0.0, 0.3) for l in all_couriers}
        self.sched = {l: Schedule(random.Random(f"{seed}/{l}"), start_ts) for l in all_couriers}

        # comportements particuliers (identifiants tirés depuis la graine)
        ca = srng.choice(self.cities)
        quiet = sorted(self.rest_by_city[ca], key=lambda r: self.rest_pop[r])
        self.a_rest = srng.choice(quiet[len(quiet) // 4: len(quiet) // 2])
        self.a_courier = srng.choice(self.couriers[ca])
        self.a_customers = srng.sample([u for u in self.customers if self.city[u] == ca], 5)
        cb, yb = srng.sample(self.cities, 2)
        self.b_courier = srng.choice(self.couriers[cb])
        self.b_city = yb
        cm, ym = srng.sample([c for c in self.cities if c != cb], 2)
        self.m_courier = srng.choice([l for l in self.couriers[cm] if l != self.a_courier])
        self.m_city = (cm, ym)
        self.m_switch = start_ts + 3.5 * 86400
        cf = srng.choice(self.cities)
        self.f_customer = srng.choice([u for u in self.customers if self.city[u] == cf
                                       and u not in self.a_customers])
        self.f_rests = srng.sample(self.rest_by_city[cf], 3)
        for u in self.a_customers + [self.f_customer]:
            self.activity[u] = 0.0

        self.orderers = Picker(self.customers, [self.activity[u] for u in self.customers])
        self.by_city = {c: [u for u in self.customers if self.city[u] == c and self.activity[u] > 0]
                        for c in self.cities}

    def pool(self, city, t):
        """Livreurs rattachés à une ville à l'instant t."""
        ls = [l for l in self.couriers[city] if l != self.m_courier]
        cm, ym = self.m_city
        if (city == cm and t < self.m_switch) or (city == ym and t >= self.m_switch + 20 * 3600):
            ls.append(self.m_courier)
        return ls

    def assign(self, rng, city, t):
        ls = self.pool(city, t)
        on = [l for l in ls if self.sched[l].on_duty(t)] or ls
        return rng.choices(on, [self.zeal[l] for l in on])[0]


# --------------------------------------------------------------------------
# Simulateur (événements discrets)
# --------------------------------------------------------------------------
class Simulator:
    def __init__(self, pop, rng, start_ts, orders_per_s, use_profile, order_counter=1):
        self.pop = pop
        self.rng = rng
        self.t = float(start_ts)
        self.rate = orders_per_s
        self.use_profile = use_profile
        self.lam_max = orders_per_s * (max(HOURLY) * max(DAILY) if use_profile else 1.0)
        self.order_no = order_counter
        self.heap = []
        self.seq = 0
        self.stats = Counter()
        self._push(self.t + rng.expovariate(self.lam_max), "NEW", None)
        self._push(self._next_corp(self.t), "CORP", None)

    def _push(self, ts, kind, payload):
        self.seq += 1
        heapq.heappush(self.heap, (ts, self.seq, kind, payload))

    @staticmethod
    def _load(t):
        d = datetime.fromtimestamp(t, tz=timezone.utc)
        return HOURLY[d.hour] * DAILY[d.weekday()]

    # ---- choix d'une commande -------------------------------------------
    def _new_order(self, t):
        rng, pop = self.rng, self.pop
        r = rng.random()
        if r < P_SCN_A:
            self._order_a(t)
            return
        r -= P_SCN_A
        if r < P_SCN_B:
            self._order_b(t)
            return
        self._order_normal(t, pop.orderers.pick(rng))

    def _basket(self, rest_id, n_items=None):
        rng, pop = self.rng, self.pop
        cat = pop.rest[rest_id]["category"] if rest_id in pop.rest else rng.choice(list(CATEGORY))
        price, prep = CATEGORY[cat]
        items = n_items or rng.choices(ITEMS, ITEMS_W)[0]
        return cat, items, sum(price * rng.lognormvariate(0.0, 0.3) for _ in range(items)), prep

    def _order_normal(self, t, cust, courier=None):
        rng, pop = self.rng, self.pop
        city = pop.city[cust]
        if rng.random() < P_UNKNOWN_RESTAURANT:
            lo, hi = UNKNOWN_RANGE[city]
            rest = f"R-{rng.randint(lo, hi)}"
        elif rng.random() < P_FAVORITE:
            rest = rng.choice(pop.favorites[cust])
        else:
            rest = pop.rest_pick[city].pick(rng)
        dist = pop.dist[cust] * rng.uniform(0.6, 1.4)
        cat, items, food, prep = self._basket(rest)
        self._lifecycle(t, cust, rest, city, food, items, dist, prep, courier=courier)

    def _order_a(self, t):
        rng, pop = self.rng, self.pop
        cust = rng.choice(pop.a_customers)
        _, items, food, _ = self._basket(pop.a_rest, n_items=1)
        self._lifecycle(t, cust, pop.a_rest, pop.rest[pop.a_rest]["city"], food * 0.8, items,
                        rng.uniform(0.3, 1.4), (4, 9), courier=pop.a_courier, p_cancel=0.0)

    def _order_b(self, t):
        rng, pop = self.rng, self.pop
        cust = rng.choice(pop.by_city[pop.b_city])
        # injecté seulement quand le titulaire du compte est en service dans sa propre ville
        if pop.sched[pop.b_courier].on_duty(t + 900):
            self._order_normal(t, cust, courier=pop.b_courier)
        else:
            self._order_normal(t, cust)

    def _next_corp(self, t):
        d = datetime.fromtimestamp(t, tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        while True:
            cand = d + timedelta(hours=10, minutes=45)
            if cand.weekday() < 5 and cand.timestamp() > t:
                return cand.timestamp() + self.rng.uniform(0, 40 * 60)
            d += timedelta(days=1)

    def _corp(self, t):
        rng, pop = self.rng, self.pop
        u = pop.f_customer
        for _ in range(rng.choice([4, 5, 5, 6])):
            t += rng.uniform(30, 360)
            rest = rng.choices(pop.f_rests, [0.6, 0.25, 0.15])[0]
            _, items, food, prep = self._basket(rest, n_items=rng.randint(4, 10))
            self._lifecycle(t, u, rest, pop.city[u], food, items, rng.uniform(2.0, 2.6), prep, p_cancel=0.01)
        self._push(self._next_corp(t + 3600), "CORP", None)

    # ---- cycle de vie d'une commande ------------------------------------
    def _lifecycle(self, t, cust, rest, city, food, items, dist, prep, courier=None, p_cancel=None):
        rng, pop = self.rng, self.pop
        order_id = f"ORD-{self.order_no:07d}"
        self.order_no += 1
        self.stats["orders"] += 1
        dist = round(min(15.0, max(0.3, dist)), 1)
        fee = min(25.0, 9.0 + 2.0 * max(0.0, dist - 2.0))
        amount = round((food + fee) * 2) / 2
        base = {"order_id": order_id, "restaurant_id": rest, "customer_id": cust, "city": city}
        load = self._load(t)
        peak = load > 1.5

        def ev(status, ts, cour):
            e = dict(base)
            e.update({"status": status, "amount_mad": float(amount), "items_count": items,
                      "distance_km": dist, "courier_id": cour, "event_time": iso(ts)})
            self._emit(ts, e)

        ev("CREATED", t, None)
        if p_cancel is None:
            p_cancel = P_CANCEL + CANCEL_CITY.get(city, 0.0) + (CANCEL_PEAK if peak else 0.0)
        cancel = rng.random() < p_cancel
        if cancel and rng.random() >= P_CANCEL_AFTER_PICKUP:
            ev("CANCELLED", t + rng.uniform(2, 25) * 60, None)
            return
        t1 = t + rng.uniform(*prep) * 60 * (1.0 + 0.25 * max(0.0, load - 1.0))
        cour = courier or pop.assign(rng, city, t1)
        ev("PICKED_UP", t1, cour)
        if cancel:
            ev("CANCELLED", t1 + rng.uniform(3, 15) * 60, cour)
            return
        ev("DELIVERED", t1 + (dist * rng.uniform(2.2, 3.8) + rng.uniform(2, 6)) * 60, cour)

    def _emit(self, ts, event):
        rng = self.rng
        arrival = ts
        if rng.random() < P_LATE:
            arrival = ts + rng.uniform(30, MAX_LATE_S)
            self.stats["late"] += 1
        self._push(arrival, "EMIT", event)
        if rng.random() < P_DUP:
            self._push(arrival + rng.uniform(1, 90), "EMIT", event)
            self.stats["dup"] += 1

    # ---- boucle principale ----------------------------------------------
    def events(self):
        """Génère indéfiniment des couples (instant_d_arrivée, événement), dans l'ordre d'arrivée."""
        rng = self.rng
        while True:
            ts, _, kind, payload = heapq.heappop(self.heap)
            self.t = ts
            if kind == "EMIT":
                yield ts, payload
            elif kind == "NEW":
                accept = True
                if self.use_profile:
                    accept = rng.random() < self.rate * self._load(ts) / self.lam_max
                if accept:
                    self._new_order(ts)
                self._push(ts + rng.expovariate(self.lam_max), "NEW", None)
            elif kind == "CORP":
                self._corp(ts)


# Nombre moyen d'événements émis par commande (sert à caler le débit)
EVENTS_PER_ORDER = 2.97


def load_restaurants(path):
    with open(path, encoding="utf-8") as f:
        txt = f.read().strip()
    if txt.startswith("["):
        return json.loads(txt)
    return [json.loads(line) for line in txt.splitlines() if line.strip()]


# --------------------------------------------------------------------------
# Modes
# --------------------------------------------------------------------------
def run_file(args, restaurants):
    start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()
    pop = Population(restaurants, args.seed, start)
    rng = random.Random(args.seed + 1)
    avg_profile = sum(HOURLY) / 24 * sum(DAILY) / 7
    rate = args.n / EVENTS_PER_ORDER / (args.days * 86400 * avg_profile)
    sim = Simulator(pop, rng, start, rate, use_profile=True)
    status = Counter()
    first = last = None
    with open(args.output, "w", encoding="utf-8") as f:
        for i, (arrival, ev) in enumerate(sim.events()):
            if i >= args.n:
                break
            f.write(json.dumps(ev, ensure_ascii=False) + "\n")
            status[ev["status"]] += 1
            first = first or ev["event_time"]
            last = ev["event_time"]
    print(f"{args.n} événements écrits dans {args.output}", file=sys.stderr)
    print(f"  période couverte : {first} -> {last}", file=sys.stderr)
    print(f"  statuts : {dict(status)}", file=sys.stderr)
    print(f"  commandes : {sim.stats['orders']}  (en retard : {sim.stats['late']}, doublons : {sim.stats['dup']})",
          file=sys.stderr)


class KafkaSink:
    def __init__(self, bootstrap, topic):
        self.topic = topic
        try:
            from kafka import KafkaProducer  # kafka-python
            self.kind = "kafka-python"
            try:
                self.p = KafkaProducer(bootstrap_servers=bootstrap.split(","), acks="all", linger_ms=20)
            except Exception as exc:  # NoBrokersAvailable, etc.
                sys.exit(f"Erreur : impossible de joindre Kafka sur {bootstrap} ({exc}). "
                         "Vérifiez --bootstrap (localhost:9092 depuis la machine hôte, "
                         "kafka:9092 depuis un conteneur du même réseau Docker).")
        except ImportError:
            try:
                from confluent_kafka import Producer  # confluent-kafka
            except ImportError:
                sys.exit("Erreur : installez kafka-python (pip install kafka-python) "
                         "ou confluent-kafka, ou utilisez --dry-run.")
            self.kind = "confluent-kafka"
            self.p = Producer({"bootstrap.servers": bootstrap, "acks": "all", "linger.ms": 20})

    def send(self, key, value):
        if self.kind == "kafka-python":
            self.p.send(self.topic, key=key.encode("utf-8"), value=value.encode("utf-8"))
        else:
            while True:
                try:
                    self.p.produce(self.topic, key=key.encode("utf-8"), value=value.encode("utf-8"))
                    break
                except BufferError:
                    self.p.poll(0.5)
            self.p.poll(0)

    def close(self):
        self.p.flush()
        if self.kind == "kafka-python":
            self.p.close()


class StdoutSink:
    def send(self, key, value):
        sys.stdout.write(value + "\n")
        sys.stdout.flush()

    def close(self):
        pass


def run_kafka(args, restaurants):
    sink = StdoutSink() if args.dry_run else KafkaSink(args.bootstrap, args.topic)
    t0_wall = time.time()
    t0_sim = t0_wall
    pop = Population(restaurants, args.seed, t0_sim - 7 * 86400)
    rng = random.Random(args.seed + 1)
    rate = args.rate / EVENTS_PER_ORDER / args.speed
    # order_id dérivés de l'heure de lancement : deux lancements successifs ne réutilisent pas les mêmes
    counter = int(t0_wall - 1_700_000_000) * 100
    # Préchauffage : la simulation démarre une heure plus tôt et les événements antérieurs au
    # lancement ne sont pas envoyés. Le flux démarre ainsi « en régime établi ».
    warmup = 3600.0
    sim = Simulator(pop, rng, t0_sim - warmup, rate, use_profile=False, order_counter=counter)
    target = "stdout (dry-run)" if args.dry_run else f"{args.bootstrap} / topic {args.topic}"
    print(f"Envoi vers {target} : ~{args.rate} événements/s (Ctrl+C pour arrêter)", file=sys.stderr)

    stop = {"flag": False}
    signal.signal(signal.SIGINT, lambda *_: stop.__setitem__("flag", True))
    signal.signal(signal.SIGTERM, lambda *_: stop.__setitem__("flag", True))

    sent = 0
    status = Counter()
    last_log, last_sent = time.time(), 0
    for arrival, ev in sim.events():
        if stop["flag"]:
            break
        if arrival < t0_sim:  # préchauffage
            continue
        wall_due = t0_wall + (arrival - t0_sim) / args.speed
        delay = wall_due - time.time()
        while delay > 0 and not stop["flag"]:
            time.sleep(min(delay, 0.5))
            delay = wall_due - time.time()
        if stop["flag"]:
            break
        sink.send(ev["order_id"], json.dumps(ev, ensure_ascii=False))
        sent += 1
        status[ev["status"]] += 1
        now = time.time()
        if now - last_log >= 10:
            print(f"[{iso(now)}] {sent} événements envoyés ({(sent - last_sent) / (now - last_log):.1f}/s)",
                  file=sys.stderr)
            last_log, last_sent = now, sent
        if args.max_events and sent >= args.max_events:
            break
        if args.duration and now - t0_wall >= args.duration:
            break
    sink.close()
    elapsed = max(time.time() - t0_wall, 1e-9)
    print(f"Terminé : {sent} événements en {elapsed:.0f} s ({sent / elapsed:.1f}/s), statuts : {dict(status)}",
          file=sys.stderr)


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser(description="Générateur de commandes FastFood.ma")
    ap.add_argument("--mode", choices=["file", "kafka"], required=True)
    ap.add_argument("--restaurants", default=os.path.join(here, "restaurants.json"),
                    help="référentiel des restaurants (défaut : restaurants.json à côté du script)")
    ap.add_argument("--seed", type=int, default=42, help="graine aléatoire (défaut : 42)")
    g = ap.add_argument_group("mode file")
    g.add_argument("--n", type=int, default=50000, help="nombre d'événements (défaut : 50000)")
    g.add_argument("--output", default="orders_history.jsonl", help="fichier de sortie")
    g.add_argument("--start", default="2026-11-09", help="premier jour simulé, AAAA-MM-JJ (défaut : 2026-11-09)")
    g.add_argument("--days", type=float, default=7, help="nombre de jours simulés (défaut : 7)")
    g = ap.add_argument_group("mode kafka")
    g.add_argument("--rate", type=float, default=20, help="événements par seconde (défaut : 20)")
    g.add_argument("--bootstrap", default="localhost:9092",
                   help="serveur(s) Kafka (défaut : localhost:9092 ; kafka:9092 depuis un conteneur)")
    g.add_argument("--topic", default="orders", help="topic Kafka (défaut : orders)")
    g.add_argument("--speed", type=float, default=1.0,
                   help="accélération du temps simulé (défaut : 1 = temps réel)")
    g.add_argument("--max-events", type=int, default=0, help="arrêt après N événements (0 = sans fin)")
    g.add_argument("--duration", type=float, default=0, help="arrêt après N secondes (0 = sans fin)")
    g.add_argument("--dry-run", action="store_true", help="affiche les événements au lieu de les envoyer")
    args = ap.parse_args()

    restaurants = load_restaurants(args.restaurants)
    if args.mode == "file":
        run_file(args, restaurants)
    else:
        run_kafka(args, restaurants)


if __name__ == "__main__":
    main()

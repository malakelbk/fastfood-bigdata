"""
Générateur de commandes FastFood.ma

  python producer.py --mode restaurants --out data/restaurants.json
  python producer.py --mode file  --n 50000 --out data/orders_history.jsonl
  python producer.py --mode kafka --rate 20 [--bootstrap kafka:9092 --topic orders]

Simule le cycle CREATED -> PICKED_UP -> DELIVERED (ou CANCELLED),
~5 % d'événements en retard (émis 1 à 15 min après leur event_time)
et ~2 % de doublons (même order_id + même status).
"""
import argparse, heapq, json, os, random, sys, time
from datetime import datetime, timezone

CITIES = {  # ville: (code, poids, lat, lon)
    "Casablanca": ("CAS", 0.40, 33.5731, -7.5898),
    "Rabat":      ("RAB", 0.25, 34.0209, -6.8416),
    "Marrakech":  ("MAR", 0.20, 31.6295, -7.9811),
    "Tanger":     ("TAN", 0.15, 35.7595, -5.8340),
}
CATEGORIES = {  # catégorie: prix par article (min, max) en MAD
    "pizza": (45, 90), "tacos": (35, 60), "sushi": (60, 120), "marocain": (40, 80),
    "burger": (40, 75), "poulet": (35, 70), "asiatique": (50, 100), "healthy": (45, 85),
}
NAMES = ["Atlas", "Medina", "Corniche", "Oasis", "Kasbah", "Riad", "Sahara", "Andalous",
         "Majorelle", "Hassan", "Ocean", "Zellige", "Argan", "Souk", "Palmier", "Detroit"]

N_RESTAURANTS, N_CUSTOMERS, N_COURIERS_PER_CITY = 200, 6000, 15
P_LATE, P_DUP = 0.05, 0.02
P_CANCEL_BEFORE_PICKUP, P_CANCEL_AFTER_PICKUP = 0.08, 0.03


def iso(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_restaurants(seed=42):
    rng = random.Random(seed)
    cities = list(CITIES)
    weights = [CITIES[c][1] for c in cities]
    out = []
    for i in range(1, N_RESTAURANTS + 1):
        city = rng.choices(cities, weights)[0]
        cat = rng.choice(list(CATEGORIES))
        _, _, lat, lon = CITIES[city]
        out.append({
            "restaurant_id": f"R-{i:03d}",
            "name": f"{cat.capitalize()} {rng.choice(NAMES)} {i}",
            "city": city,
            "category": cat,
            "latitude": round(lat + rng.uniform(-0.06, 0.06), 6),
            "longitude": round(lon + rng.uniform(-0.06, 0.06), 6),
        })
    return out


class Simulator:
    def __init__(self, restaurants, seed=None):
        self.rng = random.Random(seed)
        rng_static = random.Random(42)  # clients/livreurs identiques d'un run à l'autre
        self.restaurants = restaurants
        self.by_city = {c: [r for r in restaurants if r["city"] == c] for c in CITIES}
        # popularité inégale des restaurants (longue traîne)
        self.pop = {r["restaurant_id"]: rng_static.paretovariate(1.5) for r in restaurants}
        cities = list(CITIES)
        weights = [CITIES[c][1] for c in cities]
        self.customers = []
        for i in range(1, N_CUSTOMERS + 1):
            city = rng_static.choices(cities, weights)[0]
            favs = rng_static.sample(self.by_city[city], k=min(3, len(self.by_city[city])))
            self.customers.append((f"C-{i:05d}", city, favs))
        self.couriers = {c: [f"L-{CITIES[c][0]}-{j:02d}" for j in range(1, N_COURIERS_PER_CITY + 1)]
                         for c in CITIES}
        self.order_seq = 0
        self.pending = []   # heap (due_ts, seq, order, next_status)
        self.held = []      # heap (release_ts, seq, event) : événements en retard
        self.seq = 0

    def _push(self, heap, ts, item):
        self.seq += 1
        heapq.heappush(heap, (ts, self.seq, *item))

    def _new_order(self, t):
        rng = self.rng
        cid, city, favs = rng.choice(self.customers)
        if rng.random() < 0.6:
            r = rng.choice(favs)
        else:
            pool = self.by_city[city]
            r = rng.choices(pool, [self.pop[x["restaurant_id"]] for x in pool])[0]
        lo, hi = CATEGORIES[r["category"]]
        items = rng.randint(1, 6)
        self.order_seq += 1
        return {
            "order_id": f"ORD-{self.order_seq:07d}",
            "restaurant_id": r["restaurant_id"],
            "customer_id": cid,
            "city": city,
            "status": "CREATED",
            "amount_mad": round(sum(rng.uniform(lo, hi) for _ in range(items)), 2),
            "items_count": items,
            "distance_km": round(min(rng.lognormvariate(1.0, 0.55), 15.0), 1),
            "courier_id": rng.choice(self.couriers[city]),
            "event_time": iso(t),
        }

    def _schedule_next(self, order, t):
        rng, s = self.rng, order["status"]
        if s == "CREATED":
            if rng.random() < P_CANCEL_BEFORE_PICKUP:
                self._push(self.pending, t + rng.uniform(60, 360), (order, "CANCELLED"))
            else:
                self._push(self.pending, t + rng.uniform(180, 720), (order, "PICKED_UP"))
        elif s == "PICKED_UP":
            if rng.random() < P_CANCEL_AFTER_PICKUP:
                self._push(self.pending, t + rng.uniform(120, 600), (order, "CANCELLED"))
            else:
                travel = order["distance_km"] * rng.uniform(120, 240) + rng.uniform(120, 360)
                self._push(self.pending, t + travel, (order, "DELIVERED"))

    def step(self, t):
        """Produit les événements à émettre à l'instant t (1 nouvel événement + retards relâchés)."""
        out = []
        while self.held and self.held[0][0] <= t:
            out.append(heapq.heappop(self.held)[2])

        if self.pending and self.pending[0][0] <= t:
            due, _, order, status = heapq.heappop(self.pending)
            order = dict(order, status=status, event_time=iso(due))
            ev_t = due
        else:
            order = self._new_order(t)
            ev_t = t
        self._schedule_next(order, ev_t)

        ev = dict(order)
        if self.rng.random() < P_LATE:
            self._push(self.held, t + self.rng.uniform(60, 900), (ev,))
        else:
            out.append(ev)
        if self.rng.random() < P_DUP:
            out.append(dict(ev))
        return out

    def flush(self):
        return [heapq.heappop(self.held)[2] for _ in range(len(self.held))]


def load_or_build_restaurants(path):
    if path and os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return [json.loads(l) for l in f if l.strip()]
    return build_restaurants()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["restaurants", "file", "kafka"], required=True)
    p.add_argument("--n", type=int, default=50000, help="nb d'événements (mode file)")
    p.add_argument("--days", type=float, default=7, help="période simulée (mode file)")
    p.add_argument("--rate", type=float, default=20, help="événements/s (mode kafka)")
    p.add_argument("--out", default=None)
    p.add_argument("--restaurants", default="data/restaurants.json")
    p.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"))
    p.add_argument("--topic", default="orders")
    p.add_argument("--seed", type=int, default=None)
    a = p.parse_args()

    if a.mode == "restaurants":
        out = a.out or "data/restaurants.json"
        with open(out, "w", encoding="utf-8") as f:
            for r in build_restaurants():
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
        print(f"{N_RESTAURANTS} restaurants -> {out}")
        return

    sim = Simulator(load_or_build_restaurants(a.restaurants), seed=a.seed)

    if a.mode == "file":
        out = a.out or "data/orders_history.jsonl"
        dt = a.days * 86400 / a.n
        t = time.time() - a.days * 86400
        written = 0
        with open(out, "w", encoding="utf-8") as f:
            while written < a.n:
                for ev in sim.step(t):
                    f.write(json.dumps(ev) + "\n")
                    written += 1
                t += dt
            for ev in sim.flush():
                f.write(json.dumps(ev) + "\n")
                written += 1
        print(f"{written} événements -> {out}")
        return

    from kafka import KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers=a.bootstrap,
        key_serializer=lambda k: k.encode(),
        value_serializer=lambda v: json.dumps(v).encode(),
        acks="all", linger_ms=20,
    )
    print(f"Kafka {a.bootstrap} topic={a.topic} rate={a.rate}/s  (Ctrl+C pour arrêter)")
    sent, period, start = 0, 1.0 / a.rate, time.time()
    try:
        while True:
            for ev in sim.step(time.time()):
                producer.send(a.topic, key=ev["order_id"], value=ev)
                sent += 1
            if sent and sent % 200 == 0:
                print(f"{sent} événements envoyés ({sent / (time.time() - start):.1f}/s)", flush=True)
            time.sleep(period)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()
        print(f"Total envoyé : {sent}")


if __name__ == "__main__":
    main()
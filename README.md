# FastFood.ma – Pipeline Big Data temps réel (Spark + Kafka + MinIO)

Projet du module **Écosystème Big Data 2** (EMSI, 5ᵉ année, 2026-2027).
Pipeline de données pour FastFood.ma, une plateforme fictive de livraison de repas à Casablanca, Rabat, Marrakech et Tanger : volume de commandes, chiffre d'affaires et retards de livraison par ville.

```
producer.py → Kafka (topic orders, 3 partitions) → Spark (batch + Structured Streaming) → MinIO (lake : bronze → silver → gold)
```

## Architecture

| Service | Image | Rôle | Accès depuis le PC |
|---|---|---|---|
| `kafka` | `apache/kafka:3.7.0` (mode KRaft, sans ZooKeeper) | bus de messages, topic `orders` | `localhost:9094` |
| `spark-master` | `spark:3.5.1-python3` | Cluster Manager standalone | UI http://localhost:8080 |
| `spark-worker-1` / `spark-worker-2` | `spark:3.5.1-python3` | 2 cœurs et 2 Go chacun | UI http://localhost:8081 / 8082 |
| `minio` | `minio/minio` | Data Lake compatible S3, bucket `lake` | console http://localhost:9001 (`minioadmin` / `minioadmin`) |
| `jupyter` | construit depuis `Dockerfile.jupyter` | driver PySpark (JupyterLab) | http://localhost:8888 · Spark UI http://localhost:4040 |
| `kafka-init` / `minio-init` | – | créent le topic `orders` (3 partitions) et le bucket `lake` au démarrage | – |

L'image Jupyter est construite **à partir de la même image que les workers** : le driver et les executors ont exactement la même version de Python, ce qu'exige PySpark.

## Prérequis

- Docker Desktop (Windows/macOS) ou Docker Engine + Compose v2 (Linux)
- Environ **6 Go de RAM** disponibles pour Docker
- Ports libres : 7077, 8080-8082, 8888, 4040-4045, 9000, 9001, 9094

## Lancement

```bash
git clone <url-du-depot> fastfood-bigdata
cd fastfood-bigdata
docker compose up -d --build        # le premier build prend quelques minutes
docker compose ps                   # kafka et minio doivent être "healthy"
docker compose logs kafka-init minio-init
```

Vérifications :
- http://localhost:8080 affiche **Alive Workers: 2**
- http://localhost:9001 affiche le bucket **lake**
- topic Kafka :
  ```bash
  docker exec ff-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic orders
  ```

Arrêt : `docker compose stop` conserve les données MinIO. `docker compose down -v` supprime aussi le volume.

## Génération des données

Le générateur `producer/producer.py` simule le cycle de vie des commandes (`CREATED → PICKED_UP → DELIVERED` ou `CANCELLED`) avec environ **5 % d'événements en retard** (1 à 15 min) et environ **2 % de doublons** (même `order_id` et même `status`).

```bash
# référentiel statique : 200 restaurants (JSON Lines)
docker exec -w /workspace ff-jupyter python3 producer/producer.py --mode restaurants

# historique pour les Parties 2 à 4 (~50 000 événements sur 7 jours simulés)
docker exec -w /workspace ff-jupyter python3 producer/producer.py --mode file --n 50000 --seed 1

# flux temps réel pour les Parties 5 et 6 (20 événements/s, Ctrl+C pour arrêter)
docker exec -it -w /workspace ff-jupyter python3 producer/producer.py --mode kafka --rate 20
```

Les messages Kafka ont pour clé l'`order_id` : tous les événements d'une même commande vont dans la même partition, ce qui garantit leur ordre.

## Notebooks

Ouvrir http://localhost:8888, puis le dossier `notebooks/`, et exécuter les notebooks **dans l'ordre**.

| Notebook | Partie | Contenu |
|---|---|---|
| `01_cluster.ipynb` | 1 | SparkSession sur `spark://spark-master:7077`, version, master, `defaultParallelism` (= 4) |
| `02_rdd.ipynb` | 2 | upload de l'historique vers `lake/bronze/`, RDD : filtre, `reduceByKey`, `aggregateByKey`, top 5, lineage, mesure des temps |
| `03_dataframes.ipynb` | 3 | schémas explicites, API DataFrame et SQL, jointure broadcast, UDF ou `when/otherwise`, Parquet Silver partitionné, `explain(True)` |
| `04_graphes.ipynb` | 4 | GraphFrames : degrés, PageRank, composantes connexes, motifs, recommandation, `aggregateMessages` |
| `05_streaming.ipynb` | 5 | *(Jalon 2)* Kafka → Structured Streaming → console et Bronze, watermark |
| `06_pipeline.ipynb` | 6 | *(Jalon 2)* dédoublonnage, enrichissement, Silver, fenêtres, Gold Delta Lake, reprise sur checkpoint |

`notebooks/spark_session.py` centralise la configuration commune : master, accès S3A à MinIO, packages Maven, `spark.sql.shuffle.partitions = 8`.

> **Chaque notebook se termine par `spark.stop()`.** Une application Spark active occupe les 4 cœurs du cluster : la suivante resterait en attente (`WAITING`).

## Organisation du Data Lake (bucket `lake`)

```
lake/
  bronze/history/orders_history.jsonl      historique brut (Partie 2)
  bronze/referentiel/restaurants.json      référentiel restaurants
  bronze/orders_raw/                       JSON bruts lus dans Kafka (Partie 5)
  silver/orders_history/city=…/            Parquet nettoyé et enrichi (Partie 3)
  silver/orders_clean/city=…/date=…/       flux nettoyé et enrichi (Partie 6)
  gold/kpi_city_10min/                     Delta Lake, KPI agrégés (Partie 6)
  checkpoints/                             checkpoints Structured Streaming et GraphFrames
```

Lister le contenu :
```bash
docker exec ff-minio sh -c "mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null && mc ls --recursive local/lake/"
```

## Structure du dépôt

```
fastfood-bigdata/
├── docker-compose.yml
├── Dockerfile.jupyter
├── README.md
├── producer/producer.py
├── data/                  restaurants.json, orders_history.jsonl (générés, non versionnés pour le .jsonl)
├── notebooks/             spark_session.py + 01 à 06
└── captures/              screenshots du rapport
```

## Dépannage

| Symptôme | Cause et solution |
|---|---|
| `getaddrinfo failed` / `Could not connect to http://minio:9000` | Le notebook tourne avec le Python local (Windows), pas dans le conteneur. Ouvrir les notebooks via http://localhost:8888, ou dans VS Code : *Select Kernel → Existing Jupyter Server → `http://localhost:8888`*. |
| `Conflict. The container name … is already in use` | Un autre projet utilise le même nom de conteneur. Les nôtres sont préfixés `ff-`. |
| `exec: "python": executable file not found` | Utiliser `python3` dans les conteneurs. |
| Application Spark en `WAITING` | Une autre SparkSession occupe les cœurs : exécuter `spark.stop()` dans l'autre notebook. |
| `pull access denied for minio/mc` | L'image `minio/mc` n'est plus distribuée. L'init du bucket utilise `mc`, déjà inclus dans `minio/minio`. |
| Spark UI injoignable | Le lien du Master pointe vers `jupyter:4040`. Utiliser http://localhost:4040 (ou 4041 si une autre session tourne). |

## Auteurs

Binôme : *Malak elboukhari* – *Mouloud boutqlmount*
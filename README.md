# FastFood.ma – Pipeline Big Data (Jalon 1 : Parties 1 à 4)

Projet Écosystème Big Data 2 – EMSI 5e année IA & Science des Données.
Ce jalon couvre : cluster Spark (P1), RDD (P2), Spark SQL / DataFrames (P3), GraphFrames (P4).

## Lancement
```bash
docker compose up -d
```
| Service | URL |
|---|---|
| JupyterLab | http://localhost:8888 |
| Spark Master UI | http://localhost:8080 |
| Spark UI (application) | http://localhost:4040 |
| Console MinIO | http://localhost:9001 |

Le topic Kafka `orders` (3 partitions) et le bucket `lake` sont créés automatiquement au démarrage (`kafka-init`, `minio-init`). Identifiants MinIO : voir `docker-compose.yml`.

## Données
```bash
python producer/producer.py --mode file --n 50000
```
Copier `orders_history.jsonl` dans `lake/bronze/history/` (console MinIO ou `mc`).

## Notebooks (`notebooks/`)
| Notebook | Partie |
|---|---|
| `01_cluster.ipynb` | P1 – SparkSession connectée au Master, 2 workers |
| `02_rdd.ipynb` | P2 – CA et panier moyen par ville, top 5, lineage, DAG |
| `03_dataframes.ipynb` | P3 – StructType, SQL, jointure, UDF, Silver Parquet, `explain` |
| `04_graphes.ipynb` | P4 – GraphFrames : degrés, PageRank, composantes, motifs, aggregateMessages |

Exécuter les notebooks dans l'ordre ; chacun se termine par `spark.stop()` pour libérer les cœurs.

Les parties 5 et 6 (streaming) seront livrées au jalon 2.
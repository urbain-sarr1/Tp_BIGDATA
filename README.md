# Tp_BIGDATA

Python 3.13.0
Kafka
Zookeepper

Le fichier capteur.py (produceur) permet d'envoyer des transactions dans le cluster kafka.
Le fichier dfminio.py (consommeur) permet de consommer les données depuis le cluster kafka, les transformer et les envoyer en paquet vers minio.
Le fichier readminio.py permet de lire les paquets depuis minio en utilisant Spark.

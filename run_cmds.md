docker compose up -d

docker compose logs airflow-webserver

docker compose run --rm airflow-webserver airflow db init

docker compose exec airflow-webserver airflow users create \

docker compose exec --user spark spark-master /opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
--conf "spark.sql.streaming.debug=true" \
/opt/spark/work-dir/jobs/process_kafka_to_local.py

For manually without airflow

python3 producer/producer.py
Producers

for closing 
docker compose down
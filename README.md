## Run docker
```
docker compose up
```

## Run Producer
```
uv run python producer.py
```

## Submit Job
```bash
 docker exec  spark-master /opt/spark/bin/spark-submit  --master spark://localhost:7077 --packages org.postgresql:postgresql:42.6.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 /opt/spark/apps/spark_streaming.py
```

## Open visualization
```
streamlit run stph.py
```
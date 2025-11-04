#!/bin/bash
# Initializing Apache Airflow in Docker
#!/bin/bash
set -e
mkdir -p ./airflow/dags/files ./airflow/logs ./airflow/plugins
chmod -R 777 ./airflow

docker exec -i postgres_project psql -U admin321 -d postgres -c "DO \$\$BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='airflow') THEN CREATE ROLE airflow WITH LOGIN PASSWORD 'airflow'; END IF; END \$\$;"

docker exec -i postgres_project psql -U admin321 -d postgres <<'SQL'
SELECT 'CREATE DATABASE airflow OWNER admin321' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname='airflow')\gexec
SQL

docker exec -i postgres_project psql -U admin321 -d postgres -c "GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;"

docker compose --profile airflow up -d

MAX_TRIES=90
COUNTER=0
echo "⏳ Waiting for Apache Airflow Webserver (host port 8081)..."

while ! curl -s http://localhost:8081/health | grep -q '"status":[[:space:]]*"healthy"'; do
    COUNTER=$((COUNTER+1))
    if [ $COUNTER -ge $MAX_TRIES ]; then
        echo "❌ Airflow did not start in time"
        exit 1
    fi
    sleep 2
done

echo "✅ Airflow Webserver is up and running: http://localhost:8081"

path_data="./airflow/dags/files/data.txt"
url_data="https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0321EN-SkillsNetwork/ETL/accesslog.txt"
curl -o "$path_data" "$url_data"

echo "📂 Listing DAGs in Airflow..."
docker exec airflow-webserver airflow dags list 2>/dev/null

echo "▶️ Unpausing DAG process_web_log..."
docker exec airflow-webserver airflow dags unpause process_web_log 2>/dev/null

echo "📊 Listing DAG runs..."
docker exec airflow-webserver airflow dags list-runs -d process_web_log 2>/dev/null
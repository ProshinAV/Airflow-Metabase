echo killing old docker processes
docker-compose -f airflow/docker-compose.yaml rm -fs

echo building docker containers
docker-compose -f airflow/docker-compose.yaml up -d --build
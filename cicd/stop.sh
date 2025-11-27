echo "Stopping Airflow containers..."
docker-compose -f cicd/docker-compose.yaml down
echo "Stopping Colima VM..."
colima stop
colima start --arch aarch64 --cpu 4 --memory 8 --disk 60
docker-compose -f cicd/docker-compose.yaml build --no-cache
docker-compose -f cicd/docker-compose.yaml up airflow-init
docker-compose -f cicd/docker-compose.yaml up -d

# mkdir -p ./dags ./logs ./plugins ./config
# cd cicd
# echo -e "AIRFLOW_UID=$(id -u)" > .env
# cd ..
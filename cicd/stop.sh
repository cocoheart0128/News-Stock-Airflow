echo "Stopping Airflow containers..."
docker-compose -f cicd/docker-compose.yaml down --rmi all
#--rmi all 이미지 삭제
#-v DB 삭제 및 초기화 조심 사용 해야함

echo "Stopping Colima VM..."
colima stop
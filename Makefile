create_folders:
	mkdir -p ./dags ./logs ./plugins ./config ./data

init_airflow:
	sudo docker compose up airflow-init

restart_airflow:
	sudo docker compose down && sleep 5 && sudo docker compose up --build -d

start_airflow:
	sudo docker compose up --build -d

stop_airflow:
	sudo docker compose down
FROM apache/airflow:2.10.3
ADD requirements.txt .
RUN pip install apache-airflow==2.10.3 -r requirements.txt
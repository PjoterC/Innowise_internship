FROM apache/airflow:3.3.0-python3.12

# Install extra Python packages on top of the official Airflow image.
# The constraints file is Airflow's own pin set for this exact version — without
# it pip is free to pull a provider that drags in a different apache-airflow.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.0/constraints-3.12.txt"

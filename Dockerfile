FROM apache/airflow:2.10.3

# Copy the requirements.txt into the container
COPY requirements.txt /requirements.txt

# Install the dependencies listed in the requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

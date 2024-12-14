FROM apache/airflow:2.10.3

# Copy the requirements.txt into the container
COPY requirements.txt /requirements.txt

# Install the dependencies listed in the requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Install supervisord
RUN apt-get update && apt-get install -y supervisor && apt-get clean

# Copy the supervisord configuration
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Expose necessary ports
EXPOSE 8080 8501

# Start supervisord
CMD ["supervisord", "-c", "/etc/supervisor/supervisord.conf"]
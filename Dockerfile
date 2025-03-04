# Use the official Airflow image as a base
FROM apache/airflow:latest-python3.10


# Copy the requirements.txt file into the container
COPY requirements.txt .


# Install dependencies from the requirements.txt file
RUN pip install --prefer-binary --no-cache-dir -r requirements.txt



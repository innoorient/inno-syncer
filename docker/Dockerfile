FROM iomete/spark-py:3.2.1.1

WORKDIR "/app"

# Reset to root to run installation tasks
USER 0

# add source code to the /app directory
COPY ../setup.py setup.cfg driver.py ./
COPY ../iomete_postgresql_sync ./iomete_postgresql_sync

RUN pip install .

# Specify the User that the actual main process will run as
ARG spark_uid=185
USER ${spark_uid}
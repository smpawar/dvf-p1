FROM python:3.9-slim-bullseye

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

# Copy local code to the container image.
ENV APP_HOME /app
WORKDIR $APP_HOME

COPY execute_dvt/connections.sh ./
COPY execute_dvt/dvt_main.py ./
COPY execute_dvt/run_dvt.sh ./

# Install production dependencies.
RUN apt-get update \
    && apt-get install gcc -y \
    && apt-get clean
RUN pip install --upgrade pip
RUN pip install --upgrade google-auth
RUN pip install oauth2client
RUN pip install Flask gunicorn
RUN pip install --upgrade google-pso-data-validator
RUN pip install gcsfs
RUN pip install python-dotenv
RUN pip install oracledb

# Install google cloud SDK
RUN apt-get update && apt-get install -y apt-transport-https ca-certificates gnupg curl && \
    curl -sS https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" > /etc/apt/sources.list.d/google-cloud-sdk.list && \
    apt-get update -y && \
    apt-get install google-cloud-sdk -y && \
    apt-get install -y wget unzip libaio1



# --- Oracle Dependencies ---
# The following steps install the Oracle Instant Client, which is required
# for python-oracledb thick mode.

# 1. Set the Oracle home directory
ENV ORACLE_HOME=/opt/oracle/instantclient_21_12
ENV LD_LIBRARY_PATH=$ORACLE_HOME

# 2. Download and unzip the Oracle Instant Client
RUN mkdir -p $ORACLE_HOME && \
    cd $ORACLE_HOME && \
    wget https://download.oracle.com/otn_software/linux/instantclient/2112000/instantclient-basiclite-linux.x64-21.12.0.0.0dbru.zip && \
    unzip instantclient-basiclite-linux.x64-21.12.0.0.0dbru.zip && \
    rm instantclient-basiclite-linux.x64-21.12.0.0.0dbru.zip && \
    mv instantclient_21_12/* . && \
    rmdir instantclient_21_12

# 3. Configure the dynamic linker
RUN echo $ORACLE_HOME > /etc/ld.so.conf.d/oracle.conf && \
    ldconfig


# Return to the app directory
WORKDIR $APP_HOME
# --- End of Oracle Dependencies ---

# UNCOMMENT TO RUN AS JOB
CMD ["python", "dvt_main.py"]

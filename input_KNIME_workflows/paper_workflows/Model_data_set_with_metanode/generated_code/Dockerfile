FROM python:3.11-slim

WORKDIR ./wf_validation_contracts
ENV PYTHONPATH=./wf_validation_contracts

RUN apt-get update && \
    apt-get install -y --no-install-recommends wget git && \
    rm -rf /var/lib/apt/lists/*

RUN git clone --depth 1 --branch develop https://github.com/i3uex/MD4DSP-m2python.git .

RUN wget https://download.java.net/java/ga/jdk11/openjdk-11_linux-x64_bin.tar.gz && \
    tar -xvf openjdk-11_linux-x64_bin.tar.gz -C /usr/local && \
    ln -s /usr/local/jdk-11/bin/java /usr/bin/java

RUN pip install --no-cache-dir -r requirements.txt
RUN pip install h5py
RUN pip install pyarrow

COPY dataProcessing_Job_Model_data_set_with_metanode_KNIME_.py ./wf_validation_contracts/workflows/
COPY contracts_Job_Model_data_set_with_metanode_KNIME_.py ./wf_validation_contracts/workflows/
COPY transformations_Job_Model_data_set_with_metanode_KNIME_.py ./wf_validation_contracts/workflows/

COPY fileFormatting.py ./wf_validation_contracts/workflows/

COPY workflow_scripts.sh ./wf_validation_contracts/workflow_scripts.sh

RUN chmod +x ./wf_validation_contracts/workflow_scripts.sh

CMD ["/bin/bash", "./wf_validation_contracts/workflow_scripts.sh"]

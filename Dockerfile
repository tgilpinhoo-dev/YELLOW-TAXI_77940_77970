FROM spark:python3

USER root
ENV SPARK_HOME=/opt/spark
ENV HOME=/tmp
ENV PYTHONPATH=/opt/spark/python

# Bibliotecas usadas no ambiente dos Labs.
RUN pip install --no-cache-dir jupyterlab py4j matplotlib matplotlib-inline pandas pyarrow

RUN mkdir -p /tmp/jupyter/runtime /tmp/jupyter/data \
    && chmod -R 777 /tmp/jupyter

WORKDIR /project
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]

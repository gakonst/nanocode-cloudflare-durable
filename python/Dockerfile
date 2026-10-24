FROM python:3.13-slim
RUN pip install --no-cache-dir numpy pandas matplotlib scipy sympy networkx pillow \
 && useradd -m -d /home/user -u 1000 runner \
 && mkdir -p /workspace /opt/mpl \
 && chown runner:runner /workspace /opt/mpl \
 && su runner -s /bin/sh -c "MPLCONFIGDIR=/opt/mpl MPLBACKEND=Agg python3 -c 'import matplotlib.pyplot, numpy, pandas'"
COPY server.py /opt/server.py
EXPOSE 8080
CMD ["python3", "/opt/server.py"]

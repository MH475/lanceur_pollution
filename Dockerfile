FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY collect.py compact.py sources.py run.sh ./
RUN chmod +x run.sh
VOLUME ["/app/data"]
# Collecte en continu + compaction chaque nuit
CMD ["./run.sh"]

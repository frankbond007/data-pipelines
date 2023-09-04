# Base image with Python
FROM python:3.9
RUN apt-get update && apt-get install -y postgresql-client

WORKDIR /app

COPY requirements.txt /app/requirements.txt
COPY send_messagev2.py /app/send_message.py
COPY read_messages.py /app/read_messages.py
COPY unify_structure.py /app/unify_structure.py
COPY write_to_db.py /app/write_to_db.py
COPY entrypoint.sh /app/entrypoint.sh
RUN pip install --no-cache-dir -r requirements.txt
CMD ["sh", "entrypoint.sh"]

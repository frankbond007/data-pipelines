import psycopg2
import json
import os
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor
from typing import Optional
from unify_structure import unify_structure
from pydantic import BaseModel

class Event(BaseModel):
    id: int
    type: str
    status: str
    title: str
    user: str
    body: str
    tags: list
    closed_or_merged_at: Optional[datetime] = None
    created_or_started_at: datetime
    repository: dict


def process_data(data):
    try:
        event = Event(**data)
        validated_data = event.dict()
        if validated_data['closed_or_merged_at']:
            validated_data['closed_or_merged_at'] = \
                validated_data['closed_or_merged_at'].isoformat()
            validated_data['created_or_started_at'] = \
                validated_data['created_or_started_at'].isoformat()

        structure = (
            validated_data.get('id'),
            validated_data.get('type'),
            validated_data.get('status'),
            validated_data.get('title'),
            validated_data.get('user'),
            validated_data.get('body'),
            json.dumps(validated_data.get('tags')),
            validated_data.get('closed_or_merged_at'),
            validated_data.get('created_or_started_at'),
            validated_data.get('repository', {}).get('name'),
            validated_data.get('repository', {}).get('owner')
        )
        return structure, None  # Return tuple (structure, None) for successful processing
    except (KeyError, ValueError, TypeError) as e:
        rejected_data = (json.dumps(data),)
        return None, rejected_data  # Return tuple (None, rejected_data) for processing error

def insert_data_to_postgresql(host, port, database,
                              user, password, table,
                              data_batch, handles, sqs):

    conn = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )

    try:
        cursor = conn.cursor()
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id BIGINT PRIMARY KEY,
                type TEXT,
                status TEXT,
                title TEXT,
                "user" TEXT,
                body TEXT,
                tags JSONB,
                closed_or_merged_at TIMESTAMP WITH TIME ZONE DEFAULT NULL,
                created_or_started_at TIMESTAMP WITH TIME ZONE,
                repository_name TEXT,
                repository_owner TEXT,
                updated_at TIMESTAMP WITH TIME ZONE
            )
        """
        cursor.execute(create_table_query)

        create_rejected_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table}_rejected_data (
                data JSONB
            )
        """
        cursor.execute(create_rejected_table_query)
        with ProcessPoolExecutor() as executor:
            results = list(executor.map(process_data, data_batch))
        insert_batch = [structure for structure, _ in results if structure]
        rejected = [rejected_data for _, rejected_data in results if rejected_data]

        insert_query = f"""
            INSERT INTO {table} (
                id, type, status, title, "user", body, tags,
                closed_or_merged_at, created_or_started_at,
                repository_name, repository_owner, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s::jsonb,
                %s::timestamptz, %s::timestamptz,
                %s, %s, current_timestamp
            ) ON CONFLICT (id) DO UPDATE SET
                type = EXCLUDED.type,
                status = EXCLUDED.status,
                title = EXCLUDED.title,
                "user" = EXCLUDED."user",
                body = EXCLUDED.body,
                tags = EXCLUDED.tags,
                closed_or_merged_at = CASE WHEN EXCLUDED.closed_or_merged_at
                    IS NOT NULL THEN EXCLUDED.closed_or_merged_at ELSE NULL END,
                created_or_started_at = CASE WHEN EXCLUDED.created_or_started_at
                    IS NOT NULL THEN EXCLUDED.created_or_started_at ELSE NULL END,
                repository_name = EXCLUDED.repository_name,
                repository_owner = EXCLUDED.repository_owner,
                updated_at = current_timestamp
        """

        cursor.executemany(insert_query, insert_batch)

        if rejected:
            insert_rejected_query = f"INSERT INTO {table}_rejected_data (data) VALUES (%s)"
            cursor.executemany(insert_rejected_query, rejected)
        conn.commit()

        for handle in handles:
            sqs.delete_message(
                QueueUrl=os.environ['QUEUE_URL'],
                ReceiptHandle=handle
            )

    except (Exception, psycopg2.Error) as error:
        print(f"Error inserting data into PostgreSQL: {error}")
        if cursor:
            cursor.execute("ROLLBACK")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def process_inp(inp):
    return unify_structure(inp)

def main_entrypoint(inp_batch, handles, sqs):
    with ProcessPoolExecutor() as executor:
        unified_structure_batch = list(executor.map(process_inp, inp_batch))
    insert_data_to_postgresql(
        host=os.environ['POSTGRES_HOST'],
        port=os.environ['POSTGRES_PORT'],
        database=os.environ['POSTGRES_DB'],
        user=os.environ['POSTGRES_USER'],
        password=os.environ['POSTGRES_PASSWORD'],
        table='events',
        data_batch=unified_structure_batch,
        handles=handles,
        sqs=sqs
    )

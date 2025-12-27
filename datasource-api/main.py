import io
import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import psycopg2
from fastapi import FastAPI, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import JSONResponse
from psycopg2 import extras, sql
from sqlalchemy import create_engine, inspect, text

from kafka import KafkaProducer

app = FastAPI(
    title="Data Source API",
    description="API to upload CSV files and provide data endpoints for NiFi",
    version="1.0.0",
)

# Database configuration
DB_HOST = os.getenv("DB_HOST", "shared-db")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "datasource_db")
DB_USER = os.getenv("DB_USER", "datasource_admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "datasource_password")

# SQLAlchemy connection string
DATABASE_URL = (
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9092"
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "csv-upload-events")

# Initialize Kafka producer (lazy initialization)
_kafka_producer = None


def get_kafka_producer():
    """Get or create Kafka producer instance"""
    global _kafka_producer
    if _kafka_producer is None:
        try:
            _kafka_producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
                max_in_flight_requests_per_connection=1,
            )
        except Exception as e:
            print(f"Warning: Failed to initialize Kafka producer: {e}")
            _kafka_producer = None
    return _kafka_producer


def get_db_connection():
    """Create a database connection"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        return conn
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Database connection failed: {str(e)}"
        )


def sanitize_table_name(name: str) -> str:
    """Sanitize table name to be SQL-safe"""
    # Remove special characters and replace spaces with underscores
    name = name.lower().replace(" ", "_")
    # Keep only alphanumeric and underscore
    name = "".join(c for c in name if c.isalnum() or c == "_")
    # Ensure it starts with a letter
    if name and not name[0].isalpha():
        name = "table_" + name
    return name


def sanitize_column_name(name: str) -> str:
    """Sanitize column name to be SQL-safe"""
    # Remove special characters and replace spaces with underscores
    name = name.lower().replace(" ", "_")
    # Keep only alphanumeric and underscore
    name = "".join(c for c in name if c.isalnum() or c == "_")
    # Ensure it starts with a letter
    if name and not name[0].isalpha():
        name = "col_" + name
    return name


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        conn = get_db_connection()
        conn.close()
        return {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "database": "connected",
        }
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "timestamp": datetime.utcnow().isoformat(),
                "error": str(e),
            },
        )


@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": "Data Source API for NiFi",
        "version": "1.0.0",
        "endpoints": {
            "health": "/health",
            "upload": "POST /api/upload",
            "tables": "GET /api/tables",
            "data": "GET /api/data/{table_name}",
        },
    }


@app.post("/api/upload")
async def upload_csv(
    file: UploadFile = File(...), table_name: Optional[str] = Form(None)
):
    """
    Upload a CSV file and store it in PostgreSQL

    Args:
        file: CSV file to upload
        table_name: Optional custom table name (will be sanitized)

    Returns:
        JSON with upload status and table information
    """
    try:
        # Validate file type
        if not file.filename.endswith(".csv"):
            raise HTTPException(
                status_code=400, detail="Only CSV files are supported"
            )

        # Read CSV file
        contents = await file.read()
        df = pd.read_csv(io.BytesIO(contents))

        if df.empty:
            raise HTTPException(status_code=400, detail="CSV file is empty")

        # Auto-detect and parse datetime columns
        for col in df.columns:
            if (
                "datetime" in col.lower()
                or "date" in col.lower()
                or "time" in col.lower()
            ):
                try:
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                except:
                    pass  # Keep as is if conversion fails

        # Determine table name
        if table_name is None or table_name.strip() == "":
            # Use filename without extension
            table_name = os.path.splitext(file.filename)[0]

        table_name = sanitize_table_name(table_name)

        # Sanitize column names
        df.columns = [sanitize_column_name(col) for col in df.columns]

        # Create SQLAlchemy engine
        engine = create_engine(DATABASE_URL)

        # Store dataframe to PostgreSQL
        df.to_sql(
            table_name, engine, if_exists="append", index=False, method="multi"
        )

        # Publish event to Kafka
        kafka_message = {
            "event_type": "csv_uploaded",
            "table_name": table_name,
            "filename": file.filename,
            "rows_count": len(df),
            "columns": list(df.columns),
            "timestamp": datetime.utcnow().isoformat(),
            "api_endpoint": f"/api/data/{table_name}",
        }

        producer = get_kafka_producer()
        if producer:
            try:
                future = producer.send(KAFKA_TOPIC, kafka_message)
                # Wait for message to be sent (with timeout)
                future.get(timeout=10)
                print(f"Published event to Kafka: {kafka_message}")
            except Exception as e:
                print(f"Warning: Failed to publish to Kafka: {e}")
                # Don't fail the upload if Kafka is unavailable

        return {
            "status": "success",
            "message": f"CSV file uploaded successfully",
            "table_name": table_name,
            "rows_inserted": len(df),
            "columns": list(df.columns),
            "timestamp": datetime.utcnow().isoformat(),
            "kafka_published": producer is not None,
        }

    except pd.errors.EmptyDataError:
        raise HTTPException(
            status_code=400, detail="CSV file is empty or invalid"
        )
    except pd.errors.ParserError:
        raise HTTPException(status_code=400, detail="CSV file parsing failed")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")


@app.get("/api/tables")
async def list_tables():
    """
    List all available tables in the database

    Returns:
        JSON with list of table names and their row counts
    """
    try:
        engine = create_engine(DATABASE_URL)
        inspector = inspect(engine)
        tables = inspector.get_table_names()

        # Get row count for each table
        table_info = []
        with engine.connect() as conn:
            for table in tables:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.scalar()

                # Get column info
                columns = [col["name"] for col in inspector.get_columns(table)]

                table_info.append(
                    {
                        "table_name": table,
                        "row_count": count,
                        "columns": columns,
                    }
                )

        return {
            "status": "success",
            "total_tables": len(tables),
            "tables": table_info,
            "timestamp": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to list tables: {str(e)}"
        )


@app.get("/api/data/{table_name}")
async def get_data(
    table_name: str,
    page: int = Query(1, ge=1, description="Page number (starts from 1)"),
    page_size: int = Query(
        100, ge=1, le=1000, description="Number of records per page"
    ),
):
    """
    Get data from a specific table with pagination

    Args:
        table_name: Name of the table to query
        page: Page number (starts from 1)
        page_size: Number of records per page (max 1000)

    Returns:
        JSON with paginated data
    """
    try:
        # Sanitize table name for security
        table_name = sanitize_table_name(table_name)

        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=extras.RealDictCursor)

        # Check if table exists
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            )
        """,
            (table_name,),
        )

        if not cursor.fetchone()["exists"]:
            conn.close()
            raise HTTPException(
                status_code=404, detail=f"Table '{table_name}' not found"
            )

        # Get total count
        cursor.execute(
            sql.SQL("SELECT COUNT(*) FROM {}").format(
                sql.Identifier(table_name)
            )
        )
        total_count = cursor.fetchone()["count"]

        # Calculate pagination
        offset = (page - 1) * page_size
        total_pages = (total_count + page_size - 1) // page_size

        # Get paginated data
        cursor.execute(
            sql.SQL("SELECT * FROM {} LIMIT %s OFFSET %s").format(
                sql.Identifier(table_name)
            ),
            (page_size, offset),
        )

        rows = cursor.fetchall()
        conn.close()

        return {
            "status": "success",
            "table_name": table_name,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_records": total_count,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_previous": page > 1,
            },
            "data": rows,
            "timestamp": datetime.utcnow().isoformat(),
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to retrieve data: {str(e)}"
        )


@app.delete("/api/data/{table_name}")
async def delete_table(table_name: str):
    """
    Delete a table from the database

    Args:
        table_name: Name of the table to delete

    Returns:
        JSON with deletion status
    """
    try:
        table_name = sanitize_table_name(table_name)

        conn = get_db_connection()
        cursor = conn.cursor()

        # Check if table exists
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            )
        """,
            (table_name,),
        )

        if not cursor.fetchone()[0]:
            conn.close()
            raise HTTPException(
                status_code=404, detail=f"Table '{table_name}' not found"
            )

        # Drop table
        cursor.execute(
            sql.SQL("DROP TABLE {}").format(sql.Identifier(table_name))
        )
        conn.commit()
        conn.close()

        return {
            "status": "success",
            "message": f"Table '{table_name}' deleted successfully",
            "timestamp": datetime.utcnow().isoformat(),
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to delete table: {str(e)}"
        )


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    global _kafka_producer
    if _kafka_producer:
        _kafka_producer.close()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)

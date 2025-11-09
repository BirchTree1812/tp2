import os
import time
from pathlib import Path

import pandas as pd
import psycopg2
from neo4j import GraphDatabase

CHUNK_SIZE = 100  # how many rows to send to Neo4j per batch


# ---------- Low-level connection helpers ----------

def get_neo4j_driver():
    uri = os.environ.get("NEO4J_URI", "bolt://neo4j:7687")
    user = os.environ.get("NEO4J_USER", "neo4j")
    password = os.environ.get("NEO4J_PASSWORD", "password")
    return GraphDatabase.driver(uri, auth=(user, password))


def get_postgres_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        dbname=os.environ.get("POSTGRES_DB", "shop"),
        user=os.environ.get("POSTGRES_USER", "app"),
        password=os.environ.get("POSTGRES_PASSWORD", "app"),
    )


# ---------- Functions required by the assignment ----------

def run_cypher(query, params=None):
    """
    Executes a single Cypher query against Neo4j.
    `params` is a dict passed as query parameters.
    """
    driver = get_neo4j_driver()
    try:
        with driver.session() as session:
            result = session.run(query, **(params or {}))
            # Materialize result so connection can close cleanly
            return list(result)
    finally:
        driver.close()


def run_cypher_file(path: Path):
    """
    Executes all Cypher statements found in a .cypher file.
    Statements are assumed to be separated by ';'.
    Used here to apply schema (constraints, indexes, etc.).
    """
    with path.open("r", encoding="utf-8") as f:
        content = f.read()

    # Split on ';' and remove empty pieces
    statements = [s.strip() for s in content.split(";") if s.strip()]

    for stmt in statements:
        run_cypher(stmt)


def chunk(items, size):
    """
    Splits a list into smaller lists of length <= size.
    This lets us send data to Neo4j in batches instead of all at once.
    """
    for i in range(0, len(items), size):
        yield items[i:i + size]


def wait_for_neo4j(timeout=60):
    """
    Repeatedly tries to connect to Neo4j until it is ready or timeout expires.
    This is important in docker-compose where services start in parallel.
    """
    start = time.time()
    while True:
        try:
            driver = get_neo4j_driver()
            driver.verify_connectivity()
            driver.close()
            print("Neo4j is ready")
            return
        except Exception as e:
            if time.time() - start > timeout:
                raise RuntimeError("Neo4j not ready after {timeout}s") from e
            print("Waiting for Neo4j...", e)
            time.sleep(2)


def wait_for_postgres(timeout=60):
    """
    Repeatedly tries to connect to Postgres until it is ready or timeout expires.
    Avoids 'connection refused' errors when ETL starts too early.
    """
    start = time.time()
    while True:
        try:
            conn = get_postgres_connection()
            conn.close()
            print("Postgres is ready")
            return
        except Exception as e:
            if time.time() - start > timeout:
                raise RuntimeError("Postgres not ready after {timeout}s") from e
            print("Waiting for Postgres...", e)
            time.sleep(2)


# ---------- Main ETL ----------

def etl():
    """
    Main ETL function that migrates data from PostgreSQL to Neo4j.
    
    This function performs the complete Extract, Transform, Load process:
    1. Waits for both databases to be ready
    2. Sets up Neo4j schema using queries.cypher file
    3. Extracts data from PostgreSQL tables
    4. Transforms relational data into graph format
    5. Loads data into Neo4j with appropriate relationships
    """
    # Ensure dependencies are ready (useful when running in docker-compose)
    wait_for_postgres()
    wait_for_neo4j()

    # Apply Neo4j schema (constraints / indexes) from queries.cypher
    queries_path = Path(__file__).with_name("queries.cypher")
    if queries_path.exists():
        run_cypher_file(queries_path)

    # ---------- EXTRACT from Postgres ----------

    conn = get_postgres_connection()
    try:
        customers_df = pd.read_sql(
            "SELECT id, name, join_date::text AS join_date FROM customers", conn
        )
        categories_df = pd.read_sql(
            "SELECT id, name FROM categories", conn
        )
        products_df = pd.read_sql(
            "SELECT id, name, price, category_id FROM products", conn
        )
        orders_df = pd.read_sql(
            """
            SELECT 
                id, 
                customer_id, 
                to_char(ts, 'YYYY-MM-DD"T"HH24:MI:SS') AS ts
            FROM orders
            """,
            conn,
        )
        order_items_df = pd.read_sql(
            "SELECT order_id, product_id, quantity FROM order_items", conn
        )
        events_df = pd.read_sql(
            """
            SELECT 
                id, 
                customer_id, 
                product_id, 
                event_type, 
                to_char(ts, 'YYYY-MM-DD"T"HH24:MI:SS') AS ts
            FROM events
            """,
            conn,
        )
    finally:
        conn.close()

    # Convert pandas DataFrames to plain Python lists of dicts
    customers = customers_df.to_dict("records")
    categories = categories_df.to_dict("records")
    products = products_df.to_dict("records")
    orders = orders_df.to_dict("records")
    order_items = order_items_df.to_dict("records")
    events = events_df.to_dict("records")

    # ---------- LOAD into Neo4j (graph structure) ----------

    # 1) Category nodes
    for rows in chunk(categories, CHUNK_SIZE):
        run_cypher(
            """
            UNWIND $rows AS row
            MERGE (c:Category {id: row.id})
            SET c.name = row.name
            """,
            {"rows": rows},
        )

    # 2) Product nodes + IN_CATEGORY relationships
    for rows in chunk(products, CHUNK_SIZE):
        run_cypher(
            """
            UNWIND $rows AS row
            MERGE (p:Product {id: row.id})
            SET p.name = row.name,
                p.price = row.price
            WITH p, row
            MATCH (c:Category {id: row.category_id})
            MERGE (p)-[:IN_CATEGORY]->(c)
            """,
            {"rows": rows},
        )

    # 3) Customer nodes
    for rows in chunk(customers, CHUNK_SIZE):
        run_cypher(
            """
            UNWIND $rows AS row
            MERGE (c:Customer {id: row.id})
            SET c.name = row.name,
                c.join_date = date(row.join_date)
            """,
            {"rows": rows},
        )

    # 4) Order nodes + PLACED relationships (Customer)-[:PLACED]->(Order)
    for rows in chunk(orders, CHUNK_SIZE):
        run_cypher(
            """
            UNWIND $rows AS row
            MERGE (o:Order {id: row.id})
            SET o.ts = datetime(row.ts)
            WITH o, row
            MATCH (c:Customer {id: row.customer_id})
            MERGE (c)-[:PLACED]->(o)
            """,
            {"rows": rows},
        )

    # 5) CONTAINS relationships (Order)-[:CONTAINS {quantity}]->(Product)
    for rows in chunk(order_items, CHUNK_SIZE):
        run_cypher(
            """
            UNWIND $rows AS row
            MATCH (o:Order {id: row.order_id})
            MATCH (p:Product {id: row.product_id})
            MERGE (o)-[r:CONTAINS]->(p)
            SET r.quantity = row.quantity
            """,
            {"rows": rows},
        )

    # 6) Event relationships: VIEWED / CLICKED / ADDED_TO_CART
    view_events = [e for e in events if e["event_type"] == "view"]
    click_events = [e for e in events if e["event_type"] == "click"]
    add_events = [e for e in events if e["event_type"] == "add_to_cart"]

    # (:Customer)-[:VIEWED]->(:Product)
    for rows in chunk(view_events, CHUNK_SIZE):
        run_cypher(
            """
            UNWIND $rows AS row
            MATCH (c:Customer {id: row.customer_id})
            MATCH (p:Product {id: row.product_id})
            MERGE (c)-[r:VIEWED {id: row.id}]->(p)
            SET r.ts = datetime(row.ts)
            """,
            {"rows": rows},
        )

    # (:Customer)-[:CLICKED]->(:Product)
    for rows in chunk(click_events, CHUNK_SIZE):
        run_cypher(
            """
            UNWIND $rows AS row
            MATCH (c:Customer {id: row.customer_id})
            MATCH (p:Product {id: row.product_id})
            MERGE (c)-[r:CLICKED {id: row.id}]->(p)
            SET r.ts = datetime(row.ts)
            """,
            {"rows": rows},
        )

    # (:Customer)-[:ADDED_TO_CART]->(:Product)
    for rows in chunk(add_events, CHUNK_SIZE):
        run_cypher(
            """
            UNWIND $rows AS row
            MATCH (c:Customer {id: row.customer_id})
            MATCH (p:Product {id: row.product_id})
            MERGE (c)-[r:ADDED_TO_CART {id: row.id}]->(p)
            SET r.ts = datetime(row.ts)
            """,
            {"rows": rows},
        )

    print("ETL done.")


if __name__ == "__main__":
    etl()

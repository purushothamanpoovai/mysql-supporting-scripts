"""
Galera Table Auto-Increment Discrepancy Checker
-----------------------------------------------

Description:
-------------
This script checks the maximum primary key value per table across multiple 
MySQL hosts (or Galera nodes) for a specific date. It ensures consistency 
of auto-increment primary key values across nodes, which is especially 
important in multi-master or Galera cluster setups.

The script:
1. Connects to multiple MySQL hosts concurrently.
2. Fetches all tables in the specified database.
3. For each table:
   - Identifies the primary key.
   - Checks if a filter column exists (default: created_at).
   - Retrieves the maximum primary key value for a given date.
4. Prints results in a PrettyTable sorted by table name, comparing all hosts.
5. Highlights:
   - Discrepancies across nodes (red).
   - Missing records on the given date (yellow "N/A").
   - Tables without the filter column.
   - Tables without a primary key.
   - Tables that have no record on the given date.

Dependencies:
-------------
- Python 3.x
- pymysql
- prettytable

Install dependencies:
    pip install pymysql prettytable

Usage:
------
python galera_pk_discrepancy_checker.py --hosts host1:3306,host2,host3:3307 \
                                        --user myuser --pass mypass --db database \
                                        [--column created_at] [--days-ago 1]

Arguments:
----------
--hosts      Comma-separated list of hosts (host:port). Default port is 3306.
--user       MySQL username.
--pass       MySQL password.
--db         Database name.
--column     Column to filter by (default: created_at).
--days-ago   Number of days ago to check (default: 1 = yesterday).

Sample output:
---------------
[INFO] Using filter column: created_at, date: 2025-09-28
[INFO] Target hosts: node1:3306, node2:3306, node3:3306
[INFO] Scanned users on node1:3306
[INFO] Scanned users on node2:3306
[INFO] Scanned users on node3:3306
[INFO] Scanned orders on node1:3306
[INFO] Scanned orders on node2:3306
[INFO] Scanned orders on node3:3306
[INFO] Scanned products on node1:3306
[INFO] Scanned products on node2:3306
[INFO] Scanned products on node3:3306

+------------------+-------------+-------------+-------------+-------------+
|    Table Name    | Primary Key | node1:3306  | node2:3306  | node3:3306  |
+------------------+-------------+-------------+-------------+-------------+
| orders           | order_id    | 987654      | 987654      | 987654      |
| products         | product_id  | 44567       | 44566       | 44567       |
| sessions         | session_id  | N/A         | N/A         | N/A         |
| users            | user_id     | 12045       | 12045       | 12044       |
+------------------+-------------+-------------+-------------+-------------+

Summary:
---------
Tables missing 'created_at':
 - sessions

Tables without primary key:
 - (none)

Tables with no record on 2025-09-28:
 - (none)

"""

import pymysql
from pymysql.constants import CLIENT
from datetime import datetime, timedelta
from prettytable import PrettyTable
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import argparse
import sys

# ANSI color codes
RED = "\033[91m"
YELLOW = "\033[93m"
GREEN = "\033[92m"
RESET = "\033[0m"

lock = threading.Lock()  # for thread-safe printing


def parse_hosts(hosts_arg):
    hosts = []
    for h in hosts_arg.split(","):
        if ":" in h:
            host, port = h.split(":")
            hosts.append((host.strip(), int(port.strip())))
        else:
            hosts.append((h.strip(), 3306))
    return hosts


def get_connection(host, port, user, password, database):
    try:
        return pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            connect_timeout=5,
            read_timeout=15,
            write_timeout=15,
            client_flag=CLIENT.MULTI_STATEMENTS,
        )
    except Exception as e:
        with lock:
            print(f"{YELLOW}[WARN] Could not connect to {host}:{port} - {e}{RESET}")
        return None


def get_tables(conn):
    with conn.cursor() as cur:
        cur.execute("SHOW TABLES")
        return [row[0] for row in cur.fetchall()]


def get_primary_key(conn, table, database):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_KEY = 'PRI'
        """,
            (database, table),
        )
        row = cur.fetchone()
        return row[0] if row else None


def has_filter_column(conn, table, database, column):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s
        """,
            (database, table, column),
        )
        return cur.fetchone() is not None


def get_last_record(conn, table, pk_col, filter_column, target_date):
    with conn.cursor() as cur:
        query = f"""
            SELECT {pk_col}
            FROM {table}
            WHERE {filter_column} >= %s AND {filter_column} < %s + INTERVAL 1 DAY
            ORDER BY {filter_column} DESC
            LIMIT 1
        """
        cur.execute(query, (target_date, target_date))
        row = cur.fetchone()
        return row[0] if row else None


def scan_table(host, port, user, password, database, table, filter_column, target_date):
    conn = get_connection(host, port, user, password, database)
    if not conn:
        return (table, f"{host}:{port}", None, "CONN_ERR")

    try:
        pk_col = get_primary_key(conn, table, database)
        if not pk_col:
            return (table, f"{host}:{port}", None, "NO_PK")

        if not has_filter_column(conn, table, database, filter_column):
            return (table, f"{host}:{port}", pk_col, "NO_COLUMN")

        try:
            last_id = get_last_record(conn, table, pk_col, filter_column, target_date)
            return (table, f"{host}:{port}", pk_col, last_id if last_id else "NO_RECORD")
        except Exception as e:
            with lock:
                print(f"{YELLOW}[WARN] Error fetching {table} on {host}:{port} - {e}{RESET}")
            return (table, f"{host}:{port}", pk_col, "ERR")

    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Check last record per table across multiple MySQL hosts.")
    parser.add_argument("--hosts", required=True, help="Comma-separated list of hosts (e.g. host1:3306,host2,host3:3307)")
    parser.add_argument("--user", required=True, help="MySQL username")
    parser.add_argument("--pass", required=True, help="MySQL password")
    parser.add_argument("--db", required=True, help="Database name")
    parser.add_argument("--column", default="created_at", help="Column to filter on (default: created_at)")
    parser.add_argument("--days-ago", type=int, default=1, help="How many days ago to fetch (default: 1 = yesterday)")
    args = parser.parse_args()

    hosts = parse_hosts(args.hosts)
    user = args.user
    password = args.__dict__["pass"]
    database = args.db
    filter_column = args.column
    target_date = (datetime.now() - timedelta(days=args.days_ago)).date()

    print(f"{GREEN}[INFO] Using filter column: {filter_column}, date: {target_date}{RESET}")
    print(f"{GREEN}[INFO] Target hosts: {', '.join([f'{h}:{p}' for h, p in hosts])}{RESET}")

    conn = None
    for host, port in hosts:
        conn = get_connection(host, port, user, password, database)
        if conn:
            break
    if not conn:
        print(f"{RED}[FATAL] Could not connect to any host{RESET}")
        sys.exit(1)

    tables = get_tables(conn)
    conn.close()

    results = {}
    missing_column = []
    missing_pk = []
    missing_record = []

    tasks = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        for host, port in hosts:
            for table in tables:
                tasks.append(
                    executor.submit(scan_table, host, port, user, password, database, table, filter_column, target_date)
                )

        for future in as_completed(tasks):
            table, host, pk_col, value = future.result()
            with lock:
                print(f"{GREEN}[INFO] Scanned {table} on {host}{RESET}")

            if table not in results:
                results[table] = {"pk": pk_col, "values": {}}
            if pk_col:
                results[table]["pk"] = pk_col

            if value == "NO_COLUMN":
                if table not in missing_column:
                    missing_column.append(table)
                continue
            elif value == "NO_PK":
                if table not in missing_pk:
                    missing_pk.append(table)
                continue
            elif value == "NO_RECORD":
                if table not in missing_record:
                    missing_record.append(table)
                value = f"{YELLOW}N/A{RESET}"

            results[table]["values"][host] = value

    pt = PrettyTable()
    pt.field_names = ["Table Name", "Primary Key"] + [f"{h}:{p}" for h, p in hosts]

    for tname in sorted(results.keys()):
        info = results[tname]
        row = [tname, info["pk"]]

        # Collect values across hosts to detect mismatches
        host_values = [
            str(info["values"].get(f"{h}:{p}", f"{YELLOW}N/A{RESET}"))
            for h, p in hosts
        ]

        # Strip color codes for mismatch detection
        clean_values = [
            v for v in host_values if "N/A" not in v and "ERR" not in v
        ]

        mismatch = len(set(clean_values)) > 1

        for idx, val in enumerate(host_values):
            if mismatch and "N/A" not in val and "ERR" not in val:
                host_values[idx] = f"{RED}{val}{RESET}"

        row.extend(host_values)
        pt.add_row(row)


    print(pt)

    # --- Summary ---
    if missing_column:
        print(f"\n{RED}Tables missing '{filter_column}':{RESET}")
        for t in sorted(missing_column):
            print(f" - {t}")

    if missing_pk:
        print(f"\n{RED}Tables without primary key:{RESET}")
        for t in sorted(missing_pk):
            print(f" - {t}")

    if missing_record:
        print(f"\n{YELLOW}Tables with no record on {target_date}:{RESET}")
        for t in sorted(missing_record):
            print(f" - {t}")


if __name__ == "__main__":
    main()

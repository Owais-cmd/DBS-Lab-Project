import os
import json
from fastapi import FastAPI, HTTPException, Depends, Form,APIRouter
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import psycopg2
from ..database import get_db, init_db
from ..models import User, Item, Order, OrderItem
from sqlalchemy import func
from sqlalchemy.orm import Session
from sqlalchemy import text
import time
from ..config import settings
from ..utils.security import get_current_user, require_admin

router = APIRouter(prefix="/indexes", tags=["Indexes"])

DATABASE_URL = settings.DATABASE_URL
RECS_FILE = os.path.join(os.path.dirname(__file__), "../../../data/recommendations.json")

def get_conn():
    return psycopg2.connect(DATABASE_URL)

@router.get("/recommendations")
def get_recommendations(admin: User = Depends(require_admin)):
    if not os.path.exists(RECS_FILE):
        return []
    with open(RECS_FILE, 'r') as f:
        recs = json.load(f)
    
    # Check if indexes actually exist in the database
    conn = get_conn()
    cur = conn.cursor()
    try:
        # Get all existing indexes for the tables in recommendations
        cur.execute("""
            SELECT tablename, indexname 
            FROM pg_indexes 
            WHERE schemaname = 'public'
        """)
        existing_indexes = cur.fetchall()
        
        # Create a set of (table, column) pairs from existing indexes
        # Index name format: idx_tablename_columnname
        indexed_columns = set()
        for table, idx_name in existing_indexes:
            # Parse index name to extract column (format: idx_table_column)
            if idx_name.startswith('idx_'):
                parts = idx_name.split('_', 2)  # Split into ['idx', 'table', 'column']
                if len(parts) >= 3:
                    column = parts[2]
                    indexed_columns.add((table, column))
        
        # Update index_exists field for each recommendation
        for rec in recs:
            rec['index_exists'] = (rec['table'], rec['column']) in indexed_columns
        
        return recs
    except Exception as e:
        # If there's an error, return recommendations as-is
        return recs
    finally:
        cur.close()
        conn.close()

@router.get("/list")
def get_indexes(admin: User = Depends(require_admin)):
    """Get current list of indexes created by the system"""
    conn = get_conn()
    cur = conn.cursor()
    try:
        # Get indexes from audit table that still exist in the database
        # Get the most recent create action for each index that still exists
        cur.execute("""
            SELECT 
                a.index_name,
                a.table_name,
                a.column_name,
                a.ts as created_at,
                a.user_name,
                COALESCE(pg_size_pretty(pg_relation_size(i.indexname::regclass)), 'N/A') as size
            FROM (
                SELECT DISTINCT ON (index_name)
                    index_name, table_name, column_name, ts, user_name
                FROM index_audit
                WHERE action = 'create'
                ORDER BY index_name, ts DESC
            ) a
            JOIN pg_indexes i ON i.indexname = a.index_name
            WHERE i.schemaname = 'public'
            ORDER BY a.ts DESC;
        """)
        indexes = []
        for row in cur.fetchall():
            indexes.append({
                "index_name": row[0],
                "table_name": row[1],
                "column_name": row[2],
                "created_at": row[3].isoformat() if row[3] else None,
                "user_name": row[4],
                "size": row[5] if row[5] else "N/A"
            })
        return indexes
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()
        conn.close()

class ApplyRequest(BaseModel):
    table: str
    column: str
    force: bool = False
    user: str = "api"

@router.post("/apply")
def apply_index(req: ApplyRequest, admin: User = Depends(require_admin)):
    # must set force true to actually create index
    # build index name
    idx_name = f"idx_{req.table}_{req.column}"
    create_sql = f'CREATE INDEX CONCURRENTLY IF NOT EXISTS {idx_name} ON {req.table}({req.column});'
    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()
    # simple dry-run: check if index exists
    cur.execute("SELECT indexname FROM pg_indexes WHERE tablename=%s;", (req.table,))
    existing = [r[0] for r in cur.fetchall()]
    if req.force:
        try:
            # Check current indexes count from audit table (only successful creates)
            if(idx_name in existing):
                return {"status":"exists", "index": idx_name}
            cur.execute("""SELECT *
              FROM (
                 SELECT DISTINCT ON (index_name)
                    index_name, table_name, column_name, ts
                    FROM index_audit
                    WHERE action = 'create'
                    AND index_name IN (
                        SELECT indexname FROM pg_indexes 
                        WHERE schemaname = 'public'
                    )
                ORDER BY index_name, ts DESC   -- pick the latest row per index
                ) sub
                ORDER BY ts ASC;                  -- now sort the final results
            """)
            current_indexes = cur.fetchall()

            
            # If we have 3 or more indexes, delete the oldest one
            deleted_index = None
            if len(current_indexes) >= 3:
                oldest = current_indexes[0]  # oldest is first due to ORDER BY ts ASC
                oldest_idx_name = oldest[0]
                oldest_table = oldest[1]
                
                # Delete the oldest index
                drop_sql = f'DROP INDEX CONCURRENTLY IF EXISTS {oldest_idx_name};'
                try:
                    cur.execute(drop_sql)
                    # Audit the deletion
                    cur.execute("INSERT INTO index_audit (action,index_name,table_name,column_name,user_name,details) VALUES (%s,%s,%s,%s,%s,%s);",
                                ("delete", oldest_idx_name, oldest_table, oldest[2] if len(oldest) > 2 else None, req.user, json.dumps({"reason":"rotation_limit", "replaced_by": idx_name})))
                    deleted_index = oldest_idx_name
                except Exception as drop_err:
                    # Log drop failure but continue with creation
                    cur.execute("INSERT INTO index_audit (action,index_name,table_name,column_name,user_name,details) VALUES (%s,%s,%s,%s,%s,%s);",
                                ("delete_failed", oldest_idx_name, oldest_table, oldest[2] if len(oldest) > 2 else None, req.user, json.dumps({"error": str(drop_err)})))
            
            # Create the new index
            cur.execute(create_sql)
            # audit entry
            cur.execute("INSERT INTO index_audit (action,index_name,table_name,column_name,user_name,details) VALUES (%s,%s,%s,%s,%s,%s);",
                        ("create", idx_name, req.table, req.column, req.user, json.dumps({"note":"applied via API"})))
            conn.commit()
            
            result = {"status":"applied", "index": idx_name}
            if deleted_index:
                result["deleted_index"] = deleted_index
            return result
        except Exception as e:
            # try to record failure
            try:
                cur.execute("INSERT INTO index_audit (action,index_name,table_name,column_name,user_name,details) VALUES (%s,%s,%s,%s,%s,%s);",
                            ("create_failed", idx_name, req.table, req.column, req.user, json.dumps({"error": str(e)})))
                conn.commit()
            except:
                pass
            raise HTTPException(status_code=500, detail=str(e))
    else:
        return {"status":"dry-run", "index": idx_name, "exists": existing}


@router.get("/comparison/{idx_name}")
def comparison(idx_name: str, db: Session = Depends(get_db), admin: User = Depends(require_admin)):
    try:
        # 1️⃣ Get table + column for this index
        result = db.execute(
    text("""
        SELECT tablename, indexdef
        FROM pg_indexes
        WHERE indexname = :idx_name
    """),
    {"idx_name": idx_name}
).fetchone()

        if not result:
            raise HTTPException(404, detail="Index not found in pg_indexes")

        table_name = result[0]

        # extract column list
        # indexdef example: CREATE INDEX idx_customer_name ON customers USING btree (name)
        indexdef = result[1]
        col_part = indexdef.split("(")[1].split(")")[0]   # -> "name"
        column_list = col_part.split(",")

        column_name = column_list[0].strip()  # assume single-column index

        # -----------------------------------------------------------------------
        # 2️⃣ Connect raw psycopg2 (needed to force index or seq scan)
        # -----------------------------------------------------------------------
        conn = get_conn()
        cur = conn.cursor()

        # -----------------------------------------------------------------------
        # 3️⃣ FORCE SEQ SCAN
        # -----------------------------------------------------------------------
        cur.execute("SET enable_indexscan = OFF;")
        cur.execute("SET enable_bitmapscan = OFF;")

        seq_query = f"SELECT * FROM {table_name} WHERE {column_name} IS NOT NULL;"
        
        t1 = time.time()
        cur.execute(seq_query)
        cur.fetchall()
        seq_time = time.time() - t1

        # -----------------------------------------------------------------------
        # 4️⃣ FORCE INDEX SCAN
        # -----------------------------------------------------------------------
        cur.execute("RESET enable_indexscan ;")
        cur.execute("RESET enable_bitmapscan ;")

        idx_query = f"SELECT * FROM {table_name} WHERE {column_name} IS NOT NULL;"

        t2 = time.time()
        cur.execute(idx_query)
        cur.fetchall()
        idx_time = time.time() - t2

        # -----------------------------------------------------------------------
        # 5️⃣ Return comparison
        # -----------------------------------------------------------------------
        return {
            "index_name": idx_name,
            "table": table_name,
            "column": column_name,
            "seq_scan_time": seq_time,
            "index_scan_time": idx_time,
            "faster_scan": "index_scan" if idx_time < seq_time else "seq_scan"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        try:
            cur.close()
            conn.close()
        except:
            pass

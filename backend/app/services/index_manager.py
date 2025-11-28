from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Dict, Optional
import datetime


class IndexManager:
    """Manage database indexes."""
    
    def __init__(self, db: Session):
        self.db = db
    
    def create_index(self, table_name: str, column_name: str, index_type: str = "btree") -> Dict:
        """
        Create an index on a table column.
        
        Args:
            table_name: Name of the table
            column_name: Name of the column
            index_type: Type of index (btree, hash, gin, gist, etc.)
        
        Returns:
            Dict with status and message
        """
        index_name = f"idx_{table_name}_{column_name}_{index_type}"
        
        # Check if index already exists
        check_query = text("""
            SELECT 1 FROM pg_indexes 
            WHERE indexname = :index_name
        """)
        exists = self.db.execute(check_query, {"index_name": index_name}).fetchone()
        
        if exists:
            return {
                "status": "exists",
                "message": f"Index {index_name} already exists"
            }
        
        # Create index
        try:
            create_query = text(f"""
                CREATE INDEX {index_name} 
                ON {table_name} USING {index_type} ({column_name})
            """)
            self.db.execute(create_query)
            self.db.commit()
            
            # Log to audit (if you have an audit table, add it)
            # For now, just return success
            return {
                "status": "created",
                "message": f"Index {index_name} created successfully",
                "index_name": index_name
            }
        except Exception as e:
            self.db.rollback()
            return {
                "status": "error",
                "message": f"Failed to create index: {str(e)}"
            }
    
    def list_indexes(self) -> List[Dict]:
        """
        List all indexes in the database.
        
        Returns:
            List of index information
        """
        query = text("""
            SELECT 
                schemaname,
                tablename,
                indexname,
                indexdef
            FROM pg_indexes
            WHERE schemaname = 'public'
            ORDER BY tablename, indexname
        """)
        
        results = self.db.execute(query).fetchall()
        
        return [
            {
                "schema": r.schemaname,
                "table": r.tablename,
                "index_name": r.indexname,
                "definition": r.indexdef
            }
            for r in results
        ]
    
    def compare_query_plans(self, index_name: str, query: str) -> Dict:
        """
        Compare query execution plans with and without an index.
        
        Args:
            index_name: Name of the index
            query: SQL query to test
        
        Returns:
            Dict with both plans
        """
        try:
            # Get plan with index
            self.db.execute(text(f"SET enable_indexscan = on"))
            self.db.execute(text(f"SET enable_bitmapscan = on"))
            
            explain_with = self.db.execute(
                text(f"EXPLAIN ANALYZE {query}")
            ).fetchall()
            
            # Get plan without index (disable index scan)
            self.db.execute(text(f"SET enable_indexscan = off"))
            self.db.execute(text(f"SET enable_bitmapscan = off"))
            
            explain_without = self.db.execute(
                text(f"EXPLAIN ANALYZE {query}")
            ).fetchall()
            
            # Reset settings
            self.db.execute(text(f"SET enable_indexscan = on"))
            self.db.execute(text(f"SET enable_bitmapscan = on"))
            self.db.commit()
            
            return {
                "with_index": [row[0] for row in explain_with],
                "without_index": [row[0] for row in explain_without]
            }
        except Exception as e:
            self.db.rollback()
            return {
                "error": str(e)
            }
    
    def drop_index(self, index_name: str) -> Dict:
        """Drop an index."""
        try:
            query = text(f"DROP INDEX IF EXISTS {index_name}")
            self.db.execute(query)
            self.db.commit()
            return {
                "status": "dropped",
                "message": f"Index {index_name} dropped successfully"
            }
        except Exception as e:
            self.db.rollback()
            return {
                "status": "error",
                "message": f"Failed to drop index: {str(e)}"
            }

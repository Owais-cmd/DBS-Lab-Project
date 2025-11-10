# app/recommender_rules.py
import re
import os
import json
import pandas as pd

IN_CSV = os.path.join(os.path.dirname(__file__), "../data/pg_stats.csv")
OUT_JSON = os.path.join(os.path.dirname(__file__), "../data/recommendations.json")

def extract_candidates_from_query(query):
    # Extract columns used in WHERE and JOIN ON equalities, handling table aliases
    candidates = []
    
    # Build alias mapping: alias -> actual table name
    alias_map = {}
    
    # Parse FROM clause: FROM table_name [AS] alias
    from_pattern = r'FROM\s+([a-zA-Z0-9_]+)(?:\s+(?:AS\s+)?([a-zA-Z0-9_]+))?'
    from_match = re.search(from_pattern, query, flags=re.I)
    if from_match:
        table_name = from_match.group(1)
        alias = from_match.group(2) if from_match.group(2) else table_name
        alias_map[alias.lower()] = table_name
        alias_map[table_name.lower()] = table_name  # table name is also valid
    
    # Parse JOIN clauses: [LEFT|RIGHT|INNER|FULL] JOIN table_name [AS] alias ON ...
    join_pattern = r'(?:LEFT|RIGHT|INNER|FULL|CROSS)?\s+JOIN\s+([a-zA-Z0-9_]+)(?:\s+(?:AS\s+)?([a-zA-Z0-9_]+))?'
    join_matches = re.finditer(join_pattern, query, flags=re.I)
    for jm in join_matches:
        table_name = jm.group(1)
        alias = jm.group(2) if jm.group(2) else table_name
        alias_map[alias.lower()] = table_name
        alias_map[table_name.lower()] = table_name
    
    def resolve_table(identifier):
        """Resolve table name or alias to actual table name"""
        identifier_lower = identifier.lower()
        return alias_map.get(identifier_lower, identifier)
    
    # Extract WHERE clause columns
    # Pattern: WHERE [alias.]column [operator]
    where_pattern = r'WHERE\s+([a-zA-Z0-9_\.]+)\s*(=|>|<|>=|<=|LIKE|ILIKE|IN)\s*'
    where_matches = re.finditer(where_pattern, query, flags=re.I)
    for m in where_matches:
        col_expr = m.group(1)
        if '.' in col_expr:
            parts = col_expr.split('.', 1)
            alias_or_table = parts[0].strip()
            col_name = parts[1].strip()
            actual_table = resolve_table(alias_or_table)
            if actual_table:
                candidates.append((actual_table, col_name))
        else:
            # Column without table prefix - try to use FROM table
            if from_match:
                table_name = from_match.group(1)
                candidates.append((table_name, col_expr.strip()))
    
    # Extract JOIN ON clause columns
    # Pattern: ON [alias1.]col1 = [alias2.]col2
    join_on_pattern = r'ON\s+([a-zA-Z0-9_\.]+)\s*=\s*([a-zA-Z0-9_\.]+)'
    join_on_matches = re.finditer(join_on_pattern, query, flags=re.I)
    for m in join_on_matches:
        left_side = m.group(1).strip()
        right_side = m.group(2).strip()
        
        for side in [left_side, right_side]:
            if '.' in side:
                parts = side.split('.', 1)
                alias_or_table = parts[0].strip()
                col_name = parts[1].strip()
                actual_table = resolve_table(alias_or_table)
                if actual_table:
                    candidates.append((actual_table, col_name))
            else:
                # Column without prefix in JOIN ON - this is ambiguous, skip
                pass
    
    # Dedupe while preserving order
    uniq = list(dict.fromkeys(candidates))
    #print(f"[recommender] candidates: {uniq}")
    return uniq

def index_exists_on(conn, table, column):
    cur = conn.cursor()
    cur.execute("SELECT indexname FROM pg_indexes WHERE tablename=%s;", (table,))
    rows = cur.fetchall()
    cur.close()
    for r in rows:
        if column in r[0] or column in str(r):
            return True
    return False

def build_recommendations():
    if not os.path.exists(IN_CSV):
        print(f"[recommender] {IN_CSV} not found - run collector first")
        return
    df = pd.read_csv(IN_CSV)
    agg = {}
    for _, row in df.iterrows():
        q = row['query'] if isinstance(row['query'], str) else ""
        calls = int(row.get('calls', 0))
        total_time = float(row.get('total_exec_time', 0.0))
        mean_time = float(row.get('mean_exec_time', 0.0))
        candidates = extract_candidates_from_query(q)
        for (tbl,col) in candidates:
            key = (tbl,col)
            if key not in agg:
                agg[key] = {'calls':0,'total_time':0.0,'mean_time':0.0,'sample_queries':[]}
            agg[key]['calls'] += calls
            agg[key]['total_time'] += total_time
            agg[key]['sample_queries'].append(q)
    # build list
    recs = []
    # Use database connection to check for existing indexes (optional)
    import psycopg2
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://demo:demo@localhost:5432/demo")
    conn = psycopg2.connect(DATABASE_URL)
    for (tbl,col), stats in agg.items():
        # simple rules:
        # recommend if total calls > 50 and mean_time > 5ms and index not exists
        calls = stats['calls']
        avg_time = stats['total_time'] / max(calls,1)
        # check index_exists
        exists = index_exists_on(conn, tbl, col)
        should = calls >= 50 and not exists
        if should:
          recs.append({
            'table': tbl,
            'column': col,
            'calls': calls,
            'avg_time_ms': round(avg_time, 2),
            'index_exists': exists,
            'recommend': bool(should),
            'sample_query': stats['sample_queries'][0] if stats['sample_queries'] else None,
          })
          
    conn.close()
    # sort by calls * avg_time
    recs = sorted(recs, key=lambda r: r['calls']*r['avg_time_ms'], reverse=True)
    # Limit to top 3 recommendations
    recs = recs[:3]
    with open(OUT_JSON, 'w') as f:
        json.dump(recs, f, indent=2)
    print(f"[recommender] wrote {len(recs)} recs to {OUT_JSON}")

if __name__ == "__main__":
    build_recommendations()

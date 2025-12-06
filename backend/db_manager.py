#!/usr/bin/env python3
"""
Database Management Script
Utility commands for managing the database
"""

import sys
import psycopg2
from getpass import getpass

DB_CONFIG = {
    'dbname': 'demo',
    'user': 'demo',
    'password': 'demo',
    'host': 'localhost'
}


def get_conn():
    """Get database connection."""
    return psycopg2.connect(**DB_CONFIG)


def create_admin_user(email: str, password: str, name: str, city: str):
    """Create an admin user."""
    import hashlib
    from passlib.context import CryptContext
    
    pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
    hashed_password = pwd_context.hash(password)
    
    conn = get_conn()
    cur = conn.cursor()
    
    try:
        cur.execute(
            """
            INSERT INTO users (email, hashed_password, name, city, is_admin)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (email) DO UPDATE
            SET is_admin = true
            RETURNING id;
            """,
            (email, hashed_password, name, city, True)
        )
        user_id = cur.fetchone()[0]
        conn.commit()
        print(f"✅ Admin user created/updated: {email} (ID: {user_id})")
    except Exception as e:
        print(f"❌ Error: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()


def make_user_admin(email: str):
    """Make an existing user an admin."""
    conn = get_conn()
    cur = conn.cursor()
    
    try:
        cur.execute(
            "UPDATE users SET is_admin = true WHERE email = %s RETURNING id;",
            (email,)
        )
        result = cur.fetchone()
        if result:
            conn.commit()
            print(f"✅ User {email} is now an admin (ID: {result[0]})")
        else:
            print(f"❌ User {email} not found")
    except Exception as e:
        print(f"❌ Error: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()


def list_users(limit: int = 10):
    """List users in the database."""
    conn = get_conn()
    cur = conn.cursor()
    
    try:
        cur.execute(
            """
            SELECT id, email, name, city, is_admin, created_at
            FROM users
            ORDER BY id
            LIMIT %s;
            """,
            (limit,)
        )
        
        print(f"\n{'='*80}")
        print(f"{'ID':<6} {'Email':<30} {'Name':<20} {'City':<15} {'Admin'}")
        print(f"{'='*80}")
        
        for row in cur.fetchall():
            admin_flag = "✓" if row[4] else "✗"
            print(f"{row[0]:<6} {row[1]:<30} {row[2]:<20} {row[3]:<15} {admin_flag}")
        
        print(f"{'='*80}\n")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        cur.close()
        conn.close()


def reset_database():
    """Reset database (drop all tables)."""
    confirm = input("⚠️  This will DELETE ALL DATA. Type 'yes' to confirm: ")
    if confirm.lower() != 'yes':
        print("❌ Cancelled")
        return
    
    conn = get_conn()
    cur = conn.cursor()
    
    try:
        cur.execute("DROP TABLE IF EXISTS order_items CASCADE;")
        cur.execute("DROP TABLE IF EXISTS orders CASCADE;")
        cur.execute("DROP TABLE IF EXISTS items CASCADE;")
        cur.execute("DROP TABLE IF EXISTS users CASCADE;")
        cur.execute("DROP TABLE IF EXISTS index_audit CASCADE;")
        conn.commit()
        print("✅ Database reset complete. Run seed_db.py to repopulate.")
    except Exception as e:
        print(f"❌ Error: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()


def show_stats():
    """Show database statistics."""
    conn = get_conn()
    cur = conn.cursor()
    
    try:
        print("\n📊 Database Statistics")
        print("="*50)
        
        cur.execute("SELECT COUNT(*) FROM users;")
        print(f"👥 Users:       {cur.fetchone()[0]:,}")
        
        cur.execute("SELECT COUNT(*) FROM items;")
        print(f"📦 Items:       {cur.fetchone()[0]:,}")
        
        cur.execute("SELECT COUNT(*) FROM orders;")
        print(f"🛒 Orders:      {cur.fetchone()[0]:,}")
        
        cur.execute("SELECT COUNT(*) FROM order_items;")
        print(f"📋 Order Items: {cur.fetchone()[0]:,}")
        
        cur.execute("SELECT COUNT(*) FROM users WHERE is_admin = true;")
        print(f"👑 Admins:      {cur.fetchone()[0]:,}")
        
        print("="*50 + "\n")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        cur.close()
        conn.close()


def main():
    """Main CLI interface."""
    if len(sys.argv) < 2:
        print("""
Database Management Utility

Usage:
    python db_manager.py stats              - Show database statistics
    python db_manager.py list [N]           - List N users (default 10)
    python db_manager.py create-admin       - Create new admin user (interactive)
    python db_manager.py make-admin EMAIL   - Make existing user an admin
    python db_manager.py reset              - Reset database (WARNING: destructive)

Examples:
    python db_manager.py stats
    python db_manager.py list 20
    python db_manager.py make-admin user@example.com
    python db_manager.py create-admin
        """)
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == "stats":
        show_stats()
    
    elif command == "list":
        limit = int(sys.argv[2]) if len(sys.argv) > 2 else 10
        list_users(limit)
    
    elif command == "create-admin":
        print("\n📝 Create Admin User")
        print("="*50)
        email = input("Email: ")
        password = getpass("Password: ")
        name = input("Name: ")
        city = input("City: ")
        create_admin_user(email, password, name, city)
    
    elif command == "make-admin":
        if len(sys.argv) < 3:
            print("❌ Usage: python db_manager.py make-admin EMAIL")
            sys.exit(1)
        email = sys.argv[2]
        make_user_admin(email)
    
    elif command == "reset":
        reset_database()
    
    else:
        print(f"❌ Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()

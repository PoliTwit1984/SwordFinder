import psycopg2
from psycopg2 import sql
import os # For potentially getting system username if needed

def try_connection(user, dbname="postgres", password="swordfinder123"): # Added password
    try:
        conn_string = f"dbname='{dbname}' user='{user}' host='localhost' port='5432'"
        if password: # Only add password if provided, useful for users like 'postgres' or 'joewilson' that might not use this specific password
            conn_string += f" password='{password}'"
        
        # For 'joewilson', we might not need a password if pg_hba.conf allows trust or peer for local connections
        # However, the script provided a password, so let's try with it first.
        # If connecting as 'joewilson' fails with a password error, we can try without.
        if user == "joewilson" and password == "swordfinder123": # Specific case for joewilson if default pass fails
             try:
                 conn_no_pass = psycopg2.connect(dbname=dbname, user=user, host="localhost", port="5432")
                 conn_no_pass.autocommit = True
                 print(f"[i] Connected as user '{user}' to db '{dbname}' (no password needed or via peer/trust)")
                 return conn_no_pass
             except Exception:
                 # Fall through to try with password if no-password connect fails
                 pass


        conn = psycopg2.connect(conn_string)
        conn.autocommit = True # Important for DDL-like queries if any were run, though mostly SELECTs here
        print(f"[i] Successfully connected as user '{user}' to db '{dbname}'")
        return conn
    except Exception as e:
        print(f"[!] Could not connect as user '{user}' to db '{dbname}': {e}")
        return None

def list_databases(conn):
    databases = []
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
            databases = [row[0] for row in cur.fetchall()]
    except Exception as e:
        print(f"[!] Error listing databases: {e}")
    return databases

def list_roles(conn):
    roles = []
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT rolname FROM pg_roles;") # \du equivalent
            roles = [row[0] for row in cur.fetchall()]
    except Exception as e:
        print(f"[!] Error listing roles: {e}")
    return roles

def check_pitch_table(conn, dbname_for_log): # dbname_for_log to avoid conflict with dbname in try_connection
    try:
        with conn.cursor() as cur:
            # Check if statcast_pitches table exists first
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'statcast_pitches'
                );
            """)
            table_exists = cur.fetchone()[0]
            if table_exists:
                cur.execute("SELECT COUNT(*) FROM statcast_pitches;")
                count = cur.fetchone()[0]
                print(f"✅ Database: {dbname_for_log} — statcast_pitches table found with {count:,} pitches.")
            else:
                print(f"ℹ️ Database: {dbname_for_log} — statcast_pitches table NOT found.")
    except Exception as e:
        print(f"⚠️  Could not read from statcast_pitches in {dbname_for_log}: {e}")

def main():
    # Try likely usernames. Add more if needed.
    # The password 'swordfinder123' is assumed for 'swordfinder' role.
    # For 'postgres' and 'joewilson', password might not be needed or might be different.
    # The try_connection function will attempt with and without for 'joewilson'.
    
    # Get current system username as another candidate
    system_user = os.getlogin() if hasattr(os, 'getlogin') else "unknown_system_user"
    
    users_to_try = {
        "swordfinder": "swordfinder123",
        "postgres": None, # Often no password needed for local superuser, or might prompt/use peer
        "joewilson": None, # System username, might use peer/trust or prompt
    }
    if system_user not in users_to_try and system_user != "unknown_system_user":
        users_to_try[system_user] = None


    for user, passwd in users_to_try.items():
        print(f"\n🔍 Trying to connect as PostgreSQL user: '{user}'")
        # Initial connection to 'postgres' db to list roles and other databases
        # Use specific password for 'swordfinder', try None (or allow prompt/peer) for others
        conn_for_listing = try_connection(user, dbname="postgres", password=passwd if user == "swordfinder" else None)
        
        if conn_for_listing:
            print(f"--- Roles visible to user '{user}' ---")
            roles = list_roles(conn_for_listing)
            print(f"Roles found: {roles if roles else 'None or error listing.'}")

            print(f"--- Databases visible to user '{user}' ---")
            dbs = list_databases(conn_for_listing)
            print(f"Databases found: {dbs if dbs else 'None or error listing.'}")
            conn_for_listing.close() # Close this initial connection

            # Now, for each database found, try to connect to it as the current user
            # and check for the statcast_pitches table.
            if dbs:
                for db_name in dbs:
                    print(f"  → Checking database '{db_name}' as user '{user}'...")
                    # Use specific password for 'swordfinder', try None for others
                    db_specific_conn = try_connection(user, dbname=db_name, password=passwd if user == "swordfinder" else None)
                    if db_specific_conn:
                        check_pitch_table(db_specific_conn, db_name)
                        db_specific_conn.close()
            else:
                print(f"No databases listed for user '{user}', cannot check for tables.")
        else:
            print(f"Cannot proceed for user '{user}' due to initial connection failure.")

if __name__ == "__main__":
    main()

# Note: the module name is psycopg, not psycopg3
import psycopg

# Connect to an existing database
with psycopg.connect("dbname=hubertstinia user=hubertstinia") as conn:

    # Open a cursor to perform database operations
    with conn.cursor() as cur:

        # Open a cursor to perform database operations
            
            #cur.execute("""
            #CREATE TABLE generation
            #""") 
            
            # Execute a command: this creates a new table
            cur.execute('ALTER TABLE generation ADD PRIMARY KEY (index);')
            conn.commit()
            conn.close()
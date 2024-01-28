import os

# Define the source and target database details


# Define the command to dump the source database to a file
dump_cmd = f"pg_dump -U {source_db['user']} -h {source_db['host']} -d {source_db['dbname']} -F c -t emissions_historical -t generation_historical -t load_historical -t prices_historical > dump.sql"

# Execute the dump command
os.system(dump_cmd)

# Define the command to import the dump file into the target database
import_cmd = f"pg_restore -U {target_db['user']} -h {target_db['host']} -d {target_db['dbname']} -F c -t emissions_historical -t generation_historical -t load_historical -t prices_historical < dump.sql"

# Execute the import command
os.system(import_cmd)

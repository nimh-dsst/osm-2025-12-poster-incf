import duckdb

# Connect to the database
con = duckdb.connect('pmcid_registry.duckdb')

# 1. List and Print Tables
print("############ TABLES ############")
tables = con.execute("SHOW TABLES").fetchall()
table_list = [t[0] for t in tables]
print(table_list)

# 2. Sample data from the first table found
if table_list:
    target_table = table_list[1]
    print(f"\n############ 50 RANDOM ROWS FROM '{target_table}' ############")
    
    # Execute query
    result = con.execute(f"SELECT * FROM {target_table} USING SAMPLE reservoir(50 ROWS)")
    
    # Get column names
    columns = [desc[0] for desc in result.description]
    
    # Print Headers
    print(columns)
    print("-" * 50)
    
    # Print Rows
    for row in result.fetchall():
        print(row)
else:
    print("No tables found in database.")


import duckdb

# Connect to the database
con = duckdb.connect('pmcid_registry.duckdb')

# 1. Get the table name automatically
tables = con.execute("SHOW TABLES").fetchall()

if tables:
    table_name = tables[1][0]
    print(f"Querying table: {table_name}...\n")

    # 2. Select distinct values
    # We filter out NULLs and sort alphabetically for readability
    query = f"""
        SELECT DISTINCT source_tarball 
        FROM {table_name} 
        WHERE source_tarball IS NOT NULL 
        ORDER BY source_tarball
    """
    
    results = con.execute(query).fetchall()
    
    # 3. Print results
    print(f"Found {len(results)} unique values:")
    print("-" * 30)
    
    for row in results:
        print(row[0])
        
else:
    print("No tables found in the database.")


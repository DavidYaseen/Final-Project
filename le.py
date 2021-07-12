import psycopg2


try:
    con = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="ZHOULE")

    # For isolation: SERIALIZABLE
    con.set_isolation_level(3)
    # For atomicity
    con.autocommit = False

    cur = con.cursor()

    print("PostgreSQL server information")
    print(con.get_dsn_parameters(), "\n")
    cur.execute("SELECT version();")
    record = cur.fetchone()
    print("You are connected to -", record, "\n")
    # QUERY

    create_table_query = "CREATE TABLE IF NOT EXISTS STUDENT (" \
                         "id INT PRIMARY KEY," \
                         "name VARCHAR (50) NOT NULL," \
                         "course VARCHAR (50) NOT NULL" \
                         ")"
    cur.execute(create_table_query)

    insert_query = "INSERT INTO STUDENT (id, name, course)" \
                   "VALUES (%s, %s, %s)"
    cur.executemany(insert_query, [(1, "LeZhou", "CS623"), (2, "DavidYaseen", "CS623")])

    showdata_query = "SELECT * FROM STUDENT ORDER BY name"
    cur.execute(showdata_query)
    record = cur.fetchall()
    for row in record:
        print(row)


except (Exception, psycopg2.DatabaseError) as err:
    print(err)
    print("Transactions could not be completed so database will be rolled back before start of transactions")
    con.rollback()
finally:
    if con:
        con.commit()
        cur.close
        con.close
        print("PostgreSQL connection is now closed")
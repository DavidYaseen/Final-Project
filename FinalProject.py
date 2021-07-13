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

    # create table
    create_table_product_query = "CREATE TABLE IF NOT EXISTS Product (" \
                         "prod VARCHAR(10) PRIMARY KEY," \
                         "pname VARCHAR(10) NOT NULL," \
                         "price NUMERIC(10,1) CHECK (price > 0)" \
                         ")"
    cur.execute(create_table_product_query)

    create_table_depot_query = "CREATE TABLE IF NOT EXISTS Depot (" \
                                 "dep VARCHAR(10) PRIMARY KEY," \
                                 "addr VARCHAR(50) NOT NULL," \
                                 "volume INT CHECK (volume > 0)" \
                                 ")"
    cur.execute(create_table_depot_query)

    create_table_stock_query = "CREATE TABLE IF NOT EXISTS Stock (" \
                                 "prod VARCHAR(10)," \
                                 "dep VARCHAR(10)," \
                                 "quantity INT NOT NULL," \
                                 "FOREIGN KEY (prod) REFERENCES Product (prod)," \
                                 "FOREIGN KEY (dep) REFERENCES Depot (dep)" \
                                 ")"
    cur.execute(create_table_stock_query)

    # insert data
    product_data = [('p1', 'tape', 2.5),
                    ('p2', 'tv', 250),
                    ('p3', 'vcr', 80)]
    depot_data = [('d1', 'New York', 9000),
                  ('d2', 'Syracuse', 6000),
                  ('d4', 'New York', 2000)]
    stock_data = [('p1', 'd1', 1000),
                  ('p1', 'd2', -100),
                  ('p1', 'd4', 1200),
                  ('p3', 'd1', 3000),
                  ('p3', 'd4', 2000),
                  ('p2', 'd4', 1500),
                  ('p2', 'd1', -400),
                  ('p2', 'd2', 2000)]

    insert_product_query = "INSERT INTO Product (prod, pname, price)" \
                   "VALUES (%s, %s, %s)"
    cur.executemany(insert_product_query, product_data)

    insert_depot_query = "INSERT INTO Depot (dep, addr, volume)" \
                           "VALUES (%s, %s, %s)"
    cur.executemany(insert_depot_query, depot_data)

    insert_stock_query = "INSERT INTO Stock (prod, dep, quantity)" \
                           "VALUES (%s, %s, %s)"
    cur.executemany(insert_stock_query, stock_data)

    # show tables
    show_product_query = "SELECT * FROM Product"
    cur.execute(show_product_query)
    record = cur.fetchall()
    for row in record:
        print(row)

    show_depot_query = "SELECT * FROM Depot"
    cur.execute(show_depot_query)
    record = cur.fetchall()
    for row in record:
        print(row)

    show_stock_query = "SELECT * FROM Stock"
    cur.execute(show_stock_query)
    record = cur.fetchall()
    for row in record:
        print(row)

    print('---------------------result-------------------------')

    # delete p1 from product and stock
    deletep1_stock_query = "DELETE FROM Stock WHERE prod = 'p1'"
    cur.execute(deletep1_stock_query)

    deletep1_product_query = "DELETE FROM Product WHERE prod = 'p1'"
    cur.execute(deletep1_product_query)

    # show results
    show_product_query = "SELECT * FROM Product"
    cur.execute(show_product_query)
    record = cur.fetchall()
    for row in record:
        print(row)

    show_depot_query = "SELECT * FROM Depot"
    cur.execute(show_depot_query)
    record = cur.fetchall()
    for row in record:
        print(row)

    show_stock_query = "SELECT * FROM Stock"
    cur.execute(show_stock_query)
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

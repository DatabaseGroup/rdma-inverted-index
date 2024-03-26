import duckdb
import sys
from tqdm import tqdm


def output_list(list_id, q):
    print(f"{list_id}: {' '.join([str(item[0]) for item in q.fetchall()])}")


if __name__ == "__main__":
    tables = []

    customer_csv = "tables/customer.tbl"
    customer_header = ["C_CUSTKEY", "C_NAME", "C_ADDRESS", "C_CITY", "C_NATION", "C_REGION", "C_PHONE", "C_MKTSEGMENT"]
    customer_types = ["UINTEGER", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR"]
    tables.append(("customer", customer_csv, customer_header, customer_types))

    lineorder_csv = "tables/lineorder.tbl"
    lineorder_header = ["LO_ORDERKEY", "LO_LINENUMBER", "LO_CUSTKEY", "LO_PARTKEY", "LO_SUPPKEY", "LO_ORDERDATE", "LO_ORDERPRIORITY", "LO_SHIPPRIORITY", "LO_QUANTITY", "LO_EXTENDEDPRICE", "LO_ORDTOTALPRICE", "LO_DISCOUNT", "LO_REVENUE", "LO_SUPPLYCOST", "LO_TAX", "LO_COMMITDATE", "LO_SHIPMODE"]
    lineorder_types = ["UINTEGER", "UINTEGER", "UINTEGER", "UINTEGER", "UINTEGER", "DATE", "VARCHAR", "UINTEGER", "UINTEGER", "UINTEGER", "UINTEGER", "UINTEGER", "UINTEGER", "UINTEGER", "UINTEGER", "DATE", "VARCHAR"]
    tables.append(("lineorder", lineorder_csv, lineorder_header, lineorder_types))

    part_csv = "tables/part.tbl"
    part_header = ["P_PARTKEY", "P_NAME", "P_MFGR", "P_CATEGORY", "P_BRAND", "P_COLOR", "P_TYPE", "P_SIZE", "P_CONTAINER"]
    part_types = ["UINTEGER", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "UINTEGER", "VARCHAR"]
    tables.append(("part", part_csv, part_header, part_types))

    supplier_csv = "tables/supplier.tbl"
    supplier_header = ["S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_CITY", "S_NATION", "S_REGION", "S_PHONE"]
    supplier_types = ["UINTEGER", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR"]
    tables.append(("supplier", supplier_csv, supplier_header, supplier_types))

    # date_csv = "tables/date.tbl"
    # date_header = ["D_DATEKEY", "D_DATE", "D_DAYOFWEEK", "D_MONTH", "D_YEAR", "D_YEARMONTHNUM", "D_YEARMONTH", "D_DAYNUMINWEEK", "D_DAYNUMINMONTH", "D_DAYNUMINYEAR", "D_MONTHNUMINYEAR", "D_WEEKNUMINYEAR", "D_SELLINGSEASON", "D_LASTDAYINWEEKFL", "D_LASTDAYINMONTHFL", "D_HOLIDAYFL", "D_WEEKDAYFL"]
    # date_types = ["UINTEGER", "VARCHAR", "VARCHAR", "VARCHAR", "UINTEGER", "UINTEGER", "VARCHAR", "UINTEGER", "UINTEGER", "UINTEGER", "UINTEGER", "UINTEGER", "VARCHAR", "BOOLEAN", "BOOLEAN", "BOOLEAN", "BOOLEAN"]
    # tables.append(("date", date_csv, date_header, date_types))

    # create and read tables
    print("creating tables...", file=sys.stderr)
    for (name, csv, header, types) in tables:
        read_csv = f"read_csv_auto('{csv}', names={header}, types={types})"
        duckdb.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT {', '.join(header)} FROM {read_csv};")

    # create full table
    print("creating full table...", file=sys.stderr)
    duckdb.execute("CREATE OR REPLACE TABLE lineorderfull AS \
                   SELECT row_number() OVER () as id, * \
                   FROM lineorder, customer, part, supplier \
                   WHERE LO_CUSTKEY=C_CUSTKEY AND LO_PARTKEY=P_PARTKEY AND LO_SUPPKEY=S_SUPPKEY;")

    # SELECTION = "count(*)"
    SELECTION = "id"
    lists = []

    # define queries
    # Q1.1
    l0 = duckdb.sql(f"SELECT {SELECTION} FROM lineorderfull WHERE year(LO_ORDERDATE)=1993;")
    l1 = duckdb.sql(f"SELECT {SELECTION} FROM lineorderfull WHERE LO_DISCOUNT BETWEEN 1 AND 3;")
    l2 = duckdb.sql(f"SELECT {SELECTION} FROM lineorderfull WHERE LO_QUANTITY < 25;")
    lists.extend([l0, l1, l2])

    # Q1.2
    l3 = duckdb.sql(f"SELECT {SELECTION} FROM lineorderfull WHERE year(LO_ORDERDATE)||month(LO_ORDERDATE)='19941';")
    l4 = duckdb.sql(f"SELECT {SELECTION} FROM lineorderfull WHERE LO_DISCOUNT BETWEEN 4 AND 6;")
    l5 = duckdb.sql(f"SELECT {SELECTION} FROM lineorderfull WHERE LO_QUANTITY BETWEEN 26 AND 35;")
    lists.extend([l3, l4, l5])

    # Q1.3
    l6 = duckdb.sql(f"SELECT {SELECTION} FROM lineorderfull WHERE weekofyear(LO_ORDERDATE)=6;")
    l7 = duckdb.sql(f"SELECT {SELECTION} FROM lineorderfull WHERE year(LO_ORDERDATE)=1994;")
    l8 = duckdb.sql(f"SELECT {SELECTION} FROM lineorderfull WHERE LO_DISCOUNT BETWEEN 5 AND 7;")
    l9 = l5
    lists.extend([l6, l7, l8, l9])

    # Q2.1
    l10 = duckdb.sql(f"SELECT {SELECTION} FROM lineorderfull WHERE P_CATEGORY='MFGR#12';")
    l11 = duckdb.sql(f"SELECT {SELECTION} FROM lineorderfull WHERE S_REGION='AMERICA';")
    lists.extend([l10, l11])

    # Q2.2
    l12 = duckdb.sql(f"SELECT {SELECTION} FROM lineorderfull WHERE P_BRAND>='MFGR#2221';")
    l13 = duckdb.sql(f"SELECT {SELECTION} FROM lineorderfull WHERE P_BRAND<='MFGR#2228';")
    l14 = duckdb.sql(f"SELECT {SELECTION} FROM lineorderfull WHERE S_REGION='ASIA';")
    lists.extend([l12, l13, l14])

    # Q2.3
    l15 = duckdb.sql(f"SELECT {SELECTION} FROM lineorderfull WHERE P_BRAND='MFGR#2239';")
    l16 = duckdb.sql(f"SELECT {SELECTION} FROM lineorderfull WHERE S_REGION='EUROPE';")
    lists.extend([l15, l16])

    # Q3.1
    l17 = duckdb.sql(f"SELECT {SELECTION} FROM lineorderfull WHERE C_REGION='ASIA';")
    l18 = l14
    l19 = duckdb.sql(f"SELECT {SELECTION} FROM lineorderfull WHERE year(LO_ORDERDATE)>=1992;")
    l20 = duckdb.sql(f"SELECT {SELECTION} FROM lineorderfull WHERE year(LO_ORDERDATE)<=1997;")
    lists.extend([l17, l18, l19, l20])

    # Q3.2
    l21 = duckdb.sql(f"SELECT {SELECTION} FROM lineorderfull WHERE C_NATION='UNITED STATES';")
    l22 = duckdb.sql(f"SELECT {SELECTION} FROM lineorderfull WHERE S_NATION='UNITED STATES';")
    l23 = l19
    l24 = l20
    lists.extend([l21, l22, l23, l24])

    # MILC query
    l25 = duckdb.sql(f"SELECT {SELECTION} FROM lineorderfull WHERE C_REGION='AMERICA';")
    l26 = duckdb.sql(f"SELECT {SELECTION} FROM lineorderfull WHERE S_REGION='AMERICA';")
    l27 = duckdb.sql(f"SELECT {SELECTION} FROM lineorderfull WHERE year(LO_ORDERDATE)=1997;")
    l28 = duckdb.sql(f"SELECT {SELECTION} FROM lineorderfull WHERE P_MFGR='MFGR#1';")
    lists.extend([l25, l26, l27, l28])

    # output lists
    print("output lists...", file=sys.stderr)
    print(len(lists))
    print(len(lists))
    for list_id, list_query in tqdm(enumerate(lists), total=len(lists)):
        output_list(list_id, list_query)

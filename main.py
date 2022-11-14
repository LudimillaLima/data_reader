# Program created for reading a csv file and insert into a database
# Ludimilla Lima
import mysql.connector
from mysql.connector import errorcode
import csv

def database_connection():
    try:
        connection = mysql.connector.connect(
        host = 'xxxxxxx',
        user = 'xxxxxxx',
        password = 'xxxxxxx',
        database='xxxxxxx'
    )
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Wrong user or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Error on database")
        else:
            print(err)
    return connection


def read_csv(filename):
    with open(filename, 'r') as file:
        reader = csv.reader(file)
        header = next(reader)
        fileData = []
        for row in reader:
            fileData.append(row)
    return header, fileData

# insert a item from the supply on the database
def insert_item(cursor, data):

    for i in range(0, len(data)-1):
        item_data = list(data[i])
        query = ("Select * from item where item = %s AND item_price = %s")
        cursor.execute(query, (item_data[0], item_data[1]))
        records = cursor.fetchall()
        print(records)
        if cursor.rowcount > 0:
            print("Data already in the database")
        else:
            insert_query = ("insert into item(item, item_price) values(%s, %s)")
            cursor.execute(insert_query, (item_data[0], item_data[1]))

# Insert a region into the database
def insert_region(cursor, data):
    for i in range(0, len(data)-1):
        item_data = list(data[i])
        query = ("Select * from region where region = %s")
        cursor.execute(query, item_data)
        
        if cursor.rowcount > 0:
            print("Data already in the database")
        else:
            insert_query = ("insert into region(region) values(%s)")
            cursor.execute(insert_query, (item_data))


# Insert a responsible into the database
def insert_resp(cursor, data):

    for i in range(0, len(data)-1):
        item_data = list(data[i])
        query = ("Select * from responsible where respon = %s")
        cursor.execute(query, (item_data))
        #records = cursor.fetchall()
        if cursor.rowcount > 0:
            print("Data already in the database")
        else:
            insert_query = ("insert into responsible(respon) values(%s)")
            cursor.execute(insert_query, (item_data))

# Insert a order into the database, with basic infos and ids
def insert_item_order(cursor, data):
    
    for i in range(0, len(data)-1):
        item_data = list(data[i])
        reg = item_data[1]
        get_region_id = "select region_id from region where region = %s"
        get_resp_id = "select id from responsible where respon = %s"
        get_item_id = "select item_id from item where item = %s and item_price = %s"
        
        cursor.execute(get_region_id, [reg])
        reg_id = list(cursor.fetchone())
        print(reg_id[0])

        cursor.execute(get_resp_id, ([item_data[2]]))
        resp_id = list(cursor.fetchone())
        print(resp_id[0])


        cursor.execute(get_item_id, (item_data[3], item_data[5]))
        item_id = list(cursor.fetchone())

        insert_query = "insert into item_order(order_date, region_id, id_resp, item_id, units) values( %s, %s, %s, %s, %s)"
        cursor.execute(insert_query, (item_data[0], reg_id[0], resp_id[0], item_id[0], item_data[5]))
        
def main():
    # read the csv file
    header, fileData = read_csv("OfficeSupplies.csv")
    con = database_connection()

    cursor = con.cursor(buffered=True)
    
    #save the data into separated variables
    items = []
    regions = []
    responsible = []
    item_order = []

    for i in fileData:
        items.append({ i[3], i[5] })
        regions.append({i[1]})
        responsible.append({i[2]})
        item_order.append(i)
    
    # insert data into the database
    insert_item(cursor, items)
    insert_region(cursor, regions)
    insert_resp(cursor, responsible)
    con.commit()
    insert_item_order(cursor, item_order)
    con.commit()
    #close connection
    con.close()

if __name__ == "__main__":
    main()


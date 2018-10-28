import pika
import sqlite3
import json
from sqlite3 import Error
from abc import ABC, abstractmethod


class Writer(ABC):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def create_output(self, cur):
        pass


class CSVWriter(Writer):
    def __init__(self):
        super().__init__()

    def create_output(self, cur):

        result_set = ''
        first_row = True
        apostrophe = "'"
        tmp_str = ''

        for row in cur.fetchall():

            if first_row:
                for i, value in enumerate(row):
                    tmp_str += cur.description[i][0] + ','
                result_set += tmp_str[:-1] + '\n'
                first_row = False

            tmp_str = ''
            for i, value in enumerate(row):
                tmp_str += apostrophe + str(value) + apostrophe + ','
            result_set += tmp_str[:-1] + '\n'

        print(result_set)


class XMLWriter(Writer):
    def __init__(self):
        super().__init__()

    def create_output(self, cur):

        result_set = '<?xml version="1.0" ?>\n'
        result_set += '<MyData>\n'
        for row in cur.fetchall():
            result_set += '  <row>\n'
            for i, value in enumerate(row):
                field_name = cur.description[i][0]
                result_set += '    <' + field_name + '>' + str(value) + '</' + field_name + '>\n'
            result_set += '  </row>\n'
            result_set += '</myData>\n'

        print(result_set)


class JSONWriter(Writer):
    def __init__(self):
        super().__init__()

    def create_output(self, cur):

        result_set = [dict((cur.description[i][0], value)
                           for i, value in enumerate(row)) for row in cur.fetchall()]

        print(json.dumps(result_set))


class TableWriter(Writer):
    def __init__(self):
        super().__init__()

    def create_output(self, cur):

        for row in cur.fetchall():
            print(row)


class DataRetriever:

    def __init__(self, writer: Writer, db_path):
        self.database = db_path
        self.conn = self.create_connection()
        self._writer = writer
        self.execute_queries()
        self.conn.close()

    def execute_queries(self):

        with self.conn:

            print("1. Query all tracks")
            self.execute_query("SELECT TrackId, t.Name,Composer,g.Name AS Genre "
                               "FROM tracks t JOIN genres g ON t.genreId = g.genreId")

            print("2. Query all customers and count albums")
            self.execute_query("SELECT CustomerId,FullName,Phone,Email,FullAddress,"
                               "COUNT(DISTINCT albumId) AS albums_ordered FROM "
                               "(SELECT c.CustomerId, FirstName || ' ' || LastName AS FullName,Phone,Email,"
                               "TRIM("
                               "COALESCE(Address || ' | ','') || "
                               "COALESCE(City || ' | ','') || "
                               "COALESCE(State || ' | ','') || "
                               "COALESCE(Country || ' | ','') || "
                               "COALESCE(PostalCode || ' ','')"
                               ") AS FullAddress, "
                               "a.albumId "
                               "FROM customers c LEFT JOIN invoices i ON c.customerId=i.customerId "
                               "LEFT JOIN invoice_items ii ON i.invoiceId=ii.invoiceId "
                               "LEFT JOIN tracks t ON ii.trackId=t.trackId "
                               "LEFT JOIN albums a ON t.albumId=a.albumId) t1 "
                               "GROUP BY CustomerId,FullName,Phone,Email,FullAddress")

            print("3. Count customers by domains in each country")
            self.execute_query("SELECT SUBSTR(domain,1,INSTR(domain,'.') - 1) AS short_domain,"
                               "country,COUNT(*) FROM "
                               "(SELECT SUBSTR(email,INSTR(email,'@')+1) AS domain, country "
                               "FROM customers) t1 "
                               "GROUP BY SUBSTR(domain,1,INSTR(domain,'.') - 1),country")

            print("4. Count albums ordered per country")
            self.execute_query("SELECT Country,COUNT(*) AS albums FROM "
                               "(SELECT DISTINCT Country,i.invoiceId,a.albumId "
                               "FROM customers c LEFT JOIN invoices i ON c.customerId=i.customerId "
                               "LEFT JOIN invoice_items ii ON i.invoiceId=ii.invoiceId "
                               "LEFT JOIN tracks t ON ii.trackId=t.trackId "
                               "LEFT JOIN albums a ON t.albumId=a.albumId) t1  "
                               "GROUP BY Country")

            print("5. Most ordered album per country")
            self.execute_query("WITH albums_per_country AS "
                               "(SELECT Country,albumId,title,COUNT(*) AS albums FROM "
                               "(SELECT DISTINCT Country,i.invoiceId,a.albumId,a.title "
                               "FROM customers c LEFT JOIN invoices i ON c.customerId=i.customerId "
                               "LEFT JOIN invoice_items ii ON i.invoiceId=ii.invoiceId "
                               "LEFT JOIN tracks t ON ii.trackId=t.trackId "
                               "LEFT JOIN albums a ON t.albumId=a.albumId) t1  "
                               "GROUP BY Country,albumId,title) "
                               "SELECT * FROM albums_per_country WHERE (Country,albums) IN "
                               "(SELECT Country,MAX(albums) FROM albums_per_country GROUP BY Country)")

            print("6. Most ordered album in USA since 2011")
            self.execute_query("WITH albums_per_country AS "
                               "(SELECT albumId,title,COUNT(*) AS albums FROM "
                               "(SELECT DISTINCT Country,i.invoiceId,a.albumId,a.title "
                               "FROM customers c LEFT JOIN invoices i ON c.customerId=i.customerId "
                               "LEFT JOIN invoice_items ii ON i.invoiceId=ii.invoiceId "
                               "LEFT JOIN tracks t ON ii.trackId=t.trackId "
                               "LEFT JOIN albums a ON t.albumId=a.albumId "
                               "WHERE Country='USA' "
                               "AND strftime('%Y', i.invoiceDate)>=2011) t1  "
                               "GROUP BY albumId,title) "
                               "SELECT * FROM albums_per_country WHERE (albums) IN "
                               "(SELECT MAX(albums) FROM albums_per_country)")

            print("7. Customers with an invoice missing 2 or more items")
            self.execute_query("SELECT * FROM "
                               "(SELECT DISTINCT c.*,"
                               "CASE WHEN i.CustomerId IS NULL THEN 1 ELSE 0 END + "
                               "CASE WHEN i.InvoiceDate IS NULL THEN 1 ELSE 0 END + "
                               "CASE WHEN i.BillingAddress IS NULL THEN 1 ELSE 0 END + "
                               "CASE WHEN i.BillingCity IS NULL THEN 1 ELSE 0 END + "
                               "CASE WHEN i.BillingState IS NULL THEN 1 ELSE 0 END + "
                               "CASE WHEN i.BillingCountry IS NULL THEN 1 ELSE 0 END + "
                               "CASE WHEN i.BillingPostalCode IS NULL THEN 1 ELSE 0 END + "
                               "CASE WHEN i.Total IS NULL THEN 1 ELSE 0 END as missing "
                               "FROM invoices i "
                               "JOIN customers c ON i.customerId=c.customerId) "
                               "WHERE missing>=2")

    def create_connection(self):

        # create a database connection
        try:
            conn = sqlite3.connect(self.database)
            return conn
        except Error as e:
            print(e)

        return None

    def execute_query(self, query_string: str):
        cur = self.conn.cursor()
        cur.execute(query_string)

        # rows = cur.fetchall()

        self._writer.create_output(cur)
        # for row in rows:
        #    print(row)


def get_writer_by_output_type(output_type: str):
    return {
        'JSON': JSONWriter,
        'XML': XMLWriter,
        'CSV': CSVWriter,
        'TBL': TableWriter
    }.get(output_type, JSONWriter)


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='127.0.0.1'))
    channel = connection.channel()

    channel.queue_declare(queue='q1')

    def callback(ch, method, properties, body):
        print(" [x] Received %r" % body)

        data = json.loads(body)
        db_path = data['database']
        output_type = data['type']

        try:
            DataRetriever(get_writer_by_output_type(output_type)(), db_path)
        except Error as e:
            print(e)

    channel.basic_consume(callback,
                          queue='q1',
                          no_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == '__main__':
    main()

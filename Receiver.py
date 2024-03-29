#!/usr/bin/env python
# encoding=utf8
import pika
import sqlite3
import csv
import json
import xml.etree.ElementTree as ET
import xml.dom.minidom
import sys
reload(sys)
sys.setdefaultencoding('utf8')

table1_unique_countries = set()
table2_unique_countries = set()
table3_unique_countries = set()


def callback(ch, method, properties, body):
    print("----- Message received -----")

    parsed_packet = body.split('$')
    db_path = parsed_packet[0]
    country = parsed_packet[1]
    year = parsed_packet[2]

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute('CREATE TABLE purchase_country(country NVARCHAR(40) PRIMARY KEY UNIQUE, purchases INT)')
    except sqlite3.OperationalError:
        print("Table purchase_country is already exist, continue without creation.")
    purchase_csv(cursor, country)

    try:
        cursor.execute('CREATE TABLE items_country(country NVARCHAR(40) PRIMARY KEY UNIQUE, itemsAmount INT)')
    except sqlite3.OperationalError:
        print("Table items_country is already exist, continue without creation.")
    items_csv(cursor, country)

    albums_json(cursor, country)

    try:
        cursor.execute('CREATE TABLE top_sellers(country NVARCHAR(40), title NVARCHAR(160), year INT, topSellerAmount INT, UNIQUE (country, year))')
    except sqlite3.OperationalError:
        print("Table top_sellers is already exist, continue without creation.")
    specific_xml(cursor, country, year)
    conn.commit()
    conn.close()


def purchase_csv(cursor, country):
    if country in table1_unique_countries:
        return
    outputfile = 'purchase-per-country.csv'
    result = cursor.execute('SELECT billingCountry, COUNT(*) as purchases '
                            'FROM invoices WHERE billingCountry=\''+country+'\'')
    with open(outputfile, mode='a') as purchase_file:
        purchase_writer = csv.writer(purchase_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for output in result:
            purchase_writer.writerow([output[0], output[1]])
            cursor.execute('INSERT or IGNORE INTO purchase_country VALUES(\''+output[0]+'\','+str(output[1])+')')
    table1_unique_countries.add(country)


def items_csv(cursor, country):
    if country in table2_unique_countries:
        return
    outputfile = 'items-per-country.csv'
    result = cursor.execute('SELECT invoices.BillingCountry, SUM(Quantity) as ItemsAmount '
                            'FROM invoice_items '
                            'JOIN invoices '
                            'ON invoices.InvoiceId==invoice_items.InvoiceId '
                            'WHERE billingCountry=\''+country+'\'')
    with open(outputfile, mode='a') as items_file:
        items_writer = csv.writer(items_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for output in result:
            items_writer.writerow([output[0], output[1]])
            cursor.execute('INSERT or IGNORE INTO items_country VALUES(\'' + output[0] + '\',' + str(output[1]) + ')')
    table2_unique_countries.add(country)


def albums_json(cursor, country):
    outputfile = 'albums-per-country.json'
    result = cursor.execute('SELECT DISTINCT invoices.BillingCountry, albums.Title '
                            'FROM invoices '
                            'JOIN invoice_items ON invoices.InvoiceId==invoice_items.InvoiceId '
                            'JOIN tracks ON tracks.TrackId==invoice_items.TrackId '
                            'JOIN albums ON albums.AlbumId==tracks.AlbumId '
                            'WHERE billingCountry=\''+country+'\' '
                            'ORDER BY BillingCountry')
    data = {}
    with open(outputfile, mode='w') as json_file:
        for output in result:
            if output[0] not in data:
                data[output[0]] = [output[1]]
            else:
                data[output[0]].append(output[1])
        json.dump(data, json_file, indent=3, ensure_ascii=False)


def specific_xml(cursor, country, year):
    if (country, year) in table3_unique_countries:
        print table3_unique_countries
        return
    outputfile = 'best-seller-rock-albums.xml'
    result = cursor.execute('SELECT BillingCountry, Title, Year, MAX(SellsAmount) as topSellerAmount '
                            'FROM('
                                'SELECT invoices.BillingCountry, albums.Title, '
                                'SUM(invoice_items.Quantity) as SellsAmount, '
                                'strftime(\'%Y\', invoices.InvoiceDate) as Year '
                                'FROM invoices JOIN invoice_items ON invoices.InvoiceId==invoice_items.InvoiceId '
                                'JOIN tracks ON tracks.TrackId==invoice_items.TrackId '
                                'JOIN albums ON albums.AlbumId==tracks.AlbumId '
                                'JOIN genres ON genres.GenreId==tracks.GenreId '
                                'WHERE Year>\'' + year + '\'AND genres.Name==\'Rock\' AND invoices.BillingCountry==\''+country+'\''
                                'GROUP BY albums.Title) '
                            'GROUP BY BillingCountry')
    with open(outputfile, mode='w') as xml_file:
        albums_info = ET.Element('albums_info')
        for output in result:
            country = ET.SubElement(albums_info, 'country')
            country.set('name', output[0])

            title = ET.SubElement(country, 'title')
            year = ET.SubElement(country, 'year')
            top_seller = ET.SubElement(country, 'top_seller')

            title.text = output[1]
            year.text = str(output[2])
            top_seller.text = str(output[3])
            cursor.execute('INSERT or IGNORE INTO top_sellers VALUES(\'' + output[0] + '\',\'' + output[1] + '\',' + str(output[2]) +',' + str(output[3]) + ')')

        mydata = ET.tostring(albums_info)
        dom = xml.dom.minidom.parseString(mydata)
        indent_xml = dom.toprettyxml()
        xml_file.write(indent_xml)
    table3_unique_countries.add((country, year))


def listen():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='query')
    channel.basic_consume(queue='query', on_message_callback=callback, auto_ack=True)

    print('-----  Waiting for messages... To exit press CTRL+C or stop execution')
    channel.start_consuming()


if __name__ == "__main__":
    listen()

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


def callback(ch, method, properties, body):
    print("----- Message received -----")

    parsed_packet = body.split('$')
    db_path = parsed_packet[0]
    country = parsed_packet[1]
    year = parsed_packet[2]

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    purchase_csv(cursor, country, year)
    items_csv(cursor, country, year)
    albums_json(cursor, country, year)
    specific_xml(cursor, country, year)
    conn.commit()
    conn.close()


def purchase_csv(cursor, country, year):
    outputfile = 'purchase-per-country.csv'
    result = cursor.execute('SELECT billingCountry, COUNT(*) FROM invoices GROUP BY billingCountry')
    with open(outputfile, mode='w') as purchase_file:
        purchase_writer = csv.writer(purchase_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for output in result:
            purchase_writer.writerow([output[0], output[1]])
    print "> " + outputfile + " was created"


def items_csv(cursor, country, year):
    outputfile = 'items-per-country.csv'
    result = cursor.execute('SELECT invoices.BillingCountry, SUM(Quantity) as ItemsAmount '
                            'FROM invoice_items '
                            'JOIN invoices '
                            'WHERE invoices.InvoiceId==invoice_items.InvoiceId '
                            'GROUP BY invoices.BillingCountry')
    with open(outputfile, mode='w') as items_file:
        items_writer = csv.writer(items_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for output in result:
            items_writer.writerow([output[0], output[1]])
    print "> " + outputfile + " was created"


def albums_json(cursor, country, year):
    outputfile = 'albums-per-country.json'
    result = cursor.execute('SELECT DISTINCT invoices.BillingCountry, albums.Title '
                            'FROM invoices '
                            'JOIN invoice_items ON invoices.InvoiceId==invoice_items.InvoiceId '
                            'JOIN tracks ON tracks.TrackId==invoice_items.TrackId '
                            'JOIN albums ON albums.AlbumId==tracks.AlbumId '
                            'ORDER BY BillingCountry')
    data = {}
    with open(outputfile, mode='w') as json_file:
        for output in result:
            if output[0] not in data:
                data[output[0]] = [output[1]]
            else:
                data[output[0]].append(output[1])
        json.dump(data, json_file, indent=3, ensure_ascii=False)
    print "> " + outputfile + " was created"


def specific_xml(cursor, country, year):
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
                                'WHERE Year>\'' + year + '\'AND genres.Name==\'Rock\' '
                                'GROUP BY albums.Title '
                                'ORDER BY BillingCountry) '
                            'GROUP BY BillingCountry')

    # belgium, Germany
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
        mydata = ET.tostring(albums_info)
        dom = xml.dom.minidom.parseString(mydata)
        indent_xml = dom.toprettyxml()
        xml_file.write(indent_xml)
    print "> " + outputfile + " was created"




def listen():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='query')
    channel.basic_consume(queue='query', on_message_callback=callback, auto_ack=True)

    print('-----  Waiting for messages... To exit press CTRL+C or stop execution')
    channel.start_consuming()


if __name__ == "__main__":
    listen()

"""
This is a fork/port of Phil Ashlock's Community Board scraper from scraperwiki:
    https://classic.scraperwiki.com/scrapers/city_representatives_-_nyc_community_boards/

"""
import scraperwiki
from bs4 import BeautifulSoup

import sqlite3
import urlparse

conn = sqlite3.connect('data.sqlite')
c = conn.cursor()
url = "http://www.nyc.gov/html/cau/html/cb/cb.shtml"


def create_or_wipe_table(cursor):
    try:
        cursor.execute("""
            CREATE TABLE community_boards
                (name text, neighborhoods text, address text, email text,
                 phone text, chair text, district_manager text,
                 board_meeting text, cabinet_meeting text)
        """)
    except sqlite3.OperationalError:
        cursor.execute('DELETE FROM community_boards')


def parse_info_line(info, label):
    # Find the line
    line = filter(lambda s: label.lower() in s.lower(), cb_info)[0]

    # Strip tags
    line = BeautifulSoup(line).get_text()

    # Get just the part after the label
    return line.split(': ')[1].strip()


def get_borough_urls():
    html = scraperwiki.scrape(url)
    soup = BeautifulSoup(html)

    # Find all the paragraphs within the main_content cell
    paragraphs = soup.find("td", id="main_content").find_all("p")

    # Create a list of borough urls
    return [urlparse.urljoin(url, l['href']) for l in paragraphs[1].find_all("a")]


create_or_wipe_table(c)


# For every boro in our list, scrape the info for that community board
for boro in get_borough_urls():
    html = scraperwiki.scrape(boro)
    soup = BeautifulSoup(html)

    # We just want one kind of table, the cb_table class
    cb_tables = soup.find_all("table", {"class":"cb_table"})
    print "Parsing %d tables in %s." % (len(cb_tables), boro)

    for table in cb_tables:
        rows = table.find_all('tr')
        cb_name = rows[0].get_text().strip()

        inner_table = rows[1].find_all('table')[0]
        inner_rows = inner_table.find_all('tr')
        neighborhoods = inner_rows[0].find_all("td")[1].get_text().strip()
        cb_info = inner_rows[1].find_all("td")[1]
        precincts = inner_rows[2].find_all("td")[1].get_text().strip()
        precinct_phones = inner_rows[3].find_all("td")[1].get_text().strip()

        email = None
        try:
            cb_info = str(cb_info).split('<br/>')

            address = ' '.join([s.strip() for s in cb_info[1:4]])
            phone = parse_info_line(cb_info, 'phone')
            email = parse_info_line(cb_info, 'email')
            chair = parse_info_line(cb_info, 'chair')
            district_manager = parse_info_line(cb_info, 'district manager')
            board_meeting = parse_info_line(cb_info, 'board meeting')
            cabinet_meeting = parse_info_line(cb_info, 'cabinet meeting')
        except IndexError, TypeError:
            pass

        print 'Inserting CB', cb_name
        c.execute("""
            INSERT INTO community_boards (name, neighborhoods, address, email,
                  phone, chair, district_manager, board_meeting, cabinet_meeting) 
                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (cb_name, neighborhoods, address, email, phone, chair,
            district_manager, board_meeting, cabinet_meeting)
        )

conn.commit()
c.close()

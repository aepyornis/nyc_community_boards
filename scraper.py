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

create_or_wipe_table(c)

html = scraperwiki.scrape(url)
soup = BeautifulSoup(html)

# In this case I know (because I looked at the page source), that we're looking for one 
# td with the id attribute "main_content" and then we want to find the paragraphs within 
# that block of HTML.

# first, find the "main_content" cell
lump = soup.find("td", id="main_content");

# then find all the paragraphs within that block of HTML
paragraphs = lump.find_all("p")

# The second paragraph, or paragraphs[1] in our list syntax has the actual community boards. 
# We can find the "anchors" in that paragraph and pull out their "href" attributes
# to see the full list of pages we want to scrape.
for anchor in paragraphs[1].find_all("a"):
    print anchor['href']
    print urlparse.urljoin(url,anchor['href'])
    
# Create an empty list (or array).
boro_urls = []

# Instead of printing the URLs to the screen, append them to our list.
for anchor in paragraphs[1].find_all("a"):
    boro_urls.append(urlparse.urljoin(url,anchor['href']))

# Now comes the fun part. For every boro in our boro_urls list, we're going to do some scraping. 
for boro in boro_urls:
    html = scraperwiki.scrape(boro)
    soup = BeautifulSoup(html)
    # we just want one kind of table, the cb_table class
    cb_tables = soup.find_all("table", {"class":"cb_table"})
    print "Parsing %d tables in %s." % (len(cb_tables), boro)
    # now this all starts to look a whole lot like https://scraperwiki.com/scrapers/foreclosures --
    # we know how to put the rows from a table into a data store!
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

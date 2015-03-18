"""
This is a fork/port of Phil Ashlock's Community Board scraper from scraperwiki:
    https://classic.scraperwiki.com/scrapers/city_representatives_-_nyc_community_boards/

"""
import scraperwiki
from bs4 import BeautifulSoup

import sqlite3
import traceback
import urlparse

url = "http://www.nyc.gov/html/cau/html/cb/cb.shtml"
insert_sql = """
    INSERT INTO community_boards (borough, name, neighborhoods, address, email,
          phone, chair, district_manager, board_meeting, cabinet_meeting) 
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
insert_params = ('name', 'neighborhoods', 'address', 'email', 'phone', 'chair',
                 'district_manager', 'board_meeting', 'cabinet_meeting')


def create_or_wipe_table(cursor):
    try:
        cursor.execute("""
            CREATE TABLE community_boards
                (borough text, name text, neighborhoods text, address text,
                 email text, phone text, chair text, district_manager text,
                 board_meeting text, cabinet_meeting text)
        """)
    except sqlite3.OperationalError:
        cursor.execute('DELETE FROM community_boards')


def parse_info_line(info, labels):
    if isinstance(labels, str):
        labels = (labels,)
    def line_matches(line):
        return len(filter(lambda label: label.lower() in line.lower(), labels)) >= 1
    try:
        # Find the line
        line = filter(line_matches, info)[0]
    except IndexError:
        return None

    # Strip tags
    line = BeautifulSoup(line).get_text()

    # Get just the part after the label
    splitters = (':', '-')
    for splitter in splitters:
        split_line = line.split(splitter, 1)
        if len(split_line) > 1:
            return split_line[1].strip()
    return None


def get_borough_urls():
    html = scraperwiki.scrape(url)
    soup = BeautifulSoup(html)

    # Find all the paragraphs within the main_content cell
    paragraphs = soup.find("td", id="main_content").find_all("p")

    # Create a list of borough urls
    return [urlparse.urljoin(url, l['href']) for l in paragraphs[1].find_all("a")]


def scrape_board(table):
    rows = table.find_all('tr')
    inner_table = rows[1].find_all('table')[0]
    inner_rows = inner_table.find_all('tr')
    cb_info = inner_rows[1].find_all("td")[1]

    board = {
        'name': rows[0].get_text().strip(),
        'neighborhoods': inner_rows[0].find_all("td")[1].get_text().strip(),
        'precincts': inner_rows[2].find_all("td")[1].get_text().strip(),
        'precinct_phones': inner_rows[3].find_all("td")[1].get_text().strip(),
    }

    try:
        cb_info = str(cb_info).split('<br/>')

        board.update({
            'address': ' '.join([s.strip() for s in cb_info[1:4]]),
            'phone': parse_info_line(cb_info, 'phone'),
            'email': parse_info_line(cb_info, 'email'),
            'chair': parse_info_line(cb_info, ('chair', 'chairperson')),
            'district_manager': parse_info_line(cb_info, 'district manager'),
            'board_meeting': parse_info_line(cb_info, ('board meeting', 'board metting')),
            'cabinet_meeting': parse_info_line(cb_info, 'cabinet meeting'),
        })
        return board
    except IndexError:
        print 'Failed to load %s. Skipping.' % board['name']
        traceback.print_exc()
        return None


conn = sqlite3.connect('data.sqlite')
c = conn.cursor()
create_or_wipe_table(c)

# For every boro in our list, scrape the info for that community board
for boro in get_borough_urls():
    html = scraperwiki.scrape(boro)
    soup = BeautifulSoup(html)

    borough = soup.find_all("span", {"class": "area_header"})[0].get_text()
    borough = borough.replace('Community Boards', '').strip()
    print borough

    # We just want one kind of table, the cb_table class
    cb_tables = soup.find_all("table", {"class":"cb_table"})
    print "Parsing %d tables in %s." % (len(cb_tables), boro)

    for table in cb_tables:
        board = scrape_board(table)

        if board:
            print 'Inserting CB', board['name']
            params = [borough,] + [board[key] for key in insert_params]
            c.execute(insert_sql, params)

conn.commit()
c.close()

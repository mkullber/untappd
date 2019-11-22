# usage:
#
#pl = PageLoader()
#pl.login('username', 'password')
#my_checkins = get_checkins(pl=pl, username='username')
#venue_checkins = get_checkins(pl=pl, venue_id=7768425)


from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import *
import requests
from bs4 import BeautifulSoup
import pytz
import dateutil.parser
import re
import codecs
import sys
import os
import datetime


LOG_PRINT = True
LOG_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

USER_AGENT = 'oma himmeli/0.1'
BASE_URL = 'https://untappd.com/'
LOGIN_URL = BASE_URL + 'login'
CHECKINS_URL = BASE_URL + 'user/{username}'
CHECKINS_MORE_URL = BASE_URL + 'profile/more_feed/{username}/{checkin_id}?v2=true'

VENUE_CHECKINS_URL = BASE_URL + 'venue/{venue_id}'
VENUE_CHECKINS_MORE_URL = BASE_URL + 'venue/more_feed/{venue_id}/{checkin_id}?v2=true'
CHECKIN_TIMEZONE = 'UTC'
CHECKIN_DATE_FORMAT = '%a, %m %b %Y %H:%M:%S %z'

sys.stdout = codecs.getwriter('utf8')(sys.stdout.buffer)


def log(s):
    if LOG_PRINT:
        print('Untappd [%s] %s' % (datetime.datetime.now().strftime(LOG_DATETIME_FORMAT), s))


class PageLoader:
    def __init__(self, execute=True):
        self.response = None
        self.session = requests.Session()
        self.execute = execute
        self.verbose = True

    def get(self, url, data=None, headers={}, method='GET'):
        if not self.execute:
            return ""
        headers.update({'User-Agent': USER_AGENT})
        if self.verbose:
            log('Getting page %s (%s), data=%s, headers=%s, cookies=%s' %
                (url, method, data, headers, self.session.cookies))
        self.response = self.session.request(method=method, url=url, data=data,
                                             headers=headers, timeout=5, allow_redirects=True, stream=True)
        log('Got %s (%s), status_code: %s, size: %s' %
            (url, method, self.response.status_code, len(self.response.text)))
        log('  headers: %s\n  request: %s\n body: %s' % (self.response.headers,
                                                         self.response.request.headers, self.response.request.body))
        if self.verbose:
            log('  response cookies: %s' % self.response.cookies)
            log('  session cookies: %s' % self.session.cookies)
        return self.response.text

    def login(self, username, password):
        if not self.execute:
            return
        page = self.get(LOGIN_URL)
        login_soup = BeautifulSoup(page, 'lxml')
        session_key = login_soup.find('input', {'name': 'session_key'}).get('value')
        login_params = {'username': username, 'password': password, 'session_key': session_key}
        return self.get(LOGIN_URL, data=login_params, method='POST')

    def check_response(self):
        if self.response.ok and self.response.text is not None and len(self.response.text) > 0:
            return True
        return False


Base = declarative_base()


class Checkin(Base):
    __tablename__ = 'checkins'

    id = Column(Integer, primary_key=True, nullable=False)
    username = Column(String(64), nullable=False)
    checkin_id = Column(Integer, nullable=False)
    beer_id = Column(String(64), nullable=False)
    beer_url = Column(String(255))
    beer_name = Column(String(255))
    brewery_id = Column(String(64))
    brewery_name = Column(String(255))
    venue_id = Column(Integer)
    venue_name = Column(String(255))
    timestamp = Column(DateTime)
    rating = Column(Float)
    serving = Column(String(64))
    comment = Column(Text)
    badges_str = Column(Text)
    venue_checkin = Column(Boolean)

    def __init__(self, checkin_soup, venue_checkin=False):
        self.venue_checkin = venue_checkin
        self.checkin_id = int(checkin_soup['data-checkin-id'])
        texts = checkin_soup.find('p', {'class': 'text'}).find_all('a')
        self.username = texts[0]['href'].split('/')[-1]
        self.beer_id = texts[1]['href'].split('/')[-1]
        self.beer_url = texts[1]['href']
        self.beer_name = texts[1].get_text()
        self.brewery_id = texts[2]['href'].split('/')[-1]
        self.brewery_name = texts[2].get_text()
        self.venue_id = None
        self.venue_name = None
        if len(texts) > 3:
            self.venue_id = int(texts[3]['href'].split('/')[-1])
            self.venue_name = texts[3].get_text()
        self.rating = None
        rating = checkin_soup.find('span', {'class': 'rating'})
        if rating:
            self.rating = float(re.sub(r'\D', '', rating['class'][-1])) / 100
        self.serving = None
        # this isn't always present (=if rating, purchased location and badges not present)
        serving = checkin_soup.find('p', {'class': 'serving'})
        if serving:
            self.serving = serving.get_text().strip()
        self.time = checkin_soup.find('a', {'class': 'time'}).get_text()
        self.timestamp = dateutil.parser.parse(self.time)
        self.comment = None
        comment = checkin_soup.find('p', {'class': 'comment-text'})
        if comment:
            self.comment = comment.get_text().strip()
        self.badges = []
        badges = checkin_soup.find('div', {'class': 'checkin-comment'})
        if badges:
            for badge in badges.find_all('span', {'class': 'badge'}):
                self.badges.append(badge.find('img')['alt'])
        self.badges_str = ', '.join(self.badges)
        # print self.checkin_id, self.beer_id, self.beer_name, self.brewery_id, self.brewery_name, self.venue_id, self.venue_name, self.time, self.rating, self.comment, self.badges

    def get_csv_line(self):
        badges_str = ', '.join(self.badges)
        fields = []
        for field in self.username, self.checkin_id, self.beer_id, self.beer_name, self.brewery_id, self.brewery_name, self.venue_id, self.venue_name, self.time, self.rating, self.serving, self.comment, badges_str:
            field_text = '"%s"' % str(field).replace('"', '\\"')
            fields.append(field_text)
        return ';'.join(fields)


class CheckinParser:
    def __init__(self, venue_checkin=False, use_database=True, stop_at_checkin=None, checkin_max_age=None):
        self.checkins = []
        self.last_checkin_id = None
        self.in_progress = True
        self.venue_checkin = venue_checkin
        self.use_database = use_database
        self.stop_at_checkin = stop_at_checkin
        self.pl = None
        self.checkin_max_age = checkin_max_age

    def parse(self, page):
        count = 0
        checkins_soup = BeautifulSoup(page, 'lxml')
        checkins_container = checkins_soup.find('div', {'id': 'main-stream'})
        if checkins_container:
            checkins_soup = checkins_container
        else:
            # this is verified venue? bit different url..
            if checkins_soup.find('div', {'class': 'report-menu'}):
                page = self.pl.get(self.pl.response.url + '/activity')
                checkins_soup = BeautifulSoup(page, 'lxml')
                checkins_container = checkins_soup.find('div', {'id': 'main-stream'})
        for checkin_soup in checkins_soup.find_all('div', {'class': 'item'}):
            count += 1
            new_checkin = Checkin(checkin_soup, venue_checkin=self.venue_checkin)
            if self.checkin_max_age:
                if new_checkin.timestamp < (pytz.timezone('UTC').localize(datetime.datetime.now()) - self.checkin_max_age):
                    self.in_progress = False
            if self.stop_at_checkin and self.stop_at_checkin >= int(new_checkin.checkin_id):
                self.in_progress = False
            if not self.in_progress:
                log('Stopping at check-in %s @ %s' % (new_checkin.checkin_id,
                                                      new_checkin.timestamp.strftime(LOG_DATETIME_FORMAT)))
                break
            print(new_checkin.get_csv_line())
            self.checkins.append(new_checkin)
        if len(self.checkins) > 0:
            self.last_checkin_id = self.checkins[-1].checkin_id
        return count


def get_checkins(pl, username=None, venue_id=None, resume_from_checkin=None, stop_at_checkin=None, checkin_max_age=None):
    if username is None and venue_id is None:
        return False
    venue_checkin = venue_id is not None
    cp = CheckinParser(venue_checkin=venue_checkin, stop_at_checkin=stop_at_checkin, checkin_max_age=checkin_max_age)
    cp.pl = pl
    if venue_checkin:
        checkins_url = VENUE_CHECKINS_URL.format(venue_id=venue_id)
    else:
        checkins_url = CHECKINS_URL.format(username=username)
    if stop_at_checkin:
        cp.stop_at_checkin = stop_at_checkin

    if resume_from_checkin:
        cp.last_checkin_id = resume_from_checkin
    else:
        page = cp.pl.get(checkins_url)
        count = cp.parse(page)

    if cp.in_progress:
        while True:
            if venue_checkin:
                morepage_url = VENUE_CHECKINS_MORE_URL.format(venue_id=venue_id, checkin_id=cp.last_checkin_id)
            else:
                morepage_url = CHECKINS_MORE_URL.format(username=username, checkin_id=cp.last_checkin_id)
            headers = {'X-Requested-With': 'XMLHttpRequest', 'Referer': checkins_url}

            # doesn't work now as login doesn't set cookie untappd_user_v3_e
            page = cp.pl.get(morepage_url, headers=headers)
            count = cp.parse(page)
            if count == 0 or not cp.in_progress:
                break

    return cp.checkins

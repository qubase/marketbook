# -*- coding: utf-8 -*-
from pymongo.errors import DuplicateKeyError

__author__ = 'mr'

import sys
import datetime
import random
import time
import re
import traceback
import subprocess
from xml.etree import ElementTree as ET
from configparser import  ConfigParser
from PyQt4.QtGui import *
from PyQt4.QtCore import *
from PyQt4.QtWebKit import *
from PyQt4.QtNetwork import *
from bs4 import BeautifulSoup
from pymongo import MongoClient

class CrawlerConfig(ConfigParser):
    def __init__(self):
        ConfigParser.__init__(self)
        self.id = None
        self.name = None
        self.on = False
        self.force = False
        self.ttl = 365
        self.sleep = 0

    def parseCrawlerConfig(self):
        file = cfg.get('main', 'crawler-config')
        id = cfg.get('main', 'crawler-id')
        root = ET.parse(file).getroot()
        mbElem = root.find("crawler/[@id='" + id + "']")
        self.id = id
        self.on = True if mbElem.attrib['status'] == '1' else False
        self.name = mbElem.find('name').text
        self.sleep = int(int(mbElem.find('sleep').text) / 1000)
        self.ttl = int(mbElem.find('ttl').text)


class Crawler(QWebView):
    def __init__(self, app, cfg):
        self.app = app
        self.cfg = cfg
        self.sitemap = []
        self.modelList = []
        self.listings = []
        self.nextList = None
        self.nextPage = None
        self.nextModified = None
        self.baseUrl = "http://www.marketbook.de"
        self.requests = 0
        self.noTitle = 0
        QWebView.__init__(self)
        self.loadFinished.connect(self._loadFinished)
        uri = "mongodb://" + \
              cfg.get('mongo', 'user') + ":" + \
              cfg.get('mongo', 'pass') + "@" + \
              cfg.get('mongo', 'host') + ":" + \
              cfg.get('mongo', 'port') + "/" + \
              cfg.get('mongo', 'db')
        client = MongoClient(uri)
        self.db = client[cfg.get('mongo', 'db')]
        doc = self.db['meta.marketbook'].find_one()
        if doc is not None:
            self.round = doc['round'] if doc['round'] is not None else 0
        else:
            self.round = 0

    def _loadFinished(self):
        html = self.page().mainFrame().toHtml()
        soup = BeautifulSoup(html)
        #if this is a content page, recognize page type from URL and parse the relevant information
        if soup.title != None:
            self.noTitle = 0
            if soup.title.string.strip() == 'ERROR':
                #this session was terminated by server, need to restart crawling with new session (handled by external cron)
                self.saveMetaData()
                self.terminate('Error page')
            try:
                url = self.url().toString()
                if '/manulist.aspx' in url:
                    self.parseSitemap(soup)
                elif '/modellist.aspx' in url:
                    self.parseModelList(soup)
                elif '/list.aspx' in url:
                    self.parseList(soup)
                elif '/detail.aspx' in url:
                    self.parseListing(soup)
                elif 'registration/passport.aspx' in url:
                    # this is a known but not interesting page type
                    self.log('Not interested in this type of page: ' + url)
                else:
                    self.nextPage = None
                    self.saveMetaData()
                    self.terminate("Unknown page type: " + url)
                self.proceed()
            except Exception as e:
                self.terminate(format_exception(e))
        else:
            self.log("Page doesn't contain a title, considered not a finished page loading (expects redirection, ...)")
            self.log(html)
            self.noTitle += 1
            if self.noTitle > 1 or "<html><head></head><body>" in html:
                self.nextPage = None #not to get to the same No Title situation in case of immediate termination
                self.nextModified = None
                self.log("Too many No Title pages or a corrupt page, proceeding to the next page")
                self.proceed()

    def proceed(self):
        self.saveMetaData()
        self.nextPage = None
        self.nextModified = None
        self.cfg.parseCrawlerConfig()
        if not self.cfg.on and not self.cfg.force:
            self.terminate("Via configuration file")
        self.requests += 1
        self.log("Number of requests: " + str(self.requests) + "/" + str(self.cfg.getint("main", "max-requests")))
        if self.requests >= self.cfg.getint("main", "max-requests"):
            self.terminate("Max requests exceeded: " + str(self.requests))
        else:
            self.loadNextPage()

    def loadNextPage(self):
        time.sleep(self.cfg.sleep)

        if self.cfg.getboolean('main', 'proxy'):
            while not self.proxyActive():
                time.sleep(1)

        if self.nextPage != None:
            self.log("Loading next page directly: " + self.nextPage)
            self.load(QUrl(self.nextPage))
        else:
            if len(self.modelList) > 0:
                self.nextPage = self.modelList.pop(0)
            elif len(self.listings) > 0:
                record = self.listings.pop(0)
                self.nextPage = record["url"]
                self.nextModified = record["modified"]
            elif self.nextList != None:
                self.nextPage = self.nextList
            elif len(self.sitemap) > 0:
                self.nextPage = self.sitemap.pop(0)
            else: # end of round
                self.log("End of round: " + str(self.round))
                self.round += 1
                self.nextPage = None
                self.saveMetaData()
                self.terminate("End of round")
            if self.nextPage is not None:
                self.log("Next page chosen from meta data: " + self.nextPage)
                self.load(QUrl(self.nextPage))

    def proxyActive(self):
        out = subprocess.Popen(['ps', 'aux'], stdout=subprocess.PIPE).communicate()[0]
        tor, polipo = (False, False)
        for line in out.split(b"\n"):
            if b"/etc/init.d/tor restart" in line \
                or b"/etc/init.d/tor restart" in line \
                or b"/etc/init.d/tor stop" in line \
                or b"/etc/init.d/tor reload" in line \
                or b"/etc/init.d/tor force-reload" in line \
                or b"/etc/init.d/polipo restart" in line \
                or b"/etc/init.d/polipo stop" in line \
                or b"/etc/init.d/polipo force-reload" in line:
                self.log("Proxy inactive")
                return False
            if b"/usr/sbin/tor" in line:
                self.log("Proxy: found Tor")
                tor = True
            if b"/usr/bin/polipo" in line:
                self.log("Proxy: found Polipo")
                polipo = True
        return tor and polipo

    def parseSitemap(self, soup):
        self.log('Parsing sitemap: ' + self.url().toString())
        mans = soup.find(id='ctl00_ContentPlaceHolder1_DrillDown1_trInformation')
        if mans != None:
            links = mans.find_all('a')
            cnt = len(links)
            for i in range(0, cnt):
                text = links[i].string
                pattern = re.compile("\(([0-9]+)\)")
                listingsCnt = int(pattern.search(text).group(1))
                href = links[i]['href']
                if listingsCnt < 10000:
                    self.sitemap.append(self.baseUrl + href.replace('drilldown/modellist.aspx', 'list/list.aspx'))
                else:
                    self.modelList.append(self.baseUrl + href)
            random.shuffle(self.sitemap)
        else:
            self.terminate('No manufacturers to parse in manufacturer list')

    def parseModelList(self, soup):
        self.log('Parsing model list: ' + self.url().toString())
        mans = soup.find(id='ctl00_ContentPlaceHolder1_DrillDown1_trInformation')
        if mans != None:
            links = mans.find_all('a')
            cnt = len(links)
            for i in range(0, cnt):
                href = links[i]['href'];
                if 'mdlx=exact' in href:
                    self.sitemap.append(self.baseUrl + href)
            random.shuffle(self.sitemap)
        else:
            self.terminate('No models to parse in model list')

    def parseList(self, soup):
        self.log('Parsing List: ' + self.url().toString())
        links = soup.find_all(id='aDetailsLink')
        modifieds = soup.find_all("span", {"class": "date-time3"})
        cnt = len(links)
        self.listings = []
        listingsCnt = 0
        duplicatesCnt = 0
        for i in range(0, cnt):
            url = self.baseUrl + links[i]['href']
            if not self.isDuplicateListing(url):
                listingsCnt += 1
                pattern = re.compile("([0-9]{1,2}\.[0-9]{1,2}\.[0-9]{1,4})")
                date = datetime.datetime.strptime(pattern.search(modifieds[i].get_text()).group(1), "%d.%m.%Y")
                record = {"url": url, "modified": date}
                self.listings.append(record)
            else:
                duplicatesCnt += 1
        self.log("List loaded, new: " + str(listingsCnt) + ", duplicates: " + str(duplicatesCnt))
        random.shuffle(self.listings)
        #check if there's next page available
        pager = soup.find(id='ctl00_ContentPlaceHolder1_ctl18_Paging1_tblPaging')
        if pager != None and pager.a.string == 'Drüken Sie hier':
            self.nextList = self.baseUrl + pager.a['href']
        else:
            self.nextList = None

    def parseListing(self, soup):
        url = self.url().toString()
        self.log('Parsing Listing: ' + url)
        price = soup.find(id='listingpricevalue')
        priceVal = None
        if price != None:
            priceVal = price.string.strip()
            if priceVal == 'Auf Anfrage':
                priceVal = None
        manufacturer = None
        model = None
        year = None
        country = None
        region = None
        company = None
        counter = None
        serial = None
        category = None
        listingTitle = soup.find(id="hListingTitle")
        if listingTitle != None:
            category = soup.title.string.strip()[(len(listingTitle.string) + 1):].replace(' zum Verkauf Zu MarketBook.de', '')
        info = soup.find("td", {"class": "info"})
        if info == None:
            info = soup.find("td", {"class": "infonoborder"})
        if info != None and info.h5 != None:
            company = info.h5.string
        specs = soup.find(id='specs')
        if specs != None:
            specsNames = specs.find_all('th')
            specsValues = specs.find_all('td')
            cnt = len(specsNames)
            for i in range(0, cnt):
                if specsNames[i].string == 'Jahr':
                    year = specsValues[i].string
                elif specsNames[i].string == 'Hersteller':
                    manufacturer = specsValues[i].string
                elif specsNames[i].string == 'Typ':
                    model = specsValues[i].string
                elif specsNames[i].string == 'Betriebsstunden':
                    counter = specsValues[i].string
                elif specsNames[i].string == 'Serien Nummer':
                    serial = specsValues[i].string
                elif specsNames[i].string == 'Ort':
                    regionCountry = specsValues[i].string
                    crumbs = regionCountry.split(',')
                    if len(crumbs) == 1:
                        country = crumbs[0].strip()
                    elif len(crumbs) > 1:
                        country = crumbs[len(crumbs) - 1].strip()
                        region = ', '.join(crumbs[0:(len(crumbs) - 1)])
                        if len(region) == 0:
                            region = None
        date = self.nextModified if self.nextModified != None else datetime.datetime.utcnow()
        doc = {"date":      date,
               "createdAt": datetime.datetime.utcnow(),
               "ttl":       datetime.datetime.utcnow() + datetime.timedelta(days=int(self.cfg.ttl)),
               "url":       url,
               "todo":      1,
               "portalId":  int(self.cfg.id)}
        if manufacturer != None:
            doc["manName"] = manufacturer
        if model != None:
            doc["modelName"] = model
        if year != None:
            doc["year"] = year
        if priceVal != None:
            doc["price"] = priceVal
            doc["currency"] = "EUR"
        if region != None:
            doc["region"] = region
        if country != None:
            doc["country"] = country
        if category != None:
            doc["category"] = category
            doc["catLang"] = "DE"
        if counter != None:
            doc["counter"] = counter
        if company != None:
            doc["company"] = company
        if serial != None:
            doc["serial"] = serial

        if manufacturer == None or model == None:
            self.log("Mandatory fields missing: " + url)
        else:
            try:
                self.db.listings.insert(doc)
            except DuplicateKeyError as e:
                self.log(str(e))

    def run(self, url):
        self.log("Starting crawler")
        self.nextPage = url[int(self.round) % len(url)]
        self.loadMetaData()
        if self.cfg.getboolean('main', 'gui'):
            self.show()
        self.loadNextPage()

    def terminate(self, message=""):
        self.log('Terminating crawler: ' + message)
        self.app.quit()

    def log(self, message):
        if self.cfg.getboolean('log', 'log'):
            doc = {'date': datetime.datetime.utcnow(),
                   'ttl': datetime.datetime.utcnow() + datetime.timedelta(hours=int(self.cfg.get('log', 'ttl-hours'))),
                   'message': message}
            self.db['log.marketbook'].insert(doc)
        if self.cfg.getboolean('log', 'debug'):
            print(message)

    def saveMetaData(self):
        doc = {'nextPage': self.nextPage,
               'nextModified': self.nextModified,
               'nextList': self.nextList,
               'sitemap': self.sitemap,
               'modelList': self.modelList,
               'listings': self.listings,
               'round': self.round}
        self.db['meta.marketbook'].remove()
        self.db['meta.marketbook'].insert(doc)
        self.log("Metadata saved")

    def loadMetaData(self):
        self.log("Loading metadata")
        doc = self.db['meta.marketbook'].find_one()
        if doc != None:
            self.nextPage = doc['nextPage'] if doc['nextPage'] is not None else self.nextPage
            self.nextModified = doc['nextModified']
            self.nextList = doc['nextList']
            self.sitemap = doc['sitemap'] if doc['sitemap'] is not None else []
            self.listings = doc['listings'] if doc['listings'] is not None else []
            self.modelList = doc['modelList'] if doc['modelList'] is not None else []

            # if there are any links to process do not load the sitemap again
            if len(self.sitemap) > 0 \
                or len(self.modelList) > 0 \
                or len(self.listings) > 0:
                self.nextPage = None

            self.log("Metadata loaded successfully")
        else:
            self.log("No metadata to load")
        if self.nextPage != None and self.isDuplicateListing(self.nextPage):
            self.log("Metadata NextPage is a duplicate listing, removing from URL queue")
            self.nextPage = None

    def isDuplicateListing(self, url):
        doc = self.db.listings.find_one({"url": url})
        if doc == None:
            return False
        else:
            return True

def format_exception(e):
    exception_list = traceback.format_stack()
    exception_list = exception_list[:-2]
    exception_list.extend(traceback.format_tb(sys.exc_info()[2]))
    exception_list.extend(traceback.format_exception_only(sys.exc_info()[0], sys.exc_info()[1]))

    exception_str = "Traceback (most recent call last):\n"
    exception_str += "".join(exception_list)
    # Removing the last \n
    exception_str = exception_str[:-1]

    return exception_str

if __name__ == '__main__':
    cfg = CrawlerConfig()
    cfg.read('marketbook.ini')
    cfg.parseCrawlerConfig()
    force = False
    for i in range(0, len(sys.argv)):
        if sys.argv[i] == "--force":
            cfg.force = True
    #if the crawler is off, just exit unless need to force
    if not cfg.on and not cfg.force:
        sys.exit(1)
    app = QApplication(sys.argv)
    if cfg.getboolean('main', 'proxy'):
        proxy = QUrl(cfg.get('main', 'proxy-url'))
        QNetworkProxy.setApplicationProxy(QNetworkProxy(QNetworkProxy.HttpProxy, proxy.host(), proxy.port(), proxy.userName(), proxy.password()))
        print("Using application proxy:", proxy.toString())
    crawler = Crawler(app, cfg)
    crawler.run(['http://www.marketbook.de/drilldown/manulist.aspx?lp=TH', 'http://www.marketbook.de/drilldown/manulist.aspx?lp=MAT'])
    sys.exit(app.exec_())
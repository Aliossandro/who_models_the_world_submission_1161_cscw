import requests
from bs4 import BeautifulSoup
import re
import sys

def adminScraper(adm):
    months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
    years = ['2013', '2014', '2015', '2016', '2017']
    resultList = []
    for year in years:
        for month in months:
            wiki = "https://www.wikidata.org/wiki/Wikidata:Requests_for_permissions/" + adm + "/" + month + '_' + year
            # wiki = "https://www.wikidata.org/wiki/Wikidata:Requests_for_permissions/" + adm + "/"  + year
            page = requests.get(wiki)
            if page.status_code is 200:
                page_text = page.content

                soup = BeautifulSoup(page.content, 'html.parser')
                # archivedDisc = soup.findAll("div", class_="boilerplate metadata discussion-archived")
                # for disc in archivedDisc:
                #     if re.search('<b> Done</b>', str(disc)):
                #         user = re.search('title="User:(.*?)">', str(disc)).group(1)
                #         date = month + " " + year
                #         userdate = user + ',' + date + '\n'
                #         resultList.append(userdate)
                archivedAlt = soup.findAll("span", class_="mw-headline")
                for disc in archivedAlt:
                    user = re.search('<span class="mw-headline" id="(.*?)">', str(disc)).group(1)
                    date = month + " " + year
                    userdate = user + ',' + date + '\n'
                    resultList.append(userdate)


    fileWrite = adm + '_list.csv'
    with open(fileWrite, 'w') as f:
        for line in resultList:
            f.write(line)
        f.close()


def main():
    # create_table()
    # path = '/Users/alessandro/Documents/PhD/userstats'
    path = sys.argv[1]
    adminScraper(path)


if __name__ == "__main__":
    main()




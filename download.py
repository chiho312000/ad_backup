import json, re, os, asyncio, sys
from bs4 import BeautifulSoup
from tqdm import tqdm
from ad_lib import *

domain_name = 'https://hk.appledaily.com'
requests = getRequestInstance(120)

def parseArgs():
  parser = getArgumentParser('Download all news from AppleDaily')
  parser.add_argument(
    '-l', '--list',
    action='store_true',
    dest='fetch_list',
    help='Download new article list.'
  )
  arguments = parser.parse_args()
  return arguments

async def getLinksFromDate(date):
  url = f'https://hk.appledaily.com/archive/{date}/'

  @Allow_Retries_Async
  async def getHtml():
    async with requests.get(url) as response: 
      return await response.text()

  html = await getHtml()
  if not html: 
    tqdm.write(f'Downloading article list from {date} stopped because of continuous errors.')
    return {
      'date': date,
      'links': []
    }

  soup = BeautifulSoup(html, 'lxml')
  body = soup.select_one('#section-body')

  anchorTags = body.find_all('a', class_=lambda x: x.count('-') == 1)
  articleLinks = [ domain_name + str(link['href']) for link in anchorTags if link.has_attr('href') ]

  return {
    'date': date,
    'links': articleLinks
  }

async def getArticleLists():
  arguments = parseArgs()
  dates = dateGenerator(arguments.start_date, arguments.end_date)
  articlesLists = []

  pbar = tqdm(total=len(dates))

  print('Downloading article list ......')

  async def getLinksAndUpdatePbar(date):
    links = await getLinksFromDate(date)
    pbar.update()
    return links

  for i in range(0, len(dates), 32):
    dateRange = dates[i:i+32]
    fetchTasks = [getLinksAndUpdatePbar(date) for date in dateRange]
    listSection = [await task for task in asyncio.as_completed(fetchTasks, timeout=None)]
    listSection.sort(key=lambda x: x['date'], reverse=True)

    articlesLists += listSection
    with open('articlesLists_temp.json', 'w') as jsonFile:
      jsonFile.write(json.dumps(articlesLists, indent=2, ensure_ascii=False))

  pbar.close()
  return articlesLists

@Allow_Retries_Async
async def getArticle(url, index):
  async with requests.get(url) as response:
    html = await response.text()
    if not html: raise TypeError('HTML is null')
    articleRawData = re.search(r'Fusion.globalContent=\s*(.*?)};', html, flags=re.DOTALL)[1]
    try:
      data = json.loads(articleRawData + '}')
    except: # raw data shows error already.
      print(articleRawData)
      return None
  
  data['_index'] = index
  return data

async def main():
  if not os.path.exists('./data'): os.mkdir('./data')
  arguments = parseArgs()

  # Now loads downloaded articles.
  with open('articlesLists.json', 'r') as jsonFile:
    articlesLists = json.load(jsonFile) or []
  
  if arguments.fetch_list:
    articlesLists.extend(await getArticleLists())
    with open('articlesLists.json', 'w') as jsonFile:
      json.dump(articlesLists, jsonFile)

  downloadDates = set(dateGenerator(arguments.start_date, arguments.end_date))

  for articlesList in articlesLists:
    date = articlesList['date']
    links = articlesList['links']

    if len(links) == 0 or date not in downloadDates: continue

    # Download articles using 16 processes.
    print(f'Downloading articles on {date} ......')
    articles = await get_results_with_progress(
      [getArticle(url, index) for index, url in enumerate(links) if date in url]
    )
    articles.sort(key=lambda x: x['_index'])

    with open(f'./data/{date}.json', 'w', encoding='utf-8') as jsonFile:
      json.dump(articles, jsonFile, indent=2, ensure_ascii=False)

  await requests.close()
  print('Download completed!')
  
if __name__ == '__main__':
  loop = asyncio.get_event_loop()
  loop.run_until_complete(main())
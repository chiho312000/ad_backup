import json, asyncio, os, ffmpeg
from ad_lib import *
from PIL import Image, ImageFile

ImageFile.LOAD_TRUNCATED_IMAGES = True

@Allow_Retries_Async
async def downloadFile(args):
  url, filepath, filetype = args

  targetPath = os.path.join(root_path, filepath)

  async def downloadImage():
    async with requests.get(url) as response:
      if response.status != 200:
        print(f'Error {response.status} occurred when downloading image from {url}, retrying ......')
        raise
      with open(targetPath, 'wb') as targetFile:
        while True:
          segment = await response.content.read(256*1024)
          if not segment: break
          targetFile.write(segment)

    # Shrink image size.
    with Image.open(targetPath) as image:
      compressed = image.convert('RGB')
      compressed.thumbnail((2000, 2000), Image.LANCZOS)

      compressed.save(targetPath, 'jpeg', optimize=True, quality=90)

    return filepath

  async def downloadStream():
    tqdm.write(f'Downloading video stream from {url}')
    ffmpeg.input(url).output(targetPath, **{'loglevel': 'error'}).run()
    return filepath
  
  if filetype == 'video':
    return await downloadStream()
  else:
    return await downloadImage()

def getDownloadInfoTuple(element, date, filetype = 'image'):
  url = element['url']
  extension = 'mp4' if filetype == 'video' else url.split('.')[-1]
  filename = f"{element['_id']}.{extension.replace('/', '')}"
  filepath = os.path.join('.', date, filename)
  return (url, filepath, filetype)

def getDownloadLinksFromArticles(articles, date):
  linksCollection = []

  for article in articles:
    links = [
      getDownloadInfoTuple(element, date) 
      for element in article.get('content_elements', [])
      if 'url' in element
    ]
    promo_item = article.get('promo_items', {}).get('basic', None)
    if promo_item:
      itemType = promo_item['type']

      if itemType == 'image':
        links.append(getDownloadInfoTuple(promo_item, date))
      elif itemType == 'video':
        # Download Thumbnail
        thumbnail = promo_item['promo_image']
        thumbnail['_id'] = promo_item['_id']
        links.append(getDownloadInfoTuple(thumbnail, date))

        # Download the video
        '''
        video = promo_item['streams'][0]
        video['_id'] = promo_item['_id']
        links.append(getDownloadInfoTuple(video, date, 'video'))
        '''
      
    linksCollection.extend(links)

  return linksCollection

async def main():
  if not os.path.exists('./assets'): os.mkdir('./assets')
  downloadDates = set(dateGenerator(arguments.start_date, arguments.end_date))

  downloadedFiles = os.listdir('./data')
  if not arguments.is_ascending: downloadedFiles = reversed(downloadedFiles)

  for dataFile in downloadedFiles:
    date = dataFile.split('.')[0]
    if date not in downloadDates: continue

    os.makedirs(f'./assets/{date}', exist_ok=True)
    print(f'Downloading assets from news on {date} ......')
    with open(f'./data/{dataFile}', 'r', encoding='utf-8') as jsonFile:
      articles = json.load(jsonFile)
    
    links = getDownloadLinksFromArticles(articles, date)

    await get_results_with_progress([downloadFile(link) for link in links])

  await requests.close()

root_path = './assets/'
parser = getArgumentParser('Download assets for AppleDaily articles')
parser.add_argument('-a', '--ascending', action='store_true', dest='is_ascending', help='Sort the dates in ascending order instead of descending.')
arguments = parser.parse_args()

if __name__ == '__main__':
  requests = getRequestInstance(60*60)
  loop = asyncio.get_event_loop()
  loop.run_until_complete(main())
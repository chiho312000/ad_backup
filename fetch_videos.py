import json, asyncio, os, shutil
from ad_lib import *
from PIL import Image, ImageFile, UnidentifiedImageError
from io import BytesIO

ImageFile.LOAD_TRUNCATED_IMAGES = True

@Allow_Retries_Async
async def downloadClip(url, index):
  storage = BytesIO()
  async with requests.get(url) as response:
    if response.status != 200:
      print(f'Error {response.status} occurred when downloading clip from {url}, retrying ......')
      raise

    while True:
      segment = await response.content.read(256*1024)
      if not segment: break
      storage.write(segment)

  return (storage, index)

@Allow_Retries_Async
async def downloadFile(args):
  url, filepath, fileExtension = args
  if fileExtension == 'ts':
    return await downloadFromM3U8(url, filepath)
  else:
    videoBytesTuple = await downloadClip(url, None)
    videoBytes = videoBytesTuple[0]
    with open(filepath, 'wb') as mp4File:
      mp4File.write(videoBytes.getbuffer())

@Allow_Retries_Async
async def downloadFromM3U8(mapUrl, filepath):  
  rootUrl = '/'.join(mapUrl.split('/')[:-1])

  async with requests.get(mapUrl) as response:
    sourceMapText = await response.text()
    sourceMap = sourceMapText.split('\n')

  # First uncommented line in 
  sourceMap = [line for line in sourceMap if '#' not in line]
  clipMapUrl = rootUrl + '/' + sourceMap[0]

  async with requests.get(clipMapUrl) as response:
    clipMapText = await response.text()
  clipMap = clipMapText.split('\n')

  # Download all clips
  clipsUrl = [rootUrl + '/' + line for line in clipMap if '#' not in line and '.ts' in line]
  unorderedClips = await get_results([downloadClip(url, index) for index, url in enumerate(clipsUrl)])

  # Reorder sequence of clips
  clips = [None]*len(unorderedClips)
  for clip, index in unorderedClips:
    clips[index] = clip

  with open(filepath, 'wb') as videoFile:
    [videoFile.write(clip.getbuffer()) for clip in clips]

  return filepath

def getDownloadInfoTuple(element, date):
  url = element['url']
  extension = 'ts' if url.split('.')[-1] == 'm3u8' else 'mp4'
  filename = f"{element['_id']}.{extension}"
  filepath = os.path.join(root_path, date, filename)
  return (url, filepath, extension)

def getDownloadLinksFromArticles(articles, date):
  links = []

  for article in articles:
    promo_item = article.get('promo_items', {}).get('basic', None)
    if promo_item:
      itemType = promo_item['type']

      if itemType == 'video':
        video = promo_item['streams'][0]
        video['_id'] = promo_item['_id']
        links.append(getDownloadInfoTuple(video, date))

  return links

async def main():
  if not os.path.exists('./assets_videos'): os.mkdir('./assets_videos')
  downloadDates = set(dateGenerator(arguments.start_date, arguments.end_date))

  downloadedFiles = os.listdir('./data')
  if not arguments.is_ascending: downloadedFiles = reversed(downloadedFiles)

  for dataFile in downloadedFiles:
    date = dataFile.split('.')[0]
    if date not in downloadDates: continue

    os.makedirs(f'./assets_videos/{date}', exist_ok=True)
    print(f'Downloading assets from news on {date} ......')
    with open(f'./data/{dataFile}', 'r', encoding='utf-8') as jsonFile:
      articles = json.load(jsonFile)
    
    links = getDownloadLinksFromArticles(articles, date)

    await get_results_with_progress([downloadFile(link) for link in links])

  await requests.close()

root_path = os.path.join('.', 'assets_videos')
parser = getArgumentParser('Download assets for AppleDaily articles')
parser.add_argument('-a', '--ascending', action='store_true', dest='is_ascending', help='Sort the dates in ascending order instead of descending.')
arguments = parser.parse_args()

if __name__ == '__main__':
  requests = getRequestInstance(60*60)
  loop = asyncio.get_event_loop()
  loop.run_until_complete(main())
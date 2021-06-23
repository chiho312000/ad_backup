import datetime, sys, asyncio, time, argparse, aiohttp
from tqdm import tqdm

def getArgumentParser(description='Download all assets from AppleDaily'):
  parser = argparse.ArgumentParser(description=description)
  parser.add_argument(
    '-s', '--start', 
    action='store', 
    dest='start_date', 
    default='20020101',
    help='Specify Start date.'
  )
  parser.add_argument(
    '-e', '--end', 
    action='store', 
    dest='end_date',
    default=datetime.datetime.now().strftime('%Y%m%d'),
    help='Specify end date.'
  )
  return parser

def dateGenerator(startDate, endDate):
  start = datetime.datetime.strptime(startDate, '%Y%m%d')
  end = datetime.datetime.strptime(endDate, '%Y%m%d')

  dates = [
    (start + datetime.timedelta(days=x)).strftime('%Y%m%d') \
    for x in range(0, (end-start).days)
  ]
  return list(reversed(dates))

def Allow_Retries_Async(fn):
  async def wrapper(*args, **kwargs):
    retryCount = 0
    while retryCount <= 5:
      try:
        return await fn(*args, **kwargs)
      except:
        print(sys.exc_info())
        print(f'Unexpected error {sys.exc_info()[0]} occurred, retrying operations ......')
        retryCount += 1

      await asyncio.sleep(2)
    raise OSError('Maximum retries exceeded')
  return wrapper

def Allow_Retries(fn):
  def wrapper(*args, **kwargs):
    retryCount = 0
    while retryCount <= 5:
      try:
        return fn(*args, **kwargs)
      except KeyboardInterrupt:
        print('Received keyboard interrupt.')
        sys.exit(0)
      except:
        print(sys.exc_info())
        print(f'Unexpected error {str(sys.exc_info()[0])} occurred, retrying operations ......')
        retryCount += 1

      time.sleep(10)
    raise sys.exc_info()[0]
  return wrapper

def getRequestInstance(timeout_seconds=60, limit=32):
  connector = aiohttp.TCPConnector(limit=limit)
  session = aiohttp.ClientSession(
    connector=connector, 
    timeout=aiohttp.ClientTimeout(total=timeout_seconds)
  )
  return session

async def get_results_with_progress(tasks):
  return [await task for task in tqdm(asyncio.as_completed(tasks), total=len(tasks))]
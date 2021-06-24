import os

dataFiles = os.listdir('data')
datesWithData = [filename[:8] for filename in dataFiles]

assetFolders = os.listdir('assets')
missingAssets = [date for date in datesWithData if date not in assetFolders]

print(missingAssets)
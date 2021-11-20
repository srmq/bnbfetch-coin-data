# bnbFetchCoinData.py
#     Copyright (C) 2021  Sergio Queiroz <srmq@srmq.org>

#     This program is free software: you can redistribute it and/or modify
#     it under the terms of the GNU Affero General Public License as
#     published by the Free Software Foundation, either version 3 of the
#     License, or (at your option) any later version.

#     This program is distributed in the hope that it will be useful,
#     but WITHOUT ANY WARRANTY; without even the implied warranty of
#     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#     GNU Affero General Public License for more details.

#     You should have received a copy of the GNU Affero General Public License
#     along with this program.  If not, see <https://www.gnu.org/licenses/>.
import httpx
import asyncio
import time
import bisect
import csv
from pathlib import Path
import json
import argparse
from dateutil import parser
import datetime

def getSpotURL():
    return 'https://api.binance.com'

async def getSymbolHistory(uptoMillis, symbol, rateLimiter, interval, outputDir):
    klinesURL = getSpotURL() + "/api/v3/klines"

    fle = outputDir / f"{symbol}.csv"
    fle.touch(exist_ok=True)
    with open(fle, 'r+', newline='') as f:
        lineCount = 0
        csvReader = csv.reader(f, delimiter=',')
        lastTS = 0
        for line in csvReader:
            if lineCount != 0:
                if (lastTS) < int(line[6]): # close time
                    lastTS = int(line[6])
            lineCount += 1
            if lineCount % 1000 == 0:
                await asyncio.sleep(0)
        csvWriter = csv.writer(f)
        if (lineCount == 0):
            csvWriter.writerow(['Open time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close time', 'Quote asset volume', 'Number of trades', 'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore'])
        if (lastTS == 0):
            lastTS = 1499990400000
        else:
            lastTS += 1
        finished = False
        while (not finished):
            while (not await rateLimiter.addWeightAndRequest(1, 1)):
                await asyncio.sleep(0)
            params = {'symbol': symbol, 'interval': interval, 'startTime': lastTS, 'limit': 1000}
            async with httpx.AsyncClient() as client:
                r = await client.get(klinesURL, params=params, timeout=30.0)
            result = r.json()
            if (len(result) == 0):
                finished = True
            for kline in result:
                if(len(kline) != 12):
                    print("Warning, kline of %s does not have 12 members. kline read was: %s"%(symbol, str(kline)))
                else:
                    if (kline[6] >= uptoMillis):
                        finished = True
                        break
                    else:
                        csvWriter.writerow(kline)
                    if ((kline[6] + 1) > lastTS):
                        lastTS = kline[6] + 1       
    return symbol + ' ... OK'            
        
async def getExchangeInfo():
    spotExchangeInfoURL = getSpotURL() + "/api/v3/exchangeInfo"

    async with httpx.AsyncClient() as client:
        r = await client.get(spotExchangeInfoURL)
    return r.json()

async def getFirstTimestamp(tradeSymbol):
    firstTSURL = getSpotURL() + "/api/v3/klines"
    params = {'symbol': tradeSymbol, 'interval': '1m', 'startTime': '1499990400000', 'limit': 1}
    async with httpx.AsyncClient() as client:
        r = await client.get(firstTSURL, params=params, timeout=30.0)
    jsonR = r.json() 
    return jsonR[0][0]

class RateLimiter:
    intervalMultiplier = {
        'SECOND': 1,
        'MINUTE': 60,
        'DAY': 86400
    }

    async def addWeightAndRequest(self, weightToAdd, nRequestToAdd):
        async with self.lock:
            limitForWeight = self.limitDict.get('REQUEST_WEIGHT')
            respectWeightLimits = True
            if limitForWeight is not None:
                for weightLimit in limitForWeight['limitList']:
                    eachsecs = weightLimit['eachsecs']
                    limitForSecs = weightLimit['limit']
                    goBackTime = time.time() - eachsecs - 1
                    n = bisect.bisect_left(limitForWeight['requests'], goBackTime)
                    n = len(limitForWeight['requests']) - n
                    if (n + weightToAdd) >= limitForSecs:
                        respectWeightLimits = False
                        return False

            limitForRequests = self.limitDict.get('RAW_REQUESTS')
            respectRawLimits = True
            if limitForRequests is not None:
                for rawLimit in limitForRequests['limitList']:
                    eachsecs = rawLimit['eachsecs']
                    limitForSecs = rawLimit['limit']
                    goBackTime = time.time() - eachsecs - 1
                    n = bisect.bisect_left(limitForRequests['requests'], goBackTime)
                    n = len(limitForRequests['requests']) - n
                    if (n + nRequestToAdd) >= limitForSecs:
                        respectRawLimits = False
                        return False
            assert (respectWeightLimits and respectRawLimits)
            if limitForWeight is not None:
                for i in range(0, weightToAdd):
                    limitForWeight['requests'].append(time.time())
                while(len(limitForWeight['requests']) > limitForWeight['maxlen']):
                    limitForWeight['requests'].pop(0)
            if limitForRequests is not None:
                for i in range(0, nRequestToAdd):
                    limitForRequests['requests'].append(time.time())
                while(len(limitForRequests['requests']) > limitForRequests['maxlen']):
                    limitForRequests['requests'].pop(0)
        return True

    def updateRateLimits(self, rateLimits):
        self.limitDict = {}
        for rateLimit in rateLimits:
            rateType = rateLimit['rateLimitType']
            limitForRate = self.limitDict.get(rateType)
            if (limitForRate is None):
                limitForRate = {'maxlen': -1, 'limitList': [], 'requests': []}
                self.limitDict[rateType] = limitForRate
            newlimit = {
                'eachsecs': RateLimiter.intervalMultiplier[rateLimit['interval']]*rateLimit['intervalNum'], 
                'limit': rateLimit['limit']}
            limitForRate['limitList'].append(newlimit)
            if (newlimit['limit'] > limitForRate['maxlen']):
                limitForRate['maxlen'] = newlimit['limit']

    def __init__(self, exchangeInfo):
        self.updateRateLimits(exchangeInfo['rateLimits'])
        self.lock = asyncio.Lock()

def gotGetHistoryResult(future):
    print(future.result())

async def getHistories(coros, maxnum = 50):
    runningTasks = []
    firstRun = min(len(coros), maxnum)
    for i in range(0, firstRun):
        task = asyncio.ensure_future(coros[i])
        task.add_done_callback(gotGetHistoryResult)
        runningTasks.append(task)

    if (firstRun < len(coros)):
        i = firstRun
        while i < len(coros):
            await asyncio.sleep(0)
            for t in runningTasks:
                if (t.done()):
                    runningTasks.remove(t)
                    task = asyncio.ensure_future(coros[i])
                    task.add_done_callback(gotGetHistoryResult)
                    runningTasks.append(task)
                    i = i + 1
                    break
    while (len(runningTasks)) > 0:
        await asyncio.sleep(0)
        for t in runningTasks:
            if (t.done()):
                runningTasks.remove(t)

def dtStringToMillis(strDate):
    epoch = datetime.datetime.utcfromtimestamp(0)
    dt = parser.parse(strDate)
    result = (dt - epoch).total_seconds()*1000.0
    return result


async def main():
    exchangeInfo = await getExchangeInfo()
    rateLimiter = RateLimiter(exchangeInfo)
    # get exchange info has weight 10
    await rateLimiter.addWeightAndRequest(10, 1)

    relevantCoinDict = {}
    with open('coins.csv') as relevantCoinFile:
        csvReader = csv.reader(relevantCoinFile, delimiter=',')
        for row in csvReader:
            relevantCoinDict[row[1]] = row[0]
    
    tradeSymbols = set()
    touchedCoins = set()
    for coinSymbol in exchangeInfo['symbols']:
        if (coinSymbol['baseAsset'] in relevantCoinDict) and (coinSymbol['quoteAsset'] in relevantCoinDict):
            tradeSymbols.add(coinSymbol['symbol'])
            touchedCoins.add(coinSymbol['baseAsset'])
            touchedCoins.add(coinSymbol['quoteAsset'])

    argParser = argparse.ArgumentParser()
    argParser.add_argument('--end', help='Fetch coin historical data up to this datetime')
    argParser.add_argument('--interval', choices=['1m', '1d'], default='1m', help='1m (default) or 1d interval')
    argParser.add_argument('--source_dir', default='./coindata', help='Path where .csv files are stored (default is ./coindata)')


    args = argParser.parse_args()
    endTimeInMillis = round(time.time() * 1000) if (args.end is None) else dtStringToMillis(args.end)
    outputDir = Path(args.source_dir)
    if (outputDir.exists()):
        if not outputDir.is_dir():
            raise ValueError('argument for --source_dir exists but is not a directory')
    else:
        outputDir.mkdir(parents=True)
    
    getSymbolDataCoros = [getSymbolHistory(endTimeInMillis, symbol, rateLimiter, args.interval, outputDir) for symbol in tradeSymbols]
    await getHistories(getSymbolDataCoros)

    unseenCoins = relevantCoinDict.keys() - touchedCoins
    coinMetadata = {}
    coinMetadata['included-coins'] = list(touchedCoins)
    coinMetadata['trade-symbols'] = list(tradeSymbols)
    coinMetadata['excluded-coins'] = list(unseenCoins)
    coinMetadata['exchange-info'] = exchangeInfo
    with open(outputDir / "coin-metadata.json", "w") as metaFile:
        json.dump(coinMetadata, metaFile)


    time.sleep(3)
    print("All finished")


if __name__ == "__main__":
    asyncio.run(main())

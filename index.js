const fs = require('fs');
const AdmZip = require("adm-zip");
const readLastLines = require('read-last-lines');
const axios = require('axios');
const { RestClient } = require('ftx-api');
require('dotenv').config();

class ByteBuffer {

  constructor (size) {
    this.buffer = new Uint8Array(size);
    this.index = 0;
  }

  writeToFile (file) {
    fs.writeFileSync(file, this.buffer);
  }

  writeInts(nums) {
    const intArray = new Int32Array(nums.length);

    for (let i = 0; i < nums.length; i++) {
      intArray[i] = nums[i];
    }

    const byteArray = new Int8Array(intArray.buffer);

    for (let i = 0; i < byteArray.length; i++) {
      this.buffer[this.index++] = byteArray[i];
    }
  }

  writeFloats(nums) {
    const floatArray = new Float32Array(nums.length);

    for (let i = 0; i < nums.length; i++) {
      floatArray[i] = nums[i];
    }

    const byteArray = new Int8Array(floatArray.buffer);

    for (let i = 0; i < byteArray.length; i++) {
      this.buffer[this.index++] = byteArray[i];
    }
  }
}

const Binance = require('node-binance-api');
const { inflate } = require('zlib');
const binance = new Binance().options({
  APIKEY: process.env.BINANCE_APIKEY,
  APISECRET: process.env.BINANCE_APISECRET,
  recvWindow: 30000
});

const ftxClient = new RestClient(
  '3emPPGOKwTEHB2DUIWViROFduPEUaj5kalABqmXX',
  'bcxc_bpc-a5XmfSTXxYhL81b1FzRbnnYqEUtoc1s',
  {
    subAccountName: 'bot'
  }
);

const intervalToMs = (interval) => {
  const num = parseInt(interval.substr(0, interval.length - 1));
  const identifier = interval[interval.length - 1];

  if (identifier === 's') return num * 1000;
  if (identifier === 'm') return num * 60 * 1000;
  if (identifier === 'h') return num * 3600 * 1000;
  if (identifier === 'd') return num * 3600 * 24 * 1000;
  if (identifier === 'w') return num * 3600 * 24 * 7 * 1000;

  throw new Error('Unable to parse interval');
};

function spotCandlesticks (pairCode, interval, options) {
  return new Promise((resolve, reject) => {
    binance.candlesticks(pairCode, interval, (error, ticks, symbol) => {
      if (error) {
        reject(error);
      } else {
        resolve(ticks);
      }
    }, options);
  });
}

async function loadCandles (pairCode, interval, time, futures, exchange) {
  if (exchange === 'binance') {
    const options = {
      limit: futures ? 1500 : 500,
      startTime: time
    };

    const ticks = await (futures ? binance.futuresCandles : spotCandlesticks)(pairCode, interval, options);

    return ticks.map(stick => {
      const [time, open, high, low, close] = stick;
      return {
        time: time,
        open: open,
        close: close,
        low: low,
        high: high
      };
    });
  } else if (exchange === 'oanda') {
    const response = await axios.get(`https://api-fxpractice.oanda.com/v3/instruments/${pairCode}/candles`, {
      headers: {
        'Authorization': 'Bearer 0d75cb8df7893ceff26b8a6cdb1f0f36-09476e9a2215911ebe433589b5a8e424'
      },
      params: {
        granularity: interval.substr(interval.length - 1, 1).toUpperCase() + interval.substr(0, interval.length - 1),
        count: 5000,
        from: Math.floor(time / 1000)
      }
    });

    return response.data.candles.filter(candle => candle.complete && new Date(candle.time) >= time).map(candle => ({
      time: new Date(candle.time).getTime(),
      open: candle.mid.o,
      close: candle.mid.c,
      low: candle.mid.l,
      high: candle.mid.h
    }));
  } else if (exchange === 'ftx') {
    while (true) {
      const startTime = Math.floor(time / 1000);
      const endTime = startTime + (5000 * 3600);

      const response = await ftxClient.getHistoricalPrices({
        market_name: pairCode,
        resolution: Math.floor(intervalToMs(interval) / 1000),
        limit: 5000,
        start_time: startTime,
        end_time: endTime
      });

      if (response.result.length === 0 && endTime < new Date().getTime() / 1000) {
        time = endTime * 1000;
        continue;
      }

      return response.result;
    }
  } else {
    throw new Error(`Unknown exchange '${exchange}'`);
  }
}

async function downloadPair (pairCode, interval, futures, exchange, startDate) {
  const file = `cache/${pairCode}-${interval}-${futures ? 'futures' : 'spot'}-${exchange}-data.csv`;

  let time = startDate.getTime();
  let append = false;

  if (fs.existsSync(file)) {
    const line = await readLastLines.read(file, 1);
    if (typeof line == 'string' && line.length > 0) {
      time = parseInt(line.split(',')[0]);
      append = true;
    }
  }

  console.log(`Downloading ${pairCode} starting at ${new Date(time)}`);

  const allSticks = [];
  let hasMore = false;

  while (true) {
    try {
      const sticks = await loadCandles(pairCode, interval, time, futures, exchange);
      if (sticks.length == 0) break;

      allSticks.push(...sticks);
      time = sticks[sticks.length - 1].time + 1000;

      if (allSticks.length >= 50000) {
        hasMore = true;
        console.log(`Downloaded 50,000 sticks to time ${time}`)
        break;
      }
    } catch (e) {
      console.error(e);
    }
  }

  const lines = [append ? '' : 'timestamp,open,high,low,close'];
  for (const s of allSticks) {
    lines.push(`${s.time},${s.open},${s.high},${s.low},${s.close}`);
  }

  const csv = lines.join('\n');

  if (append) {
    fs.appendFileSync(file, csv);
  } else {
    fs.writeFileSync(file, csv);
  }

  return hasMore;
}

async function getSymbols (futures, exchange, tradingSymbol) {
  let symbols = [];

  if (exchange === 'binance') {
    if (futures) {
      symbols = (await binance.futuresExchangeInfo()).symbols.map(s => s.symbol);
    } else {
      symbols = (await binance.exchangeInfo()).symbols.map(s => s.symbol);
    }
  } else if (exchange === 'oanda') {
    const response = await axios.get('https://api-fxpractice.oanda.com/v3/accounts/101-012-19478979-002/instruments', {
      headers: {
        'Authorization': 'Bearer 0d75cb8df7893ceff26b8a6cdb1f0f36-09476e9a2215911ebe433589b5a8e424'
      }
    });
    symbols = response.data.instruments.map(instrument => instrument.name);
  } else if (exchange === 'ftx') {
    const response = await ftxClient.listAllFutures();
    symbols = response.result.filter(item => item.name.endsWith('-PERP')).map(item => item.name);
  } else {
    throw new Error(`Unknown exchange '${exchange}'`);
  }

  return (tradingSymbol ? symbols.filter(symbol => symbol.endsWith(tradingSymbol)) : symbols).filter(s => process.env.FILTER.indexOf(s) !== -1);
}

async function download (interval, tradingSymbol, futures, exchange, startDate) {
  const symbols = await getSymbols(futures, exchange, tradingSymbol);
  let i = 0;

  for (const symbol of symbols) {
    let hasMore = false;
    do {
      hasMore = await downloadPair(symbol, interval, futures, exchange, startDate);
    } while (hasMore);
    console.log(`Downloaded ${++i}/${symbols.length}`);
  }
}

async function convertText (file, text, delimiter, dateFormat = 'ms', ohlcIndex = [1, 2, 3, 4], skip = 1, reverse = false) {
  const lines = text.split('\n').filter(line => line.length > 0).slice(skip);

  if (reverse) {
    lines.reverse();
  }

  let prevTime = 0;

  for (let i = 0; i < lines.length; i++) {
    const parts = lines[i].split(delimiter);

    let time = 0;

    if (dateFormat === 'ms') {
      if (parseInt(parts[0]) < 100000000000) parts[0] = parts[0] + '0000';
      time = parseInt(parts[0]) / 1000;
    } else if (dateFormat === 'datetime') {
      time = new Date(
        parseInt(parts[0].substr(0, 4)),
        parseInt(parts[0].substr(4, 2)) - 1,
        parseInt(parts[0].substr(6, 2)),
        parseInt(parts[0].substr(9, 2)),
        parseInt(parts[0].substr(11, 2)),
        parseInt(parts[0].substr(13, 2)),
      ).getTime();
    } else if (dateFormat === 's') {
      time = parseInt(parts[0]);
    } else if (dateFormat === 'half') {
      if (parts[0].indexOf('.') === -1) {
        time = Math.floor(parseInt(parts[0]) / 1000);
      } else {
        time = Math.floor(parseFloat(parts[0]));
      }
    } else {
      throw new Error('Unknown date format ' + dateFormat);
    }

    if (time === prevTime) {
      lines.splice(i, 1);
      i--;
      continue;
    }

    prevTime = time;
  }

  const buffer = new ByteBuffer((lines.length) * 20);

  for (let i = 0; i < lines.length; i++) {
    const parts = lines[i].split(delimiter);

    let time = 0;

    if (dateFormat === 'ms') {
      if (parseInt(parts[0]) < 100000000000) parts[0] = parts[0] + '000';
      time = parseInt(parts[0]) / 1000;
    } else if (dateFormat === 'datetime') {
      time = new Date(
        parseInt(parts[0].substr(0, 4)),
        parseInt(parts[0].substr(4, 2)) - 1,
        parseInt(parts[0].substr(6, 2)),
        parseInt(parts[0].substr(9, 2)),
        parseInt(parts[0].substr(11, 2)),
        parseInt(parts[0].substr(13, 2)),
      ).getTime();
    } else if (dateFormat === 's') {
      time = parseInt(parts[0]);
    } else if (dateFormat === 'half') {
      if (parts[0].indexOf('.') === -1) {
        time = Math.floor(parseInt(parts[0]) / 1000);
      } else {
        time = Math.floor(parseFloat(parts[0]));
      }
    } else {
      throw new Error('Unknown date format ' + dateFormat);
    }

    buffer.writeInts([
      Math.floor(time)
    ]);
    buffer.writeFloats([
      parseFloat(parts[ohlcIndex[0]]),
      parseFloat(parts[ohlcIndex[1]]),
      parseFloat(parts[ohlcIndex[2]]),
      parseFloat(parts[ohlcIndex[3]])
    ]);
  }

  fs.mkdirSync('../trading-simulator/public/data/' + file.split('/').slice(0, -1).join('/'), {recursive: true});

  buffer.writeToFile('../trading-simulator/public/data/' + file.replace('.csv', '.bin').replace('.zip', '.bin'));
  console.log('Converted ' + file);
}

async function convert (interval, tradingSymbol, futures, exchange) {
  const symbols = await getSymbols(futures, exchange, tradingSymbol);

  for (const symbol of symbols) {
    const file = `cache/${symbol}-${interval}-${futures ? 'futures' : 'spot'}-${exchange}-data.csv`;
    if (!fs.existsSync(file)) continue;

    const text = fs.readFileSync(file).toString();
    await convertText(file, text, ',');
  }
}

function unzipCSV (file) {
  const zip = new AdmZip(file);
  const zipEntries = zip.getEntries();

  for (const zipEntry of zipEntries) {
    if (zipEntry.entryName.endsWith('.csv')) {
      return zipEntry.getData().toString('utf8'); 
    }
  }
}

async function convertFX () {
  const dirs = fs.readdirSync('fx');

  for (const dir of dirs) {
    const files = fs.readdirSync('fx/' + dir);

    for (let file of files) {
      file = 'fx/' + dir + '/' + file;

      const text = unzipCSV(file);

      await convertText(file, text, ';', 'datetime');
    }
  }
}

async function convertHistorical () {
  const pairs = ['BTCUSDT', 'ETHUSDT'];

  for (const pair of pairs) {
    const file = `history/${pair}-1h-historical-data.csv`;

    const text = fs.readFileSync(file).toString();

    await convertText(file, text, ',', 'ms', [3, 4, 5, 6], 1, true);
  }
}

async function calculateSlippage () {
  const orders = (await binance.futuresAllOrders())
    .filter(order => order.status === 'FILLED' && order.type === 'MARKET');

  let total = 0;
  let count = 0;
  
  for (const order of orders) {
    const ticks = await binance.futuresCandles(order.symbol, '1h', {
      limit: 1,
      endTime: order.updateTime
    });

    const averagePrice = parseFloat(order.avgPrice);
    const open = parseFloat(ticks[0][1]);

    let change = (open - averagePrice) / averagePrice;

    if (
      (order.side === 'BUY' && order.origType === 'STOP_MARKET') || 
      (order.side === 'SELL' && order.origType === 'MARKET')
    ) {
      change *= -1;
    }

    total += change;
    count ++;

    console.log(`${count}/${orders.length}`);
  }

  console.log(`${((total / count) * 100).toFixed(2)} %`);
}

const mode = process.argv[2];

if (mode === '1h') {
  download('1h', 'USDT', true, 'binance', new Date('2017-01-01'))
    .then(() => convert('1h', 'USDT', true, 'binance'))
    .catch(console.error);
} else if (mode === '1h-ftx') {
  download('1h', undefined, true, 'ftx', new Date('2017-01-01'))
    .then(() => convert('1h', undefined, true, 'ftx'))
    .catch(console.error);
} else if (mode === 'spot') {
  download('1h', 'USDT', false, 'binance', new Date('2017-01-01'))
    .then(() => convert('1h', 'USDT', false, 'binance'))
    .catch(console.error);
} else if (mode === 'btc') {
  download('1h', 'BTC', false, 'binance', new Date('2017-01-01'))
    .then(() => convert('1h', 'BTC', false, 'binance'))
    .catch(console.error);
} else if (mode === '1m') {
  download('1m', 'USDT', true, 'binance', new Date('2021-05-20'))
    .then(() => convert('1m', 'USDT', true, 'binance'))
    .catch(console.error);
} else if (mode === 'oanda') {
  download('1h', undefined, true, 'oanda', new Date('2000-01-01'))
    .then(() => convert('1h', undefined, true, 'oanda'))
    .catch(console.error);
} else if (mode === 'history') {
  convertHistorical().catch(console.error);
} else if (mode === 'fx') {
  convertFX().catch(console.error);
} else if (mode === 'volume-btc') {
  binance.prevDay().then(result => console.log(
    Object.values(result)
      .filter(item => item.symbol.endsWith('BTC'))
      .sort((a, b) => parseFloat(b.quoteVolume) - parseFloat(a.quoteVolume))
      .slice(0, 50)
      .map(item => item.symbol)
  ));
} else if (mode === 'volume-usdt') {
  binance.futuresDaily().then(result => console.log(
    Object.values(result)
      .filter(item => item.symbol.endsWith('USDT'))
      .sort((a, b) => parseFloat(b.quoteVolume) - parseFloat(a.quoteVolume))
      .slice(0, 50)
      .map(item => item.symbol)
  ));
} else if (mode === 'slippage') {
  calculateSlippage().catch(console.error);
} else if (mode === 'ftx') {
  // ftxClient.listAllFutures().then(result => {
  //   console.log(result.result.filter(item => item.name.endsWith('-PERP')).map(item => item.name));
  // });

  // client.getMarket('BTC-PERP').then(response => console.log(response.result));

  // ftxClient.getBalances().then(response => {
  //   const portfolio = response.result.reduce((a, item) => a + item.usdValue, 0);
  //   console.log('Portfolio', portfolio);
    
  //   ftxClient.getPositions().then(positionsResponse => {
  //     console.log('Available', portfolio - positionsResponse.result.reduce((a, item) => a + item.collateralUsed, 0));
  //   });
  // });

  ftxClient.getPositions(true).then(response => console.log(response.result.filter(item => item.size > 0)));

  // ftxClient.getHistoricalPrices({
  //   market_name: 'ADA-PERP',
  //   resolution: 3600,
  //   limit: 10
  // }).then(response => console.log(response.result));

  // client.listAllFutures().then(result => {
  //   console.log(result.result
  //     .filter(item => item.name.endsWith('-PERP'))
  //     .sort((a, b) => b.volumeUsd24h - a.volumeUsd24h)
  //     .map(item => `${item.name} - ${item.volumeUsd24h}`)
  //     .slice(0, 20)
  //   );
  // }).catch(err => {
  //   console.error("getMarkets error: ", err);
  // });
}

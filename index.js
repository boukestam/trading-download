const fs = require('fs');
const AdmZip = require("adm-zip");
const readLastLines = require('read-last-lines');
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

async function loadCandles (pairCode, interval, time, futures) {
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
}

async function downloadPair (pairCode, interval, futures, startDate) {
  const file = `cache/${pairCode}-${interval}-${futures ? 'futures' : 'spot'}-data.csv`;

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
      const sticks = await loadCandles(pairCode, interval, time, futures);
      if (sticks.length == 0) break;

      allSticks.push(...sticks);
      time = sticks[sticks.length - 1].time + 1;

      if (allSticks.length >= 50000) {
        hasMore = true;
        break;
      }
    } catch { }
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

async function download (interval, tradingSymbol, futures, startDate) {
  const info = await (futures ? binance.futuresExchangeInfo : binance.exchangeInfo)();
  const symbols = info.symbols.map(s => s.symbol).filter(symbol => symbol.endsWith(tradingSymbol));

  for (const symbol of symbols) {
    let hasMore = false;
    do {
      hasMore = await downloadPair(symbol, interval, futures, startDate);
    } while (hasMore);
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

  fs.mkdirSync('../webworkers-comlink-typescript-react/public/data/' + file.split('/').slice(0, -1).join('/'), {recursive: true});

  buffer.writeToFile('../webworkers-comlink-typescript-react/public/data/' + file.replace('.csv', '.bin').replace('.zip', '.bin'));
  console.log('Converted ' + file);
}

async function convertBinance (interval, tradingSymbol, futures) {
  const info = await (futures ? binance.futuresExchangeInfo : binance.exchangeInfo)();
  const symbols = info.symbols.map(s => s.symbol).filter(symbol => symbol.endsWith(tradingSymbol));

  for (const symbol of symbols) {
    const file = `cache/${symbol}-${interval}-${futures ? 'futures' : 'spot'}-data.csv`;
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
    const file = `history/Binance_${pair}_1h.csv`;

    const text = fs.readFileSync(file).toString();

    await convertText(file, text, ',', 'half', [3, 4, 5, 6], 2, true);
  }
}

binance.prevDay().then(result => console.log(
  Object.values(result)
    .filter(item => item.symbol.endsWith('BTC'))
    .sort((a, b) => parseFloat(b.quoteVolume) - parseFloat(a.quoteVolume))
    .slice(0, 50)
    .map(item => item.symbol)
));

const mode = process.argv[2];

if (mode === 'futures') {
  download('1h', 'USDT', true, new Date('2017-01-01'))
    .then(() => convertBinance('1h', 'USDT', true))
    .catch(console.error);
} else if (mode === 'spot') {
  download('1h', 'USDT', false, new Date('2017-01-01'))
    .then(() => convertBinance('1h', 'USDT', false))
    .catch(console.error);
} else if (mode === 'btc') {
  download('1h', 'BTC', false, new Date('2017-01-01'))
    .then(() => convertBinance('1h', 'BTC', false))
    .catch(console.error);
} else if (mode === '1m') {
  download('1m', 'USDT', true, new Date('2021-05-20'))
    .then(() => convertBinance('1m', 'USDT', true))
    .catch(console.error);
} else if (mode === 'history') {
  convertHistorical().catch(console.error);
} else if (mode === 'fx') {
  convertFX().catch(console.error);
}

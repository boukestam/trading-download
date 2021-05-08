const fs = require('fs');
const readLastLines = require('read-last-lines');
require('dotenv').config();


const Binance = require('node-binance-api');
const binance = new Binance().options({
  APIKEY: process.env.BINANCE_APIKEY,
  APISECRET: process.env.BINANCE_APISECRET,
  recvWindow: 30000
});

const START_DATE = new Date('2017-01-01');

async function loadCandles (pairCode, interval, time) {
  const options = {
    limit: 1500,
    startTime: time
  };

  const ticks = await binance.futuresCandles(pairCode, interval, options);

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

async function downloadPair (pairCode) {
  const file = `${pairCode}-1m-data.csv`;

  let time = new Date(START_DATE).getTime();
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

  while (true) {
    try{
    const sticks = (await loadCandles(pairCode, process.env.INTERVAL, time));
    if (sticks.length == 0) break;

    allSticks.push(...sticks);
    time = sticks[sticks.length - 1].time + 1;
    }catch{}
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
}

async function download () {
  const info = await binance.futuresExchangeInfo();
  const symbols = info.symbols.map(s => s.symbol).filter(symbol => symbol.endsWith('USDT'));

  for (const symbol of symbols) {
    await downloadPair(symbol);
  }
}

class ByteBuffer {
  constructor (size) {
    this.buffer = new Uint8Array(size);
    this.index = 0;
  }

  writeToFile (file) {
    fs.writeFileSync(file, this.buffer);
  }

  writeInts (nums) {
    const intArray = new Int32Array(nums.length);

    for (let i = 0; i < nums.length; i++) {
      intArray[i] = nums[i];
    }

    const byteArray = new Int8Array(intArray.buffer);

    for (const byte of byteArray) {
      this.buffer[this.index++] = byte;
    }
  }

  writeFloats (nums) {
    const floatArray = new Float32Array(nums.length);

    for (let i = 0; i < nums.length; i++) {
      floatArray[i] = nums[i];
    }

    const byteArray = new Int8Array(floatArray.buffer);

    for (const byte of byteArray) {
      this.buffer[this.index++] = byte;
    }
  }
}

async function convert () {
  const info = await binance.futuresExchangeInfo();
  const symbols = info.symbols.map(s => s.symbol).filter(symbol => symbol.endsWith('USDT'));

  for (const symbol of symbols) {
    const file = `${symbol}-1m-data.csv`;
    if (!fs.existsSync(file)) continue;

    const text = fs.readFileSync(file).toString();
    const lines = text.split('\n');

    const buffer = new ByteBuffer((lines.length - 1) * 20);

    for (let i = 1; i < lines.length; i++) {
      const parts = lines[i].split(',');

      buffer.writeInts([
        Math.floor(parseInt(parts[0]) / 1000)
      ]);
      buffer.writeFloats([
        parseFloat(parts[1]),
        parseFloat(parts[2]),
        parseFloat(parts[3]),
        parseFloat(parts[4])
      ]);
    }

    buffer.writeToFile('../trading-ui/public/data/' + file.replace('.csv', '.bin'));
    console.log('Converted ' + file);
  }
}

//download().catch(console.error);
convert().catch(console.error);
const StreamrClient = require('streamr-client');
const _ = require('lodash');
const axios = require('axios');

// Ahoy Hacker, fill in this!
const STREAM_NAME = 'COINMARKETCAP';
const POLL_FREQUENCY = 25 * 1000;

const API_KEY = process.env.API_KEY
if (API_KEY === undefined) {
  throw new Error('Must export environment variable API_KEY');
}

// Initialize Streamr-Client library
const client = new StreamrClient({
    apiKey: API_KEY
});
let tickerStreams = {};

main().catch(console.error);

async function main() {
    // Get a Stream (creates one if does not already exist)
    const streamGlobal = await client.getOrCreateStream({
        name: 'COINMARKETCAP_GLOBAL'
    });
    console.info("Initialized stream:", streamGlobal.id);

    const allTickersStream = await client.getOrCreateStream({
        name: 'Coinmarketcap Top 100'
    });
    console.info("Initialized stream:", allTickersStream.id);

    // Generate streams for each ticker/symbol (BTC, ETH, ...)
    const tickers = await getTickersData();

    // Create streams, collect to tickerStreams by ticker. For ex. tickerStream['BTC']
    for (let symbol in tickers) {
        const stream = client.getOrCreateStream({
            name: `Coinmarketcap data for ${symbol}`
        });
        if (tickers.hasOwnProperty(symbol)) {
            tickerStreams[symbol] = stream;
        }
    }

    axios.all(_.values(tickerStreams)).then((values) => {
        console.log('All streams created: ');
    });
    // Generate and produce randomized data to Stream
    await globalGenerateEventAndSend(streamGlobal, 0);
    await sendAllTickersStream(allTickersStream, 0);
    await tickersGenerateEventAndSend(tickerStreams, 0);
}

async function globalGenerateEventAndSend(stream, i) {
    // Create stream for coinmarketcap global data
    try {
        const globalResponse = await axios.get('https://api.coinmarketcap.com/v2/global/');
        const globalDataPoint = globalToDataPoint(globalResponse.data, i);
        await stream.produce(globalDataPoint);
        console.info('Event sent:', globalDataPoint);
    } catch (error) {
        console.log(error);
    }
    // Send next package in 3 seconds
    setTimeout(globalGenerateEventAndSend.bind(null, stream, i + 1), POLL_FREQUENCY);
}

async function getTickersData() {
    let tickers = {};
    try {
        const tickerResponse = await axios.get('https://api.coinmarketcap.com/v2/ticker/?convert=BTC');
        // console.log(tickerResponse.data);
        _.forEach(tickerResponse.data.data, (ticker) => {
           tickers[ticker.symbol] = ticker;
        });
        return tickers;
        // return _.take(tickers, 3);
    } catch (error) {
        console.log(error);
    }
}

async function tickersGenerateEventAndSend(tickerStreams, i) {
// TODO: when sending responses, use .all()
    // Create stream for
    console.log('tickerStreams: ', tickerStreams);
    try {
        const tickers = await getTickersData();
        // console.log('TickerResponse: ', tickers);
        for (let symbol in tickers) {
            if (tickers.hasOwnProperty(symbol)) {
                const tickerDataPoint = tickerToDataPoint(tickers[symbol], i);
                let stream = await tickerStreams[symbol];
                stream.produce(tickerDataPoint);
                // console.log('tickerStream for ' + symbol, stream);
                console.info(`Event sent for ${symbol}:`, tickerDataPoint);
            }
        }
    } catch (error) {
        console.log(error);
    }
    setTimeout(tickersGenerateEventAndSend.bind(null, tickerStreams, i + 1), POLL_FREQUENCY);
}

async function sendAllTickersStream(stream, i) {
    // Create stream for
    try {
        const tickerResponse = await getTickersData();
        await stream.produce(tickerResponse);
        console.info('AllTickersStream event sent:', tickerResponse);
    } catch (error) {
        console.log(error);
    }
    setTimeout(sendAllTickersStream.bind(null, stream, i + 1), POLL_FREQUENCY);
}


function globalToDataPoint(response, messageNo) {
    return {
        "messageNo": messageNo,
        "active_cryptocurrencies": response.data.active_cryptocurrencies,
        "bitcoin_percentage_of_market_cap": response.data.bitcoin_percentage_of_market_cap,
        "total_market_cap_USD": response.data.quotes.USD.total_market_cap,
        "total_volume_USD": response.data.quotes.USD.total_volume_24h,
        "last_updated": response.data.last_updated
    };
}

function tickerToDataPoint(response, messageNo) {
    let dataPoint = {
        name: response.name,
        messageNo: messageNo,
        symbol: response.symbol,
        circulating_supply: response.circulating_supply,
        total_supply: response.total_supply,
        max_supply: response.max_supply,
        price_USD: response.quotes.USD.price,
        volume_24h_USD: response.quotes.USD.volume_24h,
        market_cap_USD: response.quotes.USD.market_cap,
        percent_change_1h_USD: response.quotes.USD.percent_change_1h,
        percent_change_24h_USD: response.quotes.USD.percent_change_24h,
        percent_change_7d_USD: response.quotes.USD.percent_change_7d,
        last_updated: response.last_updated
    };
    if (response.quotes.BTC) {
        dataPoint.price_USD =  response.quotes.BTC.price,
        dataPoint.volume_24h_BTC = response.quotes.BTC.volume_24h,
        dataPoint.market_cap_BTC = response.quotes.BTC.market_cap,
        dataPoint.percent_change_1h_BTC = response.quotes.BTC.percent_change_1h,
        dataPoint.percent_change_7d_BTC = response.quotes.BTC.percent_change_7d,
        dataPoint.percent_change_24h_BTC = response.quotes.BTC.percent_change_24h
    }
    return dataPoint;
}
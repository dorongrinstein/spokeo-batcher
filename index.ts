import * as fs from 'fs'
const parse = require('csv-parse/lib/sync')
const http = require('axios')
const secrets = require('./secrets.json')

const baseUrl = 'https://demo-api.spokeo.com/people'

let file = fs.readFileSync('./names.csv')
const records = parse(file, {
    columns: false,
    skip_empty_lines: true
});

function lastName(words: string) {
    var n = words.split(" ");
    return n[n.length - 1];
}

interface Record {
    record: any,
    query: string,
    result: any,
    idx: number
}

let queries: Record[] = [];
let counter = 0;

for (let i = 0; i < records.length; i++) {
    let last = lastName(records[i][0])
    let address = records[i][1]
    let zip = records[i][4]
    if (last.toLowerCase() != 'llc') {
        let q = '?last_name=' + last
        q += '&address=' + address
        q += '&zip=' + zip
        queries.push({ record: records[i], query: q, result: null, idx: counter++ });
    }
}

async function process(line: Record, itemNumber: number) {
    let result = await fetchData(line.query)
    queries[line.idx].result = result.data
    let fileName = `./files/${line.idx}.json`
    console.log('writing ' + fileName)
    fs.writeFileSync(fileName, JSON.stringify(queries[line.idx]))
}

function fetchData(data: any) {
    let instance = http.create({
        baseURL: baseUrl,
        timeout: 2000,
        headers: { "X-API-KEY": secrets.api_key }
    });

    return instance.get(data);
}

let max = queries.length;
const batchSize = 60;
const sleepBetweenBatches = 65; //seconds
let curr = 0;

function processAll() {
    let localMax = curr + batchSize
    for (let i = curr; i < localMax; i++) {
        if (i >= max)
            return;
        let result = process(queries[i], i)
        queries[queries[i].idx].result = result
        curr++;
    };
    setTimeout(function () {
        processAll();
    }, sleepBetweenBatches * 1000)
}


setTimeout(function () {
    fs.writeFileSync('./files/all.json', JSON.stringify(queries))
}, 12 * 60 * 1000)
processAll();
console.log('processing')
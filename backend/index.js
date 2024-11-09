const express = require('express');
const fs = require('fs');
const csv = require('csv-parser');
const app = express();
const PORT = 3000;

const data = '../spark/content/content-csv.csv'

let clusteringResults = [];

fs.createReadStream(`${data}`)
    .pipe(csv())
    .on('data', (row) => {
        clusteringResults.push(row);
    })
    .on('end', () => {
        console.log('CSV file successfully processed');
    });

app.get('/api/clustering-results', (req, res) => {
    res.json(clusteringResults);
});

app.get('/api/clustering-results/:transID', (req, res) => {
    const transID = req.params.transID;
    const result = clusteringResults.find((item) => item.transID === transID);
    if (result) {
        res.json(result);
    } else {
        res.status(404).send('Transaction ID not found');
    }
});

app.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);
});

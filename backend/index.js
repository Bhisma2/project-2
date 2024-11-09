const express = require('express');
const fs = require('fs');
const csv = require('csv-parser');
const app = express();
const PORT = 3000;

const batchFiles = [
    '../spark/cluster/batch1.csv',
    '../spark/cluster/batch2.csv',
    '../spark/cluster/batch3.csv'
];

let clusteringResults = [];

function readBatchFile(filePath) {
    return new Promise((resolve, reject) => {
        const results = [];
        fs.createReadStream(filePath)
            .pipe(csv())
            .on('data', (row) => results.push(row))
            .on('end', () => {
                clusteringResults = clusteringResults.concat(results); // Gabungkan hasil ke array utama
                console.log(`CSV file ${filePath} successfully processed`);
                resolve();
            })
            .on('error', reject);
    });
}

Promise.all(batchFiles.map(readBatchFile))
    .then(() => {
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
    })
    .catch((error) => console.error('Error loading batch files:', error));

const express = require('express');
const fs = require('fs');
const csv = require('csv-parser');
const app = express();
const PORT = 3000;

app.use(express.json()); // To parse JSON bodies in POST requests

const batchFiles = [
    '../spark/cluster/batch1.csv',
    '../spark/cluster/batch2.csv',
    '../spark/cluster/batch3.csv'
];

let clusteringResults = [];

// Function to read batch CSV files and combine the results
function readBatchFile(filePath) {
    return new Promise((resolve, reject) => {
        const results = [];
        fs.createReadStream(filePath)
            .pipe(csv())
            .on('data', (row) => results.push(row))
            .on('end', () => {
                clusteringResults = clusteringResults.concat(results); // Combine results
                console.log(`CSV file ${filePath} successfully processed`);
                resolve();
            })
            .on('error', reject);
    });
}

// Read all batch files
Promise.all(batchFiles.map(readBatchFile))
    .then(() => {
        // Endpoint 1: Get all clustering results
        app.get('/api/clustering-results', (req, res) => {
            console.log("Fetching all clustering results.");
            res.json(clusteringResults);
        });

        // Endpoint 2: Get clustering result for a specific transID
        app.get('/api/clustering-results/:transID', (req, res) => {
            const transID = req.params.transID;
            console.log(`Searching for clustering result with transID: ${transID}`);
            const result = clusteringResults.find((item) => item.transID === transID);
            if (result) {
                console.log(`Result found for transID ${transID}:`, result);
                res.json(result);
            } else {
                console.log(`No result found for transID: ${transID}`);
                res.status(404).send('Transaction ID not found');
            }
        });

        // Endpoint 3: Get clustering result for a specific payAmount
        app.get('/api/clustering-by-payAmount/:payAmount', (req, res) => {
            const payAmount = parseFloat(req.params.payAmount);
            if (isNaN(payAmount)) {
                console.log(`Invalid payAmount provided: ${req.params.payAmount}`);
                return res.status(400).send('Invalid payAmount');
            }
            console.log(`Searching for results with payAmount >= ${payAmount}`);
            const results = clusteringResults.filter((item) => parseFloat(item.payAmount) >= payAmount);
            if (results.length > 0) {
                console.log(`Found ${results.length} results with payAmount >= ${payAmount}`);
                res.json(results);
            } else {
                console.log(`No results found with payAmount >= ${payAmount}`);
                res.status(404).send('No results found with that payAmount');
            }
        });

        // Endpoint 4: Filter results by payCardBank
        app.get('/api/clustering-by-payCardBank/:payCardBank', (req, res) => {
            const payCardBank = req.params.payCardBank.toLowerCase().trim();
            console.log(`Searching for results with payCardBank: '${payCardBank}'`);
        
            const filteredResults = clusteringResults.filter((item) => {
                if (item.payCardBank) {
                    return item.payCardBank.toLowerCase().trim().includes(payCardBank);
                }
                return false;
            });
            
            if (filteredResults.length > 0) {
                console.log(`Found ${filteredResults.length} results for payCardBank: ${payCardBank}`);
                res.json(filteredResults);
            } else {
                console.log(`No results found for payCardBank: ${payCardBank}`);
                res.status(404).send('No results found');
            }
        });              

        // Start the server
        app.listen(PORT, () => {
            console.log(`Server is running on http://localhost:${PORT}`);
        });
    })
    .catch((error) => console.error('Error loading batch files:', error));

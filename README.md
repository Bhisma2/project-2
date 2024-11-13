# Kafka Study case Project

## Transjakarta - Public Transportation Transaction

| Nama | NRP |
| ---------------------- | ---------- |
| Bhisma Elki Pratama | 5027221005 |
| Siti Nur Ellyzah | 5027221014 |
| Azzahra Sekar Rahmadina | 5027221035 |

## Tentang Dataset

### Deskripsi
Dataset ini adalah **simulasi data transaksi** Transjakarta, perusahaan transportasi umum di Jakarta, Indonesia. Transjakarta menyediakan layanan bus besar (BRT), bus sedang dan besar (non-BRT), serta minibus (Mikrotrans). Data ini diciptakan menggunakan Python dengan Faker dan Random, berdasarkan **data master asli** dari [Transjakarta GTFS Feed](https://ppid.transjakarta.co.id/pusat-data/data-terbuka/transjakarta-gtfs-feed), namun transaksi yang ditampilkan adalah data dummy.

### Tujuan Dataset
Dataset ini bertujuan membantu **analis data menguji kerangka analisis** tanpa menunggu data transaksi nyata. Walau data transaksinya hanya simulasi, data master yang digunakan adalah data nyata dari Transjakarta. Dengan dataset ini, analisis dapat dilakukan, seperti **identifikasi rute yang ramai**, **rute dengan lalu lintas padat**, dan dimensi lainnya.

### Inspirasi
Transjakarta terus berkembang sebagai perusahaan transportasi umum, namun data transaksi untuk analisis publik masih jarang tersedia. Dataset ini memberikan kesempatan untuk melakukan **analisis mendalam terkait operasi Transjakarta**, membantu studi dan pengembangan solusi transportasi yang lebih baik.

Dataset ini adalah **simulasi** dan **tidak mencerminkan data asli** Transjakarta.

## Langkah-langkah

1. Setup Docker
Atur konfigurasi docker pada file `docker-compose.yaml` sesuai kebutuhan kemudian jalankan command `docker-compose up -f` untuk menjalankan service yang dibutuhkan
![Screenshot 2024-11-12 170005](https://github.com/user-attachments/assets/8621e415-d53b-44fd-aa31-29301b653772)

2. Setup Producer dan Consumer
Buat script untuk menjalankan skenario producer dan consumer. Pastikan consumer dapat menerima aliran data yang dikirim producer berdasarkan dataset yang tersedia.
  ```bash
  cd kafka
  python3 producer.py
  python3 consumer.py
  ```
**producer.py**
![Screenshot 2024-11-12 171535](https://github.com/user-attachments/assets/b661680b-0f9f-445b-b5d2-677602b0e84c)

**consumer.py**
![Screenshot 2024-11-12 171553](https://github.com/user-attachments/assets/e502adf5-e32d-4023-94ec-5e431cf098dd)

3. Modelling Data

Terdapat 3 model dengan skema sebagai berikut:
- Model 1: Batch 1
- Model 2: Batch 1 + Batch 2
- Model 3: Batch 1 + Batch 2 + Batch 3

Hasilnya runningnya sebagai berikut :
![image](https://github.com/user-attachments/assets/03ae37bd-49e8-41e3-9608-35b1ce341f2d)
![image](https://github.com/user-attachments/assets/d3a20188-3787-4adc-a034-5812578e7111)
![image](https://github.com/user-attachments/assets/e1652c68-5cce-438a-9800-e10bf22b98da)

Berdasarkan training ini didapatkan hasil akurasi terbaik yaitu dengan menggunakan 2 batch dengan Best Silhouette Score for Model 2 with k=2: 0.7848913932070153 

Kemudian data ini akan di training dengan menggunakan algoritma K-Means dan hasil terbaik akan disimpan dalam bentuk `.csv` dan digunakan dalam endpoint

4. Routing Endpoints

Membuat file index.js untuk membuat endpoint 

## Hasil

### A. Endpoint for a specific transID

1. endpoint yang digunakan `'/api/clustering-results/:transID'`
2. Hasil
- Request
```bash
http://localhost:3000/api/clustering-results/YFYK070A9C91UE
```
- Response
```bash
{
  "transID": "YFYK070A9C91UE",
  "payCardBank": "dki",
  "payAmount": "3500.0",
  "prediction": "0"
}
```

### B. Endpoint for a specific payAmount
1. endpoint yang digunakan `/api/clustering-by-payAmount/:payAmount`
2. Hasil
- Request
```bash
http://localhost:3000/api/clustering-by-payAmount/3500.0
```
- Response
```bash
  {
    "transID": "EIIW227B8L34VB",
    "payCardBank": "emoney",
    "payAmount": "3500.0",
    "prediction": "4"
  },
```

### C. Endpoint Filter results by payCardBank
1. endpoint yang digunakan `'/api/clustering-by-payCardBank/:payCardBank'`
2. Hasil
- Request
```bash
http://localhost:3000/api/clustering-by-payCardBank/dki
```
- Response
```bash
  {
    "transID": "LGXO740D2N47GZ",
    "payCardBank": "dki",
    "payAmount": "3500.0",
    "prediction": "0"
  },
```




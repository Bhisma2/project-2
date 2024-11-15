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
Model 1: Menggunakan data selama 5 menit pertama.
Model 2: Menggunakan data selama 5 menit kedua.
Model 3: Menggunakan data selama 5 menit ketiga.

Hasilnya runningnya sebagai berikut :
![image](https://github.com/user-attachments/assets/f91ca132-e1ec-4e7f-ba01-feb2fe954144)
![image](https://github.com/user-attachments/assets/6e275143-ae35-4f47-9b05-702c70456db1)
![image](https://github.com/user-attachments/assets/6559230e-c58a-49b3-812c-274082d4f530)



Berdasarkan training ini didapatkan hasil akurasi terbaik yaitu dengan menggunakan 2 batch dengan Best Silhouette Score for Model 2 with k=2: 0.7848913932070153 

Kemudian data ini akan di training dengan menggunakan algoritma K-Means dan hasil terbaik akan disimpan dalam bentuk `.csv` dan digunakan dalam endpoint

4. Routing Endpoints

Membuat file index.js untuk membuat endpoint 

## Hasil

### A. Endpoint for a All Clustering Result

1. endpoint yang digunakan `'/api/clustering-results/'`
2. Hasil
- Request
```bash
http://localhost:3000/api/clustering-results/
```
- Response
```bash
{
    "transID": "EIIW227B8L34VB",
    "payCardBank": "emoney",
    "payAmount": "3500.0",
    "prediction": "4"
  },
  {
    "transID": "LGXO740D2N47GZ",
    "payCardBank": "dki",
    "payAmount": "3500.0",
    "prediction": "0"
  },
```

![WhatsApp Image 2024-11-13 at 09 52 09_429162a2](https://github.com/user-attachments/assets/adfb7fa2-584e-4882-8dc5-16ba316d9598)


### B. Endpoint for a specific transID

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
![WhatsApp Image 2024-11-13 at 09 52 09_4b40cd85](https://github.com/user-attachments/assets/2ff0c9c9-7173-46d5-b49b-e11d3ddf7076)



### C. Endpoint for a specific payAmount
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

![WhatsApp Image 2024-11-13 at 09 52 09_53b8f96f](https://github.com/user-attachments/assets/65dc8aee-749f-4073-9e11-853306338c9b)


### D. Endpoint Filter results by payCardBank
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
![WhatsApp Image 2024-11-13 at 09 52 09_999537c1](https://github.com/user-attachments/assets/ea7b8498-671b-4f16-84b5-16b2ed3ab3cd)

![WhatsApp Image 2024-11-13 at 09 54 05_0dcc7e6e](https://github.com/user-attachments/assets/4c48be17-a781-46bb-9146-361d2d2d2a0d)




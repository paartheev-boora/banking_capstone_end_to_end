# Day-1 Deliverable — Step-by-step README

---

## 1. Create Storage Account (ADLS Gen2 enabled)

1. Under **Advanced** enable **Hierarchical namespace** (this makes it ADLS Gen2).
2. After deployment, open the storage account, create container `raw`.
3. Inside `raw` create directories (virtual folders):
   - `atm/`
   - `upi/`
   - `customers/`
   You can create them in the portal by "Add Directory" inside the `raw` container.

---

## 2. Create Azure Function project locally (Event Grid trigger)

---

## 3. Run & Test Locally

---

## 4. Deploy Event Grid Function to Azure

1. After deployment, go to the Function App → **Configuration** → add application settings:
   - `AzureWebJobsStorage` → storage connection string
   - `QueueConnection` → storage connection string (used by queue output binding)

> **Important**: In production avoid storing secrets in app settings; use Key Vault and managed identity.
---

## 5. Create Storage Queue in the Storage Account
1. Create queue named: `ingestionqueue`.
---

## 6. Configure Event Grid Subscription for specific containers

---

## 7. Upload test files into the container folders

---

## 9. Monitoring & Troubleshooting

- **Function logs**
- **Queue messages**
- **Event Grid**

---

## 10. Result (screenshot)

Below is the screenshot showing messages in the `ingestionqueue` after uploading files to the `raw` container.

<img width="1854" height="878" alt="Screenshot 2025-12-05 115647" src="https://github.com/user-attachments/assets/dde3a834-f5e5-4a4d-931f-5fa8b8210f3a" />


---

# Day 2 Deliverable

## Started with day 1 deliverables but using Service Bus Queue this time as per requirement
## Done with it and got the results as expected.
### <img width="1919" height="852" alt="Screenshot 2025-12-06 102933" src="https://github.com/user-attachments/assets/f3c5ef79-636d-4842-82d5-d36b78ea5dea" />
## Started with 2nd day deliverables.
##### The Service Bus Queue Trigger function was successfully reading queues from Service Bus queue.
##### But the Function could not download the blob content using blob url from queue message.
##### It displayed a Authentication error 
##### for which I have decided to create a identity in Function App and create a role assignment in blob storage account using that identity.
##### Meanwhile, the cleanup has occured. And this stated actions were not only the part but also other errors from deployment as informed in the group.
##### I have done it on local using a VM in a free account first then and figured out it's working with a blob_url as input and fetching data to cosmos DB. 
##### Then, followed with role assignment.
##### <img width="1580" height="768" alt="sample cosmos 2" src="https://github.com/user-attachments/assets/1baa48a6-868d-4de4-a66d-8daeada2a0b2" />
##### <img width="853" height="666" alt="countof cosmos table" src="https://github.com/user-attachments/assets/3d2ea68b-10cd-4db0-9b32-40abb8d39157" />
##### Then after ran the parthday2.ipynb notebook to fetch the data from cosmos DB to silver and gold containers.
## 📌 2. Features Implemented

---

### ✅ **Schema Inference**

Automatically infers:

- **Required fields**
- **Field types:** `string`, `number`, `timestamp`  
  *(Derived from the first row of data)*

---

### ✅ **Row-Level Validation**

Each record is validated to ensure:

- No missing required fields  
- Correct datatype  
- Valid timestamps  
- Numeric fields are valid  

⚠️ **Invalid rows are skipped automatically.**

---

### ✅ **Transaction Classification**

Adds a new field **`transaction_type`** based on description & amount.

| Source | Classification |
|--------|----------------|
| **ATM** | WITHDRAWAL, DEPOSIT, OTHER |
| **UPI** | PAYMENT, OTHER |

---

### ✅ **Suspicious Activity Detection**

Two fraud-detection rules are implemented:

---

#### **1. High Value Transactions**

- **ATM ≥ ₹20,000**  
- **UPI ≥ ₹50,000**

These trigger **`HIGH_VALUE`** alerts.

---

#### **2. Rapid ATM Withdrawals**

Detects rapid withdrawal patterns:

- **3+ withdrawals**  
- **Within 5 minutes**  
- **From the same account**

### PySpark ETL for Silver and Gold Layers
#### 🧠 Code Approach

---

### 🔌 **1. Spark & Storage Initialization**
- Initializes a Spark session using the **Cosmos DB Spark Connector**.
- Connects to **ADLS Gen2** using **shared key authentication** for secure, controlled access.

---

### 📥 **2. Raw Data Ingestion**
Loads raw datasets from Cosmos DB containers:

- **ATMTransactions**
- **UPIEvents**
- **AccountProfile**

---

### ✔️ **3. Data Validation**
- Performs record-level validation using **row counts** on loaded DataFrames.
- Ensures data completeness before processing.

---

### 🥈 **4. Silver Layer Transformation**
Transforms raw data into clean, structured Silver tables by:

- Renaming columns for consistency  
- Classifying transactions (e.g.,  
  - `"ATM_WITHDRAWAL"` → negative ATM amounts  
  - `"UPI_PAYMENT"` → debit UPI transactions  
)  
- Casting columns to correct data types (timestamps, numeric fields, etc.)
- Joining with customer profiles to **detect anomalies** such as mismatched geo-locations.
- Flagging suspicious and fraudulent activity.

---

### 🥇 **5. Gold Layer Transformation**
Standardizes and aggregates transformed data:

- Unifies **ATM & UPI schemas** into common formats.
- Generates:
  - **FactTransactions** – enriched transaction-level fact table  
  - **FactFraudDetection** – logs all fraud-flagged events  
  - **FactCustomerActivity** – captures UPI customer behavior with derived activity types  

---

### 📦 **6. Optimized Storage Output**
- Writes and **overwrites** transformed datasets as **Parquet files** into the ADLS Gold container.
- Enables efficient querying for analytics and reporting workloads.

---
## 📅 Day 3: Synapse DW Schema and ETL Setup

---

## 🧠 Code Approach

### 🏗️ **1. Synapse Workspace & Dimensional Modeling**
- Configures **Synapse SQL workspace** with scripts to create dimensional tables/views such as:  
  - **DimCustomer**  
  - **DimAccount**  
  - **FactTransactions**  
  - **FactFraudDetection**  
  - **FactCustomerActivity**
- Uses **OPENROWSET** and auto-generated SQL to bulk load data from ADLS Parquet into Synapse.

---

### 🚀 **2. PySpark Initialization for ETL**
- Initializes **PySpark** in Synapse notebook.  
- Connects to Cosmos DB using **Cosmos DB Spark connector**.  
- Authenticates to ADLS Gen2 using **shared key** for secure data ingestion.

---

### 📥 **3. Raw Data Loading**
- Loads source data directly from Cosmos DB collections, e.g.:  
  ```python
  atm_raw = spark.read.format("cosmos.oltp")...






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

---





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
### The Service Bus Queue Trigger function was successfully reading queues from Service Bus queue.
### But the Function could not download the blob content using blob url from queue message.
### It displayed a Authentication error 
### for which I have decided to create a identity in Function App and create a role assignment in blob storage account using that identity.
### Meanwhile, the cleanup has occured. And this stated actions were not only the part but also other errors from deployment as informed in the group.

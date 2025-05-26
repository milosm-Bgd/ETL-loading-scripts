

Below is a complete documentation of the provided T‑SQL scripts, with explanations for each query/step, 
the purpose of key expressions, and why specific techniques are used.

---

# Complete Documentation for the POS Fingerprint & Mapping Process

This series of queries is part of a data warehousing process designed for mapping point‐of‑sale (POS) merchant information. The process does the following:

1. **Populates a monthly mapping table** (PosMonthlyFullMap) with transaction and fingerprint data.
2. **Checks for unmapped records** (to identify “problem children” that lack fingerprint associations).
3. **Inserts new POS Fingerprint records** into the finance shared schema after computing unique merchant “fingerprints” using consistent hash values.

Each query has been carefully constructed to ensure data consistency and proper linking between source systems.

---

## Query 1: Insert into PosMonthlyFullMap

```sql
use Dinintake;

truncate table Dinsandbox.[QA].[PosMonthlyFullMap];

insert into Dinsandbox.[QA].[PosMonthlyFullMap]
select 
    t.id, 
    t.transactionid, 
    fin.fingerprintid, 
    HASHBYTES('SHA1', 
        CAST(
            CONCAT(
                UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[MerchantNumber])),'') AS NVARCHAR(100))), ',',
                UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[MerchantLegalName])),'') AS NVARCHAR(100))), ',',
                UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[MerchantName])),'') AS NVARCHAR(100))), ',',
                UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[AddressLine01])),'') AS NVARCHAR(100))), ',',
                UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[CityName])),'') AS NVARCHAR(100))), ',',
                UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[StateProvince])),'') AS NVARCHAR(100))), ',',
                UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[PostalCode])),'') AS NVARCHAR(100))), ',',
                UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[CountryCode])),'') AS NVARCHAR(100)))
            ) AS NVARCHAR(1000)
        )
    ) as hash_value,
    dv.id
from Dinbilling.billing.detailvolumehistorydata dv   
inner join Dinintake.dbo.transactionheader t 
    on t.transactionid = dv.transactionid   
LEFT join Dinshared.finance.posfingerprints fin 
    on fin.simhash = HASHBYTES('SHA1', 
         CAST(
             CONCAT(
                 UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[MerchantNumber])),'') AS NVARCHAR(100))), ',',
                 UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[MerchantLegalName])),'') AS NVARCHAR(100))), ',',
                 UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[MerchantName])),'') AS NVARCHAR(100))), ',',
                 UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[AddressLine01])),'') AS NVARCHAR(100))), ',',
                 UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[CityName])),'') AS NVARCHAR(100))), ',',
                 UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[StateProvince])),'') AS NVARCHAR(100))), ',',
                 UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[PostalCode])),'') AS NVARCHAR(100))), ',',
                 UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[CountryCode])),'') AS NVARCHAR(100)))
             ) AS NVARCHAR(1000)
         )
    )
where dv.txndate >= '2024-10-01'
```

### **Purpose:**
- **Staging Data for Mapping:**  
  The query populates the `PosMonthlyFullMap` table used for monthly reconciliation of POS transactions.
- **Joining Source and Fingerprint Data:**  
  It extracts transaction header information from the `transactionheader` table (using an inner join with a detail table `detailvolumehistorydata`) and attempts to “map” each transaction to a POS fingerprint record.
- **Hash Computation for Fingerprinting:**  
  It computes a hash value using SHA1 over a concatenated, standardized string of merchant attributes:
  - **Data Standardization:**  
    Each field (Merchant Number, Merchant Legal Name, Merchant Name, Address, City, State, Postal Code, Country) is trimmed, converted to uppercase, and concatenated with commas as separators.  
  - **Purpose:**  
    This ensures that minor differences in spacing or casing do not affect the computed hash value and allows matching with pre‑computed fingerprints.
- **Left Join with POSFingerprints:**  
  The query joins to the `posfingerprints` table on the computed hash (simhash). If a match is found, the `fingerprintid` is included; otherwise, it remains null.

### **Key Points:**
- **Truncation:**  
  Before inserting, the target QA table is truncated to ensure no stale data remains.
- **Filtering Date:**  
  Only transactions where `dv.txndate` is after October 1, 2024, are considered.
- **Consistent Hashing:**  
  The use of the same hash expression in both the SELECT list and in the LEFT JOIN guarantees consistency when matching records.

---

## Query 2: Check Unmapped Records ("Problem Children")

```sql
select iif(pr.clientcode is null, 0, 1) as problemchild, 
       th.*, 
       fl.*
from  [DinSandbox].qa.[PosMonthlyFullMap]  p
inner join dbo.TransactionHeader th on th.id = p.Id
inner join Lookups.BankTypes b on b.DisplayName = th.BankType
inner join lookups.creditcardtypes c on c.displayname = th.creditcardtype
inner join Staging.RecordFlags fl on fl.id = th.id
left join Staging.ProblemChildren pr on pr.ClientCode = th.ClientCode 
     and pr.CreditCardTypeId = c.Id 
     and pr.BankTypeId = b.Id
where p.PosFingerprintId is null
order by 1, fl.PosFingerprintId
```

### **Purpose:**
- **Identify Unmapped Transactions:**  
  This query scans the `PosMonthlyFullMap` view/table for transactions that did not successfully map to a POS fingerprint (i.e. where `PosFingerprintId` is null).
- **Flag “Problem Children”:**  
  The expression `iif(pr.clientcode is null, 0, 1) as problemchild` is used to mark whether the transaction header’s client code (in combination with credit card and bank type) appears in the `ProblemChildren` table.
  
### **How It Works:**
- **Join Context:**  
  - Transactions from `PosMonthlyFullMap` are joined back to the corresponding `TransactionHeader` (via `th.id`) to access additional details.
  - Additional attributes such as bank type and credit card type are fetched from lookup tables.
  - RecordFlags and a left join to ProblemChildren are made to attach any known issues.
- **Filter Condition:**  
  The `where p.PosFingerprintId is null` clause narrows the output to only transactions that were not mapped based on fingerprint matching.
  
### **Key Points:**
- **Validation & Diagnostics:**  
  This query is meant for an analyst or ETL developer to diagnose which transactions remain unmapped so that further investigation (or remediation) can occur.
- **Ordering Results:**  
  The ordering by the computed problemchild code and then by `fl.PosFingerprintId` helps in focusing on the problematic records.

---

## Query 3: Insert New POS Fingerprints

### **Declaration Section:**
```sql
declare @startDate date = FORMAT(DATEADD(month,-1,GETDATE()), 'yyyy-MM-01'),
        @endDate date = EOMONTH(DATEADD(month,-1,GETDATE())), 
        @logId int = (select max(logid) from Dinshared.finance.posfingerprints);
```

- **Purpose:**  
  - **Date Range:**  
    These variables define the analysis timeframe for the previous month.
    - `@startDate`: First day of the previous month.
    - `@endDate`: Last day of the previous month.
  - **Log ID:**  
    `@logId` captures the current maximum log ID from the `posfingerprints` table to be referenced when new records are inserted.

### **INSERT Statement:**
```sql
INSERT INTO DinShared.finance.POSFingerprints 
    (SimHash, SimHashNoMid, MerchantNumber, MerchantLegalName, MerchantName, AddressLine01,               
     CityName, StateProvince, PostalCode, CountryCode, Certified, ClientCount,               
     BankFeedCount, AnalysisStartDate, AnalysisEndDate, Quality, LogID, CreatedOn, CreatedBy, MappingLastAttempt)        
SELECT     
	-- Computed SIMHASH for full merchant attributes:
    HASHBYTES('SHA1', 
        CAST(
            CONCAT(
                UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[MerchantNumber])),'') AS NVARCHAR(100))), ',',
                UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[MerchantLegalName])),'') AS NVARCHAR(100))), ',',
                UPPER(CAST(ISNULL(REPLACE(LTRIM(RTRIM(t.[MerchantName])),'REV:',''),'') AS NVARCHAR(100))), ',',
                UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[AddressLine01])),'') AS NVARCHAR(100))), ',',
                UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[CityName])),'') AS NVARCHAR(100))), ',',
                UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[StateProvince])),'') AS NVARCHAR(100))), ',',
                UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[PostalCode])),'') AS NVARCHAR(100))), ',',
                UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[CountryCode])),'') AS NVARCHAR(100)))
            ) AS NVARCHAR(1000)
        )
    ) as simhash,
	
	-- Computed SIMHASH for merchant attributes without MerchantNumber:
    HASHBYTES('SHA1', 
        CAST(
            CONCAT(
                UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[MerchantLegalName])),'') AS NVARCHAR(100))), ',',
                UPPER(CAST(ISNULL(REPLACE(LTRIM(RTRIM(t.[MerchantName])),'REV:',''),'') AS NVARCHAR(100))), ',',
                UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[AddressLine01])),'') AS NVARCHAR(100))), ',',
                UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[CityName])),'') AS NVARCHAR(100))), ',',
                UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[StateProvince])),'') AS NVARCHAR(100))), ',',
                UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[PostalCode])),'') AS NVARCHAR(100))), ',',
                UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[CountryCode])),'') AS NVARCHAR(100)))
            ) AS NVARCHAR(1000)
        )
    ) as simhashnomid,
	
	-- Standardizing merchant fields using IIF to convert empty strings to NULL:
    IIF(LTRIM(RTRIM(t.merchantNumber)) = '', null, LTRIM(RTRIM(t.merchantNumber))) merchantNumber,  
    IIF(LTRIM(RTRIM(t.merchantLegalName)) = '', null, LTRIM(RTRIM(t.merchantLegalName))) merchantLegalName,
    IIF(LTRIM(RTRIM(REPLACE(t.[MerchantName],'REV:',''))) = '', null, LTRIM(RTRIM(REPLACE(t.[MerchantName],'REV:','')))) MerchantName,
    IIF(LTRIM(RTRIM(t.addressLine01)) = '', null, LTRIM(RTRIM(t.addressLine01))) addressLine01,
    IIF(LTRIM(RTRIM(t.cityName)) = '', null, LTRIM(RTRIM(t.cityName))) cityName,
    IIF(LTRIM(RTRIM(t.StateProvince)) = '', null, LTRIM(RTRIM(t.StateProvince))) StateProvince,
    IIF(LTRIM(RTRIM(t.PostalCode)) = '', null, LTRIM(RTRIM(t.PostalCode))) PostalCode,
    IIF(LTRIM(RTRIM(t.countrycode)) = '', null, LTRIM(RTRIM(t.countrycode))) countrycode,
    
    'N' as Certified,  -- Flag marking the record as not yet certified
    COUNT(DISTINCT t.clientcode) AS ClientCount,  -- Count of distinct client codes related to the transaction
    1 AS BankFeedCount,  -- Hard-coded bank feed count, likely a business default
    @startDate as validfrom, 
    @endDate as validto,
    CASE when t.clientcode = 'mc' then 5 else 1 end as quality,  -- Quality is determined based on client code
    @logId as logid,
    GETUTCDATE() as CreatedOn, 
    'System' as CreatedBy,
    MAX(t.TransactionDate) as LastTransactionDate
FROM dbo.TransactionHeader t  
LEFT OUTER JOIN DinShared.finance.POSFingerprints fin 
    ON fin.SimHash = HASHBYTES('SHA1', 
           CAST(
               CONCAT(
                   UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[MerchantNumber])),'') AS NVARCHAR(100))), ',',
                   UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[MerchantLegalName])),'') AS NVARCHAR(100))), ',',
                   UPPER(CAST(ISNULL(REPLACE(LTRIM(RTRIM(t.[MerchantName])),'REV:',''),'') AS NVARCHAR(100))), ',',
                   UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[AddressLine01])),'') AS NVARCHAR(100))), ',',
                   UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[CityName])),'') AS NVARCHAR(100))), ',',
                   UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[StateProvince])),'') AS NVARCHAR(100))), ',',
                   UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[PostalCode])),'') AS NVARCHAR(100))), ',',
                   UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[CountryCode])),'') AS NVARCHAR(100)))
               ) AS NVARCHAR(1000)
           )
       )
WHERE t.transactiondate BETWEEN @startDate AND @endDate    
  AND ( 
         (t.countrycode IN ('US', 'PR', 'VI', 'GU', 'AS', 'MP', 'PW', 'UM', 'CA')           
          AND t.MccCode IN ('5814', '5813', '5812', '5811', '5499', '5462', '5441')
         )
         OR t.ClientCode = 'MC'
      )
  AND fin.FingerprintID IS NULL  -- Only insert new fingerprints (i.e., where no matching fingerprint already exists)
  AND ( t.MerchantName IS NOT NULL or t.MerchantLegalName IS NOT NULL )        
GROUP BY  
    IIF(LTRIM(RTRIM(t.merchantNumber)) = '', null, LTRIM(RTRIM(t.merchantNumber))),
    IIF(LTRIM(RTRIM(t.merchantLegalName)) = '', null, LTRIM(RTRIM(t.merchantLegalName))),
    IIF(LTRIM(RTRIM(REPLACE(t.[MerchantName],'REV:',''))) = '', null, LTRIM(RTRIM(REPLACE(t.[MerchantName],'REV:','')))),
    IIF(LTRIM(RTRIM(t.addressLine01)) = '', null, LTRIM(RTRIM(t.addressLine01))),
    IIF(LTRIM(RTRIM(t.cityName)) = '', null, LTRIM(RTRIM(t.cityName))),
    IIF(LTRIM(RTRIM(t.StateProvince)) = '', null, LTRIM(RTRIM(t.StateProvince))),
    IIF(LTRIM(RTRIM(t.PostalCode)) = '', null, LTRIM(RTRIM(t.PostalCode))),
    IIF(LTRIM(RTRIM(t.countrycode)) = '', null, LTRIM(RTRIM(t.countrycode))),
    HASHBYTES('SHA1', CAST(
        CONCAT(
            UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[MerchantNumber])),'') AS NVARCHAR(100))), ',',
            UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[MerchantLegalName])),'') AS NVARCHAR(100))), ',',
            UPPER(CAST(ISNULL(REPLACE(LTRIM(RTRIM(t.[MerchantName])),'REV:',''),'') AS NVARCHAR(100))), ',',
            UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[AddressLine01])),'') AS NVARCHAR(100))), ',',
            UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[CityName])),'') AS NVARCHAR(100))), ',',
            UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[StateProvince])),'') AS NVARCHAR(100))), ',',
            UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[PostalCode])),'') AS NVARCHAR(100))), ',',
            UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[CountryCode])),'') AS NVARCHAR(100)))
        ) as NVARCHAR(1000)
    )),
    HASHBYTES('SHA1', CAST(
        CONCAT(
            UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[MerchantLegalName])),'') AS NVARCHAR(100))), ',',
            UPPER(CAST(ISNULL(REPLACE(LTRIM(RTRIM(t.[MerchantName])),'REV:',''),'') AS NVARCHAR(100))), ',',
            UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[AddressLine01])),'') AS NVARCHAR(100))), ',',
            UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[CityName])),'') AS NVARCHAR(100))), ',',
            UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[StateProvince])),'') AS NVARCHAR(100))), ',',
            UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[PostalCode])),'') AS NVARCHAR(100))), ',',
            UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[CountryCode])),'') AS NVARCHAR(100)))
        ) as NVARCHAR(1000)
    )),
    CASE WHEN t.clientcode = 'mc' THEN 5 ELSE 1 END
```

### **Purpose:**
- **Insert New Fingerprint Records:**  
  This INSERT statement adds new records into the `POSFingerprints` table for merchants that do not yet have a fingerprint entry. The fingerprint is a unique identifier computed from key merchant attributes.
- **Fingerprint Computation:**  
  Two separate hash values are computed:
  - **SimHash:** Uses a full concatenation of merchant details (including MerchantNumber).
  - **SimHashNoMid:** Excludes the MerchantNumber, in case it needs a comparison without that attribute.
- **Data Standardization:**  
  The use of `LTRIM(RTRIM(...))` and `UPPER(...)` ensures that data is cleaned (removing extra spaces) and standardized (converted to uppercase) before concatenation.
- **Conditional Field Handling:**  
  The use of `IIF` converts empty strings to `NULL` for fields like merchantNumber, merchantLegalName, etc.
- **Additional Attributes and Grouping:**  
  Other fields such as Certified flag, ClientCount, BankFeedCount, and quality (which is set conditionally) are calculated.
- **Filtering:**  
  - The WHERE clause restricts the transactions to those between the start and end dates (previous month).  
  - It includes only transactions from specific country codes and MCC codes or with a specific client code (`MC`).
  - It ensures that only transactions where no corresponding fingerprint already exists (i.e. `fin.FingerprintID IS NULL`) are processed.
  - It also excludes transactions where both MerchantName and MerchantLegalName are NULL.
- **Grouping:**  
  The GROUP BY clause makes sure that rows are aggregated based on the standardized merchant fields and their computed hashes. This aggregation computes metrics like `ClientCount` and picks the latest transaction date (`MAX(t.TransactionDate)`) for `LastTransactionDate`.

### **Key Points:**
- **Consistent Hashing:**  
  The same logic used in Query 1 for computing the hash is used here, ensuring consistency in the merchant identifier.
- **Business Rules:**  
  The conditions on country codes, MCC codes, and client code ensure only valid transactions are considered for fingerprint generation.
- **Quality Field:**  
  The CASE expression (when t.clientcode = 'mc' then 5 else 1) assigns a quality score based on business criteria.
- **Log and Date Metadata:**  
  The new records are stamped with the analysis date range, the log ID, current UTC time, and a system creator label, which supports auditability.

---

# Summary

- **Query 1 (PosMonthlyFullMap Insertion):**  
  Loads monthly transaction header data into a QA mapping table by computing a unique merchant hash. It links to POSFingerprints when available, using a LEFT JOIN. This prepares the data for mapping diagnostics.

- **Query 2 (Unmapped Records Check):**  
  Performs a validation to find records in the mapping table that failed to obtain a POS fingerprint (i.e. where PosFingerprintId is null). It joins to additional lookup and flag tables to report detailed transaction attributes and identify “problem children.”

- **Query 3 (Insert New POS Fingerprints):**  
  Computes and inserts new fingerprint records into the `POSFingerprints` table based on transactions from the previous month. Merchant attributes are standardized and concatenated to create a SHA1 hash, which uniquely identifies each merchant. Business rules filter out already mapped records and determine quality.

Each query works together as part of a larger ETL process to ensure that new transactions are correctly mapped to merchant fingerprints in the data warehouse, enabling consistent reporting and analysis in the POS and finance domains.

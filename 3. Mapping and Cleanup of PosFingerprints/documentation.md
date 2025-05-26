
Below is the complete documentation for the “Update and Map Patron Dimensions” process. This documentation explains each query block and its purpose, the logic used, and how the components integrate to update and map patron (customer) data into the data warehouse dimensions and facts.

---

# Overview

This process is part of a data warehousing ETL routine that updates the Patron dimensions and maps transactions to their corresponding unique patrons. It performs the following tasks:

1. Executes a stored procedure to update and map the patron dimension.
2. Calculates date parameters for the previous month (both as dates and numeric keys).
3. Inserts new Amex (non-employee) proxy records into the DimUniquePatron table when they are not already present.
4. Updates the FactTransaction table with the mapped UniquePatronId based on the patron proxy.
5. Prepares a staging table for unique patrons based on employee IDs.
6. Inserts new unique patron records based on employee details into DimUniquePatron.
7. Updates FactTransaction rows that relate to these employee-based patrons.
8. Provides a final check for unmapped transactions and ensures that any remaining unmapped records are inserted and updated appropriately.

Each part is detailed below.

---

# 1. Update Patron Dimension

```sql
exec dbo.UpdateMapDimPatron;
```

**Purpose & Explanation:**
- This executes an existing stored procedure, `UpdateMapDimPatron`, which is assumed to:
  - Refresh or update the base patron dimension (likely updating attributes, cleansing data, or applying additional mapping rules).
  - It is the first step to ensure that the subsequent mappings work against a current version of the patron dimension.

---

# 2. Set Date Parameters for Unique Patron Processing

```sql
use [DinDWHS];  

declare @date date = dateadd(month, -1, getutcdate());
declare @startDate date = format(@date, 'yyyy-MM-01');
declare @endDate date = eomonth(@startDate);
declare @startDateInt int = cast(format(@date, 'yyyyMM01') as int);
declare @endDateInt int = cast(format(eomonth(@date), 'yyyyMMdd') as int);
select @startDate, @endDate, @startDateInt, @endDateInt;
```

**Purpose & Explanation:**
- **Context:**  
  These declarations are used to dynamically calculate the date range for the previous month.
- **Variables:**
  - `@date` is calculated as the current UTC date minus one month.
  - `@startDate` is set to the first day of that month (`yyyy-MM-01` format).
  - `@endDate` is the last day of the month (using `eomonth`).
  - `@startDateInt`/`@endDateInt` are numeric (integer) representations—formatted as `YYYYMM01` and `YYYYMMdd` respectively. These are often used for joining against DateKey fields in fact tables.
- **Usage:**  
  These parameters determine the period for which the update and mapping will be performed on FactTransaction and related dimensions.

---

# 3. Insert/Update Amex ProxyId’s into DimUniquePatron

```sql
insert into dbo.DimUniquePatron(proxyid, FirstName, Lastname, EmailAddress, ClientID, GeographyID, ZipCode, creditcardnum, myDinuserid)
select p.proxyid, p.FirstName, p.Lastname, p.EmailAddress, p.ClientID, p.GeographyID, p.ZipCode, p.CC_Last4Digits, p.myDinuserid
from dbo.dimpatron p
left join dbo.dimuniquepatron up on up.ProxyID = p.proxyid
where p.ProxyID not like '%[_]%'
  and up.UniquePatronId is null;
```

**Purpose & Explanation:**
- **Insertion:**  
  Inserts new non-employee (Amex) patron records into `DimUniquePatron` that do not already exist.
- **Logic:**
  - A LEFT JOIN is performed between the source `dimpatron` and the target `dimuniquepatron` on `ProxyID`.
  - The `WHERE` clause filters out records where:
    - The proxy ID contains an underscore (using `NOT LIKE '%[_]%'`), implying only one type of proxy is eligible.
    - There is no matching unique patron already present (i.e. `up.UniquePatronId IS NULL`).
- **Result:**  
  Only new Amex proxy records are inserted into the unique patron dimension.

---

# 4. Update FactTransaction with Mapped UniquePatronId for Amex Prospect

```sql
update ft 
set ft.UniquePatronId = up.UniquePatronId
from dbo.FactTransaction ft
inner join dbo.DimPatron p on p.ID = ft.Patron_ID
inner join dbo.DimUniquePatron up on up.ProxyID = p.ProxyID
where ft.DateKey between @startDateInt and @endDateInt
  and p.ProxyID not like '%[_]%'
  and isnull(ft.UniquePatronId, 0) = 0;
```

**Purpose & Explanation:**
- **Update:**  
  The query updates transaction fact records to map them to the correct UniquePatronId.
- **Logic:**
  - Joins are made between `FactTransaction`, `DimPatron`, and `DimUniquePatron` using the ProxyID from `DimPatron`.
  - The update is limited to transactions within the computed date range (`@startDateInt` to `@endDateInt`).
  - Only records without an assigned UniquePatronId (i.e. `isnull(ft.UniquePatronId, 0) = 0`) and those meeting the valid proxy condition are updated.
- **Result:**  
  This ensures that Amex patrons that were inserted earlier are now linked to the related fact transactions.

---

# 5. Prepare Staging Table with Unique Patrons Based on Employee ID

```sql
truncate table DinDWHSEtl.Staging.UniquePatronTransactionos;

insert into DinDWHSEtl.Staging.UniquePatronTransactionos
select distinct 
    dv.id AS dvhdid, 
    th.clientcode, 
    th.employeeid, 
    right(concat('0000', th.creditcardnum), 4) creditcardnum
from Dindb.Dinbilling.billing.detailvolumehistorydata dv
inner join Dindb.Dinintake.dbo.transactionheader th 
    on dv.transactionid = th.transactionid
where dv.txndate between @startDate and @endDate
  and th.employeeid is not null
  and th.proxyid is null;
```

**Purpose & Explanation:**
- **Staging:**  
  A temporary staging table (`UniquePatronTransactionos`) is prepared to capture unique patron identifiers based on employee information.
- **Logic:**
  - Truncates the staging table to remove prior data.
  - Inserts distinct records, selecting:
    - A unique ID from `detailvolumehistorydata` as `dvhdid`
    - The client code, employee id, and the last 4 digits of the credit card (formatted with a left-padded '0000').
  - The filter ensures only rows with an employee ID present and where the transaction has no existing proxy mapping (i.e. `th.proxyid is null`).
- **Result:**  
  This staging table will later be used to construct new unique patron records for employees.

---

# 6. Insert New Employee-based Unique Patrons into DimUniquePatron

```sql
insert into dbo.DimUniquePatron(ProxyID, FirstName, Lastname, EmailAddress, ClientID, GeographyID, ZipCode, CreditCardNum, MyDinUserId)
select distinct 
    concat(uu.clientcode, '_', uu.employeeid, '_', uu.creditcardnum), 
    'Unknown', 'Unknown', 'Unknown', 
    c.clientid, 0, 'Unknown', 
    uu.creditcardnum, 1
from DinDWHSEtl.Staging.UniquePatronTransactionos uu    
inner join dbo.DimClient c on c.clientcode = uu.clientcode
left join dbo.dimuniquepatron up on up.proxyid = concat(uu.clientcode, '_', uu.employeeid, '_', uu.creditcardnum)
where up.UniquePatronId is null
  and nullif(uu.employeeid, '') is not null;
```

**Purpose & Explanation:**
- **Insertion for Employee Records:**  
  New unique patron records are created for employees (those with an employee ID) by synthesizing a new `ProxyID` using a concatenation of `clientcode`, `employeeid`, and `creditcardnum`.
- **Logic:**
  - Constructs a composite key using `concat(uu.clientcode, '_', uu.employeeid, '_', uu.creditcardnum)`.
  - Inserts default values ('Unknown') for personal fields since they are not available.
  - Ensures that only records not already present in `DimUniquePatron` (via the LEFT JOIN and `up.UniquePatronId is null` condition) are inserted.
- **Result:**  
  This populates the unique patron dimension with employee-based records, expanding the coverage beyond just Amex proxies.

---

# 7. Update FactTransaction for Employee-Based Patrons

```sql
update ft 
set ft.UniquePatronId = up.UniquePatronId
from dbo.FactTransaction ft
inner join DinDWHSEtl.Staging.UniquePatronTransactionos uu on uu.dvhdid = ft.DVHD_ID 
inner join dbo.DimUniquePatron up on up.ProxyID = concat(uu.clientcode, '_', uu.employeeid, '_', uu.creditcardnum)
where ft.DateKey between @startDateInt and @endDateInt
  and isnull(ft.UniquePatronId, 0) = 0;
```

**Purpose & Explanation:**
- **Mapping Update:**  
  This update query maps FactTransaction records (that originated from transactions with employee IDs) to the unique patron dimension records just inserted.
- **Logic:**
  - Joins the staging table `UniquePatronTransactionos` (using `dvhdid`) with FactTransaction.
  - Joins with `DimUniquePatron` using the generated composite `ProxyID`.
  - Only updates records within the defined date range and those without an existing UniquePatronId.
- **Result:**  
  Ensures that employee transactions are now accurately linked to their newly created unique patron records.

---

# 8. Update the Remaining Transactions

```sql
update ft 
set ft.UniquePatronId = up.UniquePatronId
from dbo.FactTransaction ft
inner join dbo.DimPatron p on p.ID = ft.Patron_ID  
inner join dbo.DimUniquePatron up on up.ProxyID = p.ProxyID 				
where ft.DateKey between @startDateInt and @endDateInt
  and isnull(ft.UniquePatronId, 0) = 0;
```

**Purpose & Explanation:**
- **Catch-all Update:**  
  For any remaining transactions that still have a null UniquePatronId (and that have not been mapped by the employee-based update), this query performs a final mapping.
- **Logic:**
  - Uses a join between FactTransaction, DimPatron, and DimUniquePatron on the ProxyID.
  - Restricts the update to the target date range and only affects transactions with a UniquePatronId still equal to 0.
- **Result:**  
  This step ensures an exhaustive update so that every transaction in the period is associated with a UniquePatronId.

---

# 9. Validation – Count Unmapped Fact Transactions

```sql
select count(*)
from dbo.FactTransaction 
where UniquePatronId is null 
  and DateKey between @startDateInt and @endDateInt;
```

**Purpose & Explanation:**
- **Validation Query:**  
  Counts the number of FactTransaction records (in the specified date range) that still have a null UniquePatronId.
- **Interpretation:**  
  A zero count indicates that the mapping process was successful. A non-zero count would signal unmapped transactions that may require additional investigation or remediation.

---

# 10. Final Insertion into DimUniquePatron (Additional Insert)

```sql
insert into dbo.dimuniquepatron(proxyid, FirstName, Lastname, EmailAddress, ClientID, GeographyID, ZipCode, creditcardnum, myDinuserid)
select p.proxyid, p.FirstName, p.Lastname, p.EmailAddress, p.ClientID, p.GeographyID, p.ZipCode, p.CC_Last4Digits, p.myDinuserid
from dbo.dimpatron p
inner join dbo.FactTransaction ft on ft.Patron_ID = p.id  
left join dbo.dimuniquepatron up on up.ProxyID = p.proxyid  
where 1=1							  
  and up.UniquePatronId is null
  and ft.DateKey between @startDateInt and @endDateInt
  and isnull(ft.UniquePatronId, 0) = 0;
```

**Purpose & Explanation:**
- **Extra Insertion Pass:**  
  This step re-examines the dimpatron table and FactTransaction to ensure that any remaining patron records (for transactions in the date range) not yet in DimUniquePatron are inserted.
- **Logic:**
  - Uses an inner join with FactTransaction to limit the records to those that have been used in transactions during the period.
  - The LEFT JOIN condition filters out records that were already inserted.
  - The WHERE clause also ensures that the FactTransaction mapping remains null.
- **Result:**  
  This acts as a fallback to capture any unmapped Amex proxy records not handled in earlier steps.

---

# 11. Final Update of FactTransaction to Map Newly Inserted Unique Patrons

```sql
update ft 
set ft.UniquePatronId = up.UniquePatronId 
from dbo.FactTransaction ft
inner join dbo.DimPatron p on p.ID = ft.Patron_ID
inner join dbo.DimUniquePatron up on up.ProxyID = p.ProxyID 
where ft.DateKey between @startDateInt and @endDateInt
  and isnull(ft.UniquePatronId, 0) = 0;
```

**Purpose & Explanation:**
- **Final Mapping Update:**  
  After the additional insert operation, this update query ensures that any FactTransaction records (within the date range) that still have a null UniquePatronId are now updated with the new mapping.
- **Logic:**
  - The join conditions guarantee that records from FactTransaction are joined to DimPatron and then to the updated DimUniquePatron using the proxy identifier.
  - The update only affects those transactions which have not yet been linked (i.e., where UniquePatronId is still 0).
- **Result:**  
  This step finalizes the mapping process, ensuring complete coverage of transactions with a valid UniquePatronId.

---

# Summary

This documentation covers the update and mapping process for patron dimensions and fact transactions:

- It starts by refreshing the patron dimension using a stored procedure.
- Date parameters for the previous month are calculated and passed to subsequent queries.
- New proxy-based records are inserted into the unique patron dimension when they are not already present.
- FactTransaction records are updated in three phases:
  1. Mapping Amex proxy-based records.
  2. Mapping employee ID–based records by using a dedicated staging table.
  3. A final catch-all update to ensure all transactions are mapped.
- Additional validation is performed by counting transactions with a null UniquePatronId.
- A final fallback insertion and update pass is executed to capture any records missed earlier.

By following these steps, the process maintains accurate and complete mapping between source transaction records and the unique patron dimension, ensuring data integrity for downstream reporting and analytics.

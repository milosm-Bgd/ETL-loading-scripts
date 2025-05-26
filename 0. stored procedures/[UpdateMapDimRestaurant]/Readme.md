

# Analysis of the SQL Stored Procedure `UpdateMapDimRestaurant`

## 1. Overall Purpose
The overall purpose of the `UpdateMapDimRestaurant` stored procedure is to:
1. Populate and maintain the `DwhsEtl.Mapping.RestaurantMmnInfo` table with data from the `DB.Master.dbo.Din_Master_Merchant_Numbers` table.   
	(Mmn = Master merchant numbers) 
2. Merge the data from the `DwhsEtl.Mapping.RestaurantMmnInfo` table into the `DWHS.dbo.staging_DimRestaurant` table.
3. Populate the `DWHS.dbo.DimRestaurant` table with the data from the `DWHS.dbo.staging_DimRestaurant` table.
4. Populate the `DWHS.dbo.DimRestaurantGroup` table with any new restaurant groups found in the `DWHS.dbo.staging_DimRestaurant` table.
5. Update the `DWHS.dbo.FactTransaction` table with the correct `RestaurantKey` and `RestaurantGroupKey` values.

## 2. Main Steps/Operations
The stored procedure performs the following main steps:

1. **Populate `DwhsEtl.Mapping.RestaurantMmnInfo` table**:
   - Truncates the `DwhsEtl.Mapping.RestaurantMmnInfo` table.
   - Inserts data from the `DB.Master.dbo.Din_Master_Merchant_Numbers` table into the `DwhsEtl.Mapping.RestaurantMmnInfo` table, using a ranking logic to 
   - select the most relevant record for each site.

2. **Merge data into `DWHS.dbo.staging_DimRestaurant` table**:
   - Performs a MERGE operation to update or insert data into the `DWHS.dbo.staging_DimRestaurant` table, using the data from the 
   `DwhsEtl.Mapping.RestaurantMmnInfo` table and various other tables (e.g., `DB.Master.dbo.Din_Restaurant_Locations_Primary`, 
   `DB.Master.dbo.Din_Restaurant_Corporations`, `DB.Master.dbo.Din_Restaurant_Chains`, etc.).

3. **Populate `DWHS.dbo.DimRestaurant` table**:
   - Performs a MERGE operation to update or insert data into the `DWHS.dbo.DimRestaurant` table, using the data from the `DWHS.dbo.staging_DimRestaurant` table.

4. **Populate `DWHS.dbo.DimRestaurantGroup` table**:
   - Identifies any new restaurant groups in the `DWHS.dbo.staging_DimRestaurant` table and inserts them into the `DWHS.dbo.DimRestaurantGroup` table.

5. **Update `DWHS.dbo.FactTransaction` table**:
   - Updates the `RestaurantKey` and `RestaurantGroupKey` columns in the `DWHS.dbo.FactTransaction` table based on the data in the `DWHS.dbo.DimRestaurant` table.

## 3. Tables Interacted With
The stored procedure interacts with the following tables:

**Source Tables**:
- `DB.Master.dbo.Din_Master_Merchant_Numbers`
- `DB.Master.dbo.Din_Restaurant_Locations_Primary`
- `DB.Master.dbo.Din_Restaurant_Corporations`
- `DB.Master.dbo.Din_Restaurant_Chains`
- `DB.Master.dbo.Din_Contract_Restaurant_Master`
- `DB.Master.dbo.Din_Restaurant_Cuisines`
- `DB.Master.dbo.Din_Restaurant_Types`
- `DB.Master.dbo.Din_Restaurant_Status`
- `DB.Master.dbo.Din_Billing_Group`
- `DB.Master.dbo.RestaurantDiningStyleType`
- `dbo.DimSalesForceRestaurant`

**Destination Tables**:
- `DwhsEtl.Mapping.RestaurantMmnInfo`	-- stands for MasterMerchantNumber
- `DWHS.dbo.staging_DimRestaurant`
- `DWHS.dbo.DimRestaurant`
- `DWHS.dbo.DimRestaurantGroup`
- `DWHS.dbo.FactTransaction`

## 4. Key Transformations and Business Logic
The stored procedure performs the following key transformations and applies business logic:

1. **Populating `DwhsEtl.Mapping.RestaurantMmnInfo`**:
   - Ranks the records from `DB.Master.dbo.Din_Master_Merchant_Numbers` table based on the `drp_dsp` column to select the most relevant record for each site.

2. **Merging into `DWHS.dbo.staging_DimRestaurant`**:
   - Performs a MERGE operation to update or insert data, handling various cases for the `rest_location_name` and `restaurant_name` columns.
   - Applies various COALESCE and ISNULL functions to handle null values and default to 'N/A' where appropriate.
   - Calculates the `ProgramStartDateKey` and `ProgramEndDateKey` columns based on the `start_Date_1`, `start_Date_2`, and `OOB_Date` columns from the 
     `DB.Master.dbo.Din_Restaurant_Locations_Primary` table.

3. **Populating `DWHS.dbo.DimRestaurant`**:
   - Performs a MERGE operation to update or insert data, handling various cases for the `RestaurantLocationName`, `LegalEntityName`, `ChainName`, and other columns.
   - Applies various COALESCE and ISNULL functions to handle null values and default to 'N/A' where appropriate.
   - Calculates the `RestaurantGroupKey` by matching the `ChainName` and `LegalEntityName` columns to the `RestaurantGroupName` column in the `DWHS.dbo.DimRestaurantGroup` table.

4. **Populating `DWHS.dbo.DimRestaurantGroup`**:
   - Identifies any new restaurant groups in the `DWHS.dbo.staging_DimRestaurant` table and inserts them into the `DWHS.dbo.DimRestaurantGroup` table.

5. **Updating `DWHS.dbo.FactTransaction`**:
   - Updates the `RestaurantKey` and `RestaurantGroupKey` columns in the `DWHS.dbo.FactTransaction` table based on the data in the `DWHS.dbo.DimRestaurant` table.

## 5. Data Flow
The data flows through the stored procedure as follows:

1. Data is extracted from the source tables (`DB.Master.dbo.Din_Master_Merchant_Numbers`, `DB.Master.dbo.Din_Restaurant_Locations_Primary`, etc.) and loaded into the `DwhsEtl.Mapping.RestaurantMmnInfo` table.
2. The data from the `DwhsEtl.Mapping.RestaurantMmnInfo` table is then merged into the `DWHS.dbo.staging_DimRestaurant` table.
3. The data from the `DWHS.dbo.staging_DimRestaurant` table is then merged into the `DWHS.dbo.DimRestaurant` table.
4. Any new restaurant groups found in the `DWHS.dbo.staging_DimRestaurant` table are inserted into the `DWHS.dbo.DimRestaurantGroup` table. (proveriti skriptu koju sam pravio u okt/nov 2024, folder Incremental)
5. The `RestaurantKey` and `RestaurantGroupKey` columns in the `DWHS.dbo.FactTransaction` table are updated based on the data in the `DWHS.dbo.DimRestaurant` table.

## 6. Performance Considerations and Issues
1. **Truncating `DwhsEtl.Mapping.RestaurantMmnInfo` table**: Truncating the table before inserting new data may be more efficient than deleting the existing data, as it avoids the need to log the deletions.

2. **Ranking and Selecting Records in `DwhsEtl.Mapping.RestaurantMmnInfo`**: The ranking logic used to select the most relevant record for each site may have performance implications, especially if the `DB.Master.dbo.Din_Master_Merchant_Numbers` table is large. Consider optimizing the query or indexing the relevant columns.

3. **Nested Queries and Joins**: The stored procedure uses several nested queries and complex joins, which may impact performance, especially for large datasets. Consider breaking down the queries into smaller, more manageable parts or optimizing the join conditions.

4. **Handling Null Values**: The extensive use of COALESCE and ISNULL functions to handle null values may have a slight performance impact. Evaluate whether these functions can be optimized or if the data can be cleansed before processing.

5. **Updating `DWHS.dbo.FactTransaction`**: The update of the `RestaurantKey` and `RestaurantGroupKey` columns in the `DWHS.dbo.FactTransaction` table may be time-consuming, especially if the table is large. Consider optimizing the update logic or exploring alternative approaches, such as using a staging table or partitioning the data.

6. **Potential Locking and Concurrency Issues**: The stored procedure performs several MERGE operations, which may lead to locking and concurrency issues if multiple instances of the procedure are executed simultaneously. Ensure that the appropriate isolation levels and locking strategies are in place to mitigate these issues.

Overall, the stored procedure performs a significant amount of data processing and transformation, which may have performance implications, especially for large datasets. Continuous monitoring, optimization, and testing are recommended to ensure the procedure's efficiency and scalability.


##Summary of the Stored Procedure

**Overall Purpose**

The procedure is designed to reshape and consolidate restaurant-related data. It populates a mapping table (DwhsEtl.Mapping.RestaurantMmnInfo) 
from source tables and then uses that data to update staging and final dimension tables (DWHS.dbo.staging_DimRestaurant, DWHS.dbo.DimRestaurant, 
DWHS.dbo.DimRestaurantGroup) as well as the fact table (DWHS.dbo.FactTransaction).

**Key Operations**

Truncating the mapping table to clear out old data.
Using a ranked query (to filter and choose the most appropriate record per site) to insert fresh data into the mapping table.
Merging data into a staging table that feeds into the main dimension tables.
Propagating new restaurant groups and updating the fact table with relevant keys from the dimensions.
Incorporating various data-handling techniques like COALESCE and several JOIN conditions, which may have performance implications.

**Performance Considerations**

The truncation approach and ranking logic help manage the data efficiently but might need indexing for better performance.
Complex nested queries and multiple MERGE operations require careful handling to avoid locking or concurrency issues, especially in large datasets.

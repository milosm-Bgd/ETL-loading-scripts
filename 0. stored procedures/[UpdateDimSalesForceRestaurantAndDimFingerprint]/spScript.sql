USE [DinDWHS]
GO
/****** Object:  StoredProcedure [dbo].[spUpdateDimSalesForceRestaurantAndDimFingerprint]    Script Date: 3/16/2025 6:37:19 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

--This stored procedure, [dbo].[spUpdateDimSalesForceRestaurantAndDimFingerprint], performs 
--ETL (Extract, Transform, Load) operations to update or synchronize data in the Dimensional 
--(Dim) tables DimSalesForceRestaurant, DimSalesForceBrand, and other related tables such as 
--FactTransaction and DimFingerprint. The procedure works within a transaction to ensure data 
--consistency.

--Here’s what each section is doing:

ALTER PROCEDURE [dbo].[spUpdateDimSalesForceRestaurantAndDimFingerprint]
-- WITH ENCRYPTION, RECOMPILE, EXECUTE AS CALLER|SELF|OWNER| 'user_name'
AS BEGIN
SET XACT_ABORT ON
BEGIN TRY
BEGIN TRANSACTION -- Purpose: Ensures that if an error occurs during the execution of the procedure, the transaction is automatically rolled back, preserving data consistency.

--Updating DimSalesForceRestaurant
	ALTER TABLE DinDWHS.[dbo].DimFingerprint NOCHECK CONSTRAINT [FK_DimFingerprint_DimSFRestaurant];
	ALTER TABLE DinDWHS.[dbo].FactTransaction NOCHECK CONSTRAINT [FK_FactTransaction_DimSFRestaurant];
	ALTER TABLE DinDWHS.[dbo].FactTransaction NOCHECK CONSTRAINT [FK_FactTransaction_DimFingerprint];
	ALTER TABLE DinDWHS.[dbo].[DimSalesForceRestaurant] NOCHECK CONSTRAINT [FK_DimSFRestaurant_DimSFRestaurant];
	ALTER TABLE [dbo].[DimRestaurant] nocheck constraint [FK_DimRestaurant_DimSalesForceRestaurant];-- Purpose: 
	--Temporarily disables foreign key constraints on related tables to allow updates and inserts without 
	--constraint violations. This step is necessary if the updates affect data integrity constraints but must 
	--be carefully managed to avoid leaving the database in an inconsistent state.

--Updating DimSalesForceBrand
	MERGE [DinDWHS].[dbo].[DimSalesForceBrand] AS target
	USING (
			SELECT
				bp.Id as BrandProfileId,
				bp.BrandName,
				bp.RestaurantSegment,
				CASE 
					WHEN bp.DiningGroupSegment IS NULL AND RestaurantSegment IN ('QSR', 'FAST CASUAL') THEN 'LSR'
					WHEN bp.DiningGroupSegment IS NULL AND RestaurantSegment IN ('MIDSCALE', 'UPSCALE', 'FINE DINING', 'CASUAL') THEN 'FSR'
					ELSE bp.DiningGroupSegment
				END as DiningGroupSegment,
				bp.PrimaryCuisine,
				TRY_CAST (bp.CateringAmountThreshold AS DECIMAL(18,4)) as CateringAmountThreshold,
				TRY_CAST (bp.LargeEventThreshold AS DECIMAL(18,4)) as LargeEventThreshold
			FROM Dindb.[DinIntegrations].[SalesForce].[BrandProfile] bp
			INNER JOIN (select distinct BrandProfileId COLLATE SQL_Latin1_General_CP1_CS_AS BrandProfileId from Dindb.[DinIntegrations].[SalesForce].[SfSync]) sfs
			ON bp.Id COLLATE SQL_Latin1_General_CP1_CS_AS = sfs.BrandProfileId COLLATE SQL_Latin1_General_CP1_CS_AS
			where nullif(bp.BrandName,'') is not null
		) AS source
	ON target.BrandProfileId COLLATE SQL_Latin1_General_CP1_CS_AS = source.BrandProfileId COLLATE SQL_Latin1_General_CP1_CS_AS
	WHEN MATCHED 
	AND 
			ISNULL(target.BrandName, '') <> ISNULL(source.BrandName, '') OR
			ISNULL(target.RestaurantSegment, '') <> ISNULL(source.RestaurantSegment, '') OR
			ISNULL(target.DiningGroupSegment, '') <> ISNULL(source.DiningGroupSegment, '') OR
			ISNULL(target.PrimaryCuisine, '') <> ISNULL(source.PrimaryCuisine, '') OR
			ISNULL(target.CateringAmountThreshold, -1) <> ISNULL(source.CateringAmountThreshold, -1) OR
			ISNULL(target.LargeEventThreshold, -1) <> ISNULL(source.LargeEventThreshold, -1) 
	THEN 
		UPDATE SET
			target.BrandName = source.BrandName,
			target.RestaurantSegment = source.RestaurantSegment,
			target.DiningGroupSegment = source.DiningGroupSegment,
			target.PrimaryCuisine = source.PrimaryCuisine,
			target.CateringAmountThreshold = source.CateringAmountThreshold,
			target.LargeEventThreshold = source.LargeEventThreshold 
	WHEN NOT MATCHED THEN
		INSERT (BrandProfileId, BrandName, RestaurantSegment, DiningGroupSegment, PrimaryCuisine, CateringAmountThreshold, LargeEventThreshold )
		VALUES (source.BrandProfileId, 
				source.BrandName, 
				source.RestaurantSegment, 
				source.DiningGroupSegment, 
				source.PrimaryCuisine, 
				source.CateringAmountThreshold, 
				source.LargeEventThreshold);
 --  Purpose:
--Updates or inserts data into the DimSalesForceBrand table to reflect the latest information from the BrandProfile 
--table in the Integrations.SalesForce schema.

--Matching Criteria:

--Matches records in DimSalesForceBrand and BrandProfile based on BrandProfileId.
--Only updates records if there are changes in fields like BrandName, RestaurantSegment, etc.
--Insert Logic: Inserts new records into the DimSalesForceBrand table if they don’t exist in the target table.

--This continuation of the stored procedure [dbo].[spUpdateDimSalesForceRestaurantAndDimFingerprint] includes further 
--steps to update or insert data into multiple dimension tables (DimSalesForceRestaurant, DimFingerprint) and facts 
--(FactTransaction), ensuring data consistency and alignment with source systems like Salesforce.

--Updating DimSalesForceRestaurant
	MERGE DinDWHS.[dbo].[DimSalesForceRestaurant] AS target
	USING (
			SELECT  DISTINCT b.AccountID COLLATE SQL_Latin1_General_CP1_CS_AS AS [SFKey], 
					b.[GUID], 
					b.AccountName,
					isnull(dzc.geographyID, 0) AS GeographyID,
					b.BillingAddressLine1 AS [BillingAddress],
					b.BillingStateProvince AS [BillingState],
					b.BillingCity AS [BillingCity],
					ISNULL(b.LocationType,'') AS [LocationType],
					ISNULL(db.BrandName, b.Brand) Brand,
					ISNULL(b.Type,'') Type, 
					CASE WHEN b.EAPV_date='1899-12-30 00:00:00.000' THEN 'Unverified' ELSE 'Verified' END AS [EAPV_status],
					b.AccountOwner,
					b.PrimaryCuisine AS [PrimaryCuisine], b.RestaurantProfile AS [RestaurantProfile], b.FoodserviceType AS [FoodserviceType],
					IIF(ISNULL(b.ZupplerLocation,0)=1,'Yes','No') AS [ZupplerLocation],
					b.Grade,
					ISNULL(b.RestaurantTier,'N/A') RestaurantTier,
					ISNULL(b.Segment,'N/A') Segment,
					ISNULL(b.CuisineType,'N/A') CuisineType,
					b.[Rank],
					ISNULL(db.Id,0) as SalesForceBrandId
			FROM DinDB.DinIntegrations.SalesForce.SfSync AS b
			--LEFT JOIN DinDb.DinIntegrations.SalesForce.BrandProfile AS bp ON bp.Id collate latin1_general_cs_as = b.BrandProfileId collate latin1_general_cs_as
				LEFT JOIN [DinDWHS].[dbo].[DimSalesForceBrand] db ON db.BrandProfileId collate latin1_general_cs_as = b.BrandProfileId collate latin1_general_cs_as
					LEFT JOIN [DinDWHS].dbo.DimZIPCodes DZC ON DZC.ZipCode= CASE WHEN LEN(b.BillingZipPostalCode)=10 and b.BillingZipPostalCode like '%-%'
							THEN SUBSTRING(b.BillingZipPostalCode,1,CHARINDEX('-', b.BillingZipPostalCode)-1)
							WHEN LEN(b.BillingZipPostalCode)=4
							THEN RIGHT(CONCAT('00000',ISNULL(b.BillingZipPostalCode,'')),5)
							ELSE b.BillingZipPostalCode
							END
	  ) AS source 
	on (Target.SFKey COLLATE SQL_Latin1_General_CP1_CS_AS = source.SFKey COLLATE SQL_Latin1_General_CP1_CS_AS)
	WHEN MATCHED 
		THEN UPDATE
	   SET target.[GUID] = source.[guid],
		target.AccountName = source.[AccountName],
		target.GeographyID = source.GeographyID,
		target.BillingAddress = source.[BillingAddress],
		target.BillingState = source.[BillingState],
		target.BillingCity = source.[BillingCity],
		target.LocationType = source.[LocationType],
		target.Brand = source.brand,
		target.[Type] = source.[Type],
		target.EAPV_status = source.[EAPV_status],
		target.AccountOwner = source.AccountOwner,
		target.PrimaryCuisine = source.[PrimaryCuisine],
		target.RestaurantProfile = source.[RestaurantProfile],
		target.FoodserviceType = source.[FoodserviceType],
		target.ZupplerNetwork = source.[ZupplerLocation],
		target.Grade = source.Grade,
		target.RestaurantTier = source.RestaurantTier,
		target.Segment = source.Segment,
		target.CuisineType = source.CuisineType,
		target.[Rank] = source.[Rank],
		target.[SalesForceBrandId] = source.[SalesForceBrandId]
	WHEN NOT MATCHED BY TARGET 
		THEN INSERT(
		[SFKey]
		  ,[GUID]
		  ,[AccountName]
		  ,[GeographyID]
		  ,[BillingAddress]
		  ,[BillingState]
		  ,[BillingCity]
		  ,[LocationType]
		  ,[Brand]
		  ,[Type]
		  ,[EAPV_status]
		  ,[AccountOwner]
		  ,[PrimaryCuisine]
		  ,[RestaurantProfile]
		  ,[FoodserviceType]
		  ,ZupplerNetwork
		  ,Grade
		  ,RestaurantTier
		  ,Segment
		  ,CuisineType
		  ,[Rank]
		  ,[SalesForceBrandId]
	   ) 
	 VALUES
	   (
	   source.[SFKey]
	   ,source.[GUID]
	   ,source.[AccountName]
	   ,source.GeographyID
	   ,source.[BillingAddress]
	   ,source.[BillingState]
	   ,source.[BillingCity]
	   ,source.[LocationType]
	   ,source.brand
	   ,source.[Type]
	   ,source.[EAPV_status]
	   ,source.AccountOwner
	   ,source.PrimaryCuisine
	   ,source.RestaurantProfile
	   ,source.FoodserviceType
	   ,source.ZupplerLocation
	   ,source.Grade
	   ,source.RestaurantTier
	   ,source.Segment
	   ,source.CuisineType
	   ,source.[Rank]
	   ,source.[SalesForceBrandId]
	   )
	WHEN NOT MATCHED BY SOURCE AND target.SFkey COLLATE SQL_Latin1_General_CP1_CS_AS <>'XXXXXXXXXXXXXXX' 
	THEN DELETE;


	UPDATE DFR -- 2nd part 
 -- Purpose: Updates the parent-child relationships for Salesforce restaurants. Links 
 -- restaurants (DFR) with their parent accounts (DFR_parent) based on the ParentAccountID 
 -- in the SfSync table.
	SET DFR.SFParentRestaurantKey=DFR_parent.SFRestaurantKey
	FROM [DinDWHS].[dbo].[DimSalesForceRestaurant] AS DFR
		JOIN DinDB.DinIntegrations.SalesForce.SfSync s ON DFR.SFKey COLLATE SQL_Latin1_General_CP1_CS_AS = s.AccountID COLLATE SQL_Latin1_General_CP1_CS_AS
		LEFT JOIN [DinDWHS].[dbo].[DimSalesForceRestaurant] DFR_parent ON DFR_parent.SFKey COLLATE SQL_Latin1_General_CP1_CS_AS = s.ParentAccountID COLLATE SQL_Latin1_General_CP1_CS_AS;
--Updating DimFingerprint
	MERGE DinDWHS.[dbo].[DimFingerprint] AS target
	USING (
	  SELECT DISTINCT
	   pos.FingerprintID AS FingerprintID,
	   isnull(sfr.SFRestaurantKey,1) AS SFRestaurantKey,
	   pos.MerchantNumber AS MerchantNumber, 
	   pos.MerchantLegalName AS MerchantLegalName, 
	   pos.MerchantName AS MerchantName, 
	   pos.AddressLine01 AS AddressLine01, 
	   pos.CityName AS CityName, 
	   pos.StateProvince AS StateProvince, 
	   pos.PostalCode AS PostalCode, 
	   pos.CountryCode AS CountryCode, 
	   pos.SalesForceID AS SalesForceID,
	   CONCAT(CAST(pos.AnalysisStartDate AS VARCHAR(10)), '--><-- ',CAST(pos.AnalysisEndDate AS VARCHAR(10))) AS [AnalysisPeriod],
	   ISNULL(ps.DisplayName,'Unknown') AS Segmentation,
	   v.DisplayName AS Vendor,
	   pp.DisplayName AS PaymentProcessor
	  -- CASE ISNULL(pos.Segmentation,'')
			--WHEN 'a' THEN 'Airport & Rail/Bus Restaurants'
			--WHEN 'c' THEN 'Chain'
			--WHEN 'e' THEN 'Event Planning/Caterers'
			--WHEN 'g' THEN 'Gentlemen''s Clubs'
			--WHEN 'h' THEN 'Hotels/Resorts/Casinos'
			--WHEN '.i' THEN 'Independent'
			--WHEN 'l' THEN 'Liquor Stores'
			--WHEN 'm' THEN 'Military'
			--WHEN 'o' THEN 'Onsite'
			--WHEN 'r' THEN 'Retail outlets'
			--WHEN 's' THEN 'Sports & entertainment'
			--WHEN 't' THEN 'Travel Centers'
			--WHEN 'v' THEN 'Pay Platforms, Delivery/Online Ordering Sites'
			--WHEN 'vending machine' THEN 'Vending Machine'
			--WHEN 'u' THEN 'Unknown'
			----WHEN 'd' THEN 'Delivery'
			--ELSE 'No segmentations'
			--END AS Segmentation

	  from DinDB.[DinShared].[Finance].[PosFingerprints] AS pos
	  left join Dindb.Dinshared.[Finance].[PosVendors] AS v ON v.Id = ISNULL(pos.Vendor,0)
	  left join Dindb.Dinshared.[Finance].[PosPaymentProcessors] AS pp ON pp.Id = ISNULL(pos.PaymentProcessor,0)
	  left join Dindb.Dinshared.[Finance].[PosSegmentation] as ps ON ps.id = pos.segmentation
		LEFT JOIN DinDWHS.[dbo].[DimSalesForceRestaurant] AS sfr ON pos.SalesforceID COLLATE SQL_Latin1_General_CP1_CS_AS = sfr.SFKey COLLATE SQL_Latin1_General_CP1_CS_AS
	  ) AS source 
	ON (target.FingerprintID = source.FingerprintID)
	WHEN MATCHED 
		THEN UPDATE 
	   SET target.SalesForceID = source.SalesForceID,
		target.SFRestaurantKey = source.SFRestaurantKey,
		target.Segmentation = source.Segmentation,
		target.Vendor = source.Vendor,
		target.PaymentProcessor = source.PaymentProcessor
	WHEN NOT MATCHED BY TARGET 
		THEN INSERT(    
		FingerprintID, 
		SFRestaurantKey, 
		MerchantNumber, 
		MerchantLegalName, 
		MerchantName, 
		AddressLine01,
		CityName, 
		StateProvince, 
		PostalCode, 
		CountryCode, 
		SalesForceID, 
		AnalysisPeriod,
		Segmentation,
		Vendor,
	    PaymentProcessor
		  ) 
	 VALUES
	   ( 
		source.FingerprintID, 
		source.SFRestaurantKey, 
		source.MerchantNumber, 
		source.MerchantLegalName, 
		source.MerchantName, 
		source.AddressLine01, 
		source.CityName, 
		source.StateProvince, 
		source.PostalCode, 
		source.CountryCode, 
		source.SalesForceID, 
		source.AnalysisPeriod,
		source.Segmentation,
		source.Vendor,
	    source.PaymentProcessor
	  ) 
	WHEN NOT MATCHED BY SOURCE AND target.FingerprintID<>0 
	THEN DELETE;
	-- Purpose: Synchronizes DimFingerprint with data from PosFingerprints, linking 
	-- fingerprints to Salesforce restaurants (SFRestaurantKey).

--Updating FactTransaction
		UPDATE FT
		SET FT.SFRestaurantKey = DF.SFRestaurantKey
		FROM DinDWHS.dbo.FactTransaction AS FT
			LEFT JOIN DinDWHS.dbo.DimFingerprint AS DF ON DF.FingerprintID = FT.FingerprintID
		WHERE FT.SFRestaurantKey <> DF.SFRestaurantKey AND DF.FingerprintID IS NOT NULL AND FT.FingerprintID <> 0;
		UPDATE FT
		SET FT.SFRestaurantKey = ISNULL(DSFR.SFRestaurantKey,1)
		FROM DinDWHS.dbo.FactTransaction AS FT
			LEFT JOIN DinDWHS.dbo.DimSalesForceRestaurant AS DSFR ON FT.SFRestaurantKey = DSFR.SFRestaurantKey
		WHERE DSFR.SFRestaurantKey IS NULL;

--- The final part of the stored procedure focuses on synchronizing data in the FactTransaction table and ensuring 
-- its alignment with the updated DimFingerprint and DimSalesForceRestaurant dimension tables. It also includes 
-- error handling and transaction management to maintain data consistency and ensure any failures are handled gracefully.

--Purpose: Synchronizes the SFRestaurantKey in the FactTransaction table using data from the 
--DimFingerprint table.

--Process:
--The FactTransaction table is updated where:
--SFRestaurantKey in FactTransaction differs from the SFRestaurantKey in DimFingerprint 
--(ensures only mismatched rows are updated).

--FingerprintID exists in both the FactTransaction and DimFingerprint tables (i.e., 
--valid and non-null).

--FactTransaction.FingerprintID is not 0 (avoids processing invalid or default rows).

--The LEFT JOIN ensures that even rows with no match in DimFingerprint will not block 
--processing of valid matches.

--Outcome: Ensures that transaction records in FactTransaction point to the correct 
--Salesforce restaurant (SFRestaurantKey) based on the fingerprinting information.


		EXEC [dbo].[UpdateSalesforceRestaurantRankings];

COMMIT TRANSACTION
	END TRY
	BEGIN CATCH
	SELECT  
		ERROR_NUMBER() AS ErrorNumber  
		,ERROR_SEVERITY() AS ErrorSeverity  
		,ERROR_STATE() AS ErrorState  
		,ERROR_PROCEDURE() AS ErrorProcedure  
		,ERROR_LINE() AS ErrorLine  
		,ERROR_MESSAGE() AS ErrorMessage;
	IF @@TRANCOUNT>0
	ROLLBACK TRANSACTION
	END CATCH
END
--Purpose: Ensures that any errors during the procedure result in a rollback of the entire transaction, preserving database integrity.

--Process:

--TRY Block: 

--If the code runs successfully, the transaction is committed at the end.

--CATCH Block:

--If an error occurs, relevant details are captured and displayed (ERROR_NUMBER, ERROR_SEVERITY, etc.).
--If the transaction is still open (@@TRANCOUNT > 0), it is rolled back to undo any partial changes.
--Outcome: Guarantees data consistency by ensuring that either:
	--All updates and inserts complete successfully and are committed.
	--No changes are applied if an error occurs, avoiding partial updates.

--Key Highlights

--Data Synchronization Across Dimensional Tables: Updates the SFRestaurantKey in FactTransaction using data from both DimFingerprint and DimSalesForceRestaurant. 
-- This ensures that all transaction records are tied to valid dimensional data.
--Error Handling and Rollbacks: The TRY...CATCH block protects the database from corruption or inconsistency by rolling back the transaction if any part of the procedure fails.
--Integration with Rankings Procedure: Delegates the task of updating Salesforce restaurant rankings to a separate procedure, streamlining the ETL process and ensuring modularity.

--Overall Flow of the Procedure

--Disable constraints temporarily to allow seamless updates across related tables.
--Update or insert into DimSalesForceBrand and DimSalesForceRestaurant tables to reflect the latest Salesforce data.
--Establish parent-child relationships in DimSalesForceRestaurant.
--Synchronize DimFingerprint with point-of-sale fingerprint data.
--Update the FactTransaction table to ensure consistency with the dimensional tables.
--Call the rankings update procedure to recalculate any metrics dependent on the updated data.
--Ensure atomicity and consistency by committing the transaction or rolling it back in the event of an error.

--Final Outcome

--This procedure ensures that the dimensional and fact tables are fully synchronized with updated Salesforce and fingerprinting data. By handling edge cases 
-- (e.g., missing SFRestaurantKey, invalid fingerprints) and leveraging comprehensive error handling, it maintains the integrity and accuracy of the data warehouse.

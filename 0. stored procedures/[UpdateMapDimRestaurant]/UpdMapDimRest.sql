USE [DinDWHS]
GO
/****** Object:  StoredProcedure [dbo].[UpdateMapDimRestaurant]    Script Date: 3/11/2025 4:08:49 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[UpdateMapDimRestaurant]
AS

BEGIN	

	--- fillStaging_Restaurant_V2 ----

	TRUNCATE TABLE DinDwhsEtl.Mapping.RestaurantMmnInfo;


	WITH mmnBasedInfo as 
	(
		SELECT a.site_ID, a.rest_location_name, a.cleaned_restaurant_name, a.clean_billing_id, a.clean_restuarant_group,a.restaurant_name, a.salesp
		FROM (SELECT DISTINCT RANK() OVER(partition by site_ID, rest_location_name, cleaned_restaurant_name, clean_billing_id, clean_restuarant_group, restaurant_name ORDER BY mmn.drp_dsp) as rank1,
			site_ID, rest_location_name, cleaned_restaurant_name, clean_billing_id, clean_restuarant_group, restaurant_name, mmn.drp_dsp as salesp
			FROM DinDB.DinMaster.dbo.Din_Master_Merchant_Numbers AS mmn
			WHERE site_id in (SELECT ID FROM DinDB.DinMaster.dbo.Din_Restaurant_Locations_Primary AS rlp)
			) AS a
		WHERE a.rank1 = 1
	)
	INSERT INTO DinDwhsEtl.Mapping.RestaurantMmnInfo
	SELECT RANK() OVER( partition by site_ID order by case 
					WHEN (tt.rest_location_name=rlp.Restaurant_Name or tt.rest_location_name=rlp.Din_name)
						and (tt.restaurant_name=rlp.Restaurant_Name or tt.restaurant_name=rlp.Din_name)
													THEN 1
					WHEN (tt.rest_location_name=rlp.Restaurant_Name or tt.rest_location_name=rlp.Din_name)
						and tt.restaurant_name!=rlp.Restaurant_Name and tt.restaurant_name!=rlp.Din_name
													THEN 2
					WHEN tt.rest_location_name!=rlp.Restaurant_Name and tt.rest_location_name=rlp.Din_name
						 and (tt.restaurant_name=rlp.Restaurant_Name or tt.restaurant_name=rlp.Din_name)
													THEN 3
					ELSE 4 END,
	tt.rest_location_name, tt.clean_billing_id, tt.restaurant_name, tt.clean_restuarant_group, tt.cleaned_restaurant_name) as ranking, tt.*, rlp.restaurant_name as RLP_restaurantName, rlp.Din_name as RLP_DinName

	FROM mmnBasedInfo AS tt 
	INNER JOIN DinDB.DinMaster.dbo.Din_Restaurant_Locations_Primary AS rlp 
	ON tt.site_id = rlp.id 
	WHERE 1=1 
	ORDER BY 2, 1
	
	--------------------------------------

	--;WITH duplicatedRestaurantInfo as
	--(
	--	SELECT DISTINCT site_ID
	--	FROM DinDwhsEtl.Mapping.RestaurantMmnInfo
	--	WHERE ranking > 1
	--)
	--INSERT INTO DinDwhsEtl.dbo.RestMmnInfoDirtyData
	--([site_id], [ID], [acquier_mid], [rest_location_name], [acquirer_type], [rebate], [match_note], [location_number], [payment_method], [start_date], [cleaned_restaurant_name], [clean_city], [clean_billing_id], [clean_restuarant_group], [drp_dsp], [restaurant_name], [address_line_1], [address_line_2], [city], [state], [zipcode])
	--SELECT site_id, ID, acquier_mid, rest_location_name, acquirer_type, rebate, match_note, location_number, payment_method, [start_date], cleaned_restaurant_name, clean_city, clean_billing_id, clean_restuarant_group, drp_dsp, restaurant_name, address_line_1, address_line_2, city, [state], zipcode
	--FROM DinDB.DinMaster.dbo.Din_Master_Merchant_Numbers
	--WHERE site_id IN (SELECT site_id FROM duplicatedRestaurantInfo)

	------------------------------------

	DELETE FROM DinDwhsEtl.Mapping.RestaurantMmnInfo
	WHERE ranking!=1;

	------------------------

	MERGE DinDWHS.dbo.staging_DimRestaurant AS target
	USING (
			SELECT DISTINCT	RLP.Din_Name as [RestaurantLocationName], 
			ISNULL(RC.legal_Name, 'N/A') as [LegalEntityName],
			ISNULL(rchain.chain, 'N/A') as chainName, 
			ISNULL(mmn.clean_restaurant_group, 'N/A') as clean_restaurant_group, 
			ISNULL(rlp.Restaurant_Name, 'N/A') as restaurant_name, 
			ISNULL (CASE WHEN LEN(rlp.Contract_ID)>5 THEN rlp.Contract_ID 
						 ELSE CASE WHEN LEN(rc.contractID)<6 THEN NULL ELSE rc.contractID END
					END, 'N/A') as [ContractID],
			COALESCE(crm.salesperson, mmn.salesp, 'N/A') as [SalesPerson], 
			ISNULL(rcui.cuisine, 'N/A') as primaryCuisine,
			ISNULL(CASE rt.[type]
					  WHEN 'National Chain' THEN 'Chain'
					  WHEN 'Regional Chain' THEN 'Chain'
					  ELSE rt.[type] END,'N/A') as entityType,
			ISNULL(rds.DiningStyleDescription, 'N/A') as [SegmentType],
			ISNULL(rs.[status], 'N/A') as [RestaurantStatus], 
			0 as [AverageDiningPrice],
			ISNULL(mmn.clean_billing_id, 'N/A') as billingGroup,
			COALESCE(
			CAST(replace(convert(varchar(10),case when isDate(rlp.start_Date_2)=1 then convert(datetime,rlp.start_Date_2, 20) else null end,21),'-','') as int),
			CAST(replace(convert(varchar(10),case when isDate(rlp.start_Date_1)=1 then convert(datetime,rlp.start_Date_1, 20) else null end,21),'-','') as int),
			null) AS [ProgramStartDateKey],
			CAST(replace(convert(varchar(10),case when isDate(rlp.OOB_Date)=1 then convert(datetime,rlp.OOB_Date, 20) else null end,21),'-','') as int) as  [ProgramEndDateKey],
			RLP.ID as RLP_ID,
			RLP.Zip as zipCode, 
			RLP.CIty as city, 
			RLP.Location_Number as LocationNumber,
			RLP.Address_1 as [address],
			ISNULL(RLP.Restaurant_Name,'N/A') as RestName,
			ISNULL(RLP.SoldBy, 'N/A') as SoldBy,
			IIF(YEAR(RLP.[Cater_Start_Date]) < 2005, NULL, CAST(replace(convert(varchar(10),case when isDate(RLP.[Cater_Start_Date])=1 then convert(datetime,RLP.[Cater_Start_Date], 20) else null end,21),'-','') as int)) as CaterStartDate,
			IIF(YEAR(RLP.[Cater_End_Date]) < 2005, NULL, CAST(replace(convert(varchar(10),case when isDate(RLP.[Cater_End_Date])=1 then convert(datetime,RLP.[Cater_End_Date], 20) else null end,21),'-','') as int)) as CaterEndDate,
			ISNULL(sfr.Brand, 'N/A') as SalesForceBrand,
			ISNULL(NULLIF(RLP.Owner_Legal_Entity,''), 'N/A') AS OwnerLegalEntity,
			ISNULL(NULLIF(RLP.Owner_Legal_Entity_Id,''), 'N/A') AS OwnerLegalEntityId,
			ISNULL(NULLIF(RLP.Contracted_Legal_Entity,''), 'N/A') AS ContractedLegalEntity

			FROM [DinDB].[DinMaster].[dbo].[Din_Restaurant_Locations_Primary] AS RLP
			LEFT OUTER JOIN DinDwhsEtl.Mapping.RestaurantMmnInfo AS mmn ON RLP.ID = mmn.site_id
			LEFT OUTER JOIN [DinDB].[DinMaster].[dbo].[Din_Restaurant_Corporations] AS RC  ON rlp.parent_id = rc.ID
			LEFT OUTER JOIN [DinDB].[DinMaster].[dbo].[Din_Restaurant_Chains] AS rchain    ON rchain.ID = rlp.chain_ID
			LEFT OUTER JOIN [DinDB].[DinMaster].[dbo].[Din_Contract_Restaurant_Master] AS crm 
													ON crm.contractID = CASE WHEN LEN(rlp.Contract_ID)>5 THEN rlp.Contract_ID 
																			 ELSE
																				 CASE WHEN LEN(rc.contractID)<6 THEN NULL ELSE rc.contractID END
																			 END
			LEFT OUTER JOIN [DinDB].[DinMaster].[dbo].[Din_Restaurant_Cuisines] AS rcui  ON rcui.ID = RLP.primary_cuisine_type AND rcui.primary_type = 1
			LEFT OUTER JOIN [DinDB].[DinMaster].[dbo].[Din_Restaurant_Types] AS rt	   ON rt.ID = rlp.entity_type
			LEFT OUTER JOIN [DinDB].[DinMaster].[dbo].[Din_Restaurant_Status] AS rs	   ON rs.ID = rlp.active_partner
			LEFT OUTER JOIN [DinDB].[DinMaster].[dbo].[Din_Billing_Group] AS bg		   ON bg.contractID = rc.contractID
			LEFT OUTER JOIN [DinDB].[DinMaster].[dbo].[RestaurantDiningStyleType] AS rds ON rds.DiningStyleType = CASE WHEN RLP.Dining_Style IN ('Dining_Style','null')
																															 THEN NULL
																															 ELSE RLP.Dining_Style
																															 END
			LEFT JOIN dbo.DimSalesForceRestaurant sfr on sfr.GUID = cast(rlp.id as varchar(10))
			) AS source 
	ON (Target.rlp_id = source.RLP_ID)

	WHEN MATCHED 
		THEN UPDATE SET target.[RestaurantLocationName] = source. [RestaurantLocationName],
						target.[LegalEntityName] = source.[LegalEntityName],
						target.[ChainName] = source.[chainName],
						target.[ContractID] = source.[ContractID],
						target.[SalesPerson] = source.[SalesPerson],
						target.[EntityType] = source.entityType,
						target.[PrimaryCuisine] = source.primaryCuisine,
						target.[SegmentType] = source.[SegmentType],
						target.[RestaurantStatus] = source.[RestaurantStatus],
						target.[AverageDiningPrice] = source.[AverageDiningPrice],
						target.[BillingGroupName] = source.billingGroup,
						target.[ProgramStartDateKey] = source.[ProgramStartDateKey],
						target.[ProgramEndDateKey] = source.[ProgramEndDateKey],
						target.[ZipCode] = source.zipCode,
						target.[City] = source.City,
						target.[LocationNumber] = source.LocationNumber,
						target.[Address]=source.[Address],
						target.RestaurantGroup_clean = source.clean_restaurant_group,
						target.[RestaurantName] = source.RestName,
						target.[SoldBy] = source.SoldBy,
						target.[CaterStartDate] = source.CaterStartDate,
						target.[CaterEndDate] = source.CaterEndDate,
						target.SalesForceBrand = source.SalesForceBrand,
						target.OwnerLegalEntity = source.OwnerLegalEntity,
						target.OwnerLegalEntityId = source.OwnerLegalEntityId,
						target.ContractedLegalEntity = source.ContractedLegalEntity
	WHEN NOT MATCHED BY TARGET 
		THEN INSERT(	   
		   [RestaurantLocationName]
		  ,[LegalEntityName]
		  ,[chainName]
		  ,[ContractID]
		  ,[SalesPerson]
		  ,[EntityType]
		  ,[PrimaryCuisine]
		  ,[SegmentType]
		  ,[RestaurantStatus]
		  ,[AverageDiningPrice]
		  ,[BillingGroupName]
		  ,[ProgramStartDateKey]
		  ,[ProgramEndDateKey]
		  ,[RLP_ID]
		  ,[ZipCode]
		  ,[City]
		  ,[LocationNumber]
		  ,[Address]
		  ,RestaurantGroup_clean 
		  ,[RestaurantName]
		  ,[SoldBy] 
		  ,[CaterStartDate]
		  ,[CaterEndDate]
		  ,SalesForceBrand
		  ,OwnerLegalEntity
		  ,OwnerLegalEntityId 
		  ,ContractedLegalEntity
		  ) 
		VALUES
		  ( source.[RestaurantLocationName],
			source.[LegalEntityName],
			source.[ChainName],
			source.[ContractID],
			source.[SalesPerson],
			source.entityType,
			source.primaryCuisine,
			source.[SegmentType],
			source.[RestaurantStatus],
			source.[AverageDiningPrice],
			source.billingGroup,
			source.[ProgramStartDateKey],
			source.[ProgramEndDateKey],
			source.RLP_ID,
			source.zipCode,
			source.city, 
			source.LocationNumber,
			source.[Address],
 			source.clean_restaurant_group,
			source.RestName,
			source.SoldBy,
			source.CaterStartDate,
			source.CaterEndDate,
			source.SalesForceBrand,
			source.OwnerLegalEntity,
			source.OwnerLegalEntityId,
			source.ContractedLegalEntity
			) 
	WHEN NOT MATCHED BY SOURCE
	THEN DELETE;
	--OUTPUT $action, inserted.*, Deleted.*;


	/********************************************** fillDimRestaurant **********************************************************************/

	WITH qry1 as 
	(
		SELECT DISTINCT replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(RestaurantGroup_clean,'''',''),',',''),'.',''),'!',''),'É','E'),'/','-'),' - ','-'),'BEDFORD THE','THE BEDFORD'), 'DINOSAUR BAR-B-QUE', 'DINOSAUR BBQ'),'ZIZIKIS RESTAURANT','ZIZIKIS') as restaurantgroup_clean, [ProgramStartDateKey], [ProgramEndDateKey]
		FROM [DinDWHS].[dbo].[staging_DimRestaurant]
		WHERE replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(RestaurantGroup_clean,'''',''),',',''),'.',''),'!',''),'É','E'),'/','-'),' - ','-'),'BEDFORD THE','THE BEDFORD'), 'DINOSAUR BAR-B-QUE', 'DINOSAUR BBQ'),'ZIZIKIS RESTAURANT','ZIZIKIS')
				NOT IN (SELECT replace(replace(replace(replace(replace(replace(replace(RestaurantGroupName,'''',''),',',''),'.',''),'!',''),'É','E'),'/','-'),' - ','-') 
						FROM [DinDWHS].[dbo].[DimRestaurantGroup])
	)
	INSERT INTO [DinDWHS].[dbo].[DimRestaurantGroup]
	(RestaurantGroupName, RestaurantGroupStartDateKey, RestaurantGroupEndDateKey)
	SELECT t1.RestaurantGroup_clean as RestGroup, min(t1.[ProgramStartDateKey]) as StartDate, max(t1.[ProgramEndDateKey]) as EndDate
	FROM qry1 t1 
	GROUP BY t1.RestaurantGroup_clean;
	
	------------------------------------------------------------------
	
	MERGE DinDWHS.[dbo].[DimRestaurant] AS target
	USING 
	(
	  SELECT * FROM
	  (SELECT ROW_NUMBER() OVER(PARTITION BY dr_a1.rlp_ID ORDER BY dr_a1.programEndDateKey DESC) AS RN,            
	  dr_a1.restaurantKey as restaurantKeyStaging,
	  NULL as restaurantKeyStagingARCH2,
	  COALESCE(case when dr_a1.restaurantLocationName = 'N/A' then null else dr_a1.restaurantLocationName end, 'N/A') as restaurantLocationName,
	  COALESCE(case when dr_a1.LegalEntityName = 'N/A' then null else dr_a1.LegalEntityName end, 'N/A') as LegalEntityName,
	  COALESCE(case when dr_a1.chainName = 'N/A' then null else dr_a1.chainName end, 'N/A') as chainName,
	  COALESCE(case when dr_a1.contractID = 'N/A' then null else dr_a1.contractID end, 'N/A') as contractID,
	  COALESCE(case when dr_a1.salesPerson = 'N/A' then null else dr_a1.salesPerson end, 'N/A') as salesPerson, 
	  COALESCE(case when dr_a1.entityType = 'N/A' then null else dr_a1.entityType end, 'N/A') as entityType, 
	  COALESCE(case when dr_a1.primaryCuisine = 'N/A'  then null else dr_a1.primaryCuisine end, 'N/A' ) as primaryCuisine, 
	  COALESCE(case when dr_a1.segmentType = 'N/A'  then null else dr_a1.segmentType end, 'N/A' ) as segmentType,
	  COALESCE(case when dr_a1.restaurantStatus = 'N/A' then null else dr_a1.restaurantStatus end, 'N/A') as restaurantStatus, 
	  -1 as averageDiningPrice, --one time use only - will use this field to recognize updated fields in DimRestaurant
	  COALESCE(case when dr_a1.BillingGroupName = 'N/A'  then null else dr_a1.BillingGroupName end, 'N/A' ) as BillingGroupName, 
	  COALESCE(dr_a1.programStartDateKey, null) as programStartDateKey, 
	  COALESCE(dr_a1.programEndDateKey, null) as programEndDateKey, 
	  dr_a1.rlp_ID AS RLP_ID, 
	  NULL AS merchantMasterID, 
	  ISNULL(zc.geographyID, 41802) as geographyID,
	  ISNULL(zc.zipCode, 'N/A') as zipCode,
	  COALESCE(case when len(dr_a1.city) = 0 then null else dr_a1.city end, 'N/A') as city,
	  COALESCE(case when len(dr_a1.locationNumber) = 0 then null else dr_a1.locationNumber end, null) as locationNumber,
	  COALESCE(case when len(dr_a1.[address]) = 0 then null else dr_a1.[address] end, 'N/A') as [address],
	  case when COALESCE(dr_a1.programStartDateKey, null) is not null then 'Y' else 'N' end as everContracted,
	  ISNULL(rg.RestaurantGroupKey,606) as RestaurantGroupKey,
	  COALESCE(dr_a1.RestaurantName, 'N/A') as RestaurantName_aNew,
	  'N/A' as MerchantName_aOld,
	  'N/A' as networkIND_aOld,
	  'N/A' as DisplayMerchantPreferredName_aOld,
	  'N/A' as MerchantDBAName_aOld,
	  'N/A' as address1tokens_aOld,
	  'N/A' as website_aOld,
	  ISNULL(dr_a1.SoldBy, 'N/A') as SoldBy,
	  dr_a1.[CaterStartDate] as CaterStartDate,
	  dr_a1.[CaterEndDate] as CaterEndDate,
	  dr_a1.SalesForceBrand,
	  dr_a1.OwnerLegalEntity,
	  dr_a1.OwnerLegalEntityId,
	  dr_a1.ContractedLegalEntity,
	  isnull(sfr.[SalesForceBrandId],0) [SalesForceBrandId],
	  isnull(sfr.[SFRestaurantKey],1) [SalesForceRestaurantId]
	  FROM DinDWHS.dbo.staging_DimRestaurant AS dr_a1 
	  LEFT OUTER JOIN DinDWHS.dbo.DimZipCodes AS zc	  ON zc.ZipCode = RIGHT(CONCAT('00000', dr_a1.ZipCode),5) 
	  LEFT JOIN DinDWHS.dbo.DimRestaurantGroup AS rg  
	  ON replace(replace(replace(replace(replace(replace(replace(rg.RestaurantGroupName,'''',''),',',''),'.',''),'!',''),'É','E'),'/','-'),' - ','-') 
			= COALESCE(CASE WHEN dr_a1.RestaurantGroup_clean = 'N/A' THEN NULL 
							ELSE replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(RestaurantGroup_clean,'''',''),',',''),'.',''),'!',''),'É','E'),'/','-'),' - ','-'),'BEDFORD THE','THE BEDFORD'), 'DINOSAUR BAR-B-QUE', 'DINOSAUR BBQ'),'ZIZIKIS RESTAURANT','ZIZIKIS') END, 'N/A')
	  
	  left join dbo.DimSalesForceRestaurant sfr on sfr.[GUID] = cast(dr_a1.rlp_ID as varchar(25))
	  
	  
	  ) AS T										
	  WHERE RN = 1
	) AS source 
	  ON (target.RLP_ID = source.RLP_ID) 


	WHEN MATCHED THEN UPDATE 
	SET 
		 target.[restaurantKeyStaging] = source.[restaurantKeyStaging],
		 target.[restaurantKeyStagingARCH2] = source.[restaurantKeyStagingARCH2],
		 target.[RestaurantLocationName] = source. [RestaurantLocationName],
		 target.[LegalEntityName] = source.[LegalEntityName],
		 target.[ChainName] = source.[ChainName],
		 target.[ContractID] = source.[ContractID],
		 target.[SalesPerson] = source.[SalesPerson],
		 target.[EntityType] = source.entityType,
		 target.[PrimaryCuisine] = source.primaryCuisine,
		 target.[SegmentType] = source.[SegmentType],
		 target.[RestaurantStatus] = source.[RestaurantStatus],
		 target.[AverageDiningPrice] = source.[AverageDiningPrice],
		 target.[BillingGroupName] = source.billingGroupName,
		 target.[ProgramStartDateKey] = (case when source.[ProgramStartDateKey] is null then target.[ProgramStartDateKey] else source.[ProgramStartDateKey] end),
		 target.[ProgramEndDateKey] = (case when source.[ProgramEndDateKey] is null then target.[ProgramEndDateKey] else source.[ProgramEndDateKey] end),
		 target.[RLP_ID] = source.rlp_ID,
		 target.[merchantMasterID] = source.merchantMasterID, 
		 target.[GeographyID]= source.[GeographyID],
		 target.[ZipCode] = source.zipCode,
		 target.[City] = source.City,
		 target.[LocationNumber] = source.LocationNumber,
		 target.[Address] = source.[Address],
		 target.EverContracted = source.EverContracted,
		 target.RestaurantGroupKey = (case when source.RestaurantGroupKey = 606 then target.RestaurantGroupKey else source.RestaurantGroupKey end),
		 target.RestaurantName_aNew = source.RestaurantName_aNew,
		 target.MerchantName_aOld = source.MerchantName_aOld,
		 target.networkIND_aOld = source.networkIND_aOld,
		 target.DisplayMerchantPreferredName_aOld = source.DisplayMerchantPreferredName_aOld,
		 target.MerchantDBAName_aOld = source.MerchantDBAName_aOld,
		 target.address1tokens_aOld = source.address1tokens_aOld,
		 target.website_aOld = source.website_aOld,
		 target.SoldBy = source.SoldBy,
		 target.CaterStartDate = source.CaterStartDate,
		 target.CaterEndDate = source.CaterEndDate,
		 target.SalesForceBrand = source.SalesForceBrand,
		 target.OwnerLegalEntity = source.OwnerLegalEntity,
		 target.OwnerLegalEntityId = source.OwnerLegalEntityId,
		 target.ContractedLegalEntity = source.ContractedLegalEntity,
		 target.[SalesForceBrandId] = source.[SalesForceBrandId],
		 target.[SalesForceRestaurantId] = source.[SalesForceRestaurantId]

	WHEN NOT MATCHED BY TARGET 
		THEN INSERT(    
	    [restaurantKeyStaging]
	   ,[restaurantKeyStagingARCH2]
	   ,[RestaurantLocationName]
	   ,[LegalEntityName]
	   ,[ChainName]
	   ,[ContractID]
	   ,[SalesPerson]
	   ,[EntityType]
	   ,[PrimaryCuisine]
	   ,[SegmentType]
	   ,[RestaurantStatus]
	   ,[AverageDiningPrice]
	   ,[BillingGroupName]
	   ,[ProgramStartDateKey]
	   ,[ProgramEndDateKey]
	   ,[RLP_ID]
	   ,[merchantMasterID]
	   ,[geographyID]
	   ,[ZipCode]
	   ,[City]
	   ,[LocationNumber]
	   ,[Address]
	   ,EverContracted
	   ,RestaurantGroupKey
	   ,RestaurantName_aNew
	   ,MerchantName_aOld
	   ,networkIND_aOld
	   ,DisplayMerchantPreferredName_aOld
	   ,MerchantDBAName_aOld
	   ,address1tokens_aOld
	   ,website_aOld
	   ,SoldBy
	   ,CaterStartDate
	   ,CaterEndDate
	   ,SalesForceBrand
	   ,OwnerLegalEntity
	   ,OwnerLegalEntityId
	   ,ContractedLegalEntity
	   ,[FirstProgramStartDateKey]
	   ,[SalesForceBrandId]
	   ,[SalesForceRestaurantId]
		  ) 
	VALUES
	   ( 
	  source.[restaurantKeyStaging],
	  source.[restaurantKeyStagingARCH2],
	  source.[RestaurantLocationName],
	  source.[LegalEntityName],
	  source.[ChainName],
	  source.[ContractID],
	  source.[SalesPerson],
	  source.entityType,
	  source.primaryCuisine,
	  source.[SegmentType],
	  source.[RestaurantStatus],
	  source.[AverageDiningPrice],
	  source.billingGroupName,
	  source.[ProgramStartDateKey],
	  source.[ProgramEndDateKey],
	  source.RLP_ID,
	  source.[merchantMasterID],
	  source.[geographyID],
	  source.zipCode,
	  source.city, 
	  source.LocationNumber,
	  source.[Address],
	  source.EverContracted,
	  source.RestaurantGroupKey,
	  source.RestaurantName_aNew,
	  source.MerchantName_aOld,
	  source.networkIND_aOld,
	  source.DisplayMerchantPreferredName_aOld,
	  source.MerchantDBAName_aOld,
	  source.address1tokens_aOld,
	  source.website_aOld,
	  source.SoldBy,
	  source.CaterStartDate,
	  source.CaterEndDate,
	  source.SalesForceBrand,
	  source.OwnerLegalEntity,
	  source.OwnerLegalEntityId,
	  source.ContractedLegalEntity,
	  source.[ProgramStartDateKey],
	  source.[SalesForceBrandId],
	  source.[SalesForceRestaurantId]
	  );



	update res set res.SalesForceRestaurantId = 1
	from [dbo].[DimRestaurant] res
	left join dbo.DimSalesForceRestaurant sf on sf.sfrestaurantkey = res.[SalesForceRestaurantId]
	where sf.sfrestaurantkey is null  -- da li je izvodljiv ovaj uslov ??? 


	ALTER TABLE [dbo].[DimRestaurant] check constraint [FK_DimRestaurant_DimSalesForceRestaurant];


	/********* update dimRestaurant ********/

	UPDATE t1
	SET t1.RestaurantGroupKey = t2.RestaurantGroupKey
	FROM DinDWHS.dbo.DimRestaurant t1 
	INNER JOIN DinDWHS.dbo.dimRestaurantgroup t2 ON t1.chainname = t2.restaurantgroupname or t1.LegalEntityName = t2.restaurantgroupname 
														  or Replace(t1.chainName,'É','e') =  t2.RestaurantGroupName
														  or Replace(t1.chainName,' ','')  =  t2.RestaurantGroupName
														  or Replace(t1.chainName,'''','') =  t2.RestaurantGroupName
	WHERE t1.restaurantgroupkey = 606
		  and t1.chainName not in ('N/A','Other') 
		  and t1.LegalEntityName not in ('N/A','Other')
		  and t2.RestaurantGroupKey not in (1013, 1421)
		  

	/********* update FT ********/

	declare @startdate int, @enddate int, @ss date;

	SET @ss =   FORMAT(DATEADD(month,-1,GETDATE()),'yyyy-MM-01'); 
	SET @startdate = CAST(FORMAT(@ss,'yyyyMMdd') AS int);
	SET @enddate = CAST(FORMAT(EOMONTH(@ss),'yyyyMMdd') AS int);
	
	UPDATE FT
	SET RestaurantKey = r.RestaurantKey , RestaurantGroupKey = r.RestaurantGroupKey
	FROM dbo.FactTransaction AS FT
	JOIN Dindb.Dinbilling.billing.[DetailInvoiceHistoryData] AS dihd
		ON FT.DIHD_ID = dihd.id
	JOIN dbo.DimRestaurant as r
		ON r.RLP_ID = dihd.Siteid
	WHERE DateKey between @startdate and @enddate AND DIHD_ID is not null AND FT.RestaurantKey = 0
	
	UPDATE FT
	SET RestaurantKey = r.RestaurantKey , RestaurantGroupKey = r.RestaurantGroupKey
	FROM dbo.FactTransaction FT
	JOIN [dbo].[DimFingerprint] df
		ON FT.FingerprintID = df.FingerprintID
	JOIN [dbo].[DimSalesForceRestaurant] sfr
		ON df.SFRestaurantKey = sfr.SFRestaurantKey
	JOIN dbo.DimRestaurant as r
		ON convert(varchar(10), r.rlp_id) = sfr.[GUID]
	WHERE DateKey between @startdate and @enddate AND VolumeTypeKey = 2 AND FT.RestaurantKey = 0
 
END	  

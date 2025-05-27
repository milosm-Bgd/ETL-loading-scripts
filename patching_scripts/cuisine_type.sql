
-- MAIN SCRIPT WE USE 

-- main purpose da odredi Cuisine Type za mesecno load-ovanje kocke jer dobijamo dirty podatke ili N/A podatke koje je potrebno ocistiti 
-- obicno radimo insertovanje vrednosti u zavisnosti koji Type se najcesce pojavljuje, otherwise imamo 2 specificna slucaja 
-- koristi se help ili bridge tabela koja vec sadrzi neke podatke 
-- specifican slucaj za Cuisine Type kada TYPE pripada ili "Partner" ili "Prior Relationship" , njega imamo u prvom subquery-u


----- SELECT * cases for CuisineType to be Updated
;WITH PartnerCuisine AS (
    SELECT 
        Brand,
        CuisineType as PreferredCuisine
    FROM (
        SELECT 
            ISNULL(bp.BrandName, sfs.Brand) as Brand,
            sfs.CuisineType,
            ROW_NUMBER() OVER (PARTITION BY ISNULL(bp.BrandName, sfs.Brand) ORDER BY Type) as rn
        FROM dbo.DimSalesForceRestaurant sfs
		LEFT JOIN dbo.DimSalesForceBrand bp
			ON bp.BrandName = sfs.Brand
        WHERE sfs.CuisineType != 'N/A'
        AND sfs.Type IN ('Partner', 'Prior Relationship')
    ) ranked
    WHERE rn = 1
),
MostFrequentCuisine AS (
    SELECT 
        Brand,
        CuisineType as MostCommonCuisine
    FROM (
        SELECT 
            ISNULL(bp.BrandName, sfs.Brand) Brand,
            sfs.CuisineType,
            COUNT(*) as TypeCount,
            ROW_NUMBER() OVER (PARTITION BY ISNULL(bp.BrandName, sfs.Brand) ORDER BY COUNT(*) DESC) as rn
        FROM dbo.DimSalesForceRestaurant sfs
		LEFT JOIN dbo.DimSalesForceBrand bp
			ON bp.BrandName = sfs.Brand
        WHERE sfs.CuisineType != 'N/A'
        GROUP BY 
			ISNULL(bp.BrandName, sfs.Brand), sfs.CuisineType
    ) ranked
    WHERE rn = 1
)

SELECT 
    ISNULL(bp.BrandName, sfs.Brand) Brand,
    sfs.CuisineType as CurrentCuisine,
    pc.PreferredCuisine as PartnerCuisine,
    mfc.MostCommonCuisine as CommonCuisine,
    COALESCE(pc.PreferredCuisine, mfc.MostCommonCuisine) as NewCuisine,
    COUNT(*) as RecordsToUpdate
FROM dbo.DimSalesForceRestaurant sfs
LEFT JOIN dbo.DimSalesForceBrand bp
	ON bp.BrandName = sfs.Brand
LEFT JOIN PartnerCuisine pc ON ISNULL(bp.BrandName, sfs.Brand) = pc.Brand
LEFT JOIN MostFrequentCuisine mfc ON ISNULL(bp.BrandName, sfs.Brand) = mfc.Brand
WHERE sfs.CuisineType = 'N/A'
AND (pc.PreferredCuisine IS NOT NULL OR mfc.MostCommonCuisine IS NOT NULL)
GROUP BY 
    ISNULL(bp.BrandName, sfs.Brand), 
    sfs.CuisineType, 
    pc.PreferredCuisine, 
    mfc.MostCommonCuisine
order by 1

--UPDATE sfs
--SET sfs.CuisineType = COALESCE(pc.PreferredCuisine, mfc.MostCommonCuisine)
--FROM dbo.DimSalesForceRestaurant sfs
--LEFT JOIN dbo.DimSalesForceBrand bp ON bp.Id = sfs.SalesForceBrandId
--LEFT JOIN PartnerCuisine pc ON 
--	ISNULL(bp.BrandName, sfs.Brand) = pc.Brand
--LEFT JOIN MostFrequentCuisine mfc ON 
--	ISNULL(bp.BrandName, sfs.Brand) = mfc.Brand
--WHERE sfs.CuisineType = 'N/A'
--AND (pc.PreferredCuisine IS NOT NULL OR mfc.MostCommonCuisine IS NOT NULL)



----- CHECK the cases where PARTNER Cuisine is different than COMMON Cuisine
;WITH PartnerCuisine AS (
    SELECT 
        Brand,
        CuisineType as PreferredCuisine
    FROM (
        SELECT 
            ISNULL(bp.BrandName, sfs.Brand) as Brand,
            sfs.CuisineType,
            ROW_NUMBER() OVER (PARTITION BY ISNULL(bp.BrandName, sfs.Brand) ORDER BY Type) as rn
        FROM dbo.DimSalesForceRestaurant sfs
		LEFT JOIN dbo.DimSalesForceBrand bp
			ON bp.Id = sfs.SalesForceBrandId
        WHERE sfs.CuisineType != 'N/A'
        AND sfs.Type IN ('Partner', 'Prior Relationship')
    ) ranked
    WHERE rn = 1
),
MostFrequentCuisine AS (
    SELECT 
        Brand,
        CuisineType as MostCommonCuisine
    FROM (
        SELECT 
            ISNULL(bp.BrandName, sfs.Brand) Brand,
            sfs.CuisineType,
            COUNT(*) as TypeCount,
            ROW_NUMBER() OVER (PARTITION BY ISNULL(bp.BrandName, sfs.Brand) ORDER BY COUNT(*) DESC) as rn
        FROM dbo.DimSalesForceRestaurant sfs
		LEFT JOIN dbo.DimSalesForceBrand bp
			ON bp.Id = sfs.SalesForceBrandId
        WHERE sfs.CuisineType != 'N/A'
        GROUP BY 
			ISNULL(bp.BrandName, sfs.Brand), sfs.CuisineType
    ) ranked
    WHERE rn = 1
)
SELECT 
    ISNULL(bp.BrandName, sfs.Brand) Brand,
    sfs.CuisineType as CurrentCuisine,
    pc.PreferredCuisine as PartnerCuisine,
    mfc.MostCommonCuisine as CommonCuisine,
    COUNT(*) as RecordsToUpdate
FROM dbo.DimSalesForceRestaurant sfs
LEFT JOIN dbo.DimSalesForceBrand bp
	ON bp.Id = sfs.SalesForceBrandId
LEFT JOIN PartnerCuisine pc ON ISNULL(bp.BrandName, sfs.Brand) = pc.Brand
LEFT JOIN MostFrequentCuisine mfc ON ISNULL(bp.BrandName, sfs.Brand) = mfc.Brand
WHERE sfs.CuisineType = 'N/A'
AND pc.PreferredCuisine <> mfc.MostCommonCuisine
--and bp.BrandName ='KFC'
GROUP BY 
    ISNULL(bp.BrandName, sfs.Brand), 
    sfs.CuisineType, 
    pc.PreferredCuisine, 
    mfc.MostCommonCuisine;


-------------------------------------------------------------------------------

select Brand, Type, CuisineType, count(*) from dbo.DimSalesForceRestaurant where Brand like '%KFC%'
group by Brand, Type, CuisineType
order by 1,2

--------------------------------------------------------------------------------
--PATCH FILE for FoodServiceType
;WITH PartnerCuisine AS (
    SELECT 
        Brand,
        FoodserviceType as PreferredFood
    FROM (
        SELECT 
            ISNULL(bp.BrandName, sfs.Brand) as Brand,
            sfs.FoodserviceType,
            ROW_NUMBER() OVER (PARTITION BY Brand ORDER BY Type) as rn
        FROM DinovaIntegrations.SalesForce.SfSync sfs
		LEFT JOIN [DinovaIntegrations].[SalesForce].[BrandProfile] bp
			ON bp.Id COLLATE SQL_Latin1_General_CP1_CS_AS = sfs.BrandProfileId COLLATE SQL_Latin1_General_CP1_CS_AS
        WHERE sfs.FoodserviceType IS NOT NULL
        AND sfs.Type IN ('Partner', 'Prior Relationship')
    ) ranked
    WHERE rn = 1
),
MostFrequentCuisine AS (
    SELECT 
        Brand,
        FoodserviceType as MostCommonFood
    FROM (
        SELECT 
            ISNULL(bp.BrandName, sfs.Brand) Brand,
            sfs.FoodserviceType,
            COUNT(*) as TypeCount,
            ROW_NUMBER() OVER (PARTITION BY ISNULL(bp.BrandName, sfs.Brand) ORDER BY COUNT(*) DESC) as rn
        FROM DinovaIntegrations.SalesForce.SfSync sfs
		LEFT JOIN [DinovaIntegrations].[SalesForce].[BrandProfile] bp
			ON bp.Id COLLATE SQL_Latin1_General_CP1_CS_AS = sfs.BrandProfileId COLLATE SQL_Latin1_General_CP1_CS_AS
        WHERE sfs.FoodserviceType IS NOT NULL
        GROUP BY ISNULL(bp.BrandName, sfs.Brand), sfs.FoodserviceType
    ) ranked
    WHERE rn = 1
)

SELECT 
	sfs.AccountID,
    COALESCE(pc.PreferredFood, mfc.MostCommonFood) as NewCuisine
FROM DinovaIntegrations.SalesForce.SfSync sfs
LEFT JOIN [DinovaIntegrations].[SalesForce].[BrandProfile] bp
			ON bp.Id COLLATE SQL_Latin1_General_CP1_CS_AS = sfs.BrandProfileId COLLATE SQL_Latin1_General_CP1_CS_AS
LEFT JOIN PartnerCuisine pc ON ISNULL(bp.BrandName, sfs.Brand) = pc.Brand
LEFT JOIN MostFrequentCuisine mfc ON ISNULL(bp.BrandName, sfs.Brand) = mfc.Brand
WHERE sfs.FoodserviceType IS NULL
AND (pc.PreferredFood IS NOT NULL OR mfc.MostCommonFood IS NOT NULL)
and sfs.LocationType='Location'


-----------------------------------------------------------------
-- CASE when all CuisineType are NULL
SELECT Brand, Count(*) as RestaurantCount
FROM dbo.DimSalesForceRestaurant
WHERE Rank < 1500
GROUP BY Brand
HAVING COUNT(CASE WHEN CuisineType = 'N/A' THEN 1 END) = COUNT(*)

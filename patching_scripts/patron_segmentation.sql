   /*
   PATRON SEGMENTATION skripta  + DOKU   -- (sa SQL Server-a izvorno)


  like INTRO isecak iz Emaila / dana 06. dec 2024
*/

/*   A new version of the Cube was just deployed and processed on the Analytics server.

New and updated dimensions:

•	Unique Patron Segmentation
o	New dimension with the pre-defined segmentation for each Unique Customer ( @our colleague can provide the definition and logic for each segment value)
o	Applies slicer to Transaction measures and Unique Customer dimension
•	SalesForce Brand
o	New dimension containing the information from the Brand Profile object on SalesForce
o	Applies slicer to Transaction measures and grouping and filger for SalesForce Restaurant dimension
•	Unique Patron
o	Added Unique Patron Id attribute to the dimension


New Measures:
•	Restaurants - Active -> Active Restaurant Count
o	Count of In-Network restaurants for the given period
	Resaurant needs to be in the Dinova network for at least one day
•	Restaurants - Active New -> New Active Restaurants Count
o	Count of In-Network restaurants that have the Start Date on the given period
•	Restaurants - Missing From Mc Merchants -> Missing From Mc Merchants Count
o	Count of In-Network restaurants that are missing the values in the MC Merchants file for the given period
•	Restaurants - Terminating -> Terminating Restaurants Count
o	Count of In-Network restaurants that have the Out-of-Business date set on the given period
•	Restaurants - With Invoiced Transactions -> Restaurants With Invoiced Transactions Count
o	Count of In-Network restaurants that had at least one Invoiced transaction


Dimensions that the new Measures can be sliced/filtered by : 
•	Bi Reporting Definitions
•	Restaurant
•	Restaurant Group
•	SalesForce Restaurant
•	SalesForce Brand
•	Transaction Date – Month being the lowest level available



Team,

The tern “Unique Patron” is the same as “Unique Customer”

Unique Patron SHOULD BE Unique Customer WHEN you are looking in the Cube
o	Added Unique Patron Id attribute to the dimension

	The Unique Customer/Patron  Segmentation refers to the User Persona 
ALSO
   Moving forward use “Unique Customer” Dimension AND STAY AWAY from “Customer” dimension (unless being used during the transition)


Darin the new Salesforce Brand dimension has a placeholder for Catering Threshold Amount and Large Event Amount Threshold.
This propagates from Salesforce and defaults to 150 and to 1000 respectively.
So we need to test with this for example % of Spend or % of Transactions that are Catering for a specific Brand.

*/


/*
Short desc of the scirpt: 
Calculation of diffenret metrics like total spend a unique patron / user is making on some geography level or geograpphy instance, or non-related to Minimarket or Territoty area 
generally how much he spends. So using various levels fo aggregations making such calculations and ratio which are needed for further analysis.
*/


use DinovaDWHS

declare @startDateInt int = cast(format(DATEADD(MONTH, -11, EOMONTH(GETDATE(), -1)), 'yyyyMM01') as int);	--EOMONTH(start_date, months)
declare @endDateInt int = cast(format(EOMONTH(GETDATE(), -1), 'yyyyMMdd') as int);
--select @endDateInt
---------------------------------------------------- Patron Segmentation dimension table

--    CREATE TABLE dbo.DimPatronSegmentation
--    (
--        Id INT IDENTITY(0,1),
--        PersonaName VARCHAR(50) NOT NULL,
--        Description VARCHAR(255)
--    );

--	ALTER TABLE dbo.DimPatronSegmentation
--	add CONSTRAINT PK_DimPatronSegmentation_Id PRIMARY KEY (Id)


--    INSERT INTO dbo.DimPatronSegmentation (PersonaName, Description)
--    VALUES 
--	    ('Undefined', 'Not defined'),
--        ('Road Warrior', 'Patron with maximum 80% spent in a single territory'),
--		('City person', 'Minimum 80% spent in a single territory AND Maximum 80% in a single minimarket'),
--		('Office manager', 'Minimum 80% spent in a single minimarket');

-----------------------------------------------------Junction table for the many-to-many relationship

--    CREATE TABLE dbo.FactUniquePatronSegmentation
--    (
--        Id INT IDENTITY(1,1) NOT NULL,
--		  UniquePatronId INT NOT NULL,
--        SegmentationId INT NOT NULL,
--        CONSTRAINT PK_FactUniquePatronSegmentation_Id PRIMARY KEY (Id),
--	      CONSTRAINT UC_FactUniquePatronSegmentation_UniquePatronId_SegmentationId UNIQUE (UniquePatronId,SegmentationId),
--        CONSTRAINT FK_FactUniquePatronSegmentation_UniquePatronId FOREIGN KEY (UniquePatronId) 
--			REFERENCES dbo.DimUniquePatron(UniquePatronId),
--        CONSTRAINT FK_FactUniquePatronSegmentation_SegmentationId FOREIGN KEY (SegmentationId) 
--			REFERENCES dbo.DimPatronSegmentation(Id)
--    );

--------------------------------------------------Make the MinimarketSpend table in DinovaDWHSEtl, monthly

		--CREATE TABLE [DinovaDWHSEtl].[dbo].[MinimarketSpend](
		--	UniquePatronId INT NOT NULL,
		--	SalesTerritory VARCHAR(255),
		--	DIN_DisplayMiniMarketName VARCHAR(255),
		--	MinimarketAmount DECIMAL(18,4) NOT NULL,
		--	TxnCount INT NOT NULL
		--)


-- DOKUMENTACIJA / 27.03.2025


-- idemo sa TRUNCATE metodom tabele dbo.MinimarketSpend 
TRUNCATE TABLE [DinovaDWHSEtl].[dbo].[MinimarketSpend]; 
-- idemo sa punjenjem MiniMarketSpend-a sa podacima iz FactTransaction-a JOINED sa DimZipCodes i DimUniquePatron  
INSERT INTO [DinovaDWHSEtl].[dbo].[MinimarketSpend] (UniquePatronId, SalesTerritory, DIN_DisplayMiniMarketName, MinimarketAmount, TxnCount)  
SELECT 	       ft.UniquePatronId, zc.SalesTerritory, zc.DIN_DisplayMiniMarketName, SUM(ft.Amount) AS MinimarketAmount, count(*) as TxnCount
FROM dbo.FactTransaction ft  			-- ft nam treba zbog jedinstvenog korisnickog broja , sumiranja amount-a i ukupnog broja txn-a i definisanja date range-a
JOIN dbo.DimZipCodes zc on ft.GeographyID = zc.GeographyID 		-- zc nam treba zbog punjenja DIN_DisplayMIniMarketName kolone
JOIN dbo.DimUniquePatron dup on ft.UniquePatronId = dup.UniquePatronId 		--dup nam treba zbog IsHighValue kolone u WHERE clause, gde ciljamo na "1"
WHERE dup.IsHighValue=1 
AND ft.DateKey between @startDateInt and @endDateInt 
GROUP BY ft.UniquePatronId,zc.SalesTerritory, zc.DIN_DisplayMiniMarketName 

-- svrha tabele radi kalkulisanja sume i ukupnog broja transakcija po jedinstvenoj kombinaciji korisnika(DimUniquePatron), njegove sales teritorije i minimarket-a gde deluje (DisplayMiniMarketName)


-----------------------------------------------------------Populating FactUniquePatronSegmentation, monthly
TRUNCATE TABLE dbo.FactUniquePatronSegmentation
	-- sa TotalSpend kalkulišemo i radimo grupisanje isključivo po UniquePatronId-u tj jedinstvenom korisnickom broju, radi vracanja totalnog obrta na nivou Minimarket-a koji generalno pravi
; WITH TotalSpend AS (
	SELECT 
		UniquePatronId,
		SUM(MinimarketAmount) AS TotalAmount
		FROM [DinovaDWHSEtl].[dbo].[MinimarketSpend]
		GROUP BY UniquePatronId
),	-- slicno gornjoj kalkulaciji, ovde koristimo i salesTerritory kao dodatni argument po kojem želimo da grupišemo i racunamo amount (znaci po korisniku + teritoriji)
TerritorySpend AS (
	SELECT 
		UniquePatronId,
		SalesTerritory,
		SUM(MinimarketAmount) AS TerritoryAmount
		FROM [DinovaDWHSEtl].[dbo].[MinimarketSpend]
		GROUP BY UniquePatronId, SalesTerritory 
),
RankedTerritories AS ( -- rangiranje po jedinstvenom korisniku gde sortiramo po teritori amount-u u desc order- u (vrvt zbog kasnijeg koriscenja u sl. subquery-u radi filterisanja max vrednosti tj redova)
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY UniquePatronId ORDER BY TerritoryAmount DESC) AS MaxPatronRank
	FROM TerritorySpend ts
),
MaxTerritorySpend AS ( -- zanima nas TotalSpend (prvi CTE) sveden na jedinstvene redove (za one teritorije gde najvise "obrta" pravi jedan korisnik ) prethodnog CTE-a tj. TerritorySpend-a
	SELECT rt.UniquePatronId, rt.SalesTerritory, rt.TerritoryAmount, rt.MaxPatronRank, tot.TotalAmount,   -- gde racunamo odnos ukupne sume ostvarene po teritoriji+korisniku i one druge sume   
	TerritoryAmount*1.0 / NULLIF(TotalAmount,0) AS TerritorySpendRatio 			-- tj ukupne sume samo po korisniku bez obzira na teritoriju koju opsluzuje 
	FROM RankedTerritories RankedTerritories						-- znaci ide veci stepen_agregacije / manji_stepen_agregacije = TerritorySpendRatio
	INNER JOIN TotalSpend tot ON tot.UniquePatronId=rt.UniquePatronId
	WHERE rt.MaxPatronRank=1
),
RankedMinimarkets AS (  -- rangiranje po jedinstvenom korisniku gde sortiramo po MiniMarket amount-u iz inicijalne source tabele , a radi koriscenja u narednom CTE-u  
	SELECT *,				-- radi izvlacenja samo onih korisnika koji prave najvece obrte po "Minimarket" regiji (ORDER BY MinimarketAmount DESC)
	ROW_NUMBER() OVER(PARTITION BY UniquePatronId ORDER BY MinimarketAmount DESC) AS MaxPatronRank
	FROM [DinovaDWHSEtl].[dbo].[MinimarketSpend] mms  -- gde za source imamo istu tabelu kao u prvom cte subquery-u
),
MaxMinimarketSpend AS (	-- sem prethodnog gde imamo rang koristimo i TotalSpend tj prvi CTE radi racunanja odnosa sume amounta po MiniMarket regiji (agregacija po minimarket+korisniku)  
	SELECT rmm.UniquePatronId, rmm.SalesTerritory, rmm.DIN_DisplayMiniMarketName, rmm.MiniMarketAmount, rmm.MaxPatronRank, tot.TotalAmount, --i ukupnog amaunta po korisniku 
	MiniMarketAmount*1.0 / NULLIF(TotalAmount,0) AS MMSpendRatio 										-- (nevezano na teritoriju) as MiniMarketSpendRatio
	FROM RankedMinimarkets rmm
	INNER JOIN TotalSpend tot ON tot.UniquePatronId=rmm.UniquePatronId
	WHERE rmm.MaxPatronRank=1
)
		-- sledi punjenje Fact UniquePatron segmentation tabele  ... 
INSERT INTO dbo.FactUniquePatronSegmentation (UniquePatronId, SegmentationId)
SELECT DISTINCT mats.UniquePatronId, ds.Id
FROM MaxTerritorySpend mats 	-- ... na osnovu ukrštanja MaxMinimarketSpend mats + MaxTerritorySpend mams tabela i unapred definisane dimenzije PatronSegmentation (vidi dole) 
INNER JOIN MaxMinimarketSpend mams ON mats.UniquePatronId = mams.UniquePatronId   		
INNER JOIN dbo.DimPatronSegmentation ds ON ds.PersonaName=   		-- radi mapiranja odgovarajućeg ID-a iz dimenzije PatronSegmentation (tj odg. PersonaName: da li je RoadWarrior ili CityPerson)   
CASE 										--  na odgovarajući SegmentationId unutar Fact tabele (znaci mapiranje Persone na fact tabelu tj njenu odgovarajucu kolonu) 
        WHEN mats.TerritorySpendRatio < 0.8 THEN 'Road Warrior'
END

UNION ALL

SELECT DISTINCT mats.UniquePatronId, ds.Id
FROM MaxTerritorySpend mats
INNER JOIN MaxMinimarketSpend mams ON mats.UniquePatronId = mams.UniquePatronId
INNER JOIN dbo.DimPatronSegmentation ds ON ds.PersonaName=	-- uporediti ovaj oblik uparivanja sa donjim gde imamo Office Manager-a i videti koji je "lepsi " vid identacije
CASE 
        WHEN mats.TerritorySpendRatio >= 0.8 AND mams.MMSpendRatio < 0.8 THEN 'City Person'
END

UNION ALL

SELECT DISTINCT mats.UniquePatronId, ds.Id
FROM MaxTerritorySpend mats
INNER JOIN MaxMinimarketSpend mams ON mats.UniquePatronId = mams.UniquePatronId
INNER JOIN dbo.DimPatronSegmentation ds ON ds.PersonaName = CASE WHEN mats.TerritorySpendRatio > 0.8 AND mams.MMSpendRatio >= 0.8 THEN 'Office Manager' END
;

--Inserting all the other existing Patrons as UNDEFINED (almost 9M)
INSERT INTO dbo.FactUniquePatronSegmentation (UniquePatronId, SegmentationId)
SELECT DISTINCT dup.UniquePatronId, ds.Id    -- umesto maps kao u gornjim slucajevima sad imamo dup DimUniquePatron u source-u
FROM dbo.DimUniquePatron dup
LEFT JOIN dbo.FactUniquePatronSegmentation fups ON dup.UniquePatronId = fups.UniquePatronId   -- tabela u koju radimo punjenje/ INSERTION (data consistency) + INNERJOIN sa dim Segmentation   
INNER JOIN dbo.DimPatronSegmentation ds ON ds.PersonaName = 'Undefined'	 -- interesantan JOIN ON uslov gde nemamo dve tabele koje uparujemo fakticki vec dodeljujemo PersonaName koloni jedinu preostali string value
WHERE fups.UniquePatronId IS NULL  		-- veza na gornji LEFTJOIN , primenom ovog uslova obezbedjujemo da se unose samo novi redovi tj redovi koji ne postoje vec u Fact tabeli (no duplicates)


-- dimenzija DimPatronSegmentation
Id	PersonaName	Description
0	Undefined	Not defined
1	Road Warrior	Patron with maximum 80% spent in a single territory     ( u skriti WHEN mats.TerritorySpendRatio < 0.8 THEN 'Road Warrior')
2	City person	Minimum 80% spent in a single territory AND Maximum 80% in a single minimarket (WHEN mats.TerritorySpendRatio >= 0.8 AND mams.MMSpendRatio < 0.8 THEN 'City Person')
3	Office manager	Minimum 80% spent in a single minimarket (WHEN mats.TerritorySpendRatio > 0.8 AND mams.MMSpendRatio >= 0.8 THEN 'Office Manager')



SLEDI SEKCIJA SA KREIRANJEM VIEW-a ZA SLEDECIH NEKOLIKO SEGMENTACIJA: 

1. HVD Persona Segmentation by Industry 
2. HVD Persona Segmentation by Company
3. HVD Persona Segmentation by Company and SalesTerritory
4. HVD Persona Segmentation by Company and Minimarket


--------------------------------------------------------------------- CREATING THE VIEWS

------------------------------------------#4.1 HVD Persona Segmentation by Industry   		... HVD = vrvt. HighValueDinners

DROP VIEW vw_IndustrySpend				-- pravimo kombinaciju CREATE VIEW i CTE kreacije 

CREATE VIEW vw_IndustrySpend AS 		-- pravimo IndustrySpend prikaz posredstvom izvedene TotalSpend tabele kojim racunamo ukupan Amount Minimarketa (TotalIndustrySpend) grupisan po ClientIndustriji 
WITH TotalSpend AS (
    SELECT
	c.ClientIndustry,
        SUM(m.MinimarketAmount) AS TotalIndustrySpend  -- ukupan Spend po industrijskoj grani tj. industriji klijenta
    FROM [DinovaDWHSEtl].[dbo].[MinimarketSpend] m
	INNER JOIN DimUniquePatron p ON p.UniquePatronId = m.UniquePatronId -- iako je ne koristimo explicitno, p tabela nam je potrebna kao junction između dve druge tabele, MinimarketSpend i Client 
	INNER JOIN DimClient c ON c.ClientID = p.ClientID
    GROUP BY c.ClientIndustry
)
SELECT 									-- kreiran subset TotalSpend zatim u glavnom delu selektujemo gde po jedinstvenoj kombinaciji Clientindustrije, Persone i TotalIndustrySpenda-izvedenog polja, 
	c.ClientIndustry,					-- izracunavamo 1. sumu amaunta po Minimarketu iz MinimarketSpend tabele sa DWHSEtl baze i 2. procentualni odnos sume pod 1. i TotalIndustrySpend-a iz CTE dela  
	ps.PersonaName,						-- kojim pravimo uvid u to koliko je učešće MiniMarketa amounta u odnosu na amount cele Client industrije , aliased kao PercentOfSpend
  SUM(m.MinimarketAmount) AS SpendbySegmentation, 
  CAST(SUM(m.MinimarketAmount) * 1.0 / NULLIF(ts.TotalIndustrySpend,0) AS DECIMAL(18,5)) AS PercentOfSpend
FROM [DinovaDWHSEtl].[dbo].[MinimarketSpend] m   -- radi racunanja ukupnog amounta po Minimarketu i procentualnog odnosa  
INNER JOIN dbo.FactUniquePatronSegmentation s ON s.UniquePatronId = m.UniquePatronId  -- da bi se vezala sa p tabelom (junction)
INNER JOIN DimUniquePatron p ON p.UniquePatronId = s.UniquePatronId	
INNER JOIN DimClient c ON c.ClientID = p.ClientID 		-- koristimo je radi racunanja relevantne vrednosti koju grupišemo po industriji 
INNER JOIN TotalSpend ts ON ts.ClientIndustry = c.ClientIndustry		-- radi računanja procentualnog odnosa tj. PercentOfSpend
INNER JOIN DimPatronSegmentation ps ON ps.Id=s.SegmentationId 			-- radi grupisanja po PersonaName 
group by c.ClientIndustry,ps.PersonaName, ts.TotalIndustrySpend--izvedeno polje iz subquery-a
order by 1,4 desc OFFSET 0 ROWS

------------------------------------ #4.2: HVD Persona Segmentation by Company? 				Create a View to be called from Excel
DROP VIEW vw_ClientSpend

CREATE VIEW vw_ClientSpend AS   	-- pravimo drugi prikaz ClientSpend slicnom metodologijom kao u prethodnom view-u  
WITH TotalSpend AS (				-- sada je grupisanje po vise atributa na osnovu racunanja ukupnog ClientSpend-a (sum(MinimarketAmount)) 
    SELECT							-- na osnovu uspesnog uparivanja MinimarketSpenda s DWHSEtl baze , i dve dimenzije UniquePatron i Client 
	c.ClientIndustry,
        c.ClientCode,
        c.ClientName,
        SUM(m.MinimarketAmount) AS TotalClientSpend
    FROM [DinovaDWHSEtl].[dbo].[MinimarketSpend] m
	INNER JOIN DimUniquePatron p ON p.UniquePatronId = m.UniquePatronId  -- p kao "junction" , radi uparivanja sa c tabelom 
	INNER JOIN DimClient c ON c.ClientID = p.ClientID
    GROUP BY c.ClientIndustry, c.ClientCode, c.ClientName    -- kao nastavak na gornji komentar, sada trazimo sumu po Minimarketu na osnovu jedinstvene kombinacije klijenta, ClientCode-a i Industrije
)
SELECT 
	c.ClientIndustry,				-- na osnovu CTE kreiranog subseta ili izvedene tabele sada kreiramo potreban VIEW s, gde idemo sa prikazivanejm svih polja iz subset-a 
	c.ClientCode,					-- sa ciljem da napravimo agregaciju po klijentu , Industriji, ClientCode-u + PersonaName-u cineci reprezentativan red/rekord za agregacije 
	c.ClientName,					-- 1.) sumiranje MinimarketAmount-a (aliased SpendbySegmentation) i 2.) procentualnog odnosa tj učešća MinimarketAmount-a sa većim stepenom agregacije u  
	ps.PersonaName,					-- MinimarketAmount-u sa manjim stepenom agregacije (bez PersonaName), nazvanog u CTE delu kao TotalClientSpend
  SUM(m.MinimarketAmount) AS SpendbySegmentation, 
  CAST(SUM(m.MinimarketAmount) * 1.0 / NULLIF(ts.TotalClientSpend,0) AS DECIMAL(18,5)) AS PercentOfSpend
FROM [DinovaDWHSEtl].[dbo].[MinimarketSpend] m
INNER JOIN dbo.FactUniquePatronSegmentation s ON s.UniquePatronId = m.UniquePatronId  -- radi spajanja sa ps tabelom (DimPatronSegmentation)
INNER JOIN DimUniquePatron p ON p.UniquePatronId = s.UniquePatronId -- radi spajanja sa c (Client) tabelom 
INNER JOIN DimClient c ON c.ClientID = p.ClientID  -- radi spajanja sa subquery (izvedenom) tabelom
INNER JOIN TotalSpend ts ON ts.ClientCode = c.ClientCode
INNER JOIN DimPatronSegmentation ps ON ps.Id=s.SegmentationId   	-- radi grupisanja po Personaname 
group by c.ClientIndustry,c.ClientCode,c.ClientName,ps.PersonaName, ts.TotalClientSpend   -- imamo 4 kolone iz SELECT-a + agrerirano polje iz subquery dela
order by 2,6 desc OFFSET 0 ROWS


-------------------------------- #4.3: HVD Persona Segmentation by Company and SalesTerritory? 			Create a Table to be called from Excel

--CREATE TABLE [DinovaDWHSEtl].[Reporting].ClientTerritorySpend (
--	OrderColumn INT IDENTITY (1,1),
--	ClientCode VARCHAR(255),
--	ClientName VARCHAR(255),
--    SalesTerritory VARCHAR(255),
--	PersonaName VARCHAR(255),
--	SpendbySegmentation DECIMAL(18,4) NOT NULL,
--  PercentOfSpend DECIMAL(18,4) NULL
--)

TRUNCATE TABLE [DinovaDWHSEtl].[Reporting].ClientTerritorySpend

; WITH TotalSpend AS (						-- pravimo subset TotalSpend radi punjenja ClientTerritorySpend tabele u glavnom delu query-a 
    SELECT 							-- sa ciljem da izracunamo ukupan terotorijalni Spend / obrt po jedinstvenoj kombinaciji Client-a, ClientCode-a i SalesTerritory-a 
        c.ClientCode,						-- iz MinimarketSpend tabele
        c.ClientName,
        m.SalesTerritory,
        SUM(m.MinimarketAmount) AS TotalClientTerritorySpend
    FROM [DinovaDWHSEtl].[dbo].[MinimarketSpend] m
	INNER JOIN DimUniquePatron p ON p.UniquePatronId = m.UniquePatronId 	-- p je ubacena kao junction 
	INNER JOIN DimClient c ON c.ClientID = p.ClientID
    GROUP BY c.ClientCode, c.ClientName, m.SalesTerritory     
)
INSERT INTO [DinovaDWHSEtl].[Reporting].ClientTerritorySpend 									-- i krecemo sa punjenjem ClientTerritorySpend tabele 
SELECT 							-- koristeci 3 polja iz CTE dela + Persona Name koja je takođe sadržana u grupisanju jedinstvenih kombinacija 5 polja 
	c.ClientCode,				-- gde su 4 iz main query-a + 1 polje iz CTE subquery-a kojim smo izracunali [TotalClientTerritorySpend] (iz tog razloga i pravimo ovakvu strukturu)
	c.ClientName,				-- 2 glavne agregacije : 1 suma po MinimarketAmount-u aliased SpendBySegmentation + PercentOfSpend kao odnos SpendBySegmentation (sa vecim stepenom agregacije) i 
	m.SalesTerritory,			-- i poljem TotalClientTerritorySpend iz CTE dela (sa nizim stepenom agregacije, tj. bez PersonaName)   nizi stepen agg.= manji broj kolona u group by
	ps.PersonaName,
  SUM(m.MinimarketAmount) AS SpendbySegmentation, 
  CAST(SUM(m.MinimarketAmount) * 1.0 / NULLIF(ts.TotalClientTerritorySpend,0) AS DECIMAL(18,5)) AS PercentOfSpend
FROM [DinovaDWHSEtl].[dbo].[MinimarketSpend] m
INNER JOIN dbo.FactUniquePatronSegmentation s ON s.UniquePatronId = m.UniquePatronId  		-- po uobicajenoj praksi uzimamo MiniMarketSpend, Fact-UniquePatron, DimUniquePatronDimClient, 
INNER JOIN DimUniquePatron p ON p.UniquePatronId = s.UniquePatronId							-- TotalSpend (izvedenu tabelu) i PatronSegmentation u JOIN strukturi 
INNER JOIN DimClient c ON c.ClientID = p.ClientID
INNER JOIN TotalSpend ts ON ts.ClientCode = c.ClientCode AND ts.SalesTerritory = m.SalesTerritory
INNER JOIN DimPatronSegmentation ps ON ps.Id=s.SegmentationId 
group by c.ClientCode,c.ClientName,m.SalesTerritory,ps.PersonaName, ts.TotalClientTerritorySpend	-- imamo 4 kolone iz SELECT-a + agrerirano polje iz subquery dela
order by 1,3,6 desc


------------------------------------------#4.4 HVD Persona Segmentation by Company and Minimarket

--CREATE TABLE [DinovaDWHSEtl].[Reporting].ClientMinimarketSpend (
--	OrderColumn INT IDENTITY (1,1),
--	ClientCode VARCHAR(255),
--	ClientName VARCHAR(255),
--    SalesTerritory VARCHAR(255),
--	PersonaName VARCHAR(255),
--	MiniMarketName VARCHAR(255),
--	SpendbySegmentation DECIMAL(18,4) NOT NULL,
--	PercentOfSpend DECIMAL(18,4) NULL,
--	TxnCount INT NOT NULL,
--	MMRank INT NOT NULL
--)

TRUNCATE TABLE [DinovaDWHSEtl].[Reporting].ClientMinimarketSpend  		-- cijem punjenju prethodi kreiranje 3 CTE izvedena subset-a 

;WITH TotalSpend AS (		-- prvi CTE TotalSpend radi dobijanja ukupne sume Spend-a po Minimarket regiji po jedinstvenoj kombinaciji CLient-a, ClindtCode-a, SalesTerritory-a i PersonaName-a
    SELECT 
        c.ClientCode,
        c.ClientName,
        m.SalesTerritory,
		ps.PersonaName,			-- dodajemo Persona name u odnosu na prethodni query gde smo radili segmentaciju by Company and SalesTerritory
        SUM(m.MinimarketAmount) AS TotalSalesTerritoryPersonaSpend
    FROM [DinovaDWHSEtl].[dbo].[MinimarketSpend] m   -- pravimo FROM/JOIN sekciju skoro ident. gornjoj FROM/JOIN strukturi main query-a prethodnog query-a gde radimo segm. po Company i SalesTeritorry
	INNER JOIN DimUniquePatron p ON p.UniquePatronId = m.UniquePatronId
	INNER JOIN DimClient c ON c.ClientID = p.ClientID
	INNER JOIN dbo.FactUniquePatronSegmentation s ON s.UniquePatronId = m.UniquePatronId
	INNER JOIN DimPatronSegmentation ps ON ps.Id=s.SegmentationId	
    GROUP BY c.ClientCode, c.ClientName, m.SalesTerritory, ps.PersonaName
),
AllMinimarketSpend AS (		-- drugi CTE AllMinimaketSpend gde imamo za dva stepena veci nivo agregacije (po poljima DIN_DisplayMiniMarketName + izvedenom polju TotalSalesTerritoryPersonaSpend preth. CTE-a) 
SELECT 						-- a u JOIN strukturi radimo match sa TotalSpend ts (naziv preth. CTE-a) na tri uparene kolone sa tri razlicite tabele (dim Client, MinimarketSpend i DimPatronSegmentation)
	c.ClientCode,
	c.ClientName,
	m.SalesTerritory,
	ps.PersonaName,
	m.DIN_DisplayMiniMarketName,			-- usled čega dodajemo kao dodatno polje u SELECT listi 
	SUM(m.MinimarketAmount) AS SpendbySegmentationMinimarket, 
	CAST(SUM(m.MinimarketAmount) * 1.0 / NULLIF(ts.TotalSalesTerritoryPersonaSpend,0) AS DECIMAL(18,5)) AS PercentOfSpend,	-- računanje odnosa tj učešća sume MinimarketAmount-a iz [MinimarketSpend]-a, u ... 
	SUM(TxnCount) as TxnCount		-- ... TotalSalesTerritoryPersonaSpend kalkulisanom polju prethodnog CTE-a (suma MinimarketAmount-a agregirana po 4 polja: Client,ClientCode,SalesTerritory i PersonaName)
FROM [DinovaDWHSEtl].[dbo].[MinimarketSpend] m    --... koji pravimo, misleci na PercentOfSpend, radi koriscenja u narednom CTE izvedenom subset-u radi dobijanja potrebnog ranga
INNER JOIN dbo.FactUniquePatronSegmentation s ON s.UniquePatronId = m.UniquePatronId  
INNER JOIN DimUniquePatron p ON p.UniquePatronId = s.UniquePatronId
INNER JOIN DimClient c ON c.ClientID = p.ClientID
INNER JOIN DimPatronSegmentation ps ON ps.Id=s.SegmentationId 
INNER JOIN TotalSpend ts ON ts.ClientCode = c.ClientCode AND ts.SalesTerritory = m.SalesTerritory AND ts.PersonaName=ps.PersonaName
group by c.ClientCode,c.ClientName,m.SalesTerritory,ps.PersonaName,m.DIN_DisplayMiniMarketName, ts.TotalSalesTerritoryPersonaSpend -- poslednji je kreirana kolona u pret. izvedenoj tabeli
),
RankedMinimarkets AS (
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY ClientCode,SalesTerritory,PersonaName ORDER BY ClientCode,SalesTerritory,PersonaName,PercentOfSpend DESC) AS MMRank -- ciljana particionisanje po navedenim kriterijima 
	FROM AllMinimarketSpend				-- radi ispunjavanja cilja dobijanja tj punjenja [ClientMinimarketSpend]  tabele sa prvih 10 member-a po gore navedenim kriterijima(ClientCode,SalesTerritory,PersonaName)
)

INSERT INTO [DinovaDWHSEtl].[Reporting].ClientMinimarketSpend
	SELECT * from RankedMinimarkets
	where MMRank<=10
	order by 1,3,4,6 desc 


------------------------------------------#3 What is the Unique Diners count and % of HVD?

--CREATE TABLE [DinovaDWHSEtl].[dbo].[DinersCount](
--	ClientCode VARCHAR(255),
--	ClientName VARCHAR(255),
--	UniqueDiners INT NOT NULL,
--	HVDCount INT NOT NULL,
--	PercentHVD DECIMAL(18,5) NOT NULL
--)

TRUNCATE TABLE [DinovaDWHSEtl].[dbo].[DinersCount];

declare @startDate date = cast(format(DATEADD(MONTH, -11, EOMONTH(GETDATE(), -1)), 'yyyyMM01') as date);
--declare @endDate date = cast(format(EOMONTH(GETDATE(), -1), 'yyyyMMdd') as date)

INSERT INTO [DinovaDWHSEtl].[dbo].[DinersCount]
SELECT c.ClientCode,c.ClientName,
		count(distinct UniquePatronId) as UniqueDiners,		-- broj jedinstvenih korisnickih ID-eva
		SUM (CAST((IsHighValue) AS int)) as HVDCount,		-- brojac HVD-a u tabeli 
		CAST (SUM (CAST((IsHighValue) AS int))*1.0 / count(*) AS DECIMAL (18,5)) AS percentHVD -- racunanje ucesca HVD u ukupnom dataset-u 
from dbo.DimUniquePatron p   						-- tj odnos broja HVD redova sa ukupnim br redova - tako dobijamo procentualni odnos u decimalnom obliku   
inner join DimClient c ON c.ClientID = p.ClientID
where p.LastTransactionDate >= @startDate  	-- za onaj transakcijski period mlađi od godinu dana 
group by c.ClientCode,c.ClientName 				-- kalkulacije koje dobijamo u obliku agregiranom po jedinstvenoj kombinacij Client-a i ClientCode-a



        


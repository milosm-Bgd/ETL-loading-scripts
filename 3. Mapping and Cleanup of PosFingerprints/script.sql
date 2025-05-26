
-- HINT: the table we were loading inside the previous script : dinsandbox.[QA].[PosMonthlyFullMap]

--Preparing mapping table

truncate table [dinDWHSEtl].[Mapping].[PosFingerprintsMonthly];

insert into [dinDWHSEtl].[Mapping].[PosFingerprintsMonthly]
SELECT Id, TransactionId, PosFingerprintId, SimHash, DvId
  FROM dindb.dinsandbox.[QA].[PosMonthlyFullMap];    -- koristimo tabelu kreiranu u prethodnoj skripti (Part 1) za punjenje nove tabele
															--  'dindb'- je linked server na analitickom serveru , pravimo inter-serversku konekciju
														

-- Proc for updating dimensions, na [dinDWHS] analytical bazi
exec [dbo].[spUpdateDimSalesForceRestaurantAndDimFingerprint];   
-- updejtuje SalesForce Restoranske i Fingerprint dimenzije  


update dbo.DimFingerprint set  MerchantName = Replace(MerchantName, nchar(65533) COLLATE Latin1_General_BIN2, '') -- menja datatype za MerchantName iz nchar(65533 COLLATE) u empty string
Where CharIndex(nchar(65533) COLLATE Latin1_General_BIN2, merchantname) > 0; --pod uslovom da je prethodno zadati datatype(nchar COLLATE) sadržan u samoj merchantname varijabli 

update dbo.DimFingerprint set  MerchantLegalName = Replace(MerchantLegalName, nchar(65533) COLLATE Latin1_General_BIN2, '') -- ponavljamo proces za MerchantlegalName
Where CharIndex(nchar(65533) COLLATE Latin1_General_BIN2, MerchantLegalName) > 0;

--Updating transaction data
  update ft set ft.FingerprintID = fin.FingerprintID, ft.SFRestaurantKey = fin.SFRestaurantKey -- menjamo vrednosti za FingerprintID iz FactTransaction ft sa FingerprintID iz dbo.DimFingerprint fin tabele 
  from dbo.FactTransaction ft
  inner join [dinDWHSEtl].[Mapping].[PosFingerprintsMonthly] m on m.DvId = ft.DVHD_ID -- ovde uključujemo tabelu [PosFingerprintsMonthly] iz dinDWHSetl baze
  inner join dbo.DimFingerprint fin on fin.FingerprintID = m.PosFingerprintId -- i izjednačavamo FingerprintID polja iz Dim Fingerprint tabele i FingerprintMonthly tabele 
   

--Restaurant update and Map
exec dbo.UpdateMapDimRestaurant 


-- 06.03.2025 kada validiramo da 
-- iz DIM restorana (DimRestaurant tbl) gledam ekvivalentu SF restorana, po Brandu i adresi, radimo preko Join-a (ponekad rucno u Dim SalesForce tabeli trazim pandan restorane)
-- hint: idemo od Restorana do SalesForce Restorana 
-- ako ne postoji restoran u SF, mora da se posalje nekom clerku (Mariji) , da oni dodaju u SF
-- nakon cega radimo re-sync procedure , pre toga da osvezim podatke u dimenziji , onda pokrecemo proceduru ([spUpdateDimSalesForceRestaurantAndDimFingerprint])
-- nakon toga poslednji upit u file-u (line 123) treba da vraca 0 rows - (validacija sta je ostalo nenamapirano)

USE [dinDWHS]	-- sadrzi DimRestaurant i 
					--			DimSalesForceRestaurant
					--select count(*)  -- dana 24/03/25: ukupan broj redova bez filtera 6.065.007 , sa ukljucenim filterima 9.745 red.
--select top 1000 ft.TransactionKey,ft.SFRestaurantKey,  sr.SFRestaurantKey, res.RLP_ID, sr.GUID,sr.AccountName,   --(gde sam dodao 4 nove kolone)
--res.RestaurantLocationName,		   --FactTransaction    --DimSFRestKey				(pandan RestaurantLocationName-u je AccountName)
--res.Address,
--res.*																-- (based on the DimSalesForceRestaurant join condition and retrieved results after filtering we are doing update of FT table!!!)
update ft set ft.SFRestaurantKey = sr.SFRestaurantKey				-- based on data retrieved from 2 JOINs and filtering we are able to update the first column (from FT) with the value from the DimSalesForceRestaurant 
from dbo.FactTransaction ft											-- so as the factTransaction to get updated 
--INNER JOIN #tmp ON ft.TransactionKey = #tmp.TransactionKey		-- compare from DimRestaurant if its present in SFRestaurant (line 107), and in case its not present in SF we make alert to the clerks team  
inner join dbo.DimRestaurant res on res.RestaurantKey = ft.RestaurantKey	-- after refreshing the data in DimSFRestaurant, we work on re-sync the procedure which updates Fingerprints and SFDimension
inner join dbo.DimSalesForceRestaurant sr on						-- at final step we make validation if there was smth left unmapped 
--cast(res.RLP_ID as varchar(25)) = sr.GUID							-- uzeti i napraviti dokumentaciju 
--fuzzy mappin																	
--sr.AccountName like '%The%Hart%and%The%Hunter%'
sr.AccountName like '%Akasha%'
and left(sr.BillingAddress,5) = left(res.Address,5)

where					 --kljuc je u WHERE uslovu kojim zelimo da ogranicimo scope subseta koji dobijamo
DateKey BETWEEN 20250201 AND 20250228			-- > DateKey iz FT tabele 
and VolumeTypeKey = 1		--VolumeTypeKey pripada FT tabeli  
and ft.SFRestaurantKey = 1   -- (1 = unmapped)			
--order by res.RestaurantLocationName 
-- 746 rows retrieved after running the december data , i.e. BETWEEN 20241201 AND 20241231
-- 672 rows after activated inner join with DimSalesForceRestaurant
-- 0 rows for the period BETWEEN 20250101 AND 20250228


-- upit za proveru je li unesen u dimenziju DimSalesForceRestaurant , ako ga nema radimo alert na data entry da ga unesu

SELECT * FROM dbo.DimSalesForceRestaurant 
WHERE 
AccountName LIKE '%Akasha%'   -- pandan AccountName-u je RestaurantLocationName iz DimRestaurant tbl
AND BillingAddress LIKE '9543%'		-- sukcesivno otvaramo i dodatne uslove kad vidimo da se gađaju redovi 
AND LocationType = 'Location'


--UPDATE FactTransaction
--SET SFRestaurantKey = 408208
--WHERE TransactionKey IN (628169899)


-- FactTransaction tabelu popuniti sa dodatnom kolonom koja nosi SFRestaurantKey

-- 06.03.25: ranujemo najre bez BillingGroupKey uslova 
-- da li ima neka transakcija da je Network a da je ostala nenamapirana (gledamo da je 100% network) 
-- na sta se mapira svaka transakcija, ili Restaurant ili SalesForceRestaurant

--poslednji upit u file-u
select top 1000 *		--select count(*) opcionalno ubacen
from dbo.FactTransaction
where DateKey BETWEEN 20250201 AND 20250228  --(promeniti za poslednji fakturisan mesec, tj da bude od 01.02.2025)
and VolumeTypeKey = 1
and (RestaurantKey = 0 -- referenca na Restaurant dimenziju (retko se desi da je nesto prazno u ovom slucaju) 
--or BillingGroupKey = 0
or SFRestaurantKey = 1  -- referenca na SF dimeziju (ovde da ocekuju nenamapirane transakcije )
)

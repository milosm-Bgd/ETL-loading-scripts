USE [DinDWHS]
GO
/****** Object:  StoredProcedure [dbo].[UpdateMapDimPatron]    Script Date: 3/8/2025 1:01:23 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


--procedura za update DImPatron tabele sa podacima iz Staging tabela, koja obezbedjuje generisanje i setovanje ProxyID-a i korektnu 
--uparenost sa transakcijama. Imamo hendlovanje duplikata, US, non US rekorda i kreiranje redova kada je ProxyID IS NULL. Zatim
--radi update FactTransaction tabele da poveze tacan Patron_ID.

--Kratak rezime: 

--unosi u DimPatron nove unose za US ili non-US transakcije i ujedno osigurava jedinstvenost ProxyId-a 

-- Obezbeđivanje integriteta podataka: Ovi uslovi su od suštinskog značaja da bi se obezbedilo da se ažuriraju ili umetnu samo validni, neduplikati.
-- Hendovanje podacima koji nedostaju: Uslovi pomažu u upravljanju rekordima sa nedostajućim proxiId-om i osiguravaju da se pravilno obrađuju.
-- Prevencija duplikata: Proveravanjem nultih vrednosti i postojećih redova, uslovi sprečavaju duple unose i obezbeđuju tačnost podataka.
-- Filtriranje za dalje procesuiranje: Specifični uslovi filtriraju zapise kojima je potrebna dalja obrada, poput onih sa Patron_ID = 1, što ukazuje da 
-- još nisu u potpunosti procesuirani , tj nisu mapirani.


ALTER PROCEDURE [dbo].[UpdateMapDimPatron]
AS

BEGIN

	TRUNCATE TABLE DinDwhsEtl.staging.ClProxy

	/******************************* TEMP TABLE ********************************/
	-- Temp table is counting all unique combinations of clientcode + proxyid + card-member-billing-zip-code + card-member-country-code + credit-card-num 
	-- and insert the new values into ClProxy table based on the rows from TransactionHeader + includes count of transactions, under condition 
	-- that ProxyId is not null and not belongs to DimPatron
	INSERT INTO DinDwhsEtl.staging.ClProxy
	([clientcode], [proxyid], [cardmemberbillingzipcode], [cardmembercountrycode], [creditcardnum], [txncount])
	SELECT clientcode, proxyid, cardmemberbillingzipcode, cardmembercountrycode, creditcardnum, count(*) as cnt
	FROM [DinDwhsEtl].[Staging].[TransactionHeader]  -- BITAN DETALj TransactionHeader sa Staging-a koristimo kao SOURCE za unos u Clproxy tbl-u sa Staging-a
	WHERE proxyid IS NOT NULL and proxyid NOT IN (select proxyid from DinDWHS.dbo.DimPatron)
	GROUP BY proxyid, clientcode, cardmemberbillingzipcode, cardmembercountrycode, creditcardnum

	/******************************* DUPLICATES ********************************/ 
	-- radimo punjenje DimPatron-a na osnovu staging.ClProxy tabele 
	-- razlika sa donjim kverijem, je da koristimo SELFJOIN tabele staging.ClProxy na tri kolone radi hendlovanja duplikata uz pomoc 
	-- [txncount] koja je kreirana u gornjem kveriju 
	
	INSERT INTO DinDWHS.dbo.DimPatron
	([ProxyID], [FirstName], [Lastname], [EmailAddress], [ClientID], [GeographyID], [ZipCode], [CC_Last4Digits])
	SELECT DISTINCT th.proxyid, 'Unknown' as FirstName, 'Unknown' as LastName, 'Unknown' as EmailAddress, 
	    isnull(dc.clientid, 0) as ClientId, isnull(dz.geographyid, 0) as GeographyId, isnull(LEFT(th.cardmemberbillingzipcode, 5), 'Unknown') as ZipCode, RIGHT(CONCAT('0000', th.creditcardnum), 4) as CC_Num
	FROM DinDwhsEtl.staging.ClProxy AS th		-- koji smo u gornjem kveriju napunili iz Staging.TransactionHeader-a ,zato imamo alias th
	INNER JOIN DinDwhsEtl.staging.ClProxy AS t2			ON th.proxyid = t2.proxyid 
													AND th.clientcode = t2.clientcode
													  AND th.txncount > t2.txncount -- SELFJOIN osigurava hendlovanje duplikata unosenjem redova sa vecim brojem transakcija (najvecim brojem), a txncount kolonu u temp tabeli smo punili sa funk. count(*) i nazvali je txncount jer broji transakcije po jedinst.kombin. nekoliko polja iz select-a
	LEFT OUTER JOIN DinDWHS.dbo.DimClient AS dc		ON th.clientcode = dc.clientcode  -- jer se pojavljuju u SELECT-u za punjenje 
	LEFT OUTER JOIN DinDWHS.dbo.DimZIPCodes AS dz	ON LEFT(th.cardmemberbillingzipcode, 5) = dz.zipcode -- jer se pojavljuju u SELECT-u za punjenje 
	left join DinDWHS.dbo.DimPatron p on p.ProxyID = th.proxyid   -- tabela u koju insertujemo nove vrednosti 
	WHERE th.ProxyId IS NOT NULL -- osigurava da su samo rekordi sa not null ProxyId razmatraju za insertion. Ovoje krucijalan deo jer se ProxyId koristi kao unique identifier, 
	  --and th.proxyid not in (select proxyID from DinDWHS.dbo.DimPatron)   -- jer bi unos podataka bez toga vodio nepotpunim i problematicnim unosima u DimPatron dimenziju
	  and p.ID is null	-- hocemo da osiguramo da su samo novi rekordi uneseni, cime izbegavamo unos redova koji vec postoje u DimPatron tabeli sa istim ProxyID-em (kol. [ID] je kljucni atribut jer ima PK constraint)
	  and th.cardmembercountrycode = '840'  -- refers to US 

	/******************************* US ********************************/

	INSERT INTO DinDwhs.dbo.DimPatron
	([ProxyID], [FirstName], [Lastname], [EmailAddress], [ClientID], [GeographyID], [ZipCode], [CC_Last4Digits])
	select distinct th.proxyid, 'Unknown' as FirstName, 'Unknown' as LastName, 'Unknown' as EmailAddress, 
	  isnull(dc.clientid,0) as ClientId, isnull(dz.geographyid,0) as GeographyId, isnull(LEFT(th.cardmemberbillingzipcode,5), 'Unknown') as ZipCode, RIGHT(CONCAT('0000', th.creditcardnum), 4) as CC_Number
	FROM DinDwhsEtl.staging.ClProxy AS th
	LEFT OUTER JOIN DinDWHS.dbo.DimClient AS dc		ON th.clientcode = dc.clientcode
	LEFT OUTER JOIN DinDWHS.dbo.DimZIPCodes AS dz	ON LEFT(th.cardmemberbillingzipcode, 5) = dz.zipcode 
	left join DinDWHS.dbo.DimPatron p on p.ProxyID = th.proxyid
	WHERE th.ProxyId IS NOT NULL
	  --and th.proxyid not in (select proxyID from DinDWHS.dbo.DimPatron)
	  and p.ID is null  -- da osiguramo unos tj. insertovanje samo onih rekorda koji nisu vec sadrzani u DimPatron tabeli (populacija redova koji imaju NULL za vrednost)  
	  and th.cardmembercountrycode = '840'

	/******************************* NON US ********************************/
	-- RAZLIKA U ODNOSU NA US -> nema LEFTJOIN sa ZIPCodes tabelom  
	INSERT INTO DinDWHS.dbo.DimPatron
	([ProxyID], [FirstName], [Lastname], [EmailAddress], [ClientID], [GeographyID], [ZipCode], [CC_Last4Digits])
	select distinct th.proxyid, 'Unknown' as FirstName, 'Unknown' as LastName, 'Unknown' as EmailAddress, 
	  isnull(dc.clientid, 0) as ClientID, 0 as GeographyId, isnull(th.cardmemberbillingzipcode, 'Unknowwn') as ZipCode, RIGHT(CONCAT('0000', th.creditcardnum), 4) as CC_Number
	FROM DinDwhsEtl.staging.ClProxy AS th
	LEFT OUTER JOIN DinDWHS.dbo.DimClient AS dc	ON th.clientcode = dc.clientcode
	left join DinDWHS.dbo.DimPatron p on p.ProxyID = th.proxyid
	WHERE th.ProxyId IS NOT NULL
	  --and th.proxyid not in (select proxyID from DinDWHS.dbo.DimPatron)
	  and p.ID is null
	--WHERE th.ProxyId IS NOT NULL
	--  and th.proxyid not in (select proxyID from DinDWHS.dbo.DimPatron)

	/***************** INSERT WHEN PROXYID IS NULL (CREATE) *****************/

	INSERT INTO DinDWHS.dbo.DimPatron
	([ProxyID], [FirstName], [Lastname], [EmailAddress], [ClientID], [GeographyID], [ZipCode], [CC_Last4Digits])
	SELECT DISTINCT dvhd.clientid + '_' + RIGHT(CONCAT('0000', dvhd.CC_Last4Digits), 4) as ProxyID, 'Unknown' as FirstName, 
	'Unknown' LastName, 'Unknown' as EmailAddress, isnull(dc.clientid, 0) as ClientID, 0 as GeographyId, 'Unknown' as ZipCode, 
	RIGHT(CONCAT('0000', dvhd.CC_Last4Digits), 4) as CC_Last4Digits
	FROM DinDwhsEtl.Staging.DetailVolumeHistoryData AS dvhd  -- zbog veze sa DimPatron tabelom 7 redova nize i ostalim navedenim tabela 
	INNER JOIN DinDWHS.dbo.FactTransaction AS ft 
		ON dvhd.transactionid = ft.TH_ID
	LEFT OUTER JOIN DinDWHS.dbo.DimClient AS dc 
		ON dvhd.clientid = dc.clientcode
	LEFT OUTER JOIN DinDwhsEtl.Staging.[TransactionHeader] AS TH
		ON ft.th_id = th.transactionid
	left join dbo.DimPatron p on p.ProxyID = concat(dvhd.clientid , '_' , RIGHT(CONCAT('000',dvhd.CC_Last4Digits),4))
	WHERE 1=1
	and p.ID is null  -- da osiguramo da budu insertovani samo novi redovi, tj redovi sa istim ProxiId kao iz dvhd tabele, tj oni koji nisu vec sadrzani u DimPatron 
	--and dvhd.clientid + '_' + RIGHT(CONCAT('000',dvhd.CC_Last4Digits),4) NOT IN (select proxyID from DinDwhs.dbo.DimPatron)
	  and dvhd.CC_Last4Digits IS NOT NULL
	  and ft.patron_id = 1		-- jer je ft.patron_id koriscen kao placeholder ili default value, u konk slucaju 1, koji flag-uje transakcije za dalje procesuiranje tj. jos uvek nije mapirana /linkovana na Patron-a
	  and th.proxyid is null    -- obrada tih rekorda bez ProxyID-a , tj kreiranje ProxyID-a konkretno za redove gde on nedostaje
	  --and th.clientid <> 'MC'

	/******************************* UPDATE REGULAR ********************************/
	-- radimo update regularnih redova 
	UPDATE t1		-- FactTransaction
	SET t1.[Patron_ID] = t3.id		-- id iz t3-DimPatron koristimo za update Patron_ID FactTransaction tabele
	FROM DinDWHS.dbo.FactTransaction AS t1
	INNER JOIN [DinDwhsEtl].[Staging].[TransactionHeader] AS t2  
		ON t1.th_id = t2.TransactionId  -- cita se "da je" a ne "ako je"
	INNER JOIN DinDWHS.dbo.DimPatron AS t3  
		ON t2.proxyid = t3.proxyid  -- cita se "da je" a ne "ako je"
	WHERE t2.proxyid IS NOT NULL -- neophodan uslov jer na bazi postignutog JOIN-a izmedju t2 i t3 tabele radimo UPDATE Patron_ID polja t1 tabele. Osigurava da samo redovi sa 
								-- validnim ProxyId iz TransactionHeader tabele se uzimaju u obzir. Krucijalan deo jer update proces se oslanja na matching proxyid polaj iz obe tabele 
								-- koji treba da setuje korektan Patron_ID u FT tabeli !
	and t1.patron_id = 1   -- placeholder koji implicira da transakcija jos uvek nije linkovana / mapirana na Patron-a i da zahteva dalje procesuiranje  
							-- zato i koristimo ovaj uslov za identifikaciju transakcija u FT koje nisu mapirane , prilikom ranovanja part 3 skripte!!! 
	/******************************* UPDATE CREATED ********************************/
	-- radimo update kreiranih redova (veza na 2. skriptu do poslednje koja radi unosenje za redove koji nemaju ProxyID definisan)   
	UPDATE t1		--FactTransaction		-- tj mora biti kreiran sabiranjem stringova clientcode, '_' i CC_Last4Digits iz dvhd tabele
	SET t1.[Patron_ID] = t3.id  -- vrsimo UPDATE Patron_ID za transakcije u kojima je generisan novi ProxyID
	FROM DinDWHS.dbo.FactTransaction AS t1
	INNER JOIN [DinDwhsEtl].[Staging].[DetailVolumeHistoryData] AS t2   -- umesto [TransactionHeader]
		ON t1.dvhd_id = t2.id
	INNER JOIN DinDWHS.dbo.DimPatron AS t3 
		ON t2.ClientID + '_' + RIGHT(CONCAT('0000', t2.CC_Last4Digits), 4) = t3.ProxyID
	WHERE t1.Patron_ID = 1  -- pod pretpostavkom da ovim nacinom obelezavanja kolone Patron_ID obezbedjujmeo dalje procesuiranje. Krucijalan i neophodan uslov sa kojima postizemo linkovanje transakcije sa novo kreiranim Patron-om
	and t2.CC_Last4Digits IS NOT NULL  -- ne sme da bude NULL jer ga koristimo za generisanje ProxyID-a
	--and t1.ChannelKey = 1	

END

-- na donja pitanja sam gore naveo objasnjenaj, u okvriu skripte na koju se odnosi 

- inside Insert Duplicates into DimPatron segment, why do we need the 2 conditions in WHERE clause ?
	WHERE th.ProxyId IS NOT NULL
    	AND p.ID is null 

-- in Insert Records with NULL ProxyID segment , why do we need the following conditions in WHERE clause 
	WHERE p.ID is null
	and ft.patron_id = 1
 	and th.proxyid is null 

-- in UPDATE REGULAR records segment , why do we need the following filter conditions in WHERE clause:
	WHERE t2.proxyid IS NOT NULL 
	and t1.patron_id = 1

-- in  UPDATE CREATED records segment, why do we need the following filter conditions in WHERE clause:
	WHERE t1.Patron_ID = 1 
	and t2.CC_Last4Digits IS NOT NULL


Summary
Ensuring Data Integrity: 
These conditions are essential to ensure that only valid, non-duplicate records are inserted or updated.

Handling Missing Data: 
The conditions help manage records with missing ProxyId and ensure they are processed correctly.

Preventing Duplicates: 
By checking for null values and existing records, the conditions prevent duplicate entries and ensure the accuracy of the data.

Filtering for Processing: 
The specific conditions filter out records that need further processing, like those with a Patron_ID of 1, 
indicating they have not been fully processed yet.

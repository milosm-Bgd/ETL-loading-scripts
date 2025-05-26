https://app.diagrams.net/#W58967C34B8950553%2F58967C34B8950553!s368a06e5625f441a9940e25cb5fd7b6d#%7B%22pageId%22%3A%22L_hAZGUyFOripl58_gAz%22%7D
-- full script odjednom 

use DinDWHS;


-- UPSERT-ovanje podataka u novu tabelu (tj target)
 -- uzimamo BI reporting definition produkcione podatke iz share-ovanog source-a ([BiReportingDefinitions]) 
 -- i ulivamo ih unutar dimenzione [DimBiReportingDefinitions] tabele na analiticoj bazi

  merge [DinDWHS].[dbo].[DimBiReportingDefinitions] as target		-- dimenziju [DimBiReportingDefinitions] koju merge-jemo kao target  
  using (
	SELECT [BackOfficeId],												-- koristeci 4 kolone iz source tabele sa produkcionog servera
		   [Name],
		   [BiReportingGroupOwner],
		   [BiReportingParentId] 
	FROM Dindb.Dinshared.[Business].[BiReportingDefinitions]		-- [BiReportingDefinitions] koju koristimo kao source 
	where bireportingparentid is not null								-- glavni filter da [Id] polje is not null (pun naziv BiReportingParentId), uzimajuci u obzir samo rekorde sa validnim Parentom  
  ) AS source on source.[BackOfficeId] = target.[BiBackofficeId]		-- merge-jemo target i source na BackOfficeId polju 
  when matched then update												-- ako se match-uju radimo update postojećih redova (postojece vrednosti bivaju "pregažene"): 
  set																								-- [BiReportingName],		 
  target.[BiReportingName] = source.[Name],															--[BiReportingGroupOwner] i		
  target.BiReportingGroupOwner = source.[BiReportingGroupOwner],									--[BiReportingParentId]
  target.BiReportingParentId = source.[BiReportingParentId]
  when not matched by target then insert
  (
     [BiBackofficeId]													-- ako se ne match-uju, radimo 4 polja iz inicijalnog SELECT-a iz source tabele 
	,[BiReportingName]																	-- gde menjamo nazive kolona stavljajuci u prefix BiReporting 
	,[BiReportingGroupOwner]															-- na koje lepimo vrednosti kolona iz source tabele dodavajuci nove redove 
	,[BiReportingParentId]
  )
  values (
	source.[BackOfficeId],
	source .[Name],
	source.BiReportingGroupOwner,
	source.BiReportingParentId
	);



	update res set res.BiReportingId = bi.BiReportingId		-- izmena koju radimo sa BiReportingId poljem iz target tabele [DimBiReportingDefinitions]
	from dbo.DimRestaurant res						-- u osnovi imamo JOIN sa DImRestaurant , Restaurant_Locations_Primary sa BackOfffice-a i target Bi.Report.Def. koji smo gore merge-vali 
	inner join Dindb.Dinmaster.dbo.DIn_Restaurant_Locations_Primary rlp on res.RLP_ID = rlp.id
	inner join dbo.DimBiReportingDefinitions bi on bi.BiBackofficeId  = rlp.Bi_Reporting_Id  -- DimBiReportingDefinitions gore koriscen kao target, sem toga ovaj inner join nam obezbedjuje relaciju potrebnu za update BiReportingId polja u UPDATE komandi
	where res.BiReportingId <> bi.BiReportingId; 
	-- koristeci ovaj restriktivni uslov obezbedjujemo dataset da ostane konzistentan, tj.da se unose samo oni redovi koji nisu vec prisutni u dimenziji Restaurant
	-- u dimenziji Restaurant tako sto je current dataset iz dimenzije Restaurant tj BiReportingId razlicit od novih koje treba da unesemo u dimenziju <> KOoloa u 
	-- dimenziji Restoran biva updated tako oslikava ispravnu/korektnu/azurnu vrednost iz dimenzije DimBiReportingDefinitions 

	-- update-om obezbedjujemo da se dimenzioni podaci (dimenzije Restaurant) pune koristeći up-to-date BIReporting identifikatore, što predstavlja 
	-- kljucni korak pre nego se podaci mapiraju unutar tranzakcione / Fact tabele 


	merge [dbo].[FactBiReportingRelationships] as target			-- cilj nam je da mergu-jemo podatke unutar fact tabele [FactBiReportingRelationships] , sto je target 
using (																-- koristeci source, DimRestaurant, i dve kolone u njemu: BiReportingId i RestaurantKey
		select BiReportingId, RestaurantKey
		from dbo.dimrestaurant
		) 
		as source on target.[RestaurantKey] = source.[RestaurantKey]
when matched then update 
set 
	target.[BiReportingId] = source.[BiReportingId]				-- u slucaju MATCH-vanja radimo update jedne kolone [BiReportingId]
when not matched by target then									-- u suprotnom slucaju radimo INSERT, u dve kolone pandan select statement-u 
insert(
[BiReportingId], [RestaurantKey])							
values(														
source.[BiReportingId], source.[RestaurantKey]);

-- Ovaj poslednji korak osigurava da Fact tabela FactBiReportingRelationships odražava najnovije informacije o mapiranju između restorana i njihovih 
-- billing grupa za BI Reporting, čineći te relacije dostupnim za izveštavanje i analizu. Propagira ova ažuriranja u Fact tj transakcionu tabelu koja 
-- mapira restorane prema BI reporting identifikatorima/definicijama, obezbeđujući da analitički izveštaji odražavaju trenutne odnose u grupi.

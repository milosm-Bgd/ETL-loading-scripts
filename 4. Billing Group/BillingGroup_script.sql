https://app.diagrams.net/#W58967C34B8950553%2F58967C34B8950553!s577ea8d41f2846179f5eeec40eb740ad#%7B%22pageId%22%3A%22hZX_jWn_f2lxAiJMkZBj%22%7D
--select EOMONTH(MAX(TXNDATE)) from DinBilling.billing.detailinvoicehistorydata

 --06.03.25 1st step: ranujemo script na prod serveru 

-- Production server


declare @maxDate date = (select max(txndate) from DinBilling.billing.detailinvoicehistorydata)  -- DA UTVRDIMO najsveziji tj "najviši" datum transakcija
																						-- koji kasnije koristimo da setujemo end-valid date za redove

-- radimo MERGE INTO TARGET USING SOURCE sintax za populaciju target-a [BillingGroupLocations] 
-- preko podataka dostupnih u source-u [DetailInvoiceHistoryData] + din_billing_group sa BackOffice-a

merge Dinshared.[Business].[BillingGroupLocations] as target   -- shared business dimension table, znaci koristim MERGE za popunjavanje te dimenzije na osnovu aggregated history data 
using (
select *
from (
select bg.ID BackOfficeBillingGroupId,SiteId, RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(di.BillGroup,
													'''',''),'/','-'),'É','E'),'P. F.','P F'),'B.GOOD','BGOOD') ,'  ',' '),'-TERMED',''),' - ','-'),'-TERMINATED',''),'-TERM',''),'.',''),'!',''),'_',' '),' -','-')) AS BillingGroupDisplayName, 
FORMAT(MIN(TXNDATE),'yyyy-MM-01') AS ValidFrom,   --vrsimo agregaciju history data za svaku kombinaciju SiteId i BillGroup aliased kao BillingGroupDisplayName, uslovima merge when matched operacije, cime racunamo ValidFrom , ValidTo i Rnk polja 
IIF(EOMONTH(MAX(TXNDATE)) = @maxDate ,'9999-12-31',EOMONTH(MAX(TXNDATE)))  AS ValidTo -- ValidFrom je prvi dan u mesecu najranije txn, ValidTo je poslednji dan u mesecu poslednje txn, koji eventualno dobija vrednost daleke buducnosti u slucaju da = deklarisanoj varijabli @maxDate
, ROW_NUMBER() OVER (partition by di.SiteId,RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(di.BillGroup,'''',''),'/','-'),'É','E'),'P. F.','P F'),'B.GOOD','BGOOD') ,'  ',' '),'-TERMED',''),' - ','-'),'-TERMINATED',''),'-TERM',''),'.',''),'!',''),'_',' '),' -','-'))  ORDER BY bg.ID DESC) AS Rnk
FROM DinBilling.billing.detailinvoicehistorydata di
LEFT join DinMaster.dbo.din_billing_group bg on RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(bg.billing_id,'''',''),'/','-'),'É','E'),'P. F.','P F'),'B.GOOD','BGOOD') ,'  ',' '),'-TERMED',''),' - ','-'),'-TERMINATED',''),'-TERM',''),'.',''),'!',''),'_',' '),' -','-')) = 
RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(di.BillGroup,'''',''),'/','-'),'É','E'),'P. F.','P F'),'B.GOOD','BGOOD') ,'  ',' '),'-TERMED',''),' - ','-'),'-TERMINATED',''),'-TERM',''),'.',''),'!',''),'_',' '),' -','-'))
where txndate >= '2025-02-01'
and di.siteid is not null
group by SiteId, RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(di.BillGroup,'''',''),'/','-'),'É','E'),'P. F.','P F'),'B.GOOD','BGOOD') ,'  ',' '),'-TERMED',''),' - ','-'),'-TERMINATED',''),'-TERM',''),'.',''),'!',''),'_',' '),' -','-')), bg.ID
) s where s.rnk = 1  -- veza from clause, line 17  --sve je spakovano u subquery s da bi se odradio WHERE uslov sa vracanjem origin record-a (izbegavamo duplikate)
) as source
on source.SiteId = target.SiteId
	and source.BillingGroupDisplayName = target.BillingGroupDisplayName
when matched then update		-- radimo siguran update validto kolone i eventualan update BackOfficeBillingGroupId-a
set 
target.validto = source.validto,
target.BackOfficeBillingGroupId = IIF(source.BackOfficeBillingGroupId = -1  , target.BackOfficeBillingGroupId, source.BackOfficeBillingGroupId)
when not matched by target then insert  -- obicno insertovanje novog reda sa novim ID-em ,posredstvom newid() funkcije, u slucaju not matched  
(
	Id, BackOfficeBillingGroupId, SiteId, BillingGroupDisplayName, validfrom, validto
)
values(
	NEWID(),
	source.BackOfficeBillingGroupId, source.SiteId, source.BillingGroupDisplayName, source.validfrom, source.validto
)
;  -- videti .txt file "Part 4.  Billing Group.sql" za skracenu verziju


-- Switch to analytics
-- 06.03.25 2nd step na analytical serveru

use DinDWHS;   --sadrzi [dbo].[DimBillingGroup] koja je u left join-u 


declare @maxDateKey int = (select max(datekey) from dbo.FactTransaction);
set @maxDateKey = concat(left(@maxDateKey,6), '01');  -- setovanje da bude prvi dan aktuelnog meseca u kom radimo presek (kako bismo koristili skorije transakcije prilikom update-a) 

--    PUNJENJE PODATAKA U DIMENZIJU   [dbo].[DimBillingGroup]        i to u tri navedene kolone u zagradi sa podacima iz CTE tabele koja je kreirana u prvim 
-- redovima preko [BillingGroupLocations] tabele (gde koristimo  [BillingGroupDisplayName] iz source tabele i za popunjanjanje [BillingGroupKey] i [BillingGroupName] kolona [DimBillingGroup] dimenzije)
with qry AS (		-- CTE za extracting BillingGroup informacija
select distinct BillingGroupDisplayName, BackOfficeBillingGroupId
from Dindb.Dinshared.[Business].[BillingGroupLocations]  -- linked server na analytical ([BillingGroupLocations] je shared business dimension table, tj target u prvom kveriju)
) --*****  tabela u koju smo merge-vali podatke iz dihd billing baze u gornjem kveriju 
insert into [dbo].[DimBillingGroup] ([BillingGroupKey],[BillingGroupName],[BillingGroupBackOfficeId])
select ROW_NUMBER() OVER (order by q.BillingGroupDisplayName) + (select max([BillingGroupKey]) from [dbo].[DimBillingGroup]), -- struktura koja pravi jednu kolonu kojoj se dodaje max vrednost BillingGroupKey-a posredstvom aritmeticke operacije sabiranja 
q.BillingGroupDisplayName, ISNULL(q.BackOfficeBillingGroupId,-1)															
from qry q
left join [dbo].[DimBillingGroup] bg on bg.BillingGroupName = q.BillingGroupDisplayName -- druga kolona INSERT stejtmenta
								and bg.BillingGroupBackOfficeId = q.BackOfficeBillingGroupId -- treca kolona INSERT stejtmenta
								where bg.BillingGroupKey is null -- prva kolona; da osiguramo da radimo INSERT samo onih vrednosti koje nisu već sadržane u tabeli [DimBillingGroup] bg 

--  ROW_NUMBER() po pravilu dodeljuje sukcesivno uvecane vrednosti redovima za 1, u ovom slucaju nastavljaju dodeljivanje od poslednje vrednosti BillingGroupKey ključa uvecane za 1 
--  dobijajući tako koninuiranu sekvencu vrednosti ili rastući niz krenuvši od max vrednosti BillingGroupKey-a uvećane za 1
--		PRIPREMA ZA UPDATE FACT-TRANSACTION TABELE KOJI OMOGUCAVA POVEZIVANJE TRANSAKCIJA SA BILLING-GRUPAMA
;with qry as (			-- CTE koji koristimo za top row per SiteId + prosirujemo prethodni CTE sa 2 dodante kolone : SIteId i rnk putem window funk.
select SiteId,BackOfficeBillingGroupId,BillingGroupDisplayName, ROW_NUMBER() OVER (partition by SiteId Order by validfrom desc) rnk  --  omogucavajuci da izaberemo najsvezije BilingGroup redove
from Dindb.Dinshared.[Business].[BillingGroupLocations]	 -- PONAVLJAMO u FROM istu source tabelu iz prethodnog CTE-a (share-vana dimenz. BillingGroupLocations)	-- grupisane po SiteId particiji u odnosu na validfrom 
)
-- sa UPDATE koji sledi radimo dodeljivanje vrednosti BillingGroupKey iz FT tabele vrednostima iz dimenzije dbo.DimBillingGroup tabele 
-- sto implicira da hocemo da povezemo detalje transakcije /svaki red transakcije sa odgovarajucom Billing grupom 
-- koju smo u prethodnoj skripti popunjavali sa podacima iz [BillingGroupLocations] tabele, joinovanom sa [DimBillingGroup] na dve kolone 
-- i sa filterom WHERE bg.BillingGroupKey IS NULL cime osiguramo INSERT samo onih vrednosti koje nisu već sadržane u tabeli

update ft set BillingGroupKey = bg.BillingGroupKey   -- bg je dimenziona DimBillingGroup tabela
from dbo.FactTransaction ft
inner join dbo.DimRestaurant res on res.RestaurantKey = ft.RestaurantKey  --(DimRestaurant nam treba kao spona zbog veze sa BillingGropLocations,koja stoji iza izvedenog qry)
inner join qry q on q.siteid = res.RLP_ID	-- kupeći najsvežije BillingGroup detalje/redove izmedju ostalog odg. BillingGroupKey-eve koje koristimo za filterisanje
inner join dbo.DimBillingGroup bg on bg.[BillingGroupName] = q.BillingGroupDisplayName 
where ft.VolumeTypeKey = 1		-- restriktivno polje uzimajuci samo redove sa VolumeType = 1 u obzir  (flag da je nešto fakturisano restoranima i ona figuriše u kocki kao takva) 
and q.rnk = 1					-- kupeci najsvezije BillingGroup detalje/redove (Order by validfrom desc koji pozivam sa WHERE q.rnk = 1)
and ft.datekey >= @maxDateKey   -- koristeci filter radi uvrštavanja samo najsvežijih tj najmlađih transakcija počevši od prvog dana meseca kad radimo presek  
and ft.BillingGroupKey = 0		-- osiguravajući da se ažuriraju samo novi redovi FT tabele   
									-- tj. oni redovi FT tabele koji do tada nisu imali dodeljen BillingGroup ključ

-- poenta WHERE Clause-a u MERGE i UPDATE-u:

-- Filterisanje koje primenjujemo sluzi sledecoj svrsi:

-- de-duplikacija i jedinstvenost(uniquness)
-- unutar MERGE , particionisanje koje koristimo (rnk=1) osigurava da za svaki SiteId i BillingGroupDisplayName, se koristi samo jedan red za update ili insert u share BillingGroupLocations
-- prevencija overwritting postojecih podataka
-- koristeci bg.BIllingGroupKey is null ILI ft.BillingGroupKey = 0 osigurava da se unose samo oni redovi koje nemaju vrednost, cime postojece vrednosti nisu "pregazene"	
-- data range control 
-- uslovi na txndate i @maxdatekey osigurava da su samo noviji redovi procesuirani . Sto je krucijalno za inkrementalno lodovanje cime izbegavamo update istorijskih podataka bespotrebno
-- ocuvanje konsistentnost i ispravnog povezivanja transakcija sa dimenzijom
-- radeci JOIN na novi SiteID i BillingGroupDisplayName , pritom koristeci lookup tabelu za povlacenje sa oba servera nam osigurava da samo azurirani transaction redovi su pravilno povezani 
-- sa dimenzijom BilingGroup (dimenzionim podacima)


-- inter-serverska komunikacija:
--	u prvi mah radimo na produkcionom serveru kako bismo radili data cleansing + extracting iz sharovane tabele [BillingGroupLocations] radi daljeg unosenja u dimenzionu  tabelu 
--  u drugom mahu se radi update dimenzione tabele - se kacimo na analiticki server kako bismo osigurali konzistentnost podataka , koristeci leftjoin sturkture sa tabelom u koju 
--  radimo INSERTovanje, sa uslovima obezbedjujuci da su uneseni samo novi redovi u dimezionu tabelu 

--Prvo izvrsavanaje preko Produkcionog :

--- dobijanje najsvezijih transaction date podataka 
--- merge-ovanje agregiranih billing group podataka ( iz DetailInvoiceHistoryData i BillingGroup lookup tabele) unutar shared tabele . Prilikom merge-vanja radi update postojecih 
--- redova ako postoji match tj unosi nove redove ako ne postoji 


--Zatim dolazi izvrsavanje preko Analytics servera:

--- prebacujemo context na analiticke baze
--- insertovanje novih billing group rekorda u dimenzionu tabelu ako nisu tamo prisutni
--- azuriranje UPDATE transaction rekorda (unutar FactTransaction) sa ispravnim BillingGroupKey koristeci JOIN izmedju dimRestaurant tabele i novo 
--- unesenih dim. podataka WHERE uslovi osiguravaju samo novi i neprocesuirani redovi su azurirani i uneseni, tako obezbedjujuci tacne i azurne podatke u ETL procesu 



--Other Commented JOINs or Conditions:
--For instance, the commented join on dimuniquepatron in another procedure indicates that sometimes additional checks or different data sources might be used. Here, however, it is not active.


--ADDITIONAL QUESTION regardign execution sequensce in the same batch on Sql. Ser. Man. Stud. - SSMS 

--As there are CTE for Extracting Billing Group Information and another one CTE to Pick the Top Row per Site , 
--how do we perform the execution of the entire query provided in my previous prompt?

--ANSWER

--When you execute a single batch that contains multiple CTEs (Common Table Expressions) followed by DML 
--(INSERT/UPDATE) statements, SQL Server processes the CTEs as “inline views” within that batch. In your provided query, 
--there are multiple distinct sections where CTEs are defined and then immediately used by subsequent statements. Here’s 
--how the overall execution works:

--Execution Order Overview
--Batch Execution: All parts of the query are sent to the server in one batch. SQL Server’s query optimizer will parse 
--the entire batch before starting execution.

--CTE Definition is Immediate and “Inline”:

--A CTE is defined and used only in the statement that immediately follows it.

--In your query, one CTE (for extracting billing group information) is defined and then used in an INSERT statement for 
--populating the DimBillingGroup table.

--Next, another CTE (for picking the top row per Site) is defined and then used to drive the UPDATE statement that sets 
--BillingGroupKey on the FactTransaction table.

--Step-by-Step Processing: Each “segment” of your overall script is processed independently as the batch executes:

--Phase 1 (Production/Shared update via MERGE): The MERGE statement first uses its internal derived tables (which are 
--built using a CTE) to update or insert into the shared BillingGroupLocations table.

--Phase 2 (Switch to analytics environment): After switching the database context (USE DWHS;), a new variable is declared 
--and set, and subsequent CTE definitions are used:

--The first CTE (qry) extracts distinct BillingGroupDisplayName and BackOfficeBillingGroupId from the shared table. It is 
--then consumed by an INSERT statement that loads new records into the DimBillingGroup table.

--Then, another CTE (qry again, defined with a different purpose) is used to rank billing groups per Site. That CTE is then 
--consumed in an UPDATE statement that updates the FactTransaction table.

--Single Batch, Sequential Execution: Although you have multiple CTE definitions, each is defined and immediately consumed 
--by the statement that follows it. Within a given statement:

--The CTE is first logically “built” (conceptually, its query defines a result set).

--The main query then uses that result set. Thus, you don’t run the CTEs separately; they’re part of a single execution 
--plan for that statement.

--How to Execute the Entire Query
--Run as One Batch: In SQL Server Management Studio (SSMS), you would:

--Paste the entire script (all phases, with the MERGE, USE DWHS;, the first CTE with INSERT, the CTE with UPDATE, etc.) 
--into a query window.

--Execute the script. The SQL Server engine will process the batch from top to bottom.

--Context Switching: The script includes a USE DWHS; instruction. This tells SQL Server to change the current database 
--context to DWHS for the statements that follow. Therefore, when you run the entire query batch, SQL Server will:

--Execute the production portion (MERGE and related logic) in one database.

--Then switch context (using the USE statement) and run the analytics portion (the INSERT into DimBillingGroup and the 
--UPDATE to FactTransaction) in the DWHS database.

--Multiple Statements, One Execution: All the defined CTEs, INSERTs, and UPDATEs are executed in sequence as part of 
--the same batch. They rely on each other only where the later statements reference previously defined CTEs or temporary 
--objects.

--Why This Is Useful
--Modular and Readable: Using CTEs to extract and transform data makes each segment self-contained and easier to understand.

--Sequential Correctness: By switching context and defining CTEs, you ensure that:

--Data is first updated in the production context (shared business tables).

--Afterwards, the new or updated billing group information is propagated into the analytics environment.

--Consistency: Since the entire query is executed in one batch, you know that all transformations occur in sequence 
--without the need for intermediate manual steps.

--Final Remarks
--The execution of the entire query involves:

--Submitting the full script as one batch.

--SQL Server processes the batch from the top down, evaluating and using each CTE immediately with its corresponding DML 
--statement.

--The USE statement changes the database context so that later statements run on a different database.

--Overall, this approach allows you to first update your operational data (production side) and then update your 
--analytical tables once the changes are made.

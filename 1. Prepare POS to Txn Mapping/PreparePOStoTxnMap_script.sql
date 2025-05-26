
use Dinintake;


truncate table Dinsandbox.[QA].[PosMonthlyFullMap];
insert into Dinsandbox.[QA].[PosMonthlyFullMap]
select t.id, t.transactionid, fin.fingerprintid, HASHBYTES('SHA1', CAST(CONCAT(UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[MerchantNumber])),'') AS NVARCHAR(100))),','
										  ,UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[MerchantLegalName])),'') AS NVARCHAR(100))),','   
										  ,UPPER(CAST(ISNULL(REPLACE(LTRIM(RTRIM(t.[MerchantName])),'REV:',''),'') AS NVARCHAR(100))),','
										  ,UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[AddressLine01])),'') AS NVARCHAR(100))),','
										  ,UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[CityName])),'') AS NVARCHAR(100))),','
										  ,UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[StateProvince])),'') AS NVARCHAR(100))),','
										  ,UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[PostalCode])),'') AS NVARCHAR(100))),','
										  ,UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[CountryCode])),'') AS NVARCHAR(100)))) as NVARCHAR(1000))),
										  dv.id
from Dinbilling.billing.detailvolumehistorydata dv   
inner join Dinintake.dbo.transactionheader t on t.transactionid = dv.transactionid   
LEFT join Dinshared.finance.posfingerprints fin on fin.simhash = HASHBYTES('SHA1', CAST(CONCAT(UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[MerchantNumber])),'') AS NVARCHAR(100))),','
										  ,UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[MerchantLegalName])),'') AS NVARCHAR(100))),','
										  ,UPPER(CAST(ISNULL(REPLACE(LTRIM(RTRIM(t.[MerchantName])),'REV:',''),'') AS NVARCHAR(100))),','
										  ,UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[AddressLine01])),'') AS NVARCHAR(100))),','
										  ,UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[CityName])),'') AS NVARCHAR(100))),','
										  ,UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[StateProvince])),'') AS NVARCHAR(100))),','
										  ,UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[PostalCode])),'') AS NVARCHAR(100))),','
										  ,UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[CountryCode])),'') AS NVARCHAR(100)))) as NVARCHAR(1000)))
where dv.txndate >= '2024-10-01'  


-- Check what's left unmapped  


select iif(pr.clientcode is null, 0,1) as problemchild, th.*, fl.*   -
from  [DinSandbox].qa.[PosMonthlyFullMap]  p
inner join dbo.TransactionHeader th on th.id = p.Id
--inner join Staging.RawTransactionRecords r on r.id = th.id
inner join Lookups.BankTypes b on b.DisplayName = th.BankType
inner join lookups.creditcardtypes c on c.displayname = th.creditcardtype
inner join Staging.RecordFlags fl on fl.id = th.id
left join Staging.ProblemChildren pr on pr.ClientCode = th.ClientCode and pr.CreditCardTypeId = c.Id and pr.BankTypeId = b.Id
where p.PosFingerprintId is null
order by 1,fl.PosFingerprintId


-- Insert new POS Fingerprints

declare @startDate date = FORMAT(DATEADD(month,-1,GETDATE()),'yyyy-MM-01'),
		@endDate date = EOMONTH(DATEADD(month,-1,GETDATE())), 
		@logId int = (select max(logid) from Dinshared.finance.posfingerprints); 

INSERT INTO DinShared.finance.POSFingerprints (SimHash, SimHashNoMid, MerchantNumber, MerchantLegalName, MerchantName, AddressLine01,               
	  CityName, StateProvince, PostalCode, CountryCode, Certified, ClientCount,               
	  BankFeedCount, AnalysisStartDate, AnalysisEndDate,Quality, LogID, CreatedOn, CreatedBy, MappingLastAttempt)        
	  SELECT     
	  HASHBYTES('SHA1', CAST(CONCAT(UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[MerchantNumber])),'') AS NVARCHAR(100))),','
										  ,UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[MerchantLegalName])),'') AS NVARCHAR(100))),','
										  ,UPPER(CAST(ISNULL(REPLACE(LTRIM(RTRIM(t.[MerchantName])),'REV:',''),'') AS NVARCHAR(100))),','
										  ,UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[AddressLine01])),'') AS NVARCHAR(100))),','
										  ,UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[CityName])),'') AS NVARCHAR(100))),','
										  ,UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[StateProvince])),'') AS NVARCHAR(100))),','
										  ,UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[PostalCode])),'') AS NVARCHAR(100))),','
										  ,UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[CountryCode])),'') AS NVARCHAR(100)))) as NVARCHAR(1000))) simhash,

	 HASHBYTES('SHA1', CAST(CONCAT(UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[MerchantLegalName])),'') AS NVARCHAR(100))),','
								  ,UPPER(CAST(ISNULL(REPLACE(LTRIM(RTRIM(t.[MerchantName])),'REV:',''),'') AS NVARCHAR(100))),','
								  ,UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[AddressLine01])),'') AS NVARCHAR(100))),','
								  ,UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[CityName])),'') AS NVARCHAR(100))),','
								  ,UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[StateProvince])),'') AS NVARCHAR(100))),','
								  ,UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[PostalCode])),'') AS NVARCHAR(100))),','
								  ,UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[CountryCode])),'') AS NVARCHAR(100)))) as NVARCHAR(1000))) as simhashnomid,

	  IIF(LTRIM(RTRIM(t.merchantNumber)) = '',null,LTRIM(RTRIM(t.merchantNumber))) merchantNumber,  --dodeljujemo NULL vrednost polju merchantNumber u slucaju da je empty string, u suprotnom ostaje kako jeste 
	  IIF(LTRIM(RTRIM(t.merchantLegalName)) = '',null,LTRIM(RTRIM(t.merchantLegalName))) merchantLegalName,--ponavlja se isto za sva donja polja 
	  IIF(LTRIM(RTRIM( REPLACE(t.[MerchantName],'REV:',''))) = '',null,LTRIM(RTRIM( REPLACE(t.[MerchantName],'REV:','')))) [MerchantName],
	  IIF(LTRIM(RTRIM(t.addressLine01)) = '',null,LTRIM(RTRIM(t.addressLine01))) addressLine01,
	  IIF(LTRIM(RTRIM(t.cityName)) = '',null,LTRIM(RTRIM(t.cityName))) cityName,
	  IIF(LTRIM(RTRIM(t.StateProvince)) = '',null,LTRIM(RTRIM(t.StateProvince))) StateProvince,
	  IIF(LTRIM(RTRIM(t.PostalCode)) = '',null,LTRIM(RTRIM(t.PostalCode))) PostalCode,
	  IIF(LTRIM(RTRIM(t.countrycode)) = '',null,LTRIM(RTRIM(t.countrycode))) countrycode,
	  'N' as Certified,        COUNT(DISTINCT t.clientcode) AS ClientCount,        
	  1 AS BankFeedCount,@startDate as validfrom, @endDate as validto,case when t.clientcode = 'mc' then 5 else 1 end as quality ,@logId as logid,GETUTCDATE(), 'System',
	  
	  MAX(t.TransactionDate) as LastTransactionDate
	  	  FROM dbo.TransactionHeader t  
	  LEFT OUTER JOIN DinShared.finance.POSFingerprints fin 
	  ON  fin.SimHash = HASHBYTES('SHA1', CAST(CONCAT(UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[MerchantNumber])),'') AS NVARCHAR(100))),','
										  ,UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[MerchantLegalName])),'') AS NVARCHAR(100))),','
										  ,UPPER(CAST(ISNULL(REPLACE(LTRIM(RTRIM(t.[MerchantName])),'REV:',''),'') AS NVARCHAR(100))),','
										  ,UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[AddressLine01])),'') AS NVARCHAR(100))),','
										  ,UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[CityName])),'') AS NVARCHAR(100))),','
										  ,UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[StateProvince])),'') AS NVARCHAR(100))),','
										  ,UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[PostalCode])),'') AS NVARCHAR(100))),','
										  ,UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[CountryCode])),'') AS NVARCHAR(100)))) as NVARCHAR(1000)))              
		  WHERE 1 = 1      AND t.transactiondate BETWEEN @startDate AND @endDate    
		  AND ( (t.countrycode IN ('US', 'PR', 'VI', 'GU', 'AS', 'MP', 'PW', 'UM', 'CA')           
		   AND t.MccCode IN ('5814', '5813', '5812', '5811', '5499', '5462', '5441')
		   )
		    or t.ClientCode ='MC'
		   )
		   AND fin.FingerprintID IS NULL
		   -- and t.ClientCode ='MC'
		   --and p.PosFingerprintId IS NULL    
		   AND ( t.MerchantName IS NOT NULL or t.MerchantLegalName IS NOT NULL )        
		   GROUP BY  IIF(LTRIM(RTRIM(t.merchantNumber)) = '',null,LTRIM(RTRIM(t.merchantNumber))),
	  IIF(LTRIM(RTRIM(t.merchantLegalName)) = '',null,LTRIM(RTRIM(t.merchantLegalName))),
	  IIF(LTRIM(RTRIM( REPLACE(t.[MerchantName],'REV:',''))) = '',null,LTRIM(RTRIM( REPLACE(t.[MerchantName],'REV:','')))),
	  IIF(LTRIM(RTRIM(t.addressLine01)) = '',null,LTRIM(RTRIM(t.addressLine01))),
	  IIF(LTRIM(RTRIM(t.cityName)) = '',null,LTRIM(RTRIM(t.cityName))),
	  IIF(LTRIM(RTRIM(t.StateProvince)) = '',null,LTRIM(RTRIM(t.StateProvince))),
	  IIF(LTRIM(RTRIM(t.PostalCode)) = '',null,LTRIM(RTRIM(t.PostalCode))),
	  IIF(LTRIM(RTRIM(t.countrycode)) = '',null,LTRIM(RTRIM(t.countrycode))),
	  
	  HASHBYTES('SHA1', CAST(CONCAT(UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[MerchantNumber])),'') AS NVARCHAR(100))),','
										  ,UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[MerchantLegalName])),'') AS NVARCHAR(100))),','
										  ,UPPER(CAST(ISNULL(REPLACE(LTRIM(RTRIM(t.[MerchantName])),'REV:',''),'') AS NVARCHAR(100))),','
										  ,UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[AddressLine01])),'') AS NVARCHAR(100))),','
										  ,UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[CityName])),'') AS NVARCHAR(100))),','
										  ,UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[StateProvince])),'') AS NVARCHAR(100))),','
										  ,UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[PostalCode])),'') AS NVARCHAR(100))),','
										  ,UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[CountryCode])),'') AS NVARCHAR(100)))) as NVARCHAR(1000))),

	 HASHBYTES('SHA1', CAST(CONCAT(UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[MerchantLegalName])),'') AS NVARCHAR(100))),','
								  ,UPPER(CAST(ISNULL(REPLACE(LTRIM(RTRIM(t.[MerchantName])),'REV:',''),'') AS NVARCHAR(100))),','
								  ,UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[AddressLine01])),'') AS NVARCHAR(100))),','
								  ,UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[CityName])),'') AS NVARCHAR(100))),','
								  ,UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[StateProvince])),'') AS NVARCHAR(100))),','
								  ,UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[PostalCode])),'') AS NVARCHAR(100))),','
								  ,UPPER(CAST(ISNULL(LTRIM(RTRIM(t.[CountryCode])),'') AS NVARCHAR(100)))) as NVARCHAR(1000))),
	case when t.clientcode = 'mc' then 5 else 1 end

-- Update and Map Patron Dimensions


--Update Patron Dimension
 
exec dbo.UpdateMapDimPatron;    


use [DinDWHS];  


--Unique Patron

declare @date date = dateadd(month,-1,getutcdate());
declare @startDate date = format(@date, 'yyyy-MM-01');
declare @endDate date = eomonth(@startDate);
declare @startDateInt int = cast(format(@date, 'yyyyMM01') as int);
declare @endDateInt int = cast(format(eomonth(@date), 'yyyyMMdd') as int);
select @startDate,@endDate, @startDateInt, @endDateInt;


--Insert/Update Amex ProxyId's 
insert into dbo.DimUniquePatron(proxyid, FirstName, Lastname, EmailAddress, ClientID, GeographyID, ZipCode, creditcardnum,myDinuserid)
select p.proxyid, p.FirstName, p.Lastname, p.EmailAddress, p.ClientID, p.GeographyID, p.ZipCode, p.CC_Last4Digits,p.myDinuserid
from dbo.dimpatron p
left join dbo.dimuniquepatron up on up.ProxyID = p.proxyid
where p.ProxyID not like '%[_]%'
and up.UniquePatronId is null 


update ft set ft.UniquePatronId = up.UniquePatronId
from dbo.FactTransaction ft
inner join dbo.DimPatron p on p.ID = ft.Patron_ID
inner join dbo.DimUniquePatron up on up.ProxyID = p.ProxyID
where ft.DateKey between @startDateInt and @endDateInt
and p.ProxyID not like '%[_]%'
and isnull(ft.UniquePatronId,0) = 0 


--Prepare Staging table with unique patrons based on employeeid


truncate table DinDWHSEtl.Staging.UniquePatronTransactionos;


insert  into DinDWHSEtl.Staging.UniquePatronTransactionos
  select distinct dv.id AS dvhdid, th.clientcode, th.employeeid, right(concat('0000',th.creditcardnum),4) creditcardnum
  from Dindb.Dinbilling.billing.detailvolumehistorydata dv
  inner join Dindb.Dinintake.dbo.transactionheader th on dv.transactionid = th.transactionid
  --left join dbo.dimuniquepatron up on up.proxyid = concat(th.clientcode, '_', th.employeeid, '_', th.creditcardnum)
  where 1=1
  and dv.txndate between @startDate and @endDate
  and th.employeeid is not null
  and th.proxyid is null	


  insert into dbo.DimUniquePatron(ProxyID, FirstName, Lastname, EmailAddress, ClientID, GeographyID, ZipCode, CreditCardNum, MyDinUserId)
  select distinct concat(uu.clientcode, '_', uu.employeeid, '_', uu.creditcardnum), 'Unknown','Unknown','Unknown', c.clientid,0,'Unknown', uu.creditcardnum, 1
  from DinDWHSEtl.Staging.UniquePatronTransactionos uu    
  inner join dbo.DimClient c on c.clientcode = uu.clientcode
  left join dbo.dimuniquepatron up on up.proxyid = concat(uu.clientcode, '_', uu.employeeid, '_', uu.creditcardnum)
  where up.UniquePatronId is null
  and nullif(uu.employeeid,'') is not null


--UniquePatron with employee id set
update ft set ft.UniquePatronId = up.UniquePatronId
from dbo.FactTransaction ft
inner join DinDWHSEtl.Staging.UniquePatronTransactionos uu on uu.dvhdid = ft.DVHD_ID 
inner join dbo.DimUniquePatron up on up.ProxyID = concat(uu.clientcode, '_', uu.employeeid, '_', uu.creditcardnum)
where  ft.DateKey between @startDateInt and @endDateInt
and isnull(ft.UniquePatronId,0) = 0 

--Update the rest 
update ft set ft.UniquePatronId = up.UniquePatronId
from dbo.FactTransaction ft
inner join dbo.DimPatron p on p.ID = ft.Patron_ID  
inner join dbo.DimUniquePatron up on up.ProxyID = p.ProxyID 				
where ft.DateKey between @startDateInt and @endDateInt
and isnull(ft.UniquePatronId,0) = 0 


declare @date date = dateadd(month,-1,getutcdate());
declare @startDate date = format(@date, 'yyyy-MM-01');
declare @endDate date = eomonth(@startDate);
declare @startDateInt int = cast(format(@date, 'yyyyMM01') as int);
declare @endDateInt int = cast(format(eomonth(@date), 'yyyyMMdd') as int);
--select @startDate,@endDate, @startDateInt, @endDateInt;

select count(*)
from dbo.FactTransaction 
where UniquePatronId is null 
--and Patron_Id is null
and DateKey between @startDateInt and @endDateInt


insert into dbo.dimuniquepatron(proxyid, FirstName, Lastname, EmailAddress, ClientID, GeographyID, ZipCode, creditcardnum, myDinuserid)
 select p.proxyid, p.FirstName, p.Lastname, p.EmailAddress, p.ClientID, p.GeographyID, p.ZipCode, p.CC_Last4Digits,p.myDinuserid
from dbo.dimpatron p
inner join dbo.FactTransaction ft on ft.Patron_ID = p.id  
left join dbo.dimuniquepatron up on up.ProxyID = p.proxyid  
where 1=1							  
--and p.ProxyID not like '%[_]%'
and up.UniquePatronId is null
and ft.DateKey between @startDateInt and @endDateInt
--and p.ProxyID not like '%[_]%'
and isnull(ft.UniquePatronId,0) = 0  
--Inserting Data into the Same Table Used in LEFT JOIN:


update ft set ft.UniquePatronId = up.UniquePatronId 
from dbo.FactTransaction ft
inner join dbo.DimPatron p on p.ID = ft.Patron_ID
inner join dbo.DimUniquePatron up on up.ProxyID = p.ProxyID 
where ft.DateKey between @startDateInt and @endDateInt
and isnull(ft.UniquePatronId,0) = 0



	

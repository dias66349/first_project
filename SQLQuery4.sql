ШУТКА ДЖОКЕРА
with dist_acts as (
  select
    a.Direct_Settlement,
    a.Regression,
    a.Type_affected_object,
    a.Document_Number,
    a.Recipient,
    a.GUID,
    a.Owner,
    a.Insurance_Product,
    a.Statement,
    a.Posting_Date,
    a.Line_Number,
	coalesce(a.Contract_number_culprit, '-11')  as Contract_number_culprit,
	coalesce(a.Affected_Object, '-11')  as Affected_Object,
    sum(a.Amount_Paid) over (partition by a.guid) as Amount_Paid,
    row_number() over ( partition by a.guid  order by  a.Line_Number desc ) as max_line_number
  from
    Insurance.dbo.insurance_acts a

),
dist_state as (
  select
    distinct s.GUID,
    s.Loss_Notice,
    s.Insurance_Product,
	s.Event_Date
  from
    Insurance.dbo.loss_statements s
) ,

base as (

select 
ac.GUID ,
ac.Contract_number_culprit,
ac.Document_Number,
ac.Posting_Date,
st.Insurance_Product,
st.event_Date,
ac.Type_affected_object,
    DATEADD(month, DATEDIFF(month, 0, dateadd(year, 1, ac.Posting_Date)), 0) AS StartOfMonth_2022,
	format(ac.Posting_Date , 'yyyy-MM') as yy_mm, 
ac.Amount_Paid,
ac.Affected_Object,
sum(ac.Amount_Paid) over (partition by ac.Contract_number_culprit , ac.Affected_Object , st.Event_Date) as Amount_Paid_total,
row_number() over ( partition by ac.Contract_number_culprit  ,ac.Affected_Object , st.Event_Date   order by  ac.Line_Number desc  ) as max_line_number_new
from
    dist_acts ac
    left join dist_state st on ac.Statement = st.GUID

  where
    1 = 1
	and ac.Insurance_Product = 'ОГПО авто'
    and ac.max_line_number = 1
	and ac.Type_affected_object='Транспортное средство'
    and ac.Posting_Date > '2023-01-01'
	--and  ac.Contract_number_culprit = '232CS181339A'

	)

	select 
	StartOfMonth_2022 ,
	yy_mm,
	sum(Amount_Paid) as Amount_Paid ,
	sum(case when max_line_number_new = 1 then Amount_Paid_total  end) as Amount_Paid_total ,
	count(distinct  Document_Number) as cnt ,
	count(case when max_line_number_new = 1 then 1  end ) as cnt_new
	from base 
	where Insurance_Product = 'ОГПО авто'
and Type_affected_object='Транспортное средство'
	group by StartOfMonth_2022 ,
	yy_mm
	order by StartOfMonth_2022 



	select * from Insurance.dbo.insurance_acts a
	where Statement='24531C3F-C3D0-11ED-B28F-005056A6B450'

with base as(
    select
        a.Statement
    from Insurance.dbo.insurance_acts a
	where a.Line_Number>1
),
temp as (
select a.*,
row_number() over (partition by a.guid order by a.Line_Number desc) as max_line_number
from Insurance.dbo.insurance_acts a
inner join base b on b.Statement=a.Statement 
)
select
format(temp.Posting_Date,'yyyy-MM') as yy_mm,
temp.GUID,
count(temp.GUID),
sum(temp.Amount_Paid)
	from temp
group by temp.GUID,
format(temp.Posting_Date,'yyyy-MM')
having count(temp.GUID)>3





with base as(
    select
        a.GUID
    from Insurance.dbo.insurance_acts a
	where a.Line_Number>1
)

select a.*,
row_number() over (partition by a.guid order by a.Line_Number desc) as max_line_number
from Insurance.dbo.insurance_acts a
inner join base b on b.GUID=a.GUID 


-- LINE NUMBER = 1 
select
        a.*
    from Insurance.dbo.insurance_acts a
where a.GUID='795C8D96-10FA-461D-A9AB-BE8A580AFEC2'


select
    a.*
from Insurance.dbo.insurance_acts a

---------------------------
-- LINE NUMBER > 1 

select
    a.*
    from Insurance.dbo.insurance_acts a
	where a.Line_Number>1
	order by a.GUID desc

-------------------------------------
with base as(
    select
        a.GUID
    from Insurance.dbo.insurance_acts a
	where a.Line_Number>1
),
temp as (
select a.*,
row_number() over (partition by a.guid order by a.Line_Number desc) as max_line_number
from Insurance.dbo.insurance_acts a
inner join base b on b.GUID=a.GUID 
)
select
format(temp.Posting_Date,'yyyy-MM') as yy_mm,
temp.GUID,
temp.max_line_number
	from temp;




with base as (
    select
        a.GUID ,count(a.GUID) cnt_guid
    from Insurance.dbo.insurance_acts a
	group by a.GUID
	having Count(a.GUID)>1
)

select  ac.GUID ,count(ac.GUID) cnt_guid,ac.Line_Number,COUNT(ac.Line_Number) as cnt_line_number from base b
left join Insurance.dbo.insurance_acts ac on ac.GUID=b.GUID
group by ac.GUID,ac.Line_Number
order by ac.GUID desc;



 select
        a.GUID ,count(a.GUID) cnt_guid,count(a.Line_Number)
    from Insurance.dbo.insurance_acts a
	group by a.GUID
	having Count(a.GUID)>1 and count(a.Line_Number)>Count(distinct a.Line_Number)


-------------------------------- 

select *
from Insurance.dbo.insurance_acts a
where a.GUID='E851479A-882D-4DA6-974F-46F37C47F3BF'

----------------------------------


select * from 
[dbo].[contracts] c
where c.uuid='E851479A-882D-4DA6-974F-46F37C47F3BF'


select * from Insurance.dbo.insurance_acts
where Line_Number is NULL or Line_Number=0

with base as (
    select
        a.GUID ,count(a.GUID) cnt_guid
    from Insurance.dbo.insurance_acts a
	group by a.GUID
	having Count(a.GUID)>1
)

select * from base b
left join Insurance.dbo.insurance_acts ac on ac.GUID=b.GUID
order by ac.GUID desc




select * from Insurance.dbo.insurance_acts
where Contract_number_culprit='0813221W140828W'

select * from [dbo].[contracts]  c 
--left join [dbo].[d_products] d on c.id_product= d.id
where c.contract_number='0813221W140828W'

select * from  [dbo].[loss_statements]
where Document_Number='23-003559'


-----------------------------------------------------------
select  ac.* 
from Insurance.dbo.insurance_acts ac
left join [dbo].[loss_statements] st on ac.Statement=st.GUID
left join [dbo].[loss_notices] ls on ls.GUID=st.Loss_Notice
where ac.Contract_number_culprit='0813221W140828W' 


--and ac.Document_Number='22-000327/1'


select * from [dbo].[loss_notices]
where Document_Number='GO0000000021900'

select * from [dbo].[loss_statements]
where Loss_Notice='7206AFA8-56DD-430B-8F37-280F8566C18E'
---------------------------------------------------------

select * from Insurance.dbo.insurance_acts ac
where ac.Document_Number = '23-003559/1'

22-GO-204-000002

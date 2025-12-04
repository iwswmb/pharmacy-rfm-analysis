select
	to_char(bonuscheques.datetime, 'YYYY-MM') as year_month,
	count(distinct bonuscheques.doc_id) as "Количество покупок"
from bonuscheques 
where true
    and bonuscheques.card like '2000%'
    and {{date}}
group by year_month
order by year_month;
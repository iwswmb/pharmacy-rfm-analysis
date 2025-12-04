with client_rfm_metrics as (
	select
		bonuscheques.card,
		max(max(bonuscheques.datetime::date)) over() - max(bonuscheques.datetime::date) as recency,
		count(distinct bonuscheques.doc_id) as frequency,
		sum(bonuscheques.summ_with_disc) as monetary
	from bonuscheques  
	where true
	    and bonuscheques.card like '2000%'
	    and {{date}}
	group by bonuscheques.card
),
rfm_percentile_thresholds as (
	select
		percentile_disc(0.2) within group(order by crm.recency) as percentile_20_recency,
		percentile_disc(0.51) within group(order by crm.recency) as percentile_51_recency,
		percentile_disc(0.5) within group(order by crm.frequency) as percentile_50_frequency,
		percentile_disc(0.85) within group(order by crm.frequency) as percentile_85_frequency,
		percentile_disc(0.67) within group(order by crm.monetary) as percentile_67_monetary,
		percentile_disc(0.9) within group(order by crm.monetary) as percentile_90_monetary
	from client_rfm_metrics crm
),
client_rfm_groups as (
	select
		crm.card,
		crm.recency,
		crm.frequency,
		crm.monetary,
		case
			when crm.recency > rpt.percentile_51_recency then '3'
			when crm.recency > rpt.percentile_20_recency then '2'
			else '1'
		end as recency_group,
		case
			when crm.frequency <= rpt.percentile_50_frequency then '3'
			when crm.frequency <= rpt.percentile_85_frequency then '2'
			else '1'
		end as frequency_group,
		case
			when crm.monetary <= rpt.percentile_67_monetary then '3'
			when crm.monetary <= rpt.percentile_90_monetary then '2'
			else '1'
		end as monetary_group
	from client_rfm_metrics crm
	cross join rfm_percentile_thresholds rpt
),
rfm_segments as (
    select
        crg.card,
    	concat(crg.recency_group, crg.frequency_group, crg.monetary_group) as segment
    from client_rfm_groups crg
),
loyal_groups as (
    select
        rs.card,
        case
            when rs.segment in ('213', '212', '211', '113', '112') then 'Лояльные'
            when rs.segment = '111' then 'VIP'
        end as rfm_group
    from rfm_segments rs
    where rs.segment = '111' or rs.segment in ('213', '212', '211', '113', '112')
),
shops as (
    select
        bonuscheques.shop,
        bonuscheques.card
    from bonuscheques 
    where true
        and bonuscheques.card like '2000%'
        and {{date}}
    group by bonuscheques.shop, bonuscheques.card
)
select 
    s.shop as "Аптека",
    count(lg.card) as "Количество лояльных клиентов"
from shops s
left join loyal_groups lg
    on s.card = lg.card
group by s.shop
order by count(lg.card) desc;
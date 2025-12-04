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
		crg.frequency,
		crg.monetary,
    	concat(crg.recency_group, crg.frequency_group, crg.monetary_group) as segment
    from client_rfm_groups crg
)
select
    case
        when rs.segment in ('333', '332', '331', '323', '233', '232') then 'Не тратим много сил'
        when rs.segment in ('322', '321', '313', '312', '311') then 'Нужно возвращать'
        when rs.segment in ('231', '223', '222', '133', '132', '123') then 'Есть потенциал'
        when rs.segment in ('221', '131', '122', '121') then 'Очень перспективные'
        when rs.segment in ('213', '212', '211', '113', '112') then 'Лояльные'
        when rs.segment = '111' then 'VIP'
    end as "Группа",
    sum(rs.frequency) as "Количество покупок",
    sum(rs.monetary) as "Сумма покупок"
from rfm_segments rs
group by "Группа";
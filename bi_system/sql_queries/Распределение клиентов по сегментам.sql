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
    	concat(crg.recency_group, crg.frequency_group, crg.monetary_group) as segment,
    	count(*) as client_cnt
    from client_rfm_groups crg
    group by segment
    order by segment
)
select
    case
        when rs.segment like '33_' then 'Давние редкие'
        when rs.segment like '32_' then 'Давние непостоянные'
        when rs.segment like '31_' then 'Давние постоянные'
        when rs.segment like '23_' then 'Недавние редкие'
        when rs.segment like '22_' then 'Недавние непостоянные'
        when rs.segment like '21_' then 'Недавние постоянные'
        when rs.segment like '13_' then 'Свежие редкие'
        when rs.segment like '12_' then 'Свежие непостоянные'
        when rs.segment like '11_' then 'Свежие постоянные'
    end as "Сегмент",
    sum(rs.client_cnt) as "Количество клиентов"
from rfm_segments rs
group by "Сегмент"
order by "Количество клиентов" desc;
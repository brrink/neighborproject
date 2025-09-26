
-- search to click through rate by attribution source and channel
with search_to_click as (
select e.*,
case
	when c.search_id is null then 'no'
	else 'yes'
	end as has_click

from all_search_events e 
	left join view_listing_detail_events c on e.search_id = c.search_id
)

select 
	search_term_category,
	count(distinct case when has_click = 'yes' then search_id end) / count(distinct search_id) as search_to_clic
from search_to_click
group by 1;

-- type	    0.4722
-- amenity	0.4748
-- generic	0.4759
-- location	0.4777


select 
	first_attribution_source,
	first_attribution_channel,
	count(distinct case when has_click = 'yes' then search_id end) / count(distinct search_id) as search_to_clic
from search_to_click
group by 1,2; 


-- direct	direct	    0.4767
-- organic	google	    0.4737
-- organic	google maps	0.4803
-- paid	    facebook	0.4862
-- paid	    google ads	0.4718
-- paid	    reddit	    0.4674

-- full funnel conversion by attribution source and channel
with searches as (
	select 
		search_id,
		merged_amplitude_id,
		first_attribution_source,
		first_attribution_channel,
		cast(event_date as date) as event_date
	from all_search_events
	where 1=1
	),

clicks as (
	select 
		search_id,
		merged_amplitude_id,
		listing_id, 
		cast(event_date as date) as event_date,
		count(event_uuid) as click_count
		from view_listing_detail_events
		where 1=1
	group by 1,2,3,4
	),

reservations as (
	select 
		reservation_id,
		listing_id,
		cast(created_at as date) as created_at,
		cast(approved_at as date) as approved_at,
		cast(successful_payment_collected_at as date) as successful_payment_collected_at
	from reservations 
	where 1=1
	),

funnel as (
	select 
		s.search_id,
		s.first_attribution_source,
		s.first_attribution_channel,
		s.event_date as search_date,
		c.listing_id,
		c.event_date as click_date,
		c.click_count,
		r.reservation_id,
		r.created_at,
		r.approved_at,
		r.successful_payment_collected_at
	from searches s 
		left join clicks c on s.search_id = c.search_id and s.merged_amplitude_id = c.merged_amplitude_id
		left join reservations r on s.event_date = r.created_at and c.listing_id = r.listing_id	
		)
-- takes funnel cte and aggregates to calculate conversion rates
select
    first_attribution_source,
    first_attribution_channel,

    count(distinct search_id) as total_unique_searches, -- count all unique search sessions for the channel
    count(distinct case
        		   	when successful_payment_collected_at is not null 
        			then search_id
        		   else null
    				end) as successful_payments,
    (
        count(distinct case -- calculate the conversion rate percentage
            when successful_payment_collected_at is not null
            then search_id
            else null
        end) / count(distinct search_id)
    ) * 100 as conversion_rate

from
    funnel
group by 1,2
order by 5 desc;
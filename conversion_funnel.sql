
-- conversion_funnel 

with searches as (
    select
        search_id,
        merged_amplitude_id,
        first_attribution_source,
        first_attribution_channel,
        search_type,
        search_term,
        search_term_category,
        cast(event_date as date) as search_event_date,
        cast(event_time as datetime) as search_event_time
    from
        all_search_events
),


clicks as (
    select
        search_id,
        merged_amplitude_id,
        listing_id,
        cast(event_time as datetime) as click_event_time,
        cast(event_date as date) as click_event_date,
        count(event_uuid) as click_count
    from
        view_listing_detail_events
    group by
        search_id,
        merged_amplitude_id,
        listing_id,
        click_event_time,
        click_event_date
),


reservations as (
    select
        reservation_id,
        listing_id,
        cast(created_at as date) as created_at,
        cast(created_at as datetime) as created_at_time,
        cast(approved_at as date) as approved_at,
        cast(approved_at as datetime) as approved_at_time,
        cast(successful_payment_collected_at as date) as successful_payment_collected_at,
        cast(successful_payment_collected_at as datetime) as successful_payment_collected_at_time
    from
        reservations
),


funnel as (
    select
        s.search_id,
        s.first_attribution_source,
        s.first_attribution_channel,
        s.search_type,
        s.search_term,
        s.search_term_category,
        s.search_event_date as search_date,
        s.search_event_time as search_time,
        c.listing_id,
        c.click_event_date as click_date,
        c.click_event_time as click_time,
        c.click_count,
        r.reservation_id,
        r.created_at,
        r.approved_at,
        r.approved_at_time,
        r.successful_payment_collected_at,
        r.successful_payment_collected_at_time
    from
        searches s
        left join clicks c on s.search_id = c.search_id and s.merged_amplitude_id = c.merged_amplitude_id
        left join reservations r on s.search_event_date = r.created_at and c.listing_id = r.listing_id
)


-- BELOW ARE QUERIES BASED ON FINAL funnel CTE FROM THE ABOVE QUERY


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

-- search to click through rate by attribution channel

select
    first_attribution_channel,
    count(distinct search_id) as total_searches,
    count(distinct case when click_date is not null then search_id else null end) as searches_with_click,
    (count(distinct case when click_date is not null then search_id else null end) * 100.0 / count(distinct search_id)) as search_to_click_rate_percent
from
    funnel
group by
    first_attribution_channel
order by
    search_to_click_rate_percent desc;

-- click to reservation rate by attribution channel

select
    first_attribution_channel,
    count(distinct case when click_date is not null then search_id else null end) as searches_with_click,
    count(distinct case when click_date is not null and successful_payment_collected_at is not null then search_id else null end) as reservations_from_click,
    (count(distinct case when click_date is not null and successful_payment_collected_at is not null then search_id else null end) * 100.0 / count(distinct case when click_date is not null then search_id else null end)) as click_to_reservation_rate_percent
from
    funnel
group by
    first_attribution_channel
order by
    click_to_reservation_rate_percent desc;

-- ADDITIONAL ANALYSIS BELOW - NOT PART OF FINAL QUERY

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

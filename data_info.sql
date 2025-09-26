-- view_listing_details_events
select -- even distribution
click_dma,
count(search_id)
from view_listing_detail_events
group by 1;

select -- paid is double everything else
first_attribution_source,
count(search_id)
from view_listing_detail_events
group by 1;

	/* 
	paid	44352
	organic	22081
	direct	22200 
	*/


select 
first_attribution_channel,
count(search_id)
from view_listing_detail_events
group by 1;

	/* 
	google ads	30925
	direct	22200
	google	16213
	facebook	9082
	google maps	5868
	reddit	4345 
	*/


-- all_search_events

select -- this results in an even distribution across all terms (likely a data generation anomoly)
search_term,
count(search_id) as search_count
from all_search_events
group by 1;


select -- this results in an even distribution across all terms (likely a data generation anomoly)
search_type,
count(search_id) as search_count
from all_search_events
group by 1;

select -- this results in an even distribution across all terms (likely a data generation anomoly)
search_term_category,
count(search_id) as search_count
from all_search_events
group by 1;

select -- this results in an even distribution across all terms (likely a data generation anomoly)
search_dma,
count(search_id) as search_count
from all_search_events
group by 1;

select -- paid is double organic or direct
first_attribution_source,
count(search_id) as search_count
from all_search_events
group by 1;

	/* 
	paid	49929
	organic	25158
	direct	24913 
	*/

select 
first_attribution_channel,
count(search_id) as search_count
from all_search_events
group by 1;

	/* 
	google ads	35037
	direct	24913
	google	18547
	facebook	9913
	google maps	6611
	reddit	4979 
	*/

-- reservations
select 
host_user_id, -- even distribution
count(reservation_id) as reservation_count
from reservations
group by 1 
;


select 
dma, -- even distribution
count(reservation_id) as reservation_count
from reservations
group by 1 
;

	/* 
	San Francisco	1029
	Boston	1043
	Philadelphia	1047
	Washington DC	984
	Dallas-Ft. Worth	1003
	Houston	1052
	Chicago	1024
	Atlanta	1085
	New York	1043
	Los Angeles	1053
	 */

-- Indexes for perfomance in MySQL

alter table all_search_events add index search_id (search_id);
alter table view_listing_detail_events add index search_id (search_id);
alter table view_listing_detail_events add index listing_id (listing_id);
alter table reservations add index listing_id (listing_id);
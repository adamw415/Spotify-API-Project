--longer songs tend to be less popular
SELECT duration_category
,floor(avg(popularity)) as avg_popularity
,round(sum(popular_ind)/count(distinct t.track_id) *100,2) || '%' as percent_popular
FROM `lithe-optics-373318.SPOTIFY_API.TRACKS` t 
left join `lithe-optics-373318.SPOTIFY_API.POPULARITY` p
on t.track_id = p.track_id
group by 1;

--the vast majority of releases are on Friday
with tracks as(
  select *, extract(DAYOFWEEK from cast(release_date as date)) as day_of_week 
  from `lithe-optics-373318.SPOTIFY_API.TRACKS`
)

select 
  CASE 
    WHEN day_of_week = 1 THEN 'Sunday'
    WHEN day_of_week = 2 THEN 'Monday'
    WHEN day_of_week = 3 THEN 'Tuesday'
    WHEN day_of_week = 4 THEN 'Wednesday'
    WHEN day_of_week = 5 THEN 'Thursday'    
    WHEN day_of_week = 6 THEN 'Friday'
    WHEN day_of_week = 7 THEN 'Saturday'
  END as day_of_week
  ,count(t.track_id) as release_cnt
  ,round(count(t.track_id)/(select count(track_id) from `lithe-optics-373318.SPOTIFY_API.TRACKS`),2) * 100 || '%' as release_percent_by_day_of_week
from tracks t
group by 1;

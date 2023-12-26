-- inserting distinct teams inside team table
insert into cricket.consumption.team_dim (team_name)
select distinct team_name from (
    select first_team as team_name from cricket.clean.match_table_clean
    union all 
    select second_team as team_name from cricket.clean.match_table_clean
) order by team_name;


select * from cricket.consumption.team_dim;

-- inserting into player dimension table
insert into cricket.consumption.player_dim(team_id, player_name)
select b.team_id, a.player_name
from 
    cricket.clean.players_clean_tbl a join cricket.consumption.team_dim b 
    on a.country = b.team_name
group by 
    b.team_id,
    a.player_name;

--validating data 
select * from cricket.consumption.player_dim;

--inserting into the venue dimension table
Insert into cricket.consumption.venue_dim(venue_name, city)
select 
    venue, city 
from (
    select
        venue,
        case when city is null then 'NA'
        else city 
        end as city 
    from cricket.clean.match_table_clean
    )
group by 
    venue,
    city; 
    
--validating data
select * from cricket.consumption.venue_dim;


-- inserting match type
insert into cricket.consumption.match_type_dim(match_type)
select match_type from cricket.clean.match_table_clean group by match_type;


insert into cricket.consumption.match_fact 
select 
    m.match_type_number as match_id,
    dd.date_id as date_id,
    0 as referee_id,
    ftd.team_id as first_team_id,
    std.team_id as second_team_id,
    mtd.match_type_id as match_type_id,
    vd.venue_id as venue_id,
    50 as total_overs,
    6 as balls_per_overs,
    max(case when d.team_name = m.first_team then  d.over else 0 end ) as OVERS_PLAYED_BY_TEAM_A,
    sum(case when d.team_name = m.first_team then  1 else 0 end ) as balls_PLAYED_BY_TEAM_A,
    sum(case when d.team_name = m.first_team then  d.extras else 0 end ) as extra_balls_PLAYED_BY_TEAM_A,
    sum(case when d.team_name = m.first_team then  d.extra_runs else 0 end ) as extra_runs_scored_BY_TEAM_A,
    0 fours_by_team_a,
    0 sixes_by_team_a,
    (sum(case when d.team_name = m.first_team then  d.runs else 0 end ) + sum(case when d.team_name = m.first_team then  d.extra_runs else 0 end ) ) as total_runs_scored_BY_TEAM_A,
    sum(case when d.team_name = m.first_team and player_out is not null then  1 else 0 end ) as wicket_lost_by_team_a,    
    
    max(case when d.team_name = m.second_team then  d.over else 0 end ) as OVERS_PLAYED_BY_TEAM_B,
    sum(case when d.team_name = m.second_team then  1 else 0 end ) as balls_PLAYED_BY_TEAM_B,
    sum(case when d.team_name = m.second_team then  d.extras else 0 end ) as extra_balls_PLAYED_BY_TEAM_B,
    sum(case when d.team_name = m.second_team then  d.extra_runs else 0 end ) as extra_runs_scored_BY_TEAM_B,
    0 fours_by_team_b,
    0 sixes_by_team_b,
    (sum(case when d.team_name = m.second_team then  d.runs else 0 end ) + sum(case when d.team_name = m.second_team then  d.extra_runs else 0 end ) ) as total_runs_scored_BY_TEAM_B,
    sum(case when d.team_name = m.second_team and player_out is not null then  1 else 0 end ) as wicket_lost_by_team_b,
    tw.team_id as toss_winner_team_id,
    m.toss_decision as toss_decision,
    m.match_result as match_result,
    mw.team_id as winner_team_id
     
from 
    cricket.clean.match_table_clean m
    join date_dim dd on m.event_date = dd.full_date
    join team_dim ftd on m.first_team = ftd.team_name 
    join team_dim std on m.second_team = std.team_name 
    join match_type_dim mtd on m.match_type = mtd.match_type
    join venue_dim vd on m.venue = vd.venue_name and m.city = vd.city
    join cricket.clean.delivery_clean_tbl d  on d.match_type_number = m.match_type_number 
    join team_dim tw on m.toss_winner = tw.team_name 
    join team_dim mw on m.winner= mw.team_name 
    
    group by
        m.match_type_number,
        date_id,
        referee_id,
        first_team_id,
        second_team_id,
        match_type_id,
        venue_id,
        total_overs,
        toss_winner_team_id,
        toss_decision,
        match_result,
        winner_team_id
        ;

-- validating data
select * from     cricket.clean.delivery_clean_tbl;


-- loading delivery data table
insert into delivery_fact 
select 
    d.match_type_number as match_id,
    td.team_id,
    bpd.player_id as bowler_id,
    spd.player_id as batter_id,
    nspd.player_id as non_stricker_id,
    d.over,
    d.runs, 
    -- 
    case when d.extra_runs is null then 0 else d.extra_runs end as extra_runs,
    case when d.extra_type is null then 'None' else d.extra_type end as extra_type,
    case when d.player_out is null then 'None' else d.player_out end as player_out,
    case when d.player_out_kind is null then 'None' else d.player_out_kind end as player_out_kind
from 
    cricket.clean.delivery_clean_tbl d 
    join team_dim td on d.team_name = td.team_name
    join player_dim bpd on d.bowler = bpd.player_name
    join player_dim spd on d.batter = spd.player_name
    join player_dim nspd on d.non_stricker = nspd.player_name ;


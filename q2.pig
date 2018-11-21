RUN /vol/automed/data/uscensus1990/load_tables.pig

states_with_counties = 
    JOIN state BY code,
    county BY state_code;

states_with_counties_group =
    GROUP states_with_counties
    BY state::name;

states_stats =
    FOREACH states_with_counties_group
    GENERATE group AS state_name,
        SUM(states_with_counties.population),
        SUM(states_with_counties.land_area);

states_stats_ordered = 
    ORDER states_stats
    BY state_name;

STORE states_stats_ordered INTO 'q2' USING PigStorage(',');
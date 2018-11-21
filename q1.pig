RUN /vol/automed/data/uscensus1990/load_tables.pig

states_and_counties = 
    JOIN state BY code LEFT,
    county BY state_code;

states_without_county =
    FILTER states_and_counties
    BY state_code IS NULL;

states_without_county_project = 
    FOREACH states_without_county
    GENERATE state::name AS state_name;

states_without_county_ordered = 
    ORDER states_without_county_project
    BY state_name;

STORE states_without_county_ordered INTO 'q1' USING PigStorage(',');
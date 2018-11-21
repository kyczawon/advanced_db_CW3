RUN /vol/automed/data/uscensus1990/load_tables.pig

states_with_places = 
    JOIN state BY code,
    place BY state_code;

states_with_cities =
    FILTER states_with_places
    BY type=='city';

states_with_cities_project =
    FOREACH states_with_cities
    GENERATE state::name AS state_name, place::name AS city, population;

states_with_cities_group =
    GROUP states_with_cities_project
    BY state_name;

states_biggest_cities =
    FOREACH states_with_cities_group {
        sorted = ORDER states_with_cities_project BY population desc;
        top    = limit sorted 5;
        GENERATE FLATTEN(top);
    }
    
states_biggest_cities_ordered = 
    ORDER states_biggest_cities
    BY state_name;

STORE states_biggest_cities_ordered INTO 'q4' USING PigStorage(',');
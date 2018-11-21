RUN /vol/automed/data/uscensus1990/load_tables.pig

states_with_places = 
    JOIN state BY code,
    place BY state_code;

states_with_places_group =
    GROUP states_with_places
    BY state::name;

states_cities_towns_villages =
    FOREACH states_with_places_group {
        cities =
            FILTER states_with_places
            BY type=='city';
        towns =
            FILTER states_with_places
            BY type=='town';
        villages =
            FILTER states_with_places
            BY type=='village';

        GENERATE group AS state_name,
            COUNT(cities) AS no_city,
            COUNT(towns) AS no_town,
            COUNT(villages) AS no_village;
    }
    
states_cities_towns_village_ordered = 
    ORDER states_cities_towns_villages
    BY state_name;

STORE states_cities_towns_village_ordered INTO 'q3' USING PigStorage(',');
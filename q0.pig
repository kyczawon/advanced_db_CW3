-- Load the state, county, mcd, place and zip
RUN /vol/automed/data/uscensus1990/load_tables.pig

-- Information about county population density
county_density =
    FOREACH county
    GENERATE state_code,
             name AS county_name,
             ROUND(10.0*population/land_area)/10.0 AS density;

-- Counties with high population density
high_density_county =
    FILTER county_density
    BY density>1.0;

-- Find the counties of all states, but include states without any counties
state_and_county =
    JOIN state BY code LEFT OUTER, 
         high_density_county BY state_code;

-- Project just those columns necessary for the query
state_and_county_projected =
    FOREACH state_and_county
    GENERATE name AS state_name,
             county_name,
             density;

-- Sort by state name
state_and_county_ordered =
    ORDER state_and_county_projected
    BY state_name,
       county_name;

STORE state_and_county_ordered INTO 'q0' USING PigStorage(',');

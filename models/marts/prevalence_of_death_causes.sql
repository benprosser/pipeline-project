select 
    most_fatal_cause as cause_of_death,
    COUNT(DISTINCT country_terrain) as number_of_nations
from
    {{ ref('most_common_death_causes_by_country') }}
group by 
    most_fatal_cause
order by
    number_of_nations DESC
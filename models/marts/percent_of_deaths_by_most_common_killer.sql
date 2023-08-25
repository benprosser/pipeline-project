SELECT
    tc.country_terrain AS country,
    tc.total_deaths AS total_deaths,
    nk.most_fatal_cause AS number_one_killer,
    nk.max_death_count AS number_one_killer_deaths,
    (nk.max_death_count / tc.total_deaths) * 100 AS percentage_of_total_deaths
FROM
    {{ ref('total_deaths_per_nation') }} tc
JOIN
    {{ ref('most_common_death_causes_by_country') }} nk
ON
    tc.country_terrain = nk.country_terrain

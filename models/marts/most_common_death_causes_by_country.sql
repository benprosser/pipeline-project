SELECT 
    dc.country_terrain,
    CASE
        WHEN dc.acute_hepatitis = max_death_count THEN 'acute_hepatitis'
        WHEN dc.alcohol_use_disorders = max_death_count THEN 'alcohol_use_disorders'
        WHEN dc.alzheimers_dementias = max_death_count THEN 'alzheimers_dementias'
        WHEN dc.cardiovascular_diseases = max_death_count THEN 'cardiovascular_diseases'
        WHEN dc.conflict_terrorismum = max_death_count THEN 'conflict_terrorismum'
        WHEN dc.diabetes = max_death_count THEN 'diabetes'
        WHEN dc.diarrheal_diseases = max_death_count THEN 'diarrheal_diseases'
        WHEN dc.digestive_diseases = max_death_count THEN 'digestive_diseases'
        WHEN dc.drowning = max_death_count THEN 'drowning'
        WHEN dc.tuberculosis = max_death_count THEN 'tuberculosis'
        WHEN dc.terrorism_deaths = max_death_count THEN 'terrorism_deaths'
        WHEN dc.self_harm = max_death_count THEN 'self_harm'
        WHEN dc.respiratory_diseaseses = max_death_count THEN 'respiratory_diseaseses'
        WHEN dc.protein_energy_malnutrition = max_death_count THEN 'protein_energy_malnutrition'
        WHEN dc.road_injuries = max_death_count THEN 'road_injuries'
        WHEN dc.poisonings = max_death_count THEN 'poisonings'
        WHEN dc.parkinson = max_death_count THEN 'parkinson'
        WHEN dc.nutritional_deficiencies = max_death_count THEN 'nutritional_deficiencies'
        WHEN dc.neonatal_disorders = max_death_count THEN 'neonatal_disorders'
        WHEN dc.fire_hot_substances = max_death_count THEN 'fire_hot_substances'
        WHEN dc.neoplasms = max_death_count THEN 'neoplasms'
        WHEN dc.meningitis = max_death_count THEN 'meningitis'
        WHEN dc.maternal_disorders = max_death_count THEN 'maternal_disorders'
        WHEN dc.malaria = max_death_count THEN 'malaria'
        WHEN dc.hiv_aids = max_death_count THEN 'hiv_aids'
        WHEN dc.drug_use_disorders = max_death_count THEN 'drug_use_disorders'
        WHEN dc.environmental_heat_and_cold_exposure = max_death_count THEN 'environmental_heat_and_cold_exposure'
        WHEN dc.exposure_of_nature = max_death_count THEN 'exposure_of_nature'
        WHEN dc.lower_respiratory_infectionses = max_death_count THEN 'lower_respiratory_infectionses'
        WHEN dc.liver_diseases = max_death_count THEN 'liver_diseases'
        WHEN dc.kidney_disease = max_death_count THEN 'kidney_disease'
        WHEN dc.interpersonal_violence = max_death_count THEN 'interpersonal_violence'
    END AS most_fatal_cause,
    max_death_count AS max_death_count
FROM (
    SELECT 
        dc1.country_terrain,
        GREATEST(
            dc1.acute_hepatitis,
            dc1.alcohol_use_disorders,
            dc1.alzheimers_dementias,
            dc1.cardiovascular_diseases,
            dc1.conflict_terrorismum,
            dc1.diabetes,
            dc1.diarrheal_diseases,
            dc1.digestive_diseases,
            dc1.drowning,
            dc1.tuberculosis,
            dc1.terrorism_deaths,
            dc1.self_harm,
            dc1.respiratory_diseaseses,
            dc1.protein_energy_malnutrition,
            dc1.road_injuries,
            dc1.poisonings,
            dc1.parkinson,
            dc1.nutritional_deficiencies,
            dc1.neonatal_disorders,
            dc1.fire_hot_substances,
            dc1.neoplasms,
            dc1.meningitis,
            dc1.maternal_disorders,
            dc1.malaria,
            dc1.hiv_aids,
            dc1.drug_use_disorders,
            dc1.environmental_heat_and_cold_exposure,
            dc1.exposure_of_nature,
            dc1.lower_respiratory_infectionses,
            dc1.liver_diseases,
            dc1.kidney_disease,
            dc1.interpersonal_violence
        ) AS max_death_count
    FROM {{ source('project', 'death_causes') }} as dc1
) AS max_death_cause
JOIN {{ source('project', 'death_causes') }} AS dc ON dc.country_terrain = max_death_cause.country_terrain
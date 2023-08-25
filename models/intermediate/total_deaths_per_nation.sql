SELECT
    country_terrain,
    SUM(
        acute_hepatitis +
        alcohol_use_disorders +
        alzheimers_dementias +
        cardiovascular_diseases +
        conflict_terrorismum +
        diabetes +
        diarrheal_diseases +
        digestive_diseases +
        drowning +
        tuberculosis +
        terrorism_deaths +
        self_harm +
        respiratory_diseaseses +
        protein_energy_malnutrition +
        road_injuries +
        poisonings +
        parkinson +
        nutritional_deficiencies +
        neonatal_disorders +
        fire_hot_substances +
        neoplasms +
        meningitis +
        maternal_disorders +
        malaria +
        hiv_aids +
        drug_use_disorders +
        environmental_heat_and_cold_exposure +
        exposure_of_nature +
        lower_respiratory_infectionses +
        liver_diseases +
        kidney_disease +
        interpersonal_violence
    ) AS total_deaths
FROM
    {{ source('project', 'death_causes') }} 
GROUP BY
    country_terrain
ORDER BY
    total_deaths DESC

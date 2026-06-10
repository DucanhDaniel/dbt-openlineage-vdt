select * from 
    {{ source('facebook', 'fad_age_gender_detailed_report') }}
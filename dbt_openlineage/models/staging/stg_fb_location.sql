select * from 
    {{ source('facebook', 'fad_location_detailed_report') }}
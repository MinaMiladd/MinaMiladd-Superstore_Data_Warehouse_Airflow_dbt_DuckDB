select distinct
    md5(concat_ws('|', country, region, state, city, postal_code)) as geography_key,
    country,
    region,
    state,
    city,
    postal_code
from {{ ref('stg_superstore') }}
-- In Superstore some product_ids map to more than one product_name,
-- so the key is built from both columns.
select distinct
    md5(concat_ws('|', product_id, product_name)) as product_key,
    product_id,
    product_name,
    category,
    sub_category
from {{ ref('stg_superstore') }}
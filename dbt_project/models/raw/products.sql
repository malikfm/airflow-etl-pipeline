select
    id,
    name,
    category,
    price,
    created_at,
    updated_at,
    deleted_at
from {{ source('staging', 'products') }}

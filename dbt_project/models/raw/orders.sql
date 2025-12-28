select
    id,
    user_id,
    status,
    created_at,
    updated_at
from {{ source('staging', 'orders') }}

select
    id,
    name,
    email,
    address,
    created_at,
    updated_at,
    deleted_at
from {{ source('staging', 'users') }}

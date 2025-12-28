select
    campaign_id,
    campaign_name,
    date,
    platform,
    spend,
    clicks,
    impressions
from {{ source('staging', 'marketing') }}

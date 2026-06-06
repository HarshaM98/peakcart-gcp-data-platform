with source as (

    select * from {{ source('bronze', 'bronze_inventory_snapshots') }}

),

deduped as (

    select
        *,
        row_number() over (
            partition by snapshot_id
            order by _loaded_at desc
        ) as row_num

    from source

),

renamed as (

    select
        -- primary key
        cast(snapshot_id as string)                         as snapshot_id,
        cast(warehouse_id as string)                        as warehouse_id,
        cast(product_id as string)                          as product_id,

        -- snapshot attributes
        cast(snapshot_date as date)                         as snapshot_date,
        cast(qty_on_hand as integer)                        as qty_on_hand,
        cast(qty_reserved as integer)                       as qty_reserved,

        -- derived columns
        cast(qty_on_hand as integer)
            - cast(qty_reserved as integer)                 as qty_available,

        -- quality flags
        snapshot_id is not null                             as has_snapshot_id,
        qty_on_hand >= 0                                    as is_non_negative_qty,
        qty_reserved >= 0                                   as is_non_negative_reserved,
        qty_reserved <= qty_on_hand                         as is_valid_reservation,

        -- metadata
        _loaded_at,
        _source_file

    from deduped
    where row_num = 1
      and snapshot_id is not null

)

select * from renamed

{{ config(
    tags=["mnpi","mnpi_exception"]
) }}

-- NEEDS TO BE MOVED TO RESTRICTED SCHEMA FOR MNPI

{{ config({
    "alias": "sfdc_opportunity_snapshots",
    })
}}

{{ create_snapshot_base(
    source=source('snapshots', 'sfdc_opportunity_snapshots'),
    primary_key='id',
    date_start='2019-10-01',
    date_part='day',
    snapshot_id_name='opportunity_snapshot_id' 
    )
}}

SELECT *
FROM final

select
    employee_number,
    percent_over_top_end_of_band,
    _UPDATED_AT,
    DBT_SCD_ID,
    DBT_UPDATED_AT,
    DBT_VALID_FROM,
    DBT_VALID_TO
from RAW.snapshots.sheetload_comp_band_snapshots


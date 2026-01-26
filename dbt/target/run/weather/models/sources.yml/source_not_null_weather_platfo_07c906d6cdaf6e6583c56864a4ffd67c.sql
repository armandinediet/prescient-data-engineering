
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select forecast_dt
from "weather"."public"."stg_openweather_forecast"
where forecast_dt is null



  
  
      
    ) dbt_internal_test
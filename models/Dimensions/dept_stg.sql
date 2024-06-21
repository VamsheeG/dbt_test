with source_data
as (select * 
      from {{source('seedcsvsrc','deptstgsrc')}}
    )
select * 
  from source_data 
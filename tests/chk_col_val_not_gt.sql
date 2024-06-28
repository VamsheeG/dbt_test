select *
  from {{ref('dept_stg')}}
 where deptno > 25

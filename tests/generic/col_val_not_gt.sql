{%test col_val_not_gt(model,column_name,val)%}
select {{column_name}}
  from {{model}}
 where {{column_name}} > val
 {%endtest%}

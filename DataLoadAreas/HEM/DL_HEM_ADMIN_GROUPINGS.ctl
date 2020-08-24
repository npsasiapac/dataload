load data                              
APPEND                   
into table DL_HEM_ADMIN_GROUPINGS            
fields terminated by "," optionally enclosed by '"'        
trailing nullcols
(
 LAGR_DLB_BATCH_ID CONSTANT "$batch_no"
,LAGR_DL_SEQNO RECNUM
,LAGR_DL_LOAD_STATUS CONSTANT "L", 
LAGR_AUN_CODE_PARENT CHAR "rtrim(:LAGR_AUN_CODE_PARENT)", 
LAGR_AUN_CODE_CHILD CHAR "rtrim(:LAGR_AUN_CODE_CHILD)")

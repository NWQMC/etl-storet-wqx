/*     left join wqx.result_lab_sample_prep
         on result.res_uid = result_lab_sample_prep.res_uid
       left join wqx.time_zone prep_start
         on result_lab_sample_prep.tmzone_uid_start_time = prep_start.tmzone_uid
       left join wqx.time_zone prep_end
         on result_lab_sample_prep.tmzone_uid_end_time = prep_end.tmzone_uid */
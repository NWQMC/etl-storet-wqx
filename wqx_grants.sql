show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'start time: ' || systimestamp from dual;

grant select on activity to wqp_core;
grant select on activity_conducting_org to wqp_core;
grant select on activity_media to wqp_core;
grant select on activity_media_subdivision to wqp_core;
grant select on activity_project to wqp_core;
grant select on activity_type to wqp_core;
grant select on analytical_method to wqp_core;
grant select on analytical_method_context to wqp_core;
grant select on characteristic to wqp_core;
grant select on country to wqp_core;
grant select on county to wqp_core;
grant select on detection_quant_limit_type to wqp_core;
grant select on horizontal_collection_method to wqp_core;
grant select on horizontal_reference_datum to wqp_core;
grant select on measurement_unit to wqp_core;
grant select on monitoring_location to wqp_core;
grant select on monitoring_location_type to wqp_core;
grant select on organization to wqp_core;
grant select on project to wqp_core;
grant select on result to wqp_core;
grant select on result_detect_quant_limit to wqp_core;
grant select on result_detection_condition to wqp_core;
grant select on result_lab_comment to wqp_core;
grant select on result_lab_sample_prep to wqp_core;
grant select on result_measure_qualifier to wqp_core;
grant select on result_statistical_base to wqp_core;
grant select on result_status to wqp_core;
grant select on result_temperature_basis to wqp_core;
grant select on result_time_basis to wqp_core;
grant select on result_value_type to wqp_core;
grant select on result_weight_basis to wqp_core;
grant select on sample_collection_equip to wqp_core;
grant select on sample_fraction to wqp_core;
grant select on sample_tissue_anatomy to wqp_core;
grant select on state to wqp_core;
grant select on taxon to wqp_core;
grant select on time_zone to wqp_core;
grant select on vertical_collection_method to wqp_core;
grant select on vertical_reference_datum to wqp_core;

select 'end time: ' || systimestamp from dual;

show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'transform prj_ml_weighting start time: ' || systimestamp from dual;

prompt dropping storet prj_ml_weighting indexes
exec etl_helper_prj_ml_weighting.drop_indexes('storet');

prompt populating storet project ml weighting
truncate table prj_ml_weighting_swap_storet;

insert /*+ append parallel(4) */
  into prj_ml_weighting_swap_storet (data_source_id,
                                     project_id,
                                     station_id,
                                     data_source,
                                     site_id,
                                     organization,
                                     organization_name,
                                     site_type,
                                     huc,
                                     governmental_unit_code,
                                     project_identifier,
                                     measure_value,
                                     unit_code,
                                     statistical_stratum,
                                     location_category,
                                     location_status,
                                     ref_location_type_code,
                                     ref_location_start_date,
                                     ref_location_end_date,
                                     resource_title,
                                     resource_creator,
                                     resource_subject,
                                     resource_publisher,
                                     resource_date,
                                     resource_identifier,
                                     comment_text
                                    )
select  3 data_source_id,
        project_data_swap_storet.project_id project_id,
        station_swap_storet.station_id station_id,
        'STORET' data_source,
        station_swap_storet.site_id site_id,
        project_data_swap_storet.organization organization,
        project_data_swap_storet.organization_name organization_name,
        station_swap_storet.site_type site_type,
        station_swap_storet.huc huc,
        station_swap_storet.governmental_unit_code governmental_unit_code,
        project_data_swap_storet.project_identifier project_identifier,
        monitoring_location_weight.mlwt_weighting_factor measure_value,
        measurement_unit.msunt_cd unit_code,
        monitoring_location_weight.mlwt_stratum statistical_stratum,
        monitoring_location_weight.mlwt_category location_category,
        monitoring_location_weight.mlwt_status location_status,
        reference_location_type.rltyp_cd ref_location_type_code,
        monitoring_location_weight.mlwt_ref_loc_start_date ref_location_start_date,
        monitoring_location_weight.mlwt_ref_loc_end_date ref_location_end_date,
        citation.citatn_title resource_title,
        citation.citatn_creator resource_creator,
        citation.citatn_subject resource_subject,
        citation.citatn_publisher resource_publisher,
        citation.citatn_date resource_date,
        citation.citatn_id resource_identifier,
        monitoring_location_weight.mlwt_comment comment_text
  from wqx.monitoring_location_weight
        left join wqx.reference_location_type
          on monitoring_location_weight.rltyp_uid = reference_location_type.rltyp_uid
        join wqx.measurement_unit
          on monitoring_location_weight.msunt_uid = measurement_unit.msunt_uid
        join wqx.citation
          on monitoring_location_weight.citatn_uid = citation.citatn_uid
        join wqp_core.station_swap_storet
          on monitoring_location_weight.mloc_uid = station_swap_storet.station_id
        join wqp_core.project_data_swap_storet
          on monitoring_location_weight.prj_uid = project_data_swap_storet.project_id;

commit;
select 'Building storet prj_ml_weighting complete: ' || systimestamp from dual;

prompt building storet prj_ml_weighting indexes
exec etl_helper_prj_ml_weighting.create_indexes('storet');

select 'transform prj_ml_weighting end time: ' || systimestamp from dual;

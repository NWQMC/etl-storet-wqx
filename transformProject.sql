show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'transform project start time: ' || systimestamp from dual;

prompt dropping storet project_data indexes
exec etl_helper_project.drop_indexes('storet');

prompt populating storet project
truncate table project_data_swap_storet;

insert /*+ append parallel(4) */
  into project_data (data_source_id,
                     project_id,
                     data_source,
                     organization,
                     organization_name,
                     project_identifier,
                     project_name,
                     description,
                     sampling_design_type_code,
                     qapp_approved_indicator,
                     qapp_approval_agency_name,
                     project_file_url,
                     monitoring_location_weight_url
                    )
select 3 data_source_id,
       project.prj_uid project_id,
       'STORET' data_source,
       organization.org_id organization,
       organization.org_name organization_name,
       project.prj_id project_identifier,
       project.prj_name project_name,
       project.prj_desc description,
       sampling_design_type.sdtyp_desc sampling_design_type_code,
       project.prj_qapp_approved_yn qapp_approved_indicator,
       project.prj_qapp_approval_agency_name qapp_approval_agency_name,
       case 
         when attached_object.has_blob is not null
           then '/organizations/' || pkg_dynamic_list.url_escape(organization.org_id, 'true') || '/projects/' || pkg_dynamic_list.url_escape(project.prj_id, 'true') || '/files'
         else null
       end project_file_url,
       case
         when monitoring_location_weight.has_weight is not null
           then '/organizations/' || pkg_dynamic_list.url_escape(organization.org_id, 'true') || '/projects/' || pkg_dynamic_list.url_escape(project.prj_id, 'true') || '/projectMonitoringLocationWeightings'
         else null
       end monitoring_location_weight_url
  from wqx.project
       join wqx.organization
         on project.org_uid = organization.org_uid
       left join wqx.sampling_design_type
         on project.sdtyp_uid = sampling_design_type.sdtyp_uid
       left join (select org_uid, ref_uid, count(*) has_blob
                    from wqx.attached_object
                   where 1 = tbl_uid
                     group by org_uid, ref_uid) attached_object
         on project.org_uid = attached_object.org_uid and
            project.prj_uid = attached_object.ref_uid
       left join (select prj_uid, count(*) has_weight
                    from wqx.monitoring_location_weight
                      group by prj_uid) monitoring_location_weight
         on project.prj_uid = monitoring_location_weight.prj_uid;

commit;
select 'Building storet project_data complete: ' || systimestamp from dual;

prompt building storet project_data indexes
exec etl_helper_project.create_indexes('storet');

select 'transform project_data end time: ' || systimestamp from dual;

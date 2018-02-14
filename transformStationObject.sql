show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'transform station object start time: ' || systimestamp from dual;

prompt dropping storet station_object indexes
exec etl_helper_station_object.drop_indexes('storet');

prompt populating storet station_object
truncate table station_object_swap_storet;

insert /*+ append parallel(4) */
  into station_object_swap_storet (data_source_id,
                                   object_id,
                                   data_source,
                                   organization,
                                   station_id,
                                   site_id,
                                   object_name,
                                   object_type,
                                   object_content)
select '3' data_source_id,
       attached_object.atobj_uid object_id,
       'STORET' data_source,
       organization.org_id organization,
       attached_object.ref_uid station_id,
       station.site_id,
       attached_object.atobj_file_name object_name,
       attached_object.atobj_type object_type,
       attached_object.atobj_content object_content
  from wqx.attached_object
       join wqx.organization
         on attached_object.org_uid = organization.org_uid
       join station_swap_storet station
         on attached_object.ref_uid = station.station_id
 where tbl_uid = 2;

commit;
select 'Building storet station_object complete: ' || systimestamp from dual;

prompt building storet station_object indexes
exec etl_helper_station_object.create_indexes('storet');

select 'transform station_object end time: ' || systimestamp from dual;

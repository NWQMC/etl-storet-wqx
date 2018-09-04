show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'transform activity start time: ' || systimestamp from dual;

prompt populating wqx_activity_project
truncate table wqx_activity_project;
insert /*+ append parallel(4) */ into wqx_activity_project (act_uid, project_id_list, project_name_list)
select /*+ parallel(4) */
       activity_project.act_uid,
       listagg(project.prj_id, ';') within group (order by project.prj_id) project_id_list,
       listagg(project.prj_id, ';') within group (order by project.prj_id) project_name_list
--//TODO - values too long       listagg(project.prj_name, ';') within group (order by project.prj_id) project_name_list
  from wqx.activity_project
       left join wqx.project
         on activity_project.prj_uid = project.prj_uid
    group by activity_project.act_uid;
commit;
select 'Building wqx_actvity_project complete: ' || systimestamp from dual;

prompt populating wqx_activity_conducting_org
truncate table wqx_activity_conducting_org;
insert  /*+ append parallel(4) */ into wqx_activity_conducting_org (act_uid, acorg_name_list)
select /*+ parallel(4) */
       act_uid,
       listagg(acorg_name, ';') within group (order by rownum) acorg_name_list
  from wqx.activity_conducting_org
    group by act_uid);
commit;
select 'Building wqx_activity_conducting_org complete: ' || systimestamp from dual;

prompt populating wqx_attached_object_activity
truncate table wqx_attached_object_activity;
insert /*+ append parallel(4) */ into wqx_attached_object_activity (org_uid, ref_uid, activity_object_name, activity_object_type)
select /*+ parallel(4) */
       org_uid,
       ref_uid,
       listagg(atobj_file_name, ';') within group (order by rownum) activity_object_name,
       listagg(atobj_type, ';') within group (order by rownum) activity_object_type
  from wqx.attached_object
 where tbl_uid = 3
    group by org_uid, ref_uid);
commit;
select 'Building wqx_attached_object_activity complete: ' || systimestamp from dual;

prompt populating wqx_activity_metric_sum
truncate table wqx_activity_metric_sum;
insert /*+ append parallel(4) */ into wqx_activity_metric_sum (act_uid, activity_metric_count)
select /*+ parallel(4) */
       act_uid,
       count(*) activity_metric_count
  from wqx.activity_metric
    group by act_uid;
commit;
select 'Building wqx_activity_metric_sum complete: ' || systimestamp from dual;

prompt dropping storet activity indexes
exec etl_helper_activity.drop_indexes('storet');

prompt populating activity_swap_storet
truncate table activity_swap_storet;
insert /*+ append parallel(4) */
  into activity_swap_storet (data_source_id, data_source, station_id, site_id, station_name, event_date, activity, sample_media, organization, site_type, huc, governmental_unit_code,
                             organization_name, activity_id, activity_type_code, activity_media_subdiv_name, activity_start_time, act_start_time_zone,
                             activity_stop_date, activity_stop_time, act_stop_time_zone, activity_relative_depth_name, activity_depth,
                             activity_depth_unit, activity_depth_ref_point, activity_upper_depth, activity_upper_depth_unit, activity_lower_depth,
                             activity_lower_depth_unit, project_id, project_name, activity_conducting_org, activity_comment, activity_latitude, activity_longitude,
                             activity_source_map_scale, act_horizontal_accuracy, act_horizontal_accuracy_unit, act_horizontal_collect_method,
                             act_horizontal_datum_name, assemblage_sampled_name, act_collection_duration, act_collection_duration_unit,
                             act_sam_compnt_name, act_sam_compnt_place_in_series, act_reach_length, act_reach_length_unit, act_reach_width,
                             act_reach_width_unit, act_pass_count, net_type_name, act_net_surface_area, act_net_surface_area_unit, act_net_mesh_size,
                             act_net_mesh_size_unit, act_boat_speed, act_boat_speed_unit, act_current_speed, act_current_speed_unit,
                             toxicity_test_type_name, sample_collect_method_id, sample_collect_method_ctx, sample_collect_method_name,
                             act_sam_collect_meth_qual_type, act_sam_collect_meth_desc, sample_collect_equip_name, act_sam_collect_equip_comments,
                             act_sam_prep_meth_id, act_sam_prep_meth_context, act_sam_prep_meth_name, act_sam_prep_meth_qual_type,
                             act_sam_prep_meth_desc, sample_container_type, sample_container_color, act_sam_chemical_preservative,
                             thermal_preservative_name, act_sam_transport_storage_desc, activity_object_name, activity_object_type,
                             activity_file_url, activity_metric_url)
select /*+ parallel(4) */ 
       3 data_source_id,
       'STORET' data_source,
       activity.mloc_uid station_id, 
       station.site_id,
       station.station_name
       trunc(activity.act_start_date) event_date,
       station.organization || '-' || activity.act_id activity,
       activity_media.acmed_name sample_media,
       station.organization,
       station.site_type,
       station.huc,
       station.governmental_unit_code,
       organization.org_name organization_name,
       activity.act_uid activity_id,
       activity_type.actyp_cd activity_type_code,
       activity_media_subdivision.amsub_name activity_media_subdiv_name,
       to_char(activity.act_start_time, 'hh24:mi:ss') activity_start_time,
       start_time_zone.tmzone_cd act_start_time_zone,
       to_char(activity.act_end_date, 'yyyy-mm-dd') activity_stop_date,
       nvl2(act_end_date, to_char(activity.act_end_time, 'hh24:mi:ss'), null) activity_stop_time,
       end_time_zone.tmzone_cd act_stop_time_zone,
       relative_depth.reldpth_name activity_relative_depth_name,
       activity.act_depth_height activity_depth,
       h_measurement_unit.msunt_cd activity_depth_unit,
       activity.act_depth_altitude_ref_point activity_depth_ref_point,
       activity.act_depth_height_top activity_upper_depth,
       t_measurement_unit.msunt_cd activity_upper_depth_unit,
       activity.act_depth_height_bottom activity_lower_depth,
       b_measurement_unit.msunt_cd activity_lower_depth_unit,
       wqx_activity_project.project_id_list project_id,
       wqx_activity_project.project_name_list project_name,
       wqx_activity_conducting_org.acorg_name_list activity_conducting_org,
       activity.act_comments activity_comment,
       nvl(activity.act_loc_latitude, station.latitude) activity_latitude,
       nvl(activity.act_loc_longitude, station.longitude) activity_longitude,
       activity.act_loc_source_map_scale activity_source_map_scale,
       activity.act_horizontal_accuracy,
       activity_horizontal_unit.msunt_cd act_horizontal_accuracy_unit,
       horizontal_collection_method.hcmth_name act_horizontal_collect_method,
       horizontal_reference_datum.hrdat_name act_horizontal_datum_name,
       assemblage.asmblg_name assemblage_sampled_name,
       activity.act_collection_duration,
       collection_duration.msunt_cd act_collection_duration_unit,
       activity.act_sam_compnt_name,
       activity.act_sam_compnt_place_in_series,
       activity.act_reach_length,
       reach_length.msunt_cd act_reach_length_unit,
       activity.act_reach_width,
       reach_width.msunt_cd act_reach_width_unit,
       activity.act_pass_count,
       net_type.nettyp_name net_type_name,
       activity.act_net_surface_area,
       net_surface_unit.msunt_cd act_net_surface_area_unit,
       activity.act_net_mesh_size,
       net_mesh.msunt_cd act_net_mesh_size_unit,
       activity.act_boat_speed,
       boat_speed.msunt_cd act_boat_speed_unit,
       activity.act_current_speed,
       current_speed.msunt_cd act_current_speed_unit,
       toxicity_test_type.tttyp_name toxicity_test_type_name,
       case
         when activity.act_sam_collect_meth_id is not null and
              activity.act_sam_collect_meth_context is not null
           then activity.act_sam_collect_meth_id
         else null
       end sample_collect_method_id,
       case
         when activity.act_sam_collect_meth_id is not null and
              activity.act_sam_collect_meth_context is not null
           then activity.act_sam_collect_meth_context
         else null
       end sample_collect_method_ctx,
       case
         when activity.act_sam_collect_meth_id is not null and
              activity.act_sam_collect_meth_context is not null
           then activity.act_sam_collect_meth_name
         else null
       end sample_collect_method_name,
       activity.act_sam_collect_meth_qual_type,
       activity.act_sam_collect_meth_desc,
       sample_collection_equip.sceqp_name sample_collect_equip_name,
       activity.act_sam_collect_equip_comments,
       activity.act_sam_prep_meth_id,
       activity.act_sam_prep_meth_context,
       activity.act_sam_prep_meth_name,
       activity.act_sam_prep_meth_qual_type,
       activity.act_sam_prep_meth_desc,
       container_type.contyp_name sample_container_type,
       container_color.concol_name sample_container_color,
       activity.act_sam_chemical_preservative,
       thermal_preservative.thprsv_name thermal_preservative_name,
       activity.act_sam_transport_storage_desc,
       wqx_attached_object_activity.activity_object_name,
       wqx_attached_object_activity.activity_object_type,
       case
         when wqx_attached_object_activity.ref_uid is null
           then null
         else
           '/organizations/' ||
               pkg_dynamic_list.url_escape(station.organization, 'true') || '/activities/' ||
               pkg_dynamic_list.url_escape(station.organization, 'true') || '-' ||
               pkg_dynamic_list.url_escape(activity.act_id, 'true') || '/files'
       end activity_file_url,
       case
         when wqx_activity_metric_sum.act_uid is null
           then null
           else
             '/activities/' ||
               pkg_dynamic_list.url_escape(station.organization, 'true') || '-' ||
               pkg_dynamic_list.url_escape(activity.act_id, 'true') || '/activitymetrics'
       end activity_metric_url
  from wqx.activity
       join station_swap_storet station
         on activity.mloc_uid = station.station_id
       left join wqx.sample_collection_equip
         on activity.sceqp_uid = sample_collection_equip.sceqp_uid
       left join wqx_activity_conducting_org
         on activity.act_uid = wqx_activity_conducting_org.act_uid
       left join wqx_activity_project
         on activity.act_uid = wqx_activity_project.act_uid
       left join wqx.measurement_unit b_measurement_unit
         on activity.msunt_uid_depth_height_bottom = b_measurement_unit.msunt_uid
       left join wqx.measurement_unit t_measurement_unit
         on activity.msunt_uid_depth_height_top = t_measurement_unit.msunt_uid
       left join wqx.measurement_unit h_measurement_unit
         on activity.msunt_uid_depth_height = h_measurement_unit.msunt_uid
       left join wqx.measurement_unit net_surface_unit
         on activity.msunt_uid_net_surface_area = net_surface_unit.msunt_uid
       left join wqx.time_zone end_time_zone
         on activity.tmzone_uid_end_time = end_time_zone.tmzone_uid
       left join wqx.time_zone start_time_zone
         on activity.tmzone_uid_start_time = start_time_zone.tmzone_uid
       left join wqx.activity_media_subdivision
         on activity.amsub_uid = activity_media_subdivision.amsub_uid
       left join wqx.activity_type
         on activity.actyp_uid = activity_type.actyp_uid
       left join wqx.organization
         on activity.org_uid = organization.org_uid
       left join wqx.activity_media
         on activity.acmed_uid = activity_media.acmed_uid
       left join wqx.measurement_unit activity_horizontal_unit
         on activity.msunt_uid_horizontal_accuracy = activity_horizontal_unit.msunt_uid
       left join wqx.horizontal_collection_method
         on activity.hcmth_uid = horizontal_collection_method.hcmth_uid
       left join wqx.horizontal_reference_datum
         on activity.hrdat_uid = horizontal_reference_datum.hrdat_uid
       left join wqx.assemblage
         on activity.asmblg_uid = assemblage.asmblg_uid 
       left join wqx.measurement_unit collection_duration	 
         on activity.msunt_uid_collection_duration = collection_duration.msunt_uid
       left join wqx.measurement_unit reach_length
         on activity.msunt_uid_reach_length = reach_length.msunt_uid
       left join wqx.measurement_unit reach_width
         on activity.msunt_uid_reach_width = reach_width.msunt_uid
       left join wqx.net_type
         on activity.nettyp_uid = net_type.nettyp_uid
       left join wqx.measurement_unit net_mesh
         on activity.msunt_uid_net_mesh_size = net_mesh.msunt_uid
       left join wqx.measurement_unit boat_speed
         on activity.msunt_uid_boat_speed = boat_speed.msunt_uid
       left join wqx.measurement_unit current_speed
         on activity.msunt_uid_current_speed = current_speed.msunt_uid
       left join wqx.toxicity_test_type
         on activity.tttyp_uid = toxicity_test_type.tttyp_uid
       left join wqx.container_type
         on activity.contyp_uid = container_type.contyp_uid
       left join wqx.container_color
         on activity.concol_uid = container_color.concol_uid
       left join wqx.thermal_preservative
         on activity.thprsv_uid = thermal_preservative.thprsv_uid
       left join wqx.relative_depth
         on activity.reldpth_uid = relative_depth.reldpth_uid
       left join wqx_attached_object_activity
         on activity.org_uid = wqx_attached_object_activity.org_uid and
            activity.act_uid = wqx_attached_object_activity.ref_uid
       left join wqx_activity_metric_sum
         on activity.act_uid = wqx_activity_metric_sum.act_uid;
commit;
select 'Building activity_swap_storet complete: ' || systimestamp from dual;

insert /*+ append parallel(4) */
  into activity_swap_storet (data_source_id, data_source, station_id, site_id, event_date, activity, sample_media, organization, site_type, huc, governmental_unit_code,
                             organization_name, activity_id, activity_type_code, activity_media_subdiv_name, activity_start_time, act_start_time_zone, activity_stop_date,
                             activity_stop_time, act_stop_time_zone, activity_depth, activity_depth_unit, activity_depth_ref_point, activity_upper_depth, activity_upper_depth_unit,
                             activity_lower_depth, activity_lower_depth_unit, project_id, activity_conducting_org, activity_comment, sample_aqfr_name, hydrologic_condition_name,
                             hydrologic_event_name, sample_collect_method_id, sample_collect_method_ctx, sample_collect_method_name, sample_collect_equip_name)
select 3 data_source_id,
       'STORET' data_source,
       a.*
  from (select /*+ parallel(4) */ 
               station.station_id, 
               station.site_id,
               activity_no_source.event_date,
               activity_no_source.activity,
               activity_no_source.sample_media,
               station.organization,
               station.site_type,
               station.huc,
               station.governmental_unit_code,
               station.organization_name,
               activity_no_source.activity_id,
               activity_no_source.activity_type_code,
               activity_no_source.activity_media_subdiv_name,
               activity_no_source.activity_start_time,
               activity_no_source.act_start_time_zone,
               activity_no_source.activity_stop_date,
               activity_no_source.activity_stop_time,
               activity_no_source.act_stop_time_zone,
               activity_no_source.activity_depth,
               activity_no_source.activity_depth_unit,
               activity_no_source.activity_depth_ref_point,
               activity_no_source.activity_upper_depth,
               activity_no_source.activity_upper_depth_unit,
               activity_no_source.activity_lower_depth,
               activity_no_source.activity_lower_depth_unit,
               activity_no_source.project_id,
               activity_no_source.activity_conducting_org,
               activity_no_source.activity_comment,
               activity_no_source.sample_aqfr_name,
               activity_no_source.hydrologic_condition_name,
               activity_no_source.hydrologic_event_name,
               activity_no_source.sample_collect_method_id,
               activity_no_source.sample_collect_method_ctx,
               activity_no_source.sample_collect_method_name,
               activity_no_source.sample_collect_equip_name
          from activity_no_source
               join station_swap_storet station
                 on activity_no_source.station_id + 10000000 = station.station_id) a;

commit;
select 'Building activity_swap_storet from activity_no_source complete: ' || systimestamp from dual;

prompt building storet activity indexes
exec etl_helper_activity.create_indexes('storet');

select 'transform activity end time: ' || systimestamp from dual;

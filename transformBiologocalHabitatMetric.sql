show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'transform bio_hab_metric start time: ' || systimestamp from dual;

prompt dropping storet bio_hab_metric indexes
exec etl_helper_bio_hab_metric.drop_indexes('storet');

prompt populating bio_hab_metric_swap_storet
truncate table bio_hab_metric_swap_storet;
insert /*+ append parallel(4) */
  into bio_hab_metric_swap_storet (data_source_id, data_source, station_id, site_id, organization, site_type, huc, governmental_unit_code,
                                   index_identifier, index_type_identifier, index_type_context, index_type_name, resource_title_name,
                                   resource_creator_name, resource_subject_text, resource_publisher_name, resource_date, resource_identifier,
                                   index_type_scale_text, index_score_numeric, index_qualifier_code, index_comment, index_calculated_date)
select /*+ parallel(4) */ 
       3 data_source_id,
       'STORET' data_source,
       biological_habitat_index.mloc_uid station_id, 
       station.site_id,
       station.organization,
       station.site_type,
       station.huc,
       station.governmental_unit_code,
       biological_habitat_index.bhidx_id index_identifier,
       index_type.idxtyp_id index_type_identifier,
       index_type.idxtyp_context index_type_context,
       index_type.idxtyp_name index_type_name,
       citation.citatn_title resource_title_name,
       citation.citatn_creator resource_creator_name,
       citation.citatn_subject resource_subject_text,
       citation.citatn_publisher resource_publisher_name,
       citation.citatn_date resource_date,
       citation.citatn_id resource_identifier,
       index_type.idxtyp_scale index_type_scale_text,
       biological_habitat_index.bhidx_score index_score_numeric,
       biological_habitat_index.bhidx_qualifier_cd index_qualifier_code,
       biological_habitat_index.bhidx_comment index_comment,
       biological_habitat_index.bhidx_calculated_date index_calculated_date
  from wqx.biological_habitat_index
       left join station_swap_storet station
         on biological_habitat_index.mloc_uid = station.station_id
       left join wqx.index_type
         on biological_habitat_index.idxtyp_uid = index_type.idxtyp_uid
       left join wqx.citation
         on index_type.citatn_uid = citation.citatn_uid;

commit;
select 'Building bio_hab_metric_swap_storet complete: ' || systimestamp from dual;

prompt building storet bio_hab_metric indexes
exec etl_helper_bio_hab_metric.create_indexes('storet');

select 'transform bio_hab_metric end time: ' || systimestamp from dual;


show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'transform org_data start time: ' || systimestamp from dual;

prompt dropping storet org_data indexes
exec etl_helper_org_data.drop_indexes('storet');

prompt populating org_data_swap_storet
truncate table org_data_swap_storet;
insert /*+ append parallel(4) */
  into org_data_swap_storet (data_source_id, data_source, organization_id, organization, organization_name,
                             organization_description, organization_type, tribal_code, electronic_address, telephonic, address_type_1,
                             address_text_1, supplemental_address_text_1, locality_name_1, postal_code_1,
                             country_code_1, state_code_1, county_code_1, address_type_2, address_text_2,
                             supplemental_address_text_2, locality_name_2, postal_code_2, country_code_2,
                             state_code_2, county_code_2, address_type_3, address_text_3,
                             supplemental_address_text_3, locality_name_3, postal_code_3, country_code_3,
                             state_code_3, county_code_3)
select /*+ parallel(4) */ 
       3 data_source_id,
       'STORET' data_source,
       organization.org_uid organization_id,
       organization.org_id organization,
       organization.org_name organization_name,
       organization.org_desc organization_description,
       organization.org_type organization_type,
       tribe.trb_name tribal_code,
       org_electronic_address.electronic_address,
       org_phone.telephonic,
       org_address.a_addtyp_name address_type_1,
       org_address.a_orgadd_address address_text_1,
       org_address.a_orgadd_address_supplemental supplemental_address_text_1,
       org_address.a_orgadd_locality_name locality_name_1,
       org_address.a_orgadd_postal_cd postal_code_1,
       org_address.a_cntry_cd country_code_1,
       org_address.a_st_cd state_code_1,
       org_address.a_cnty_fips_cd county_code_1,
       org_address.b_addtyp_name address_type_2,
       org_address.b_orgadd_address address_text_2,
       org_address.b_orgadd_address_supplemental supplemental_address_text_2,
       org_address.b_orgadd_locality_name locality_name_2,
       org_address.b_orgadd_postal_cd postal_code_2,
       org_address.b_cntry_cd country_code_2,
       org_address.b_st_cd state_code_2,
       org_address.b_cnty_fips_cd county_code_2,
       org_address.c_addtyp_name address_type_3,
       org_address.c_orgadd_address address_text_3,
       org_address.c_orgadd_address_supplemental supplemental_address_text_3,
       org_address.c_orgadd_locality_name locality_name_3,
       org_address.c_orgadd_postal_cd postal_code_3,
       org_address.c_cntry_cd country_code_3,
       org_address.c_st_cd state_code_3,
       org_address.c_cnty_fips_cd county_code_3
  from wqx.organization
       left join wqx.tribe
         on organization.trb_uid = tribe.trb_uid
       left join (select org_uid,
                         listagg(orgea_text || ' (' || eatyp_name || ')', ';') within group (order by orgea_uid) electronic_address
                    from wqx.org_electronic_address
                         join wqx.electronic_address_type
                           on org_electronic_address.eatyp_uid = electronic_address_type.eatyp_uid
                      group by org_uid) org_electronic_address
         on organization.org_uid = org_electronic_address.org_uid
       left join (select org_uid,
                         listagg(orgph_num ||
                                   case when orgph_ext is not null then ' x' || orgph_ext end || 
                                   ' (' || phtyp_name || ')', ';')
                             within group (order by orgph_uid) telephonic
                    from wqx.org_phone
                         join wqx.phone_type
                           on org_phone.phtyp_uid = phone_type.phtyp_uid
                      group by org_uid) org_phone
         on organization.org_uid = org_phone.org_uid
       left join (select *
                    from (select org_address.org_uid,
                                 address_type.addtyp_name,
                                 org_address.orgadd_address,
                                 org_address.orgadd_address_supplemental,
                                 org_address.orgadd_locality_name,
                                 org_address.orgadd_postal_cd,
                                 country.cntry_cd,
                                 state.st_cd,
                                 county.cnty_fips_cd,
                                 row_number() over (partition by org_address.org_uid order by org_address.orgadd_uid) addr_num
                            from wqx.org_address
                                 join wqx.address_type
                                   on org_address.addtyp_uid = address_type.addtyp_uid
                                 left join wqx.country
                                   on org_address.cntry_uid = country.cntry_uid
                                 left join wqx.state
                                   on org_address.st_uid = state.st_uid
                                 left join wqx.county
                                   on org_address.cnty_uid = county.cnty_uid)
                   pivot (min(addtyp_name) addtyp_name,
                          min(orgadd_address) orgadd_address,
                          min(orgadd_address_supplemental) orgadd_address_supplemental,
                          min(orgadd_locality_name) orgadd_locality_name,
                          min(orgadd_postal_cd) orgadd_postal_cd,
                          min(cntry_cd) cntry_cd,
                          min(st_cd) st_cd,
                          min(cnty_fips_cd) cnty_fips_cd
                           for (addr_num) in (1 a, 2 b, 3 c)
                         )
                 ) org_address
         on organization.org_uid = org_address.org_uid
 where organization.org_uid not between 2000 and 2999;
commit;

insert /*+ append parallel(4) */
  into org_data_swap_storet (data_source_id, data_source, organization_id, organization, organization_name,
                             organization_description, organization_type)
select /*+ parallel(4) */ 
       3 data_source_id,
       'STORET' data_source,
       pk_isn + 10000000  organization_id,
       organization_id organization,
       organization_name,
       organization_description,
       organization_type
  from storetw.di_org
 where source_system is null;
commit;
select 'Building org_data_swap_storet complete: ' || systimestamp from dual;

prompt building storet org_data indexes
exec etl_helper_org_data.create_indexes('storet');

select 'transform org_data end time: ' || systimestamp from dual;

class Queries():
    def organization_cluster(schema):
        return f'''WITH service_names AS (
                    SELECT
                        srv_at_loc.location_id,
                        STRING_AGG(srv.name, ', ') AS names
                    FROM "{schema}".service_at_location srv_at_loc
                    INNER JOIN "{schema}".service srv ON srv_at_loc.service_id = srv.id
                    GROUP BY srv_at_loc.location_id
                ),
                -- CTE for aggregating unique taxonomy terms per location
                taxonomy_terms AS (
                    SELECT
                        srv_at_loc.location_id,
                        STRING_AGG(DISTINCT tax_term.name, ', ') AS terms
                    FROM "{schema}".service_at_location srv_at_loc
                    INNER JOIN "{schema}".service srv ON srv_at_loc.service_id = srv.id
                    INNER JOIN "{schema}".attribute atr ON srv.id = atr.link_id
                    INNER JOIN "{schema}".taxonomy_term tax_term ON atr.taxonomy_term_id = tax_term.id
                    GROUP BY srv_at_loc.location_id
                ),
                -- CTE for denormalizing model names into clusters
                cluster AS (
                    SELECT
                        c.id,
                        m.name AS model_name
                    FROM "wa211whatcomGroup".cluster c
                    JOIN "wa211whatcomGroup".model m ON c.model_id = m.id
                ),


                -- CTE for aggregating phone numbers per organization
                organization_phones AS (
                    SELECT
                        organization_id,
                        STRING_AGG(number, ', ') AS organization_phone_numbers
                    FROM "{schema}".phone
                    GROUP BY organization_id
                ),
                -- CTE for aggregating phone numbers per location
                location_phones AS (
                    SELECT
                        location_id,
                        STRING_AGG(number, ', ') AS location_phone_numbers
                    FROM "{schema}".phone
                    GROUP BY location_id
                )
                -- Main query
                SELECT
                    '"{schema}"' AS contributor,
                    org.id AS organization_id,
                    org.name AS name,
                    org.alternate_name AS alternate_name,
                    loc.id AS location_id,
                    add.address_1 AS address_1,
                    add.address_2 as address_2,
                    org.website as o_url,
                    org.website as url,
                    add.city AS city,
                    add.postal_code AS postal_code,
                    add.state_province AS state_province,
                    add.region AS region,
                    loc.latitude AS latitude,
                    loc.longitude AS longitude,
                    org.email AS organization_email,
                    org.website AS organization_website,
                    NULL AS organization_tax_status,
                    org_phones.organization_phone_numbers,  -- Using aggregated organization phone numbers
                    loc_phones.location_phone_numbers,  -- Using aggregated location phone numbers
                    sn.names AS service_names,  -- Using pre-computed service names from CTE
                    tt.terms AS service_taxonomy_terms,  -- Using pre-computed taxonomy terms from CTE
                    org.description AS organization_description


                    -- Including all cluster ids from the cluster_info CTE

                FROM "{schema}".organization org
                -- Join with location table
                INNER JOIN "{schema}".location loc ON org.id = loc.organization_id
                -- Left join with address table, filtering for physical addresses
                LEFT JOIN "{schema}".address add ON loc.id = add.location_id AND add.address_type = 'physical'
                -- Left join with aggregated organization phones CTE
                LEFT JOIN organization_phones org_phones ON org_phones.organization_id = org.id
                -- Left join with aggregated location phones CTE
                LEFT JOIN location_phones loc_phones ON loc_phones.location_id = loc.id
                -- Left join with service_names CTE
                LEFT JOIN service_names sn ON sn.location_id = loc.id
                -- Left join with taxonomy_terms CTE
                LEFT JOIN taxonomy_terms tt ON tt.location_id = loc.id
                -- Left join with cluster_info CTE, casting org.id to UUID to match link_id
                '''
    def byModel(schema:str,groupschemaName:str,model_name:str,job_id:str):
        return f'''WITH
            -- CTE for aggregating phone numbers per organization
            organization_phones AS (
            SELECT
            organization_id,
            STRING_AGG(number, ', ') AS organization_phone_numbers
            FROM "{schema}".phone
            GROUP BY organization_id
            ),
            -- CTE for aggregating locations per organization
            organization_locations AS (
            SELECT
            org.id AS organization_id,
            STRING_AGG(DISTINCT loc.id::text, ', ') AS location_ids,
            STRING_AGG(DISTINCT addr.address_1, ', ') AS location_addresses,
            STRING_AGG(DISTINCT addr.city, ', ') AS location_cities,
            STRING_AGG(DISTINCT addr.postal_code, ', ') AS location_postal_codes,
            STRING_AGG(DISTINCT addr.state_province, ', ') AS location_state_provinces,
            STRING_AGG(DISTINCT addr.region, ', ') AS location_regions,
            STRING_AGG(DISTINCT loc.latitude::text, ', ') AS location_latitudes,
            STRING_AGG(DISTINCT loc.longitude::text, ', ') AS location_longitudes
            FROM "{schema}".organization org
            INNER JOIN "{schema}".location loc ON org.id = loc.organization_id
            LEFT JOIN "{schema}".address addr ON loc.id = addr.location_id AND addr.address_type = 'physical'
            GROUP BY org.id
            )
            -- Main query
            SELECT DISTINCT ON (org.id)
            org.id,
            '"{schema}"' AS contributor,
            org.name AS organization_name,
            org.alternate_name AS organization_alternate_name,
            org_loc.location_addresses AS location_address_1,
            org_loc.location_cities AS location_city,
            org_loc.location_postal_codes AS location_postal_code,
            org_loc.location_state_provinces AS location_state_province,
            org_loc.location_regions AS location_region,
            org.email AS organization_email,
            org.website AS organization_website,
            org_phones.organization_phone_numbers,  -- Using aggregated organization phone numbers
            org.description AS organization_description,
            org.id as org_id,
            cm.cluster_id,
            cm.model_confidence,
            cm.link_id
            FROM "{groupschemaName}".cluster_member cm
            INNER JOIN "{groupschemaName}".cluster cl ON cl.id=cm.cluster_id
            INNER JOIN "{groupschemaName}".model m ON m.id=cl.model_id
            INNER JOIN "{schema}".organization org ON org.id=cm.link_id::text
            LEFT JOIN organization_phones org_phones ON org_phones.organization_id = org.id
            LEFT JOIN organization_locations org_loc ON org_loc.organization_id = org.id
            WHERE  m.name = '{model_name}' and cl.job_id='{job_id}'
            '''
    def allClusters(schema:str,groupschemaName:str,job_id:str):
        return f'''WITH
            -- CTE for aggregating phone numbers per organization
            organization_phones AS (
            SELECT
            organization_id,
            STRING_AGG(number, ', ') AS organization_phone_numbers
            FROM "{schema}".phone
            GROUP BY organization_id
            ),
            -- CTE for aggregating locations per organization
            organization_locations AS (
            SELECT
            org.id AS organization_id,
            STRING_AGG(DISTINCT loc.id::text, ', ') AS location_ids,
            STRING_AGG(DISTINCT addr.address_1, ', ') AS location_addresses,
            STRING_AGG(DISTINCT addr.city, ', ') AS location_cities,
            STRING_AGG(DISTINCT addr.postal_code, ', ') AS location_postal_codes,
            STRING_AGG(DISTINCT addr.state_province, ', ') AS location_state_provinces,
            STRING_AGG(DISTINCT addr.region, ', ') AS location_regions,
            STRING_AGG(DISTINCT loc.latitude::text, ', ') AS location_latitudes,
            STRING_AGG(DISTINCT loc.longitude::text, ', ') AS location_longitudes
            FROM "{schema}".organization org
            INNER JOIN "{schema}".location loc ON org.id = loc.organization_id
            LEFT JOIN "{schema}".address addr ON loc.id = addr.location_id AND addr.address_type = 'physical'
            GROUP BY org.id
            )
            -- Main query
            SELECT DISTINCT ON (org.id)
            org.id,
            '"{schema}"' AS contributor,
            org.name AS organization_name,
            org.alternate_name AS organization_alternate_name,
            org_loc.location_addresses AS location_address_1,
            org_loc.location_cities AS location_city,
            org_loc.location_postal_codes AS location_postal_code,
            org_loc.location_state_provinces AS location_state_province,
            org_loc.location_regions AS location_region,
            org.email AS organization_email,
            org.website AS organization_website,
            org_phones.organization_phone_numbers,  -- Using aggregated organization phone numbers
            org.description AS organization_description,
            org.id as org_id,
            cm.cluster_id,
            cm.model_confidence,
            cm.link_id
            FROM "{groupschemaName}".cluster_member cm
            INNER JOIN "{groupschemaName}".cluster cl ON cl.id=cm.cluster_id
            INNER JOIN "{groupschemaName}".model m ON m.id=cl.model_id
            INNER JOIN "{schema}".organization org ON org.id=cm.link_id::text
            LEFT JOIN organization_phones org_phones ON org_phones.organization_id = org.id
            LEFT JOIN organization_locations org_loc ON org_loc.organization_id = org.id
            cl.job_id='{job_id}'
            '''
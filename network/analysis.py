from database import CursorFromConnectionFromPool
from database import Database


def sampling_one_to_many():
    with CursorFromConnectionFromPool() as cursor:
        cursor.execute("SET search_path TO current, public;")
        cursor.execute("DROP TABLE IF EXISTS samplepoint_one_to_many")
        cursor.execute("SELECT DISTINCT ON (end_vid) *, agg_cost/60 as cost_m, samplepoint_vertice_comparison.geom AS pointgeom \
                        INTO samplepoint_one_to_many \
                        FROM samplepoint_vertice_comparison, pgr_dijkstra( \
                            'SELECT pk as id, source, target, costs as cost, reverse_costs as reverse_cost FROM merged_ways', \
                             2, (select array_agg(id::integer) as array \
                             FROM samplepoint_vertice_comparison), \
                             TRUE) \
                        WHERE samplepoint_vertice_comparison.id = end_vid order by end_vid, agg_cost desc;")


def sampling_many_to_many(schema):
    with CursorFromConnectionFromPool() as cursor:
        cursor.execute("SET search_path TO current, public;")
        cursor.execute("SELECT count(*) \
                        FROM samplepoint_vertice_comparison;")
        sample_data = cursor.fetchall()
        if sample_data:
            sample_count = sample_data[0][0]

    with CursorFromConnectionFromPool() as cursor:
        cursor.execute("CREATE SCHEMA IF NOT EXISTS {};".format(schema))
        cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis SCHEMA {};".format(schema))
        cursor.execute("CREATE EXTENSION IF NOT EXISTS pgrouting SCHEMA {};".format(schema))
        cursor.execute("SET search_path TO {}, public;".format(schema))


    for i in range(1, sample_count):
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute("DROP TABLE IF EXISTS samplepoint_one_to_many_{}".format(i))
            cursor.execute("SELECT DISTINCT ON (end_vid) *, agg_cost/60 AS cost_m, samplepoint_vertice_comparison.geom AS pointgeom \
                            INTO samplepoint_many_to_many_{0} \
                            FROM samplepoint_vertice_comparison, pgr_dijkstra( \
                                'SELECT pk as id, source, target, costs as cost FROM merged_ways', \
                                 {0}, (SELECT array_agg(id::integer) AS array \
                                 FROM samplepoint_vertice_comparison), \
                                 TRUE) \
                            WHERE samplepoint_vertice_comparison.id = end_vid order by end_vid, agg_cost desc;".format(i))
            print(i)

def complete_comparison():
    with CursorFromConnectionFromPool() as cursor:
        cursor.execute("SET search_path = current, public;")
        cursor.execute("DROP TABLE IF EXISTS samplepoint_vertice_comparison_curfur")
        cursor.execute("SELECT current.samplepoint_vertice_comparison.GEOM, \
                        current.samplepoint_vertice_comparison.id AS cur_id, \
                        future.samplepoint_vertice_comparison.id AS fur_id \
                        INTO samplepoint_vertice_comparison_curfur \
                        FROM current.samplepoint_vertice_comparison, future.samplepoint_vertice_comparison \
                        WHERE current.samplepoint_vertice_comparison.GEOM = future.samplepoint_vertice_comparison.GEOM")
        cursor.execute("ALTER TABLE samplepoint_vertice_comparison_curfur ADD COLUMN avg_cur_cost FLOAT8; \
                        ALTER TABLE samplepoint_vertice_comparison_curfur ADD COLUMN avg_fur_cost FLOAT8; \
                        ALTER TABLE samplepoint_vertice_comparison_curfur ADD COLUMN avg_cost FLOAT8;")

    with CursorFromConnectionFromPool() as cursor:
        cursor.execute("SELECT cur_id, fur_id FROM samplepoint_vertice_comparison_curfur")
        id_data = cursor.fetchall()
        cur_id = []
        fur_id = []
        if id_data:
            for i in range(len(id_data)):
                cur_id.append(id_data[i][0])
                fur_id.append(id_data[i][1])

    for i in range(len(id_data)):
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute("WITH cur as ( \
                            SELECT DISTINCT ON (end_vid) *, agg_cost/60 as curcost_m \
                            FROM current.samplepoint_vertice_comparison, pgr_dijkstra( \
                              'SELECT pk as id, source, target, costs as cost, reverse_costs as reverse_cost FROM current.merged_ways',"
                               + str(cur_id[i]) + ",(SELECT array_agg(id::integer) AS ARRAY \
                              FROM current.samplepoint_vertice_comparison), TRUE) \
                            WHERE current.samplepoint_vertice_comparison.id = end_vid ORDER BY end_vid, agg_cost DESC) \
                            UPDATE samplepoint_vertice_comparison_curfur SET avg_cur_cost = (SELECT (sum(curcost_m)/3215) from cur) \
                            WHERE current.samplepoint_vertice_comparison_curfur.cur_id = " + str(cur_id[i]))
            print(str(cur_id[i]))

    for i in range(len(id_data)):
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute("WITH fur as ( \
                            SELECT DISTINCT ON (end_vid) *, agg_cost/60 as furcost_m \
                            FROM future.samplepoint_vertice_comparison, pgr_dijkstra( \
                              'SELECT pk as id, source, target, costs as cost, reverse_costs as reverse_cost FROM future.merged_ways',"
                               + str(fur_id[i]) + ",(SELECT array_agg(id::integer) AS ARRAY \
                              FROM future.samplepoint_vertice_comparison), TRUE) \
                            WHERE future.samplepoint_vertice_comparison.id = end_vid ORDER BY end_vid, agg_cost DESC) \
                            UPDATE samplepoint_vertice_comparison_curfur SET avg_fur_cost = (SELECT (sum(furcost_m)/3215) from fur) \
                            WHERE current.samplepoint_vertice_comparison_curfur.fur_id = " + str(fur_id[i]))
            print(str(cur_id[i]))








Database.initialise(user='postgres', password='postgres', host='localhost', database='cph_network')
complete_comparison()
#sampling_many_to_many('one_to_many')

#sampling_one_to_many()

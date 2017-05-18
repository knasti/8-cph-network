from database import CursorFromConnectionFromPool
from database import Database

Database.initialise(user='postgres', password='postgres', host='localhost', database='cph_network')


def sampling_one_to_many():
    with CursorFromConnectionFromPool() as cursor:
        cursor.execute("SET search_path TO current;")
        cursor.execute("DROP TABLE IF EXISTS samplepoint_one_to_many")
        cursor.execute("SELECT DISTINCT ON (end_vid) *, agg_cost/60 as cost_m, samplepoint_vertice_comparison.geom AS pointgeom \
                        INTO samplepoint_one_to_many \
                        FROM samplepoint_vertice_comparison, pgr_dijkstra( \
                            'SELECT pk as id, source, target, costs as cost FROM merged_ways', \
                             2, (select array_agg(id::integer) as array \
                             FROM samplepoint_vertice_comparison), \
                             FALSE) \
                        WHERE samplepoint_vertice_comparison.id = end_vid order by end_vid, agg_cost desc;")


def sampling_many_to_many():
    with CursorFromConnectionFromPool() as cursor:
        cursor.execute("SET search_path TO current;")
        cursor.execute("SELECT count(*) \
                        FROM samplepoint_vertice_comparison;")
        sample_data = cursor.fetchall()
        if sample_data:
            sample_count = sample_data[0][0]

    for i in range(sample_count):
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute("SET search_path TO current;")
            cursor.execute("DROP TABLE IF EXISTS samplepoint_many_to_many_{}".format(i))
            cursor.execute("SELECT DISTINCT ON (end_vid) *, agg_cost/60 as cost_m, samplepoint_vertice_comparison.geom AS pointgeom \
                            INTO samplepoint_many_to_many_{0} \
                            FROM samplepoint_vertice_comparison, pgr_dijkstra( \
                                'SELECT pk as id, source, target, costs as cost FROM merged_ways', \
                                 {0}, (select array_agg(id::integer) as array \
                                 FROM samplepoint_vertice_comparison), \
                                 FALSE) \
                            WHERE samplepoint_vertice_comparison.id = end_vid order by end_vid, agg_cost desc;".format(i))
            print(i)



sampling_many_to_many()

# sampling_one_to_many()

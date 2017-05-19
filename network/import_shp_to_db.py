import os, re, ogr
from database import CursorFromConnectionFromPool


def import_shp_to_db(network):
    # Before running this script do the following:
    # 1: Change rootdir to the folder in which you have your shapefiles
    # 2: Create a database that has extensions postgis and pgrouting
    # 3: Change database credentials

    # network = 0, current network
    # network = 1, future network
    # Determines which network that should be builded and setting variables and schemas accordingly
    if network == 0:
        # Creates schema for current network
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute("DROP SCHEMA IF EXISTS current CASCADE;")
            cursor.execute("CREATE SCHEMA current;")
            cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
            cursor.execute("CREATE EXTENSION IF NOT EXISTS pgrouting;")
            cursor.execute("SET search_path = current, public;")

        curfur_fname = 'cur'
    else:
        # Creates schema for future network
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute("DROP SCHEMA IF EXISTS future CASCADE;")
            cursor.execute("CREATE SCHEMA future;")
            cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
            cursor.execute("CREATE EXTENSION IF NOT EXISTS pgrouting;")
            cursor.execute("SET search_path = future, public;")


        curfur_fname = 'fur'

    rootdir = r'C:\Users\Bruger\Dropbox\myUniversity\2. secondSemester\8. Semester\3. Data\public_transport_data'
    os.chdir(rootdir)

    # Creating an empty list where shapefile-paths can be stored
    src_file = []

    # Creating an empty list for table-names
    table_names = []

    # Creating an empty list to store transport type
    transport = []


    # looping through directories and sub directories to find files
    for subdir, dirs, files in os.walk(rootdir):
        for f in files:
            # Storing the file paths in a list
            temp_file = os.path.join(subdir, f)

            # Making sure to only save shapefile-paths with network ways
            # curfur_fname determines whether or not the file is in the current or future network
            if temp_file.endswith('.shp') and 'ways' in temp_file and curfur_fname in temp_file:
                # Appending shapefile paths to a list
                src_file.append(temp_file)

                # Appending filenames to a list to use for table names
                table_names.append(re.sub('\.shp$', '', f))

                # Storing transport type
                if subdir.endswith('train'):
                    transport.append('train')
                elif subdir.endswith('metro'):
                    transport.append('metro')
                elif subdir.endswith('pedestrian'):
                    transport.append('pedestrian')
                elif subdir.endswith('Bus_ways'):
                    transport.append('bus')
                else:
                    transport.append('unknown')

    # Status for the user
    print('Shapefiles and table names stored in lists')

    for k in range(len(src_file)):
        # Creating a new table in postgres with the necessary columns
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute("DROP TABLE IF EXISTS {}".format(table_names[k]))
            cursor.execute("CREATE TABLE {} ( \
                                name VARCHAR(80), \
                                geom GEOMETRY, \
                                time_const FLOAT8, \
                                transport VARCHAR, \
                                line_number VARCHAR, \
                                connector INTEGER \
                                );".format(table_names[k]))
        # Opening the shapefile
        shapefile = ogr.Open(src_file[k])

        # Getting the layer from the shapefile
        layer = shapefile.GetLayer()

        # Looping through all features of the shapefile
        for i in range(layer.GetFeatureCount()):
            feature = layer.GetFeature(i)
            name = repr(feature.GetField("name"))
            wkt = feature.GetGeometryRef().ExportToWkt()
            time_const = feature.GetField("time_const")
            connector = feature.GetField("connector")
            line_number = feature.GetField("l_number")
            # Storing the applicable values
            with CursorFromConnectionFromPool() as cursor:
                cursor.execute("INSERT INTO {} (name, geom, time_const, connector, line_number) \
                                VALUES (%s, ST_GeometryFromText(%s, 4326), %s, %s, %s);".format(table_names[k]),
                                    [name, wkt, time_const, connector, line_number])

        # Adding transport and line_number to the tables
        with CursorFromConnectionFromPool() as cursor:
            cursor.execute("UPDATE {} SET transport = '{}';".format(table_names[k], transport[k]))

    # Status for the user
    print('Shapefiles are now imported as tables along with data from the attribute tables')

    # Creating an empty string for the union_tables
    union_tables = ''

    # Making union SQL-string for the temporary tables
    for i in range(len(table_names)):
        if i < len(table_names) - 1:
            union_tables = union_tables + " SELECT * FROM {} UNION ALL".format(table_names[i])
        else:
            union_tables = union_tables + " SELECT * FROM {}".format(table_names[i])

    # Creating the complete network through unions of the existing tables
    with CursorFromConnectionFromPool() as cursor:
        cursor.execute("DROP TABLE IF EXISTS merged_ways")
        cursor.execute("DROP TABLE IF EXISTS merged_ways_vertices_pgr")
        cursor.execute("SELECT * INTO merged_ways \
                        FROM (" + union_tables + ")t")

    # Creating source, target for network topology as well as a primary key for the table
    with CursorFromConnectionFromPool() as cursor:
        cursor.execute("ALTER TABLE merged_ways ADD COLUMN pk BIGSERIAL PRIMARY KEY; \
                        ALTER TABLE merged_ways ADD COLUMN source INTEGER; \
                        ALTER TABLE merged_ways ADD COLUMN target INTEGER; \
                        ALTER TABLE merged_ways ADD COLUMN spatial_length FLOAT8; \
                        ALTER TABLE merged_ways ADD COLUMN time_calc INTEGER; \
                        ALTER TABLE merged_ways ADD COLUMN costs INTEGER; \
                        ALTER TABLE merged_ways ADD COLUMN reverse_costs INTEGER;")

    # Calculate lengths of the ways in the network
    with CursorFromConnectionFromPool() as cursor:
        cursor.execute("UPDATE merged_ways AS mw_1 SET spatial_length = ST_Length(ST_Transform(mw_2.geom, 25832)) \
                        FROM merged_ways AS mw_2 \
                        WHERE mw_1.pk = mw_2.pk;")

    # Creating the network topology with pgrouting
    with CursorFromConnectionFromPool() as cursor:
        cursor.execute("SELECT pgr_createtopology('merged_ways', 0.0000001, 'geom', 'pk');")

    # Status for the user
    print('All tables have been merged into one table, merged_ways, and topology has been created')

    # Creating table to store sampling points on the network
    with CursorFromConnectionFromPool() as cursor:
        cursor.execute("DROP TABLE IF EXISTS sampling_points_on_network")
        cursor.execute("CREATE TABLE sampling_points_on_network ( \
                            FID INTEGER, \
                            geom GEOMETRY \
                            );")

    # Looping through directories and sub directories to find the point sampling file
    for subdir, dirs, files in os.walk(rootdir):
        for f in files:
            # Storing the file paths in a list
            temp_file = os.path.join(subdir, f)

            # Finding the sampling shapefile
            if temp_file.endswith('.shp') and 'sampling' in temp_file:
                # Appending shapefile paths to a list
                sampling_file = temp_file

    # Opening the sampling shapefile
    shapefile = ogr.Open(sampling_file)

    # Getting the layer from the shapefile
    layer = shapefile.GetLayer()

    # Looping through all features of the shapefile
    for i in range(layer.GetFeatureCount()):
        feature = layer.GetFeature(i)
        fid = repr(feature.GetField("FID"))
        wkt = feature.GetGeometryRef().ExportToWkt()
        # Storing the applicable values
        with CursorFromConnectionFromPool() as cursor:
            # Inserting data from the shapefile into the table in postgres
            cursor.execute("INSERT INTO sampling_points_on_network (FID, geom) \
                            VALUES (%s, ST_GeometryFromText(%s, 4326));", [fid, wkt])

    # Matching the geometry of sampling vertices to the ID of the network topologies
    with CursorFromConnectionFromPool() as cursor:
        cursor.execute("DROP TABLE IF EXISTS samplepoint_vertice_comparison")
        cursor.execute("SELECT merged_ways_vertices_pgr.id, sampling_points_on_network.geom as geom \
                        INTO samplepoint_vertice_comparison \
                        FROM sampling_points_on_network, merged_ways_vertices_pgr \
                        WHERE sampling_points_on_network.geom && merged_ways_vertices_pgr.the_geom;")

    print('Sampling points have been imported to the database')

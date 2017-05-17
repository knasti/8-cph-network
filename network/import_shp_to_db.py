import os, re, ogr
from database import CursorFromConnectionFromPool


def import_shp_to_db(network):
    # network = 0, current network
    # network = 1, future network

    # Before running this script do the following:
    # 1: Change rootdir to the folder in which you have your shapefiles
    # 2: Create a database that has extensions postgis and pgrouting
    # 3: Change database credentials
    if network == 0:
        rootdir = r'C:\Users\Bruger\Dropbox\myUniversity\2. secondSemester\8. Semester\3. Data\public_transport_data\current'
    else:
        rootdir = r'C:\Users\Bruger\Dropbox\myUniversity\2. secondSemester\8. Semester\3. Data\public_transport_data\future'

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
            if temp_file.endswith('.shp') and 'ways' in temp_file:
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
    print('Files stored in lists')

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
                        ALTER TABLE merged_ways ADD COLUMN costs INTEGER;")

    # Calculate lengths of the ways in the network
    with CursorFromConnectionFromPool() as cursor:
        cursor.execute("UPDATE merged_ways AS mw_1 SET spatial_length = ST_Length(ST_Transform(mw_2.geom, 25832)) \
                        FROM merged_ways AS mw_2 \
                        WHERE mw_1.pk = mw_2.pk;")

    # Creating the network topology with pgrouting
    with CursorFromConnectionFromPool() as cursor:
        cursor.execute("SELECT pgr_createtopology('merged_ways', 0.00001, 'geom', 'pk');")

    # Status for the user
    print('All tables have been merged into one table, merged_ways, and topology has been created')

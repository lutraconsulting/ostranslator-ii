SCH=$1
echo "schema $SCH"

psql -U postgres -d os2_7 -c "CREATE SCHEMA IF NOT EXISTS $SCH"

ogr2ogr -overwrite -lyr_transaction -f PostgreSQL --config GML_EXPOSE_FID NO --config PG_USE_COPY YES --config GML_GFS_TEMPLATE "/home/peter/repo_L/ostranslator-ii/OSTranslatorII/gfs/OS Mastermap Topography (v7).gfs" "PG:dbname='os2_7' host='localhost' port='5432' active_schema=$SCH user='postgres'" "/vsigzip//home/peter/repo_L/ostranslator-ii/OSTranslatorII/gfs/OS Mastermap Topography (v7) Pioneer.gz" -lco OVERWRITE=YES -lco SPATIAL_INDEX=OFF -lco PRECISION=NO

ogr2ogr -append -lyr_transaction -f PostgreSQL --config GML_EXPOSE_FID NO --config PG_USE_COPY YES --config GML_GFS_TEMPLATE "/home/peter/repo_L/ostranslator-ii/OSTranslatorII/gfs/OS Mastermap Topography (v7).gfs" "PG:dbname='os2_7' host='localhost' port='5432' active_schema=$SCH user='postgres'" /vsigzip//home/peter/Downloads/os/7214215-NT6370-10i43.gz

psql -U postgres -d os2_7 -c "SELECT COUNT(*) FROM $SCH.topographicarea;"

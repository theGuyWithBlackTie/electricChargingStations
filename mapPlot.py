import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import geopandas as gpd
from shapely.geometry import Point, Polygon

sf_path = "maps/gis_osm_landuse_a_free_1.shp"
sf = gpd.read_file(sf_path, encoding='utf-8')
sf.head()  
sf_path = "maps/AA_City_Boundary.shp"
sf = gpd.read_file(sf_path, encoding='utf-8')
#stgo_sf = sf[sf.name == 'Santiago']
stgo_shape = sf.to_crs({'init': 'epsg:4326'})
stgo_shape 
stgo_shape.plot()

roads_path = "maps/gis_osm_roads_free_1.shp"
roads = gpd.read_file(roads_path, encoding='utf-8')

roads = gpd.sjoin(roads, stgo_shape, op='intersects')
roads.plot()
roads.fclass.value_counts()

car_roads = roads
car_roads = car_roads[(car_roads.fclass == 'service') |
                       (car_roads.fclass == 'residential') |
                       (car_roads.fclass == 'tertiary') |
                       (car_roads.fclass == 'secondary') |
                       (car_roads.fclass == 'primary') |
                       (car_roads.fclass == 'motorway') |
                       (car_roads.fclass == 'secondary_link') |
                       (car_roads.fclass == 'motorway_link') |
                       (car_roads.fclass == 'living_street') |
                       (car_roads.fclass == 'tertiary_link') |
                       (car_roads.fclass == 'primary_link')
                      ]
car_roads.plot()

fig, ax = plt.subplots(figsize = (25, 25))
car_roads.plot(ax = ax)
car_roads.show()

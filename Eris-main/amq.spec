t1:=(climate_temperature(date = '20200101D'));
t2:=(t1[latitude SUM sea_surface_temperature]);
t3:=(copernicus_temperature(date = '20200101'));
t4:=(t3[latitude SUM sea_surface_temperature]);
t5:=(t2 DUNION[src] t4)[COAL src]


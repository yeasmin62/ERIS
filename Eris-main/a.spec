t1:=(climate_temperature(date = '20200101D'));
t4:=((t1{counter:=1})[date SUM sea_surface_temperature,counter]);
t5:=(t4{avg_sst:=sea_surface_temperature/counter})[avg_sst]
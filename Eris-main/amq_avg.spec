t1:=(climate_temperature(date = '20200101D'));
t2:=((t1{counter:=1})[latitude SUM sea_surface_temperature,counter]);
t3:=(t2{avg_sst:=sea_surface_temperature/counter})[avg_sst];
t4:=(copernicus_temperature(date = '20200101'));
t5:=((t4{counter:=1})[latitude SUM sea_surface_temperature,counter]);
t6:=(t5{avg_sst:=sea_surface_temperature/counter})[avg_sst];
t7:= (t3 DUNION[src] t6)[COAL src]

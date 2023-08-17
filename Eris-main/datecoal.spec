t1:=(climate_temperature);
t2:=(t1 JOIN semiday{id->date});
t3:=(t2(dateofsemiday = '20200101'));
t4:=((t3{counter:=1})[dateofsemiday,latitude,longitude SUM sea_surface_temperature,counter]);
t5:=(t4{avg_sst:=sea_surface_temperature/counter})[avg_sst];
t6:=(copernicus_temperature{date->dateofsemiday});
t7:=(t6(dateofsemiday='20200101'));
t8:=(t7{sea_surface_temperature->avg_sst})[avg_sst];
t9:=(t5 DUNION[src] t8)[COAL src]
t1:=(pathfinder_temperature);
t2:=(t1 JOIN semiday{id->date});
t3:=(t2(dateofsemiday = '20200101'));
t4:=((t3{counter:=1})[dateofsemiday,latitude,longitude SUM sea_surface_temperature,counter]);
t5:=((t4{sst:=sea_surface_temperature/counter}){latitude->degree004});
t6:=((t5 JOIN resolution004){degree004->la});
t7:=(((t6{degree020->latitude}){longitude->degree004}) JOIN resolution004{degree020->longitude});
t8:=((t7{counter1:=1})[latitude,longitude SUM sst, counter1]);
t9:=(t8{avg_sst:= sst/counter1})[avg_sst];
t10:=(copernicus_temperature{date->dateofsemiday});
t11:=(t10(dateofsemiday='20200101'));
t12:=((t11{latitude->degree005}) JOIN resolution005);
t13:=((t12{degree005->la}){degree020->latitude});
t14:=((t13{longitude->degree005}) JOIN (resolution005{degree020->longitude}));
t15:=((t14{counter:=1})[latitude,longitude SUM sea_surface_temperature, counter]);
t16:=(t15{avg_sst:=sea_surface_temperature/counter})[avg_sst];
t17:=(t9 DUNION[src] t16)[COAL src]
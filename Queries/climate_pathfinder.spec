t1:=(climate_temperature);
t2:=(t1 JOIN semiday{id->date});
t3:=(t2(dateofsemiday = '20200101'));
t4:=((t3{counter:=1})[dateofsemiday,latitude,longitude SUM sea_surface_temperature,counter]);
t5:=((t4{sst:=sea_surface_temperature/counter}){latitude->degree005});
t6:=((t5 JOIN resolution005){degree005->la});
t7:=(((t6{degree020->latitude}){longitude->degree005}) JOIN resolution005{degree020->longitude});
t8:=((t7{counter1:=1})[latitude,longitude SUM sst, counter1]);
t9:=(t8{avg_sst:= sst/counter1})[avg_sst];
t10:=(pathfinder_temperature);
t11:=(t10 JOIN semiday{id->date});
t12:=(t11(dateofsemiday = '20200101'));
t13:=((t12{counter:=1})[dateofsemiday,latitude,longitude SUM sea_surface_temperature,counter]);
t14:=((t13{sst:=sea_surface_temperature/counter}){latitude->degree004});
t15:=((t14 JOIN resolution004){degree004->la});
t16:=(((t15{degree020->latitude}){longitude->degree004}) JOIN resolution004{degree020->longitude});
t17:=((t16{counter1:=1})[latitude,longitude SUM sst, counter1]);
t18:=(t17{avg_sst:= sst/counter1})[avg_sst];
t19:= (t9 DUNION[src] t18)[COAL src] 
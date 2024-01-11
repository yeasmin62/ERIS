t1:=(climate_temperature);
t2:=(t1(date='20200101D'));
t3:=((t2{latitude->degree005}) JOIN resolution005);
t4:=((t3{degree005->la}){degree020->latitude});
t5:=((t4{longitude->degree005}) JOIN (resolution005{degree020->longitude}));
t6:=((t5{counter:=1})[latitude,longitude SUM sea_surface_temperature, counter]);
t7:=(t6{avg_sst:=sea_surface_temperature/counter})[avg_sst];
t8:=(pathfinder_temperature);
t9:=(t8(date='20200101D'));
t10:=((t9{latitude->degree004}) JOIN resolution004);
t11:=((t10{degree004->la}){degree020->latitude});
t12:=((t11{longitude->degree004}) JOIN (resolution004{degree020->longitude}));
t13:=((t12{counter:=1})[latitude,longitude SUM sea_surface_temperature, counter]);
t14:=(t13{avg_sst:=sea_surface_temperature/counter})[avg_sst];
t15:=(t7 DUNION[src] t14)[COAL src]

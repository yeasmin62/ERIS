t1:=(pathfinder_temperature);
t2:=(t1(date>='20200101D' and date <= '20200103D'));
t3:=((t2{latitude->degree004}) JOIN resolution004);
t4:=((t3{degree004->la}){degree020->latitude});
t5:=((t4{longitude->degree004}) JOIN (resolution004{degree020->longitude}));
t6:=((t5{counter:=1})[latitude,longitude SUM sea_surface_temperature, counter]);
t7:=(t6{avg_sst:=sea_surface_temperature/counter})[avg_sst];
t8:=(modisaqua_temperature);
t9:=(t8(date>='20200101D' and date <= '20200103D'));
t10:=(t9{sst:=sea_surface_temperature+273.15});
t11:=((t10{latitude->degree004}) JOIN resolution004);
t12:=((t11{degree004->la}){degree020->latitude});
t13:=((t12{longitude->degree004}) JOIN (resolution004{degree020->longitude}));
t14:=((t13{counter:=1})[latitude,longitude SUM sst, counter]);
t15:=(t14{avg_sst:=sst/counter})[avg_sst];
t16:=(t7 DUNION[src] t15)[COAL src]
t1:=(climate_temperature);
t2:=(t1 JOIN semiday{id->date});
t3:=(t2(dateofsemiday = '20200101'));
t4:=((t3{counter:=1})[dateofsemiday,latitude,longitude SUM sea_surface_temperature,counter]);
t5:=((t4{sst:=sea_surface_temperature/counter}){latitude->degree005});
t6:=((t5 JOIN resolution005){degree005->la});
t7:=(((t6{degree020->latitude}){longitude->degree005}) JOIN resolution005{degree020->longitude});
t8:=((t7{counter1:=1})[latitude,longitude SUM sst, counter1]);
t9:=(t8{avg_sst:= sst/counter1})[avg_sst];
t10:=(copernicus_temperature{date->dateofsemiday});
t11:=(t10(dateofsemiday='20200101'));
t12:=((t11{latitude->degree005}) JOIN resolution005);
t13:=((t12{degree005->la}){degree020->latitude});
t14:=((t13{longitude->degree005}) JOIN (resolution005{degree020->longitude}));
t15:=((t14{counter:=1})[latitude,longitude SUM sea_surface_temperature, counter]);
t16:=(t15{avg_sst:=sea_surface_temperature/counter})[avg_sst];
t17:=(t9 DUNION[src] t16)[COAL src];
t18:=(pathfinder_temperature);
t19:=(t18 JOIN semiday{id->date});
t20:=(t19(dateofsemiday = '20200101'));
t21:=((t20{counter:=1})[dateofsemiday,latitude,longitude SUM sea_surface_temperature,counter]);
t22:=((t21{sst:=sea_surface_temperature/counter}){latitude->degree004});
t23:=((t22 JOIN resolution004){degree004->la});
t24:=(((t23{degree020->latitude}){longitude->degree004}) JOIN resolution004{degree020->longitude});
t25:=((t24{counter1:=1})[latitude,longitude SUM sst, counter1]);
t26:=(t25{avg_sst:= sst/counter1})[avg_sst];
t27:=(modisaqua_temperature{date->dateofsemiday});
t28:=(t27(dateofsemiday='20200101'));
t29:=(t28{sst:=sea_surface_temperature+273.15});
t30:=((t29{latitude->degree004}) JOIN resolution004);
t31:=((t30{degree004->la}){degree020->latitude});
t32:=((t31{longitude->degree004}) JOIN (resolution004{degree020->longitude}));
t33:=((t32{counter:=1})[latitude,longitude SUM sst, counter]);
t34:=(t33{avg_sst:=sst/counter})[avg_sst];
t35:=(t26 DUNION[src1] t34)[COAL src1];
t36:=(t17 DUNION[src2] t35)[COAL src2]
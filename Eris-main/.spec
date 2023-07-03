t1:=(copernicus_temperature(date = '20200101'));
t2:=((t1{latitude->degree001}) JOIN resolution001);
t3:=((t2{degree001->la}){degree004->latitude});
t4:=((t3{longitude->degree001}) JOIN (resolution001{degree004->longitude}));
t5:=((t4{counter:=1})[latitude,longitude SUM chlorophyll, counter]);
t6:=(t5{avg_clr:=chlorophyll/counter})[avg_clr];



t8:=(modisaqua_temperature(date = '20200101'));
t9:=((t8{counter:=1})[latitude,longitude SUM chlorophyll, counter]);
t10:=(t9{avg_clr:=sea_surface_temperature/counter})[avg_clr];
t11:=(t6 DUNION[src] t11)[COAL src]
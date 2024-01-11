t1:=(climate_temperature[latitude SUM sea_surface_temperature]);
t2:= (copernicus_temperature[latitude SUM sea_surface_temperature]);
t3:=(t1 DUNION[discr] t2)[COAL discr]

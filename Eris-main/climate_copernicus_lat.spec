t1:=((climate_temperature(longitude = '#1'))[longitude SUM sea_surface_temperature]);
t2:= ((copernicus_temperature(longitude = '#1'))[longitude SUM sea_surface_temperature]);
t3:=(t1 DUNION[discr] t2)[COAL discr]

SELECT *
FROM climate_temperature
WHERE date ='20200101D' AND sea_surface_temperature IS NULL AND (latitude >= '+030.10 ~ +030.15' AND latitude <= '+030.35 ~ +030.40') AND (longitude >= '-017.60 ~ -017.55' AND longitude <= '-017.40 ~ -017.35')
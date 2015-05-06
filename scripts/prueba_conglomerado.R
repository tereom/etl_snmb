library(dplyr)
library(stringr)
library(tidyr)

base_input <- src_sqlite("../datos/base_input.db", create = T)

### Leer tabla conglomerates

conglomerates <- tbl(base_input, "conglomerates")
conglomerates_df <- collect(conglomerates)

conglomerado_muestra <- conglomerates_df %>%
  filter(nchar(name) <= 5)


sitio_lat <- conglomerado_muestra %>%
  select(name, central_lat, transect2_lat, transect3_lat, transect4_lat) %>%
  gather(sitio, lat, -name) %>%
  mutate(sitio = str_extract(sitio, "[[:alnum:]]+"))

sitio_lon <- conglomerado_muestra %>%
  select(name, central_lon, transect2_lon, transect3_lon, transect4_lon) %>%
  gather(sitio, lon, -name) %>%
  mutate(sitio = str_extract(sitio, "[[:alnum:]]+"))

sitio_altitude <- conglomerado_muestra %>%
  select(name, central_altitude, transect2_altitude, transect3_altitude, transect4_altitude) %>%
  gather(sitio, altitude, -name) %>%
  mutate(sitio = str_extract(sitio, "[[:alnum:]]+"))

sitio_ellipsoid <- conglomerado_muestra %>%
  select(name, central_ellipsoid, transect2_ellipsoid, transect3_ellipsoid, transect4_ellipsoid) %>%
  gather(sitio, ellipsoid, -name) %>%
  mutate(sitio = str_extract(sitio, "[[:alnum:]]+"))

sitio_gps_error <- conglomerado_muestra %>%
  select(name, central_gps_error, transect2_gps_error, transect3_gps_error, transect4_gps_error) %>%
  gather(sitio, gps_error, -name) %>%
  mutate(sitio = str_extract(sitio, "[[:alnum:]]+"))

sitios <- inner_join(sitio_lat, sitio_lon) %>%
  inner_join(sitio_altitude) %>%
  inner_join(sitio_ellipsoid) %>%
  inner_join(sitio_gps_error)

sitios
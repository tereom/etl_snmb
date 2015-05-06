# El paquete RSQLite se utiliza para conectarse a las bases de datos (fases de
# Extract y Load). El paquete de iterators se utilizará en la generación de id's
library(RSQLite)
library(iterators)
library(dplyr)
library(tidyr)
library(stringr)

######################################
# Leyendo las bases de datos
######################################
# Leyendo la base de datos original
base_input <- src_sqlite("../datos/base_input.db")

# Creando la base de datos de salida
base_output <- src_sqlite("../datos/base_output.db", create=T)
######################################

######################################
# Instanciando los generadores de id's:
######################################
generador_conglomerado_muestra_id <- icount()
# nextElem(conglomerado_muestra_id)
generador_sitio_muestra_id <- icount()
# nextElem(sitio_muestra_id)
######################################

######################################
# Creando las tablas "Conglomerado_muestra" y "Sitio_muestra", a partir de la
# tabla "conglomerates", como se especifica en el esquema: "mapeo_v2".
######################################

# Leyendo tabla conglomerates y transformándola a data frame
conglomerates <- tbl(base_input, "conglomerates")
conglomerates_df <- collect(conglomerates)

# Filtrando los conglomerados cuya clave tenga más de 5 dígitos (con errores)
conglomerates_filtrado <- conglomerates_df %>%
  filter(nchar(name) <= 5)

# Creando tabla "Conglomerado_muestra_id"
#####################################

# Generando "conglomerado_muestra_id":
conglomerado_muestra_id <- c()
for(i in 1:dim(conglomerates_filtrado)[1])
{
  conglomerado_muestra_id <- c(conglomerado_muestra_id, nextElem(generador_conglomerado_muestra_id))
}

# Agregando la nueva columna al data frame "conglomerates_filtrado"
conglomerates_filtrado_id <- data.frame(conglomerates_filtrado, conglomerado_muestra_id)

# Cambiando el nombre y tipo de las columnas, para hacerlas compatibles con el nuevo
# cliente:

#rename(conglomerates_filtrado_id, c("id"="conglomerate_id", "name"="nombre", "visit_date"="fecha_visita",))

# Separar longitud, altitud, ...

sitio_lat <- conglomerates_filtrado_id %>%
  select(id, conglomerado_muestra_id, central_lat, transect2_lat, transect3_lat, transect4_lat) %>%
  gather(sitio, lat, -id, -conglomerado_muestra_id) %>%
  mutate(sitio = str_extract(sitio, "[[:alnum:]]+"))
# Gather crea por cada campo especificado, un registro, con dos campos (más los
# campos no especificados como name): nombre del campo original y valor.

sitio_lon <- conglomerates_filtrado_id %>%
  select(id, conglomerado_muestra_id, central_lat, transect2_lat, transect3_lat, transect4_lat) %>%
  gather(sitio, lat, -id, -conglomerado_muestra_id) %>%
  mutate(sitio = str_extract(sitio, "[[:alnum:]]+"))

sitio_altitude <- conglomerates_filtrado_id %>%
  select(id, conglomerado_muestra_id, central_lat, transect2_lat, transect3_lat, transect4_lat) %>%
  gather(sitio, lat, -id, -conglomerado_muestra_id) %>%
  mutate(sitio = str_extract(sitio, "[[:alnum:]]+"))

sitio_ellipsoid <- conglomerates_filtrado_id %>%
  select(id, conglomerado_muestra_id, central_lat, transect2_lat, transect3_lat, transect4_lat) %>%
  gather(sitio, lat, -id, -conglomerado_muestra_id) %>%
  mutate(sitio = str_extract(sitio, "[[:alnum:]]+"))

sitio_gps_error <- conglomerates_filtrado_id %>%
  select(id, conglomerado_muestra_id, central_lat, transect2_lat, transect3_lat, transect4_lat) %>%
  gather(sitio, lat, -id, -conglomerado_muestra_id) %>%
  mutate(sitio = str_extract(sitio, "[[:alnum:]]+"))

sitios <- inner_join(sitio_lat, sitio_lon) %>%
  inner_join(sitio_altitude) %>%
  inner_join(sitio_ellipsoid) %>%
  inner_join(sitio_gps_error)

#sitios$date <- today()
sitios

copy_to(base_output, sitios, name = "sitios_id", temporary = FALSE)
base_output

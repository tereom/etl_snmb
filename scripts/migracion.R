
# Migracion de primera versión

# Supuestos:
# 
# 1. Los clientes finales están almacenados en carpetas de la forma 
# resources/db/snmb.db, esto supone que los clientes en archives/db/snmb.db son
# respaldos.
# 
# 2. Cada vez que encontramos un conglomerado en un cliente final, este 
# se ingresó completo, implica que si encontramos un mismo conglomerado en 
# más de un cliente estos registros serán idénticos.
# 
# Cargamos los paquetes, RSQLite nos sirve para escribir las tablas en una base
# de datos Sqlite.

#setwd("~/Documents/Documentos en uso/etl_cliente/scripts")

library(plyr)
library(dplyr)
library(tidyr)
library(stringr)
library(RSQLite)
library(RPostgreSQL)

### Copiamos los archivos de J a una carpeta local.
dir_j = "/Volumes/sacmod"

# Obtenemos rutas de bases de la forma snmb.db localizadas en dir_j
bases_rutas <- list.files(path = dir_j, 
  recursive = TRUE, full.names = TRUE, pattern = "\\.db$")

# bases con nombre igual al número de conglomerado
snmb_num <- c("46239.db","47586.db","50927.db","55160.db","55652.db","56379.db",
  "57124.db","57627.db","57905.db","58143.db","58388.db","58650.db","59144.db",
  "60406.db","60442.db","60655.db","61164.db","61225.db","61718.db","61736.db",
  "61925.db","62713.db","63192.db","63211.db","63432.db","63504.db","63955.db",
  "63987.db","64223.db","66289.db","66549.db","66847.db","67141.db","67145.db","67435.db")

snmb_nom <- 1:length(bases_rutas) %in% grep("resources/db/snmb.db", 
  bases_rutas, invert = F)
bases_snmb <- bases_rutas[snmb_nom | (basename(bases_rutas) %in% snmb_num)]
basename(bases_snmb)

write.table(bases_snmb, file = "../datos/adicionales/rutas_bases_snmb.csv", 
  sep = ",")

# copiamos a carpeta local
copiaRenombra <- function(dir_j_archivo, dir_local, id_db){
  # dir_j: directorio (con nombre archivo) donde se ubica la base a copiar
  # dir_local: directorio (sin nombre de archivo) donde se copiará la base
  # id_db: id numérico que se utilizará para distinguir bases en migración
  file.copy(dir_j_archivo, dir_local,  overwrite = FALSE)
  file.rename(from = paste(dir_local, basename(dir_j_archivo), sep = "/"), 
    to = paste(dir_local, "/", id_db, "_", "snmb.db", sep = ""))
}

snmb_id <- sapply(1:length(bases_snmb), function(i) 
   copiaRenombra(bases_snmb[i], 
   "../datos/bases_snmb_2", 1000*i))

# leerDbTab: función lee la base de datos (dir), extrae una tabla (tabla) y 
# regresa un _data frame_ con la tabla.
leerDbTab <- function(dir, tabla){
  base_input <- src_sqlite(dir)
  tabla <- tbl(base_input, tabla)
  tabla_df <- collect(tabla)
}

### Exportación de tablas
### conglomerates -> Conglomerado_muestra y Sitio_muestra

# 1. Leemos las tablas de conglomerado y creamos un id utilizando la parte 
# numérica del nombre de la base de datos y la columna id de la misma.

# Leemos las rutas de las bases de datos de entrada
bases_rutas <- list.files(path = "../datos/bases_snmb", 
  recursive = TRUE, full.names = TRUE, pattern = "\\.db$")
names(bases_rutas) <- extract_numeric(basename(bases_rutas))

# ldply asigna automáticamente el nombre del elemento de la lista como primera
# variable en el data-frame de salida
conglomerado_tabs <- ldply(bases_rutas, leerDbTab, "conglomerates")

conglomerado <- conglomerado_tabs %>%
  filter(nchar(name) <= 5) %>%  # quitamos conglomerados con nombres mal
  mutate(
    id = as.numeric(paste(.id, id, sep = "")) # creamos nueva columna id
    # Notar que al crear los id's de esta manera, eliminamos la necesidad de hacer
    # joins para obtener las llaves foráneas, ya que las conocemos de antemano.
    ) %>%
  group_by(name) %>%
    # Como hay bases de datos distintas con intersecciones no vacías, al agrupar
    # nos quedamos con el primer registro de cada conglomerado encontrado
    summarise_each(funs(first)) %>%
  ungroup() %>%
  select(-.id) # eliminamos la columna auxiliar .id

# Veamos algunos diagnósticos
table(conglomerado_tabs$name)[table(conglomerado_tabs$name) > 1]
# hay casos en los que tenemos un mismo conglomeradoo en 131 clientes (8745, 8917)
subset(conglomerado_tabs, name=="8917")
bases_snmb[109:127]

## 2. Separamos en las dos tablas Conglomerado\_muestra y Sitio\_muestra

# Importando el catálogo de municipio, ya que algunos registros contienen el muni-
# cipio numéricamente
cat_municipio <- unique(read.csv("../datos/adicionales/municipios.csv", 
  colClasses = "character", header = TRUE))

cat_municipio <- cat_municipio %>%
  mutate(CVE_ENT_MUN = paste(CVE_ENT, CVE_MUN, sep=""))

Conglomerado_muestra <- conglomerado %>%
  # Las operaciones entre el group_by() y el ungroup se hacen a nivel de id.
  # El default en dplyr es por columnas
  group_by(id) %>%
  mutate(
    municipio_temp = ifelse(municipio %in% cat_municipio$CVE_ENT_MUN,
      cat_municipio$NOM_MUN[cat_municipio$CVE_ENT_MUN == municipio], municipio)
    ) %>%
  ungroup() %>%
  mutate(
    nombre = as.integer(name), 
    fecha_visita = visit_date, 
    compania = NA_character_,
    tipo = revalue(conglomerate_type, c(
      "01) Inicial" = "1 Inicial",
      "03) Inaccesible 1" = "3 Inaccesible terreno/clima",
      "04) Inaccesible 2" = "4 Inaccesible social",
      "05) Inaccesible 3" = "5 Inaccesible gabinete",
      "07) Biodiversidad" = "7 Biodiversidad")),
    estado = state,
    municipio = municipio_temp,
    tenencia = revalue(property, c(
      "01) Ejidal" = "1 Ejidal",
      "02) Comunal" = "2 Comunal",
      "03) Propriedad Particular" = "3 Propiedad particular",
      "04) Propriedad Federal" = "4 Propiedad federal")),
    uso_suelo_tipo = NA_character_, 
    monitoreo_tipo = "SAC-MOD",
    # El campo de institución se agregó en el esquema v12
    institucion = "CONAFOR",
    vegetacion_tipo = NA_character_, 
    perturbado = NA,
    comentario = comment
    ) %>%
  # los campos se deben seleccionar en el orden en el que serán insertados
  select(id, nombre, fecha_visita, predio, compania, tipo, estado, municipio,
    tenencia, uso_suelo_tipo, monitoreo_tipo, institucion, vegetacion_tipo, perturbado,
    comentario)

# función para crear tablas de la forma: |id|sitio|valor|, para cada variable de
# latitud, longitud, etc. El nombre de la variable se obtiene del ldply.
separarSitios <- function(base_df, variable){
  base_var <- base_df  %>%
    select(id, contains(variable)) %>%
    gather(sitio, value, -id) %>%
    mutate(sitio = str_extract(sitio, "[[:alnum:]]+"))
  base_var
}

# variables que extraemos para la tabla Sitio_muestra
variables <- c("lat", "lon", "altitude", "ellipsoid", "gps_error")
names(variables) <- variables

sitio <- ldply(variables, separarSitios, base_df = conglomerado) %>%
  spread(.id, value)

Sitio_muestra <- sitio %>%
  mutate(
    conglomerado_muestra_id = as.integer(id), 
    id = 1:length(id),
    sitio_numero = revalue(sitio, c("central" = "Centro", "transect2" = "Sitio 2",
      "transect3" = "Sitio 3", "transect4" = "Sitio 4")),
    # Web2py definió los campos boolean como char(1): T o F en la base de datos
    existe = "T",
    lat = as.numeric(lat),
    lon = as.numeric(lon),
    
    # Nota: no se está revisando por errores al introducir las coordenadas (por
    # ejemplo, si su GPS estaba en grados y dividieron la expansión decimal para
    # llenar los campos correspondientes a minutos y segundos)
    lat_grado = as.integer(lat), 
    lat_min = floor(60 * abs(lat - lat_grado)),
    lat_seg = (3600 * abs(lat - lat_grado - sign(lat) * lat_min / 60)),
    lon_grado = as.integer(lon), 
    lon_min = floor(60 * abs(lon - lon_grado)),
    lon_seg = (3600 * abs(lon - lon_grado - sign(lon) * lon_min / 60)),
    
    elipsoide = ellipsoid,
    gps_error = as.numeric(gps_error),
    altitud = as.numeric(altitude),
    hay_evidencia = NA
    ) %>%
  select(id, conglomerado_muestra_id, sitio_numero, existe, lat_grado, lat_min,
    lat_seg, lon_grado, lon_min, lon_seg, altitud, gps_error, elipsoide,
    hay_evidencia)  

rm("cat_municipio", "conglomerado", "conglomerado_tabs", "sitio", "variables",
  "separarSitios")


#### invaders, file_invasores -> Transecto_especie_invasora, 
# Especie_invasora, Archivo_Especie_invasora

# Filtramos para únicamente tener registros asociados a los conglomerados en la 
# tabla Conglomerado_muestra.
invasora_tabs <- ldply(bases_rutas, leerDbTab, "invaders")
archivos_invasora_tabs <- ldply(bases_rutas, leerDbTab, "files_invasores")
# creamos nuevos ids pegando id base de datos con ids de tabla
invasora <- invasora_tabs %>%
  mutate(
    id = as.numeric(paste(.id, id, sep = "")),
    conglomerate_id = as.numeric(paste(.id, conglomerate_id, sep = ""))
  ) %>%
  filter(conglomerate_id %in% Conglomerado_muestra$id & number_individuals > 0) %>%
  # Se filtran los registros sin individuos, puesto que:
  # 1. A veces si un transecto no tenía individuos, no lo introducían, pero a ve
  # ces declaraban un invader "fantasma" y ponían en comentarios que no se habían
  # encontrado individuos. Hay que estandarizar.
  # 2. Los invaders "fantasma" pueden llenar de basura la tabla de especies inva-
  # soras.
  select(-.id) 

archivos_invasora <- archivos_invasora_tabs %>%
  mutate(
    id = as.numeric(paste(.id, id, sep = "")),
    invaders_id = as.numeric(paste(.id, invaders_id, sep = ""))
  ) %>%
  # Como se filtró para obtener "invasora", debemos filtrar esta tabla también
  # para mantener consistencia.
  filter(invaders_id %in% invasora$id)

Archivo_especie_invasora <- archivos_invasora %>%
  mutate(
    especie_invasora_id = invaders_id,
    archivo_nombre_original = str_extract(old_filename, 
      stringr::regex("[[:word:]]+\\.[[:word:]]+")),
    # Word matchea letras, números y guiones bajos.
    archivo = str_extract(filename, stringr::regex("[[:word:]]+\\.[[:word:]]+"))
    # Estamos cambiando el nombre del archivo para que cuando agrupemos en una
    # misma carpeta no tengamos archivos duplicados
    # archivo = paste(.id, archivo, sep = "_")
    ) %>%
  select(id, especie_invasora_id, archivo_nombre_original, archivo)

# Asignamos como id del transecto el id de la primera especie invasora
transecto_especies_invasoras <- invasora %>%
  separate(transect, c("nombre", "transecto_numero"), sep = "/") %>%
  group_by(conglomerate_id, transecto_numero) %>%
  mutate(
    transecto_especies_invasoras_id = first(id)
  ) %>%
  ungroup()

# Obtenemos la fecha de la tabla Conglomerado_muestra (también se usará en la
# sección de huellas y excretas)
conglomerado_fechas <- select(Conglomerado_muestra, id, fecha_visita)

Transecto_especies_invasoras_muestra <- transecto_especies_invasoras %>%
  # Unimos por conglomerate_id para obtener la fecha
  left_join(conglomerado_fechas, by = c("conglomerate_id" = "id")) %>%
  group_by(conglomerate_id, transecto_numero) %>%
  # En el summarise es donde se cambia el tamaño de la tabla, agrupando valores
  summarise_each(funs(first)) %>%
  ungroup() %>%
  mutate(
    transecto_numero = revalue(transecto_numero, 
      c("T2" = "Transecto 2", "T3" = "Transecto 3", "T4" = "Transecto 4")),
    fecha = fecha_visita,
    hora_inicio = str_extract(start_time, regex("[0-9]{2}:[0-9]{2}:[0-9]{2}")), 
    hora_termino = str_extract(stop_time, regex("[0-9]{2}:[0-9]{2}:[0-9]{2}")), 
    tecnico = technician,
    comentario = comment
  ) %>%
  select(id = transecto_especies_invasoras_id, 
    conglomerado_muestra_id = conglomerate_id, fecha,
    transecto_numero, tecnico, hora_inicio, hora_termino, comentario)

# esto se puede simplificar considerando que no hay especies con cero individuos
# debido a que filtramos estos registros
# transecto_especies_invasoras$numero_individuos = cut(
#   transecto_especies_invasoras$number_individuals, 
#   breaks = c(-Inf, 0, 5, 10, 90, Inf))
# levels(transecto_especies_invasoras$numero_individuos) <- list(
#   "No aplica" = c("(-Inf,0]", "(90, Inf]"), 
#   "1 a 5" = "(0,5]", 
#   "6 a 10" = "(5,10]", 
#   "más de 10" = "(10,90]")

Especie_invasora <- transecto_especies_invasoras %>%
  mutate(
    nombre_en_lista = ifelse(is.na(conabio_list_name),"F","T"),
    nombre_comun = ifelse(name_type == "common name", species_name, NA), 
    nombre_cientifico = ifelse(name_type != "common name", species_name, NA),
    numero_individuos = as.character(cut(number_individuals, 
      breaks = c(0, 5, 10, 90, 99), 
      labels = c("1 a 5", "6 a 10", "más de 10", "No aplica")))
  ) %>%
  select(id, transecto_especies_invasoras_id, nombre_en_lista, nombre_comun, 
    nombre_cientifico, numero_individuos)  

#invasora_tabs y conglomerado_fechas se utilizan para Huellas y excretas 
#también (ver esquema).
rm(invasora, archivos_invasora, archivos_invasora_tabs, 
  transecto_especies_invasoras)



### tracks_excrements, file_tracks_excrements -> Transecto_huellas_excretas, 
# Huella_excreta, Archivo_huella_excreta
huella_tabs <- ldply(bases_rutas, leerDbTab, "tracks_excrements")
archivos_huella_tabs <- ldply(bases_rutas, leerDbTab, "files_tracks_excrements")

# obtenemos la hora inicio y hora término de la tabla invaders
invasora_horas <- select(invasora_tabs, transect, start_time, stop_time) %>%
  group_by(transect) %>%
  summarise_each(funs(first)) %>%
  ungroup()

# creamos nuevos ids pegando id base de datos con ids de tabla
huella_excreta <- huella_tabs %>%
  left_join(invasora_horas, by = "transect") %>%
  mutate(
    id = as.numeric(paste(.id, id, sep = "")),
    conglomerate_id = as.numeric(paste(.id, conglomerate_id, sep = ""))
  ) %>%
  filter((conglomerate_id %in% Conglomerado_muestra$id) & 
      (length > 0 | width > 0)) %>%
  select(-.id) 

archivos_huella_excreta <- archivos_huella_tabs %>%
  mutate(
    id = as.numeric(paste(.id, id, sep = "")),
    tracks_excrements_id = as.numeric(paste(.id, tracks_exrements_id, sep = ""))
  ) %>%
  filter(tracks_excrements_id %in% huella_excreta$id)


Archivo_huella_excreta <- archivos_huella_excreta %>%
  mutate(
    huella_excreta_id = tracks_excrements_id,
    archivo_nombre_original = str_extract(old_filename, regex("[[:word:]]+\\.[[:word:]]+")),
    # Word matchea letras, números y guiones bajos.
    archivo = str_extract(filename, regex("[[:word:]]+\\.[[:word:]]+"))
    # Estamos cambiando el nombre del archivo para que cuando agrupemos en una
    # misma carpeta no tengamos archivos duplicados
    # archivo = paste(.id, archivo, sep = "_")
    ) %>%
  select(id, huella_excreta_id, archivo_nombre_original, archivo)

# Asignamos como id del transecto el id de la primera huella/excreta
transecto_huellas_excretas <- huella_excreta %>%  
  separate(transect, c("nombre", "transecto_numero"), sep = "/") %>%
  group_by(conglomerate_id, transecto_numero) %>%
  mutate(
    transecto_huellas_excretas_id = first(id)
  ) %>%
  ungroup()

Transecto_huellas_excretas_muestra <- transecto_huellas_excretas %>%
  # Unimos por conglomerate_id para obtener la fecha
  left_join(conglomerado_fechas, by = c("conglomerate_id" = "id")) %>%
  group_by(conglomerate_id, transecto_numero) %>%
  # En el summarise es donde se cambia el tamaño de la tabla, agrupando valores
  summarise_each(funs(first)) %>%
  ungroup() %>%
  mutate(
    transecto_numero = revalue(transecto_numero, 
      c("T2" = "Transecto 2", "T3" = "Transecto 3", "T4" = "Transecto 4")),
    fecha = fecha_visita,
    hora_inicio = str_extract(start_time, regex("[0-9]{2}:[0-9]{2}:[0-9]{2}")), 
    hora_termino = str_extract(stop_time, regex("[0-9]{2}:[0-9]{2}:[0-9]{2}")),
    tecnico = technician,
    comentario = comment
  ) %>%
  select(id = transecto_huellas_excretas_id, 
    conglomerado_muestra_id = conglomerate_id, fecha,
    transecto_numero, tecnico, hora_inicio, hora_termino, comentario)

Huella_excreta <- transecto_huellas_excretas %>%
  mutate(
    es_huella = ifelse(observation_type == "tracks", "T", "F"),
    largo = length,
    ancho = width,
    nombre_comun = species_name, 
    nombre_cientifico = species_name
  ) %>%
  select(id, transecto_huellas_excretas_id, es_huella, nombre_comun, 
    nombre_cientifico, largo, ancho)

rm(huella_excreta, archivos_huella_excreta, huella_tabs, archivos_huella_tabs,
  invasora_horas, invasora_tabs, conglomerado_fechas, transecto_huellas_excretas)



### Cámara
equipo_tabs <- ldply(bases_rutas, leerDbTab, "equip_camera")
archivos_camara_ref_tabs <- ldply(bases_rutas, leerDbTab, "files_camera_ref")
archivos_camara_tabs <- ldply(bases_rutas, leerDbTab, "files_trap_camera")
camara_tabs <- ldply(bases_rutas, leerDbTab, "trap_camera")

# creamos nuevos ids pegando id base de datos con ids de tabla y filtramos
equipo <- equipo_tabs %>% 
  mutate(
    id = as.numeric(paste(.id, id, sep = "")),
    conglomerate_id = as.numeric(paste(.id, conglomerate_id, sep = ""))
  ) %>%
  filter(conglomerate_id %in% Conglomerado_muestra$id) %>%
  select(-.id)

archivos_camara_ref <- archivos_camara_ref_tabs %>% 
  mutate(
    id = as.numeric(paste(.id, id, sep = "")),
    equipment_id = as.numeric(paste(.id, equipment_id, sep = ""))
  ) %>%
  filter(equipment_id %in% equipo$id)

archivos_camara <- archivos_camara_tabs %>% 
  mutate(
    id = as.numeric(paste(.id, id, sep = "")),
    trap_camera_id = as.numeric(paste(.id, trap_camera_id, sep = ""))
  )

camara <- camara_tabs %>%
  mutate(
    id = as.numeric(paste(.id, id, sep = "")),
    conglomerate_id = as.numeric(paste(.id, conglomerate_id, sep = "")),
    camera_id = as.numeric(paste(.id, camera_id, sep = ""))
  ) %>%
  filter((conglomerate_id %in% Conglomerado_muestra$id) &
    (id %in% archivos_camara$trap_camera_id))  %>%
    # Seleccionamos los registros de "trap_camera"" que tengan fotos referenciadas
    # (como éstos representan individuos vistos, y a cada uno se le asocian sus
    # respectivas fotos, queremos eliminar registros fantasma).
    # Ésto no se pudo hacer con "equip_camera": elimninar las cámaras sin registros
    # de "trap_camera" (especies) asociados, puesto que no sirve la llave foránea
    # de ésta última. sin embargo, ésta restricción se hará mediante un inner join
    # a la hora de obtener la hora de inicio/término.
  select(-.id)

# Camara = trap\_camara + equip\_camara
# 
# 1. Hacemos el join de tal manera que preservamos todas los renglones de camara 
# (trap\_camara).
# 
# 2. Unimos con los ids de sitios usando la variable conglomerate_id.

# Tabla para localizar "sitio_muestra_id" de una cámara, por medio de 
# "conglomerate_id" y "location_camera"
sitios_cgls <- select(Sitio_muestra, sitio_muestra_id = id, 
  conglomerate_id = conglomerado_muestra_id, sitio_numero) %>%
  mutate(
    sitio_numero = revalue(sitio_numero, c("Centro" = 1, "Sitio 2" = 2,
      "Sitio 3" = 3, "Sitio 4" = 4))
  )

# unir tablas equip_camera con trap_camera
# primero sacamos una sub-base de trap_camera con un único registro por 
# conglomerado-sitio, después unimos con sitios_cgls para obtener sitio_muestra_id

# Tablas auxiliares para formar la tabla "Camara":

# 1. camara_aux: tenemos un registro de "trap_camara" por combinación de conglomerado
# y sitio.
# 2. equipo_aux: tenemos un registro de "equip_camera" por "conglomerate_id". De
# esta manera, al hacer el outer join por "conglomerate_id"" con "camara_aux" como
# tabla maestra, tendremos un registro por combinación de conglomerado y  sitio,
# y artificios del join no cambiarán ésto.

camara_aux <- camara %>%
  group_by(location) %>%
  summarise_each(funs(first))

equipo_aux <- equipo %>%
  group_by(conglomerate_id) %>%
  summarise_each(funs(first))

# camara_id_mapeo es una tabla donde se relaciona el id de la tabla camara con el
# id de la tabla camara_aux (que, al final, será el id de la tabla Camara)

camara_id_mapeo <- camara %>%
  inner_join(camara_aux, by = c("location")) %>%
  select(id_original = id.x, id_nuevo = id.y)

# equipo_id_mapeo es una tabla donde se relaciona el id de la tabla equipo con el
# id de la tabla equipo_aux (para los registros de la tabla "files_camera_ref")

equipo_id_mapeo <- equipo %>%
  inner_join(equipo_aux, by = c("conglomerate_id")) %>%
  select(id_original = id.x, id_nuevo = id.y)

camara_sitio <- camara_aux %>%
  left_join(equipo_aux, by = c("conglomerate_id")) %>%
  mutate(
    # quitando el último dígito de location, para obtener sitio_muestra_id, con
    # ayuda de "sitios_cgls"
    sitio_numero = substr(location, start = nchar(location), 
      stop = nchar(location))
  ) %>%
  left_join(sitios_cgls, by = c("conglomerate_id", "sitio_numero"))

camara_sitio$resolution[grep("1920", camara_sitio$resolution)] <- "2MP"
# typos:
camara_sitio$resolution[grep("1020", camara_sitio$resolution)] <- "2MP"
camara_sitio$resolution[grep("1080", camara_sitio$resolution)] <- "2MP"
camara_sitio$resolution[grep("Alta", camara_sitio$resolution)] <- "2MP"

camara_sitio$resolution[grep("4800", camara_sitio$resolution)] <- "5MP"
# typo:
camara_sitio$resolution[grep("SMP", camara_sitio$resolution)] <- "5MP"
camara_sitio$resolution[grep("5", camara_sitio$resolution)] <- "5MP"


Camara <- camara_sitio %>%
  mutate(
    fecha_inicio = str_extract(start_time, regex("[0-9]{4}-[0-9]{2}-[0-9]{2}")), #(variable de trap_camera) 
    hora_inicio = str_extract(start_time, regex("[0-9]{2}:[0-9]{2}:[0-9]{2}")), #(variable de trap_camera) 
    fecha_termino = str_extract(stop_time, regex("[0-9]{4}-[0-9]{2}-[0-9]{2}")), #(variable de trap_camera) 
    hora_termino = str_extract(stop_time, regex("[0-9]{2}:[0-9]{2}:[0-9]{2}")), #(variable de trap_camera) 
    lat = as.numeric(loc_camera_lat),
    lon = as.numeric(loc_camera_lon),
    lat_grado = as.integer(lat), 
    lat_min = floor(60 * abs(lat - lat_grado)),
    lat_seg = (3600 * abs(lat - lat_grado - sign(lat) * lat_min / 60)),
    lon_grado = as.integer(lon), 
    lon_min = floor(60 * abs(lon - lon_grado)),
    lon_seg = (3600 * abs(lon - lon_grado - sign(lon) * lon_min / 60)),
    azimut = NA, 
    condiciones_ambientales = NA, 
    sensibilidad = revalue(sensitivity, c("Normal" = "Normal", "Alta" = "High",
      "Baja" = "Low")), ### revisar y empatar niveles
    comentario = paste("Comentario trap_camera:", comment.x, 
      "Comentario equip_camera", comment.y)
    ) %>%
  select(id = id.x, #nos quedamos con id de la tabla trap_camera
    sitio_muestra_id, 
    nombre = camera, 
    fecha_inicio, fecha_termino,
    hora_inicio, hora_termino,
    lat_grado, lat_min, lat_seg,
    lon_grado, lon_min, lon_seg,
    altitud = loc_camera_altitude,
    gps_error = loc_camera_gps_error,
    elipsoide = loc_camera_ellipsoid, 
    condiciones_ambientales,
    distancia_centro = distance,
    azimut,
    resolucion = resolution, sensibilidad,
    comentario
    )
# Arreglamos typos
Camara$sensibilidad[Camara$sensibilidad %in% c("Alta", "ALTA", "alta", 
  "1920X1080p", "HIGH")] <- "High"

# seleccionamos los campos de la tabla trap_camera que formarán parte de
# la tabla Archivo_camara, así como obtenemos "camara_id" con ayuda de
# camara_id_mapeo.
trap_camera_sub <- camara %>%
  inner_join(camara_id_mapeo, by = c("id" = "id_original")) %>%
  select(trap_camera_id = id, camara_id = id_nuevo, number_individuals, 
  species_name, type_first_image, type_image, type_video)

# se utiliza un inner join para eliminar las fotos que están asociadas a una
# cámara que a su vez está asociada a un conglomerado que quedó filtrado

Archivo_camara <-  inner_join(archivos_camara, trap_camera_sub, 
  by ="trap_camera_id") %>%
  mutate(
    archivo = str_extract(filename, regex("[[:word:]]+\\.[[:word:]]+")),
    # Estamos cambiando el nombre del archivo para que cuando agrupemos en una
    # misma carpeta no tengamos archivos duplicados
    # archivo = paste(.id, archivo, sep = "_"),
    archivo_nombre_original = str_extract(old_filename, regex("[[:word:]]+\\.[[:word:]]+")),
    presencia = ifelse(number_individuals > 0, "T", "F"),
    nombre_comun = species_name,
    nombre_cientifico = species_name,
    numero_individuos = ifelse(presencia == "T", number_individuals, NA)
  ) %>%
  select(id, camara_id, archivo_nombre_original, archivo, presencia, 
    nombre_comun,  # como decidir?
    nombre_cientifico, 
    numero_individuos
  )

# equipo_camara_id_mapeo es para encontrar el camara_id (cliente nuevo) a partir
# del equipment_id (cliente anterior), y así asociar una imagen de referencia
# de una cámara a su registro correspondiente.

equipo_camara_id_mapeo <- camara_sitio %>%
  select(
    camara_id = id.x,
    equipment_id = id.y
  )

Imagen_referencia_camara <- archivos_camara_ref %>%
  inner_join(equipo_id_mapeo, by = c("equipment_id" = "id_original")) %>%
  # El siguiente inner join (en lugar de left join) elimina las fotos que no
  # se pueden asociar a una cámara
  inner_join(equipo_camara_id_mapeo, by = c("id_nuevo" = "equipment_id")) %>%
  mutate(
    archivo = str_extract(filename, regex("[[:word:]]+\\.[[:word:]]+")),
    # Estamos cambiando el nombre del archivo para que cuando agrupemos en una
    # misma carpeta no tengamos archivos duplicados
    #archivo = paste(.id, archivo, sep = "_"),
    archivo_nombre_original = str_extract(old_filename, regex("[[:word:]]+\\.[[:word:]]+"))
  ) %>%
  # Dos registros de Imagen_referencia_camara tienen id's duplicados, por lo que
  # los eliminamos
  group_by(id) %>%
    summarise_each(funs(first)) %>%
   ungroup() %>%
  select(id, camara_id, archivo_nombre_original, archivo)

rm(archivos_camara, archivos_camara_ref, archivos_camara_ref_tabs, 
  archivos_camara_tabs, camara, camara_sitio, camara_tabs, camara_aux,
  camara_id_mapeo, equipo, equipo_aux, equipo_camara_id_mapeo, equipo_id_mapeo,
  equipo_tabs, sitios_cgls, trap_camera_sub)

#Imagen_referencia_camara[duplicated(Imagen_referencia_camara$id),]

### Grabadora

equipo_tabs <- ldply(bases_rutas, leerDbTab, "equip_microphone")
archivos_microfono_ref_tabs <- ldply(bases_rutas, leerDbTab, "files_microphone_ref")
archivos_microfono_tabs <- ldply(bases_rutas, leerDbTab, "files_soundscape_camera")
soundscape_tabs <- ldply(bases_rutas, leerDbTab, "soundscape")

# creamos nuevos ids pegando id base de datos con ids de tabla y filtramos
equipo <- equipo_tabs %>% 
  mutate(
    id = as.numeric(paste(.id, id, sep = "")),
    conglomerate_id = as.numeric(paste(.id, conglomerate_id, sep = ""))
  ) %>%
  filter(conglomerate_id %in% Conglomerado_muestra$id) %>%
  select(-.id)

archivos_microfono_ref <- archivos_microfono_ref_tabs %>% 
  mutate(
    id = as.numeric(paste(.id, id, sep = "")),
    equipment_id = as.numeric(paste(.id, equipment_id, sep = ""))
  ) %>%
  filter(equipment_id %in% equipo$id)

archivos_microfono <- archivos_microfono_tabs %>% 
  mutate(
    id = as.numeric(paste(.id, id, sep = "")),
    soundscape_id = as.numeric(paste(.id, soundscape_id, sep = ""))
  )

soundscape <- soundscape_tabs %>%
  mutate(
    id = as.numeric(paste(.id, id, sep = "")),
    conglomerate_id = as.numeric(paste(.id, conglomerate_id, sep = "")),
    microphone_id = as.numeric(paste(.id, microphone_id, sep = ""))
  ) %>%
  filter((conglomerate_id %in% Conglomerado_muestra$id) &
    (id %in% archivos_microfono$soundscape_id))  %>%
    # Seleccionamos los registros de "trap_camera"" que tengan fotos referenciadas
    # (como éstos representan individuos vistos, y a cada uno se le asocian sus
    # respectivas fotos, queremos eliminar registros fantasma).
    # Ésto no se pudo hacer con "equip_camera": elimninar las cámaras sin registros
    # de "trap_camera" (especies) asociados, puesto que no sirve la llave foránea
    # de ésta última. sin embargo, ésta restricción se hará mediante un inner join
    # a la hora de obtener la hora de inicio/término.
  select(-.id)

# 
# Grabadora = soundscape + equip\_microphone
# 
# 1. Hacemos el join de tal manera que preservamos todos los renglones de grabadora 
# (soundscape).
# 
# 2. Unimos con los ids de sitios usando la variable conglomerate_id.


sitios_cgls <- select(Sitio_muestra, sitio_muestra_id = id, 
  conglomerate_id = conglomerado_muestra_id, sitio_numero) %>%
  mutate(
    sitio_numero = revalue(sitio_numero, c("Centro" = 1, "Sitio 2" = 2,
      "Sitio 3" = 3, "Sitio 4" = 4))
  )

# unir tablas soundscape con trap_microphone
# primero sacamos una sub-base de soundscape con un único registro por 
# conglomerado-sitio, después unimos con sitios_cgls para obtener sitio_muestra_id

# Tablas auxiliares para formar la tabla "Grabadora":

# 1. soundscape_aux: tenemos un registro de "soundscape" por combinación de conglomerado
# y sitio.
# 2. equipo_aux: tenemos un registro de "equip_microphone" por "conglomerate_id". De
# esta manera, al hacer el outer join por "conglomerate_id"" con "soundscape_aux" como
# tabla maestra, tendremos un registro por combinación de conglomerado y sitio,
# y artificios del join no cambiarán ésto.

soundscape_aux <- soundscape %>%
  group_by(location) %>%
  summarise_each(funs(first))

equipo_aux <- equipo %>%
  group_by(conglomerate_id) %>%
  summarise_each(funs(first))

# soundscape_id_mapeo es una tabla donde se relaciona el id de la tabla soundscape con el
# id de la tabla soundscape_aux (que, al final, será el id de la tabla Grabadora)

soundscape_id_mapeo <- soundscape %>%
  inner_join(soundscape_aux, by = c("location")) %>%
  select(id_original = id.x, id_nuevo = id.y)

# equipo_id_mapeo es una tabla donde se relaciona el id de la tabla equipo con el
# id de la tabla equipo_aux (para los registros de la tabla "files_microphone_ref")

equipo_id_mapeo <- equipo %>%
  inner_join(equipo_aux, by = c("conglomerate_id")) %>%
  select(id_original = id.x, id_nuevo = id.y)

soundscape_sitio <- soundscape_aux %>%
  left_join(equipo_aux, by = c("conglomerate_id")) %>%     
  mutate(
    # quitando el último dígito de location, para obtener sitio_muestra_id, con
    # ayuda de "sitios_cgls"
    sitio_numero = substr(location, start = nchar(location), 
      stop = nchar(location))
  ) %>%
  left_join(sitios_cgls, by = c("conglomerate_id", "sitio_numero"))

Grabadora <- soundscape_sitio %>%
  mutate(
    fecha_inicio = str_extract(start_time, regex("[0-9]{4}-[0-9]{2}-[0-9]{2}")), #(variable de soundscape) 
    hora_inicio = str_extract(start_time, regex("[0-9]{2}:[0-9]{2}:[0-9]{2}")), #(variable de soundscape) 
    fecha_termino = str_extract(stop_time, regex("[0-9]{4}-[0-9]{2}-[0-9]{2}")), #(variable de soundscape) 
    hora_termino = str_extract(stop_time, regex("[0-9]{2}:[0-9]{2}:[0-9]{2}")), #(variable de soundscape) 
    lat = as.numeric(loc_mic_lat),
    lon = as.numeric(loc_mic_lon),
    lat_grado = as.integer(lat), 
    lat_min = floor(60 * abs(lat - lat_grado)),
    lat_seg = (3600 * abs(lat - lat_grado - sign(lat) * lat_min / 60)),
    lon_grado = as.integer(lon), 
    lon_min = floor(60 * abs(lon - lon_grado)),
    lon_seg = (3600 * abs(lon - lon_grado - sign(lon) * lon_min / 60)), 
    condiciones_ambientales = NA, 
    microfonos_mojados = NA,
    comentario = paste("Comentario soundscape:", comment.x, 
      "Comentario equip_microphone", comment.y)
    ) %>%
  select(id = id.x, # nos quedamos con id de la tabla soundscape
    sitio_muestra_id, 
    nombre = name, 
    fecha_inicio, fecha_termino,
    hora_inicio, hora_termino,
    lat_grado, lat_min, lat_seg,
    lon_grado, lon_min, lon_seg,
    altitud = loc_mic_altitude,
    gps_error = loc_mic_gps_error,
    elipsoide = loc_mic_ellipsoid,
    #distancia_centro = distance, (esta variable no se pregunta en el nuevo cliente)
    condiciones_ambientales,
    microfonos_mojados,
    comentario
    )

# seleccionamos los campos de la tabla soundscape que formarán parte de
# la tabla Archivo_grabadora, así como obtenemos "grabadora_id" con ayuda de
# soundscape_id_mapeo.
soundscape_sub <- soundscape %>%
  inner_join(soundscape_id_mapeo, by = c("id" = "id_original")) %>%
  select(soundscape_id = id, grabadora_id = id_nuevo, type_soundscape)

# Se utiliza un inner join para eliminar los archivos que están asociados a una
# grabadora que a su vez está asociada a un conglomerado que quedó filtrado

Archivo_grabadora <-  inner_join(archivos_microfono, soundscape_sub, 
  by ="soundscape_id") %>%
  mutate(
    archivo = str_extract(filename, regex("[[:word:]]+\\.[[:word:]]+")),
    # Estamos cambiando el nombre del archivo para que cuando agrupemos en una
    # misma carpeta no tengamos archivos duplicados
    #archivo = paste(.id, archivo, sep = "_"),
    archivo_nombre_original = str_extract(old_filename, regex("[[:word:]]+\\.[[:word:]]+")),
    es_audible = ifelse(type_soundscape == 1, "T", "F")
  ) %>%
  select(id, grabadora_id, archivo_nombre_original, archivo, es_audible)

# equipo_soundscape_id_mapeo es para encontrar el grabadora_id (cliente nuevo) a partir
# del equipment_id (cliente anterior), y así asociar una imagen de referencia
# de una grabadora a su registro correspondiente.

equipo_soundscape_id_mapeo <- soundscape_sitio %>%
  select(
    grabadora_id = id.x,
    equipment_id = id.y
  )

Imagen_referencia_microfonos <- archivos_microfono_ref %>%
  inner_join(equipo_id_mapeo, by = c("equipment_id" = "id_original")) %>%
  # El siguiente inner join (en lugar de left join) elimina las fotos que no
  # se pueden asociar a una cámara
  inner_join(equipo_soundscape_id_mapeo, by = c("id_nuevo" = "equipment_id")) %>%
  mutate(
    archivo = str_extract(filename, regex("[[:word:]]+\\.[[:word:]]+")),
    # Estamos cambiando el nombre del archivo para que cuando agrupemos en una
    # misma carpeta no tengamos archivos duplicados
    #archivo = paste(.id, archivo, sep = "_"),
    archivo_nombre_original = str_extract(old_filename, regex("[[:word:]]+\\.[[:word:]]+"))
  ) %>%
  select(id, grabadora_id, archivo_nombre_original, archivo)

rm(archivos_microfono, archivos_microfono_ref, archivos_microfono_ref_tabs,
  archivos_microfono_tabs, equipo, equipo_aux, equipo_id_mapeo, equipo_soundscape_id_mapeo,
  equipo_tabs, sitios_cgls, soundscape, soundscape_aux, soundscape_id_mapeo,
  soundscape_sitio, soundscape_sub, soundscape_tabs)

# no hay info. para crear las tablas Archivo_referencia_microfono, 
# Imagen_referencia_grabadora

###Arreglando Ids
# creamos una nueva id, de tal manera que sea incrementos de 1, 2, 3...

# Los siguientes ids se arreglan de manera automática por no ser llaves foráneas
Archivo_especie_invasora$id <- 1:length(Archivo_especie_invasora$id)
Archivo_huella_excreta$id <- 1:length(Archivo_huella_excreta$id)

Archivo_camara$id <- 1:length(Archivo_camara$id)
Imagen_referencia_camara$id <- 1:length(Imagen_referencia_camara$id)

Archivo_grabadora$id <- 1:length(Archivo_grabadora$id)
Imagen_referencia_microfonos$id <- 1:length(Imagen_referencia_microfonos$id)

# Para aquellos ids que sirven como llaves foráneas creamos catálogos
cat_cgl_ids <- data.frame(
  id = unique(Conglomerado_muestra$id)) 
cat_cgl_ids$id_sec <- 1:length(cat_cgl_ids$id)

cat_transecto_ei_ids <- data.frame(
  id = unique(Transecto_especies_invasoras_muestra$id))
cat_transecto_ei_ids$id_sec <- 1:length(cat_transecto_ei_ids$id)
cat_ei_ids <- data.frame(id = unique(Especie_invasora$id))
cat_ei_ids$id_sec <- 1:length(cat_ei_ids$id)

cat_transecto_he_ids <- data.frame(
  id = unique(Transecto_huellas_excretas_muestra$id))
cat_transecto_he_ids$id_sec <- 1:length(cat_transecto_he_ids$id)
cat_he_ids <- data.frame(id = unique(Huella_excreta$id))
cat_he_ids$id_sec <- 1:length(cat_he_ids$id)

cat_grabadora_ids <- data.frame(id = unique(Grabadora$id))
cat_grabadora_ids$id_sec <- 1:length(cat_grabadora_ids$id)

cat_camara_ids <- data.frame(id = unique(Camara$id))
cat_camara_ids$id_sec <- 1:length(cat_camara_ids$id)

## Conglomerado y Sitio
Conglomerado_muestra <- Conglomerado_muestra %>%
  left_join(cat_cgl_ids, by = "id") %>%
  mutate(id = id_sec) %>%
  select(-id_sec)
glimpse(Conglomerado_muestra)

Sitio_muestra <- Sitio_muestra %>%
  left_join(select(cat_cgl_ids, conglomerado_muestra_id = id, id_sec),
    by = "conglomerado_muestra_id") %>%
  mutate(conglomerado_muestra_id = id_sec) %>%
  select(-id_sec)
glimpse(Sitio_muestra)

## Especies Invasoras
Transecto_especies_invasoras_muestra <- Transecto_especies_invasoras_muestra %>%
  left_join(select(cat_cgl_ids, conglomerado_muestra_id = id, id_sec),
    by = "conglomerado_muestra_id") %>%
  mutate(conglomerado_muestra_id = id_sec) %>%
  select(-id_sec) %>%
  left_join(cat_transecto_ei_ids, by = "id") %>%
  mutate(id = id_sec) %>%
  select(-id_sec)
glimpse(Transecto_especies_invasoras_muestra)

Especie_invasora <- Especie_invasora %>%
  left_join(select(cat_transecto_ei_ids, transecto_especies_invasoras_id = id,
    id_sec), by = "transecto_especies_invasoras_id") %>%
  mutate(transecto_especies_invasoras_id = id_sec) %>%
  select(-id_sec) %>%
  left_join(cat_ei_ids, by = "id") %>%
  mutate(id = id_sec) %>%
  select(-id_sec)
glimpse(Especie_invasora)  
  
Archivo_especie_invasora <- Archivo_especie_invasora %>%
  left_join(select(cat_ei_ids, especie_invasora_id = id, id_sec), 
    by = "especie_invasora_id") %>%
  mutate(especie_invasora_id = id_sec) %>%
  select(-id_sec)
glimpse(Archivo_especie_invasora)

## Huellas y excretas
Transecto_huellas_excretas_muestra <- Transecto_huellas_excretas_muestra %>%
  left_join(select(cat_cgl_ids, conglomerado_muestra_id = id, id_sec),
    by = "conglomerado_muestra_id") %>%
  mutate(conglomerado_muestra_id = id_sec) %>%
  select(-id_sec) %>%
  left_join(cat_transecto_he_ids, by = "id") %>%
  mutate(id = id_sec) %>%
  select(-id_sec)
glimpse(Transecto_huellas_excretas_muestra)

Huella_excreta <- Huella_excreta %>%
  left_join(select(cat_transecto_he_ids, transecto_huellas_excretas_id = id,
    id_sec), by = "transecto_huellas_excretas_id") %>%
  mutate(transecto_huellas_excretas_id = id_sec) %>%
  select(-id_sec) %>%
  left_join(cat_he_ids, by = "id") %>%
  mutate(id = id_sec) %>%
  select(-id_sec)
glimpse(Huella_excreta)  
  
Archivo_huella_excreta <- Archivo_huella_excreta %>%
  left_join(select(cat_he_ids, huella_excreta_id = id, id_sec), 
    by = "huella_excreta_id") %>%
  mutate(huella_excreta_id = id_sec) %>%
  select(-id_sec)
glimpse(Archivo_huella_excreta)

# Cámara
Camara <- Camara %>%
  left_join(cat_camara_ids, by = "id") %>% 
  mutate(id = id_sec) %>%
  select(-id_sec)
glimpse(Camara)

Imagen_referencia_camara <- Imagen_referencia_camara %>%
  left_join(select(cat_camara_ids, camara_id = id, id_sec), 
    by = "camara_id") %>%
  mutate(camara_id = id_sec) %>%
  select(-id_sec)
glimpse(Imagen_referencia_camara)

Archivo_camara <- Archivo_camara %>%
  left_join(select(cat_camara_ids, camara_id = id, id_sec), 
    by = "camara_id") %>%
  mutate(camara_id = id_sec) %>%
  select(-id_sec)
glimpse(Archivo_camara)

# Grabadora
Grabadora <- Grabadora %>%
  left_join(cat_grabadora_ids, by = "id") %>%
  mutate(id = id_sec) %>%
  select(-id_sec)
glimpse(Grabadora)

Imagen_referencia_microfonos <- Imagen_referencia_microfonos %>%
  left_join(select(cat_grabadora_ids, grabadora_id = id, id_sec), 
    by = "grabadora_id") %>%
  mutate(grabadora_id = id_sec) %>%
  select(-id_sec)
glimpse(Imagen_referencia_microfonos)

Archivo_grabadora <- Archivo_grabadora %>%
  left_join(select(cat_grabadora_ids, grabadora_id = id, id_sec), 
    by = "grabadora_id") %>%
  mutate(grabadora_id = id_sec) %>%
  select(-id_sec)
glimpse(Archivo_grabadora)

rm(cat_camara_ids, cat_cgl_ids, cat_ei_ids, cat_grabadora_ids, cat_he_ids,
  cat_transecto_ei_ids, cat_transecto_he_ids)

save.image(file = "../datos/tablas_finales.Rdata")

### Escribiendo tablas en la base de datos SQLite

base_output <- dbConnect(RSQLite::SQLite(), "../datos/bases_salida/base_output.db")
dbWriteTable(base_output, "Conglomerado_muestra", as.data.frame(Conglomerado_muestra), overwrite = FALSE, append = TRUE)
dbWriteTable(base_output, "Sitio_muestra", as.data.frame(Sitio_muestra), overwrite = FALSE, append = TRUE)

dbWriteTable(base_output, "Transecto_especies_invasoras_muestra", as.data.frame(Transecto_especies_invasoras_muestra), overwrite = FALSE, append = TRUE)
dbWriteTable(base_output, "Especie_invasora", as.data.frame(Especie_invasora), overwrite = FALSE, append = TRUE)
dbWriteTable(base_output, "Archivo_especie_invasora", as.data.frame(Archivo_especie_invasora), overwrite = FALSE, append = TRUE)

dbWriteTable(base_output, "Transecto_huellas_excretas_muestra", as.data.frame(Transecto_huellas_excretas_muestra), overwrite = FALSE, append = TRUE)
dbWriteTable(base_output, "Huella_excreta", as.data.frame(Huella_excreta), overwrite = FALSE, append = TRUE)
dbWriteTable(base_output, "Archivo_huella_excreta", as.data.frame(Archivo_huella_excreta), overwrite = FALSE, append = TRUE)

dbWriteTable(base_output, "Camara", as.data.frame(Camara), overwrite = FALSE, append = TRUE)
dbWriteTable(base_output, "Archivo_camara", as.data.frame(Archivo_camara), overwrite = FALSE, append = TRUE)
dbWriteTable(base_output, "Imagen_referencia_camara", as.data.frame(Imagen_referencia_camara), overwrite = FALSE, append = TRUE)

dbWriteTable(base_output, "Grabadora", as.data.frame(Grabadora), overwrite = FALSE, append = TRUE)
dbWriteTable(base_output, "Archivo_grabadora", as.data.frame(Archivo_grabadora), overwrite = FALSE, append = TRUE)
dbWriteTable(base_output, "Imagen_referencia_microfonos", as.data.frame(Imagen_referencia_microfonos), overwrite = FALSE, append = TRUE)

#dbCommit(base_output)
dbDisconnect(base_output)

### Escribiendo tablas en la base de datos PostgreSQL

drv <- dbDriver("PostgreSQL")
base_output <- dbConnect(drv = drv, dbname = "snmb_fusion_1", port = 5432,
  host = "localhost", 
  user = "fpardo", password = "")

dbWriteTable(base_output, "conglomerado_muestra", 
  as.data.frame(Conglomerado_muestra), overwrite = FALSE, append = TRUE, row.names = 0)

dbWriteTable(base_output, "sitio_muestra", as.data.frame(Sitio_muestra), overwrite = FALSE, append = TRUE, row.names = 0)

dbWriteTable(base_output, "transecto_especies_invasoras_muestra", as.data.frame(Transecto_especies_invasoras_muestra), overwrite = FALSE, append = TRUE, row.names = 0)
dbWriteTable(base_output, "especie_invasora", as.data.frame(Especie_invasora), overwrite = FALSE, append = TRUE, row.names = 0)
dbWriteTable(base_output, "archivo_especie_invasora", as.data.frame(Archivo_especie_invasora), overwrite = FALSE, append = TRUE, row.names = 0)

dbWriteTable(base_output, "transecto_huellas_excretas_muestra", as.data.frame(Transecto_huellas_excretas_muestra), overwrite = FALSE, append = TRUE, row.names = 0)
dbWriteTable(base_output, "huella_excreta", as.data.frame(Huella_excreta), overwrite = FALSE, append = TRUE, row.names = 0)
dbWriteTable(base_output, "archivo_huella_excreta", as.data.frame(Archivo_huella_excreta), overwrite = FALSE, append = TRUE, row.names = 0)

dbWriteTable(base_output, "camara", as.data.frame(Camara), overwrite = FALSE, append = TRUE, row.names = 0)
dbWriteTable(base_output, "archivo_camara", as.data.frame(Archivo_camara), overwrite = FALSE, append = TRUE, row.names = 0)
dbWriteTable(base_output, "imagen_referencia_camara", as.data.frame(Imagen_referencia_camara), overwrite = FALSE, append = TRUE, row.names = 0)

dbWriteTable(base_output, "grabadora", as.data.frame(Grabadora), overwrite = FALSE, append = TRUE, row.names = 0)
dbWriteTable(base_output, "archivo_grabadora", as.data.frame(Archivo_grabadora), overwrite = FALSE, append = TRUE, row.names = 0)
dbWriteTable(base_output, "imagen_referencia_microfonos", as.data.frame(Imagen_referencia_microfonos), overwrite = FALSE, append = TRUE, row.names = 0)

# Actualizando secuencias para las id's en la base de datos PostgreSQL:

#-- Login to psql and run the following
#-- What is the result?
dbGetQuery(base_output, "SELECT MAX(id) FROM Conglomerado_muestra;")

#-- Then run...
#-- This should be higher than the last result.
dbGetQuery(base_output, "SELECT nextval('Conglomerado_muestra_id_seq');")

#-- If it's not higher... run this set the sequence last to your highest pid it. 
#-- (wise to run a quick pg_dump first...)

dbGetQuery(base_output, "SELECT setval('Conglomerado_muestra_id_seq', (SELECT MAX(id) FROM Conglomerado_muestra));")
dbGetQuery(base_output, "SELECT setval('Sitio_muestra_id_seq', (SELECT MAX(id) FROM Sitio_muestra));")

dbGetQuery(base_output, "SELECT setval('Transecto_especies_invasoras_muestra_id_seq', (SELECT MAX(id) FROM Transecto_especies_invasoras_muestra));")
dbGetQuery(base_output, "SELECT setval('Especie_invasora_id_seq', (SELECT MAX(id) FROM Especie_invasora));")
dbGetQuery(base_output, "SELECT setval('Archivo_especie_invasora_id_seq', (SELECT MAX(id) FROM Archivo_especie_invasora));")

dbGetQuery(base_output, "SELECT setval('Transecto_huellas_excretas_muestra_id_seq', (SELECT MAX(id) FROM Transecto_huellas_excretas_muestra));")
dbGetQuery(base_output, "SELECT setval('Huella_excreta_id_seq', (SELECT MAX(id) FROM Huella_excreta));")
dbGetQuery(base_output, "SELECT setval('Archivo_huella_excreta_id_seq', (SELECT MAX(id) FROM Archivo_huella_excreta));")

dbGetQuery(base_output, "SELECT setval('Camara_id_seq', (SELECT MAX(id) FROM Camara));")
dbGetQuery(base_output, "SELECT setval('Archivo_camara_id_seq', (SELECT MAX(id) FROM Archivo_camara));")
dbGetQuery(base_output, "SELECT setval('Imagen_referencia_camara_id_seq', (SELECT MAX(id) FROM Imagen_referencia_camara));")

dbGetQuery(base_output, "SELECT setval('Grabadora_id_seq', (SELECT MAX(id) FROM Grabadora));")
dbGetQuery(base_output, "SELECT setval('Archivo_grabadora_id_seq', (SELECT MAX(id) FROM Archivo_grabadora));")
dbGetQuery(base_output, "SELECT setval('Imagen_referencia_microfonos_id_seq', (SELECT MAX(id) FROM Imagen_referencia_microfonos));")

#-- if your tables might have no rows
#-- false means the set value will be returned by the next nextval() call    
#SELECT setval('your_table_id_seq', COALESCE((SELECT MAX(id)+1 FROM your_table), 1), false);

#dbCommit(base_output)
dbDisconnect(base_output)

### Creando lista con nombres de archivos

Esta lista se utilizará para buscar los archivos en la estructura de entregas
guardada en el cluster.

archivos <- c(
  Archivo_especie_invasora$archivo,
  Archivo_huella_excreta$archivo,
  Archivo_camara$archivo,
  Imagen_referencia_camara$archivo,
  Archivo_grabadora$archivo,
  Imagen_referencia_microfonos$archivo)

write.table(archivos, file = "../datos/adicionales/nombres_archivos_snmb.csv", 
  sep = ",", row.names = FALSE)

### Copiar imágenes, grabaciones y videos a carpetas

dir_j = "/Volumes/sacmod"

# en Windows
#dir_j = "//madmexservices.conabio.gob.mx/sacmod/"

# jpg hay errores por caracteres especiales en las rutas
rutas_jpg <- list.files(path = dir_j, 
  recursive = TRUE, full.names = TRUE, pattern = "\\.jpg$", ignore.case = TRUE)

rutas_wav <- list.files(path = dir_j, 
  recursive = TRUE, full.names = TRUE, pattern = "\\.wav$")

rutas_avi <- list.files(path = dir_j, 
  recursive = TRUE, full.names = TRUE, pattern = "\\.AVI$")

archivos_copiar <- read.csv("../datos/adicionales/nombres_archivos_snmb.csv",
  header = TRUE)

rutas_jpg_2 <- str_extract(rutas_jpg, regex("[[:word:]]+\\.[[:word:]]+$"))
rutas_jpg_3 <- rutas_jpg[rutas_jpg_2 %in% archivos_copiar$x]

fallas <- file.copy(rutas_jpg_3, "//madmexservices.conabio.gob.mx/sacmod/snmb_piloto/imagenes")
sum(!fallas)

rutas_wav <- list.files(path = dir_j, 
  recursive = TRUE, full.names = TRUE, pattern = "\\.wav$")

rutas_wav_2 <- str_extract(rutas_wav, regex("[[:word:]]+\\.[[:word:]]+$"))
rutas_wav_3 <- rutas_wav[rutas_wav_2 %in% archivos_copiar$x]
rutas_wav_3 <- unique(rutas_wav_3)

fallas <- file.copy(rutas_wav_3, "//madmexservices.conabio.gob.mx/sacmod/snmb_piloto/grabaciones")
sum(!fallas)

rutas_avi_2 <- str_extract(rutas_avi, regex("[[:word:]]+\\.[[:word:]]+$"))
rutas_avi_3 <- rutas_wav[rutas_avi_2 %in% archivos_copiar$x]
rutas_avi_3 <- unique(rutas_avi_3)

fallas <- file.copy(rutas_wav_3, "//madmexservices.conabio.gob.mx/sacmod/snmb_piloto/grabaciones")
sum(!fallas)

#Videos (no se copió ninguno) porque no tienen camera_id:
#videos <- archivos_camara$filename %in% grep(".AVI", archivos_camara$filename, value = TRUE)
#archivos_camara[videos,]



# Migracion del esquema v12 al esquema v15

# Supuestos:
# 
# 1. Se utiliza exáctamente el mismo muestreo de transecto para especies invasoras
# que para huellas y excretas, y los campos "conglomerado_muestra_id" y
# "transecto_numero" forman una llave del mismo, independientemente de la tabla
# en que esté registrada ("Transecto_especies_invasoras_muestra"
# o "Transecto_huellas_excretas_muestra")
# 2.

library(plyr)
library(dplyr)
library(tidyr)
library(stringi)
library(RSQLite)
library(RPostgreSQL)

## Conexión a la base de datos, enlistando las tablas y leyendo todas

base_input <- src_sqlite("../datos/2016_11_09.sqlite")

nombres_tablas <- src_tbls(base_input)
# Notar que las tablas que no fueron creadas por Web2py no vienen en el respaldo
# SQLite, sólo en el Postgres. Por ello, se deberán crear código SQL para cada una
# y correrlo en la nueva base de datos migrada, o conectarse directo a la base
# PostgreSQL.

tablas <- llply(nombres_tablas, function(nombre, base){
  collect(tbl(base, nombre), n = Inf)
}, base = base_input)

names(tablas) <- nombres_tablas

### Actualización de "Conglomerado_muestra"

################################################################################
# Conglomerado_muestra_nueva
################################################################################
Conglomerado_muestra_nueva <- tablas$Conglomerado_muestra %>%
  mutate(
    version_cliente = "3"
  )

### Actualización de "Archivo_camara"

################################################################################
# Archivo_camara_nueva
################################################################################
Archivo_camara_nueva <- tablas$Archivo_camara %>%
  mutate(
    timestamp_ecoinformatica = rep(NA, nrow(.))
  )

### Transecto_especies_invasoras_muestra y Transecto_huellas_excretas_muestra ->
### Transecto_muestra

# Revisando los nombres de las columnas.
colnames(tablas$Transecto_especies_invasoras_muestra)
colnames(tablas$Transecto_huellas_excretas_muestra)

# Revisando si no hay registros de más
nrow(tablas$Transecto_especies_invasoras_muestra)
tablas$Transecto_especies_invasoras_muestra %>%
  select(
    conglomerado_muestra_id,
    transecto_numero
  ) %>%
  unique() %>%
  nrow()
# Difieren en 1 los números de registros, quiere decir que hay un muestreo de transecto con
# mismo "conglomerado_muestra_id" y "transecto_numero" registrado dos veces.

tablas$Transecto_especies_invasoras_muestra %>%
  group_by(conglomerado_muestra_id, transecto_numero) %>%
  tally() %>%
  arrange(desc(n))
# 813 Transecto 4 es el que está duplicado con id's 880 y 881.

tablas$Transecto_especies_invasoras_muestra %>%
  filter(conglomerado_muestra_id == 813, transecto_numero == "Transecto 4")

nrow(tablas$Transecto_huellas_excretas_muestra) == tablas$Transecto_huellas_excretas_muestra %>%
  select(
    conglomerado_muestra_id,
    transecto_numero
  ) %>%
  unique() %>%
  nrow()
# Difieren en 1 los números de registros, quiere decir que hay un muestreo de transecto con
# mismo "conglomerado_muestra_id" y "transecto_numero" registrado dos veces.

tablas$Transecto_huellas_excretas_muestra %>%
  group_by(conglomerado_muestra_id, transecto_numero) %>%
  tally() %>%
  arrange(desc(n))

tablas$Transecto_huellas_excretas_muestra %>%
  filter(conglomerado_muestra_id == 1036, transecto_numero == "Transecto 2")
# 1036 Transecto 2 es el que está duplicado con id's 1408 y 1409

# Revisando el tipo de datos de los comentarios
tablas$Transecto_huellas_excretas_muestra %>%
  select(comentario) %>%
  filter(is.na(comentario) | comentario == "") %>%
  is.na() %>%
  sum()

# No hay comentarios con NA: si salen NA's, son artefactos del join.

# Construyendo "Transecto_muestra_aux" a partir de los dos anteriores:
Transecto_muestra_aux <- tablas$Transecto_especies_invasoras_muestra %>%
  # filtrando registros duplicados
  filter(id != 881) %>%
  # haciendo el join bilateral por "conglomerado_muestra_id" y "transecto_numero"
  full_join(tablas$Transecto_huellas_excretas_muestra %>%
      # con los datos filtrados
      filter(id != 1409),
    by = c("conglomerado_muestra_id", "transecto_numero"),
    suffix = c(".invasoras", ".huellas_excretas")) %>%
  # quitando NA's artefacto de los comentarios.
  replace_na(list(comentario.invasoras = "", comentario.huellas_excretas = "")) %>%
  # obteniendo los campos necesarios para "Transecto_muestra"
  transmute(
    transecto_muestra_id = 1:nrow(.),
    conglomerado_muestra_id = conglomerado_muestra_id,
    transecto_numero = transecto_numero,
    existe = "T",
    fecha = ifelse(!is.na(fecha.invasoras), fecha.invasoras, fecha.huellas_excretas),
    hora_inicio = ifelse(!is.na(hora_inicio.invasoras), hora_inicio.invasoras, hora_inicio.huellas_excretas),
    hora_termino = ifelse(!is.na(hora_termino.invasoras), hora_termino.invasoras, hora_termino.huellas_excretas),
    tecnico = ifelse(!is.na(tecnico.invasoras), tecnico.invasoras, tecnico.huellas_excretas),
    comentario = paste0(comentario.invasoras, comentario.huellas_excretas),
    transecto_especies_invasoras_id = id.invasoras,
    transecto_huellas_excretas_id = id.huellas_excretas
  )

# Revisando "Transecto_muestra_aux"
View(Transecto_muestra_aux)

Transecto_muestra_aux %>%
  group_by(conglomerado_muestra_id, transecto_numero) %>%
  tally() %>%
  arrange(desc(n))
#  Todo bien

## Actualizando "Especie_invasora" y "Huella_excreta"

################################################################################
# Especie_invasora_nueva
################################################################################
Especie_invasora_nueva <- tablas$Especie_invasora %>%
  left_join(Transecto_muestra_aux %>%
      select(
        transecto_muestra_id,
        transecto_especies_invasoras_id
      ), by = "transecto_especies_invasoras_id") %>%
  select(-transecto_especies_invasoras_id)
################################################################################

# Algunas comprobaciones.

nrow(Especie_invasora_nueva) == nrow(tablas$Especie_invasora)
View(Especie_invasora_nueva)
sum(is.na(Especie_invasora_nueva$transecto_muestra_id))

################################################################################
# Huella_excreta_nueva
################################################################################
Huella_excreta_nueva <- tablas$Huella_excreta %>%
  left_join(Transecto_muestra_aux %>%
      select(
        transecto_muestra_id,
        transecto_huellas_excretas_id
      ), by = "transecto_huellas_excretas_id") %>%
  select(-transecto_huellas_excretas_id)
################################################################################

# Algunas comprobaciones.

nrow(Huella_excreta_nueva) == nrow(tablas$Huella_excreta)
View(Huella_excreta_nueva)
sum(is.na(Huella_excreta_nueva$transecto_muestra_id))

# Actualizando "Transecto_muestra"

################################################################################
# Transecto_muestra_nueva
################################################################################
Transecto_muestra_nueva <- Transecto_muestra_aux %>%
  mutate(
    id = transecto_muestra_id
  ) %>%
  select(
    -transecto_muestra_id,
    -transecto_especies_invasoras_id,
    -transecto_huellas_excretas_id
  )

### Conteo_ave -> Avistamiento_aves
### Archivo_conteo_ave -> Archivo_avistamiento_aves

################################################################################
# Avistamiento_aves_nueva
################################################################################
Avistamiento_aves_nueva <- tablas$Conteo_ave

################################################################################
# Archivo_avistamiento_aves_nueva
################################################################################
Archivo_avistamiento_aves_nueva <- tablas$Archivo_conteo_ave %>%
  select(
    id,
    avistamiento_aves_id = conteo_ave_id,
    archivo_nombre_original,
    archivo
  )

### Actualizando de "Arbol_cuadrante"

################################################################################
# Arbol_cuadrante_nueva
################################################################################
Arbol_cuadrante_nueva <- tablas$Arbol_cuadrante %>%
  mutate(
    cambios = rep("", nrow(.)),
    forma_vida = rep("", nrow(.)),
    diametro_normal = as.character(diametro_normal)
  )

### Creando la lista con las tablas de salida:

tablas_nuevas <- tablas

tablas_nuevas$Conglomerado_muestra <- Conglomerado_muestra_nueva

tablas_nuevas$Archivo_camara <- Archivo_camara_nueva

tablas_nuevas$Especie_invasora <- Especie_invasora_nueva
tablas_nuevas$Huella_excreta <- Huella_excreta_nueva
tablas_nuevas$Transecto_especies_invasoras_muestra <- NULL
tablas_nuevas$Transecto_huellas_excretas_muestra <- NULL
tablas_nuevas$Transecto_muestra <- Transecto_muestra_nueva

tablas_nuevas$Avistamiento_aves <- Avistamiento_aves_nueva
tablas_nuevas$Conteo_ave <- NULL

tablas_nuevas$Archivo_avistamiento_aves <- Archivo_avistamiento_aves_nueva
tablas_nuevas$Archivo_conteo_ave <- NULL

tablas_nuevas$Arbol_cuadrante <- Arbol_cuadrante_nueva

names(tablas_nuevas)


##Idea: usando el fusionador v14 manejar la info de los clientes v5, luego migrar
##  los esquemas de las bases de los clientes (fusionadas) a v15, e insertarlas en
## este esquema con un fusionador v15.

### Escribiendo tablas en la base de datos SQLite

base_output <- dbConnect(RSQLite::SQLite(), "../datos/bases_salida/base_output.db")

dbWriteTable(base_output, "Conglomerado_muestra", as.data.frame(Conglomerado_muestra),
  overwrite = FALSE, append = TRUE)
dbWriteTable(base_output, "Sitio_muestra", as.data.frame(Sitio_muestra),
  overwrite = FALSE, append = TRUE)

dbWriteTable(base_output, "Transecto_especies_invasoras_muestra",
  as.data.frame(Transecto_especies_invasoras_muestra), overwrite = FALSE, append = TRUE)
dbWriteTable(base_output, "Especie_invasora", as.data.frame(Especie_invasora),
  overwrite = FALSE, append = TRUE)
dbWriteTable(base_output, "Archivo_especie_invasora", as.data.frame(Archivo_especie_invasora),
  overwrite = FALSE, append = TRUE)

dbWriteTable(base_output, "Transecto_huellas_excretas_muestra",
  as.data.frame(Transecto_huellas_excretas_muestra), overwrite = FALSE, append = TRUE)
dbWriteTable(base_output, "Huella_excreta", as.data.frame(Huella_excreta),
  overwrite = FALSE, append = TRUE)
dbWriteTable(base_output, "Archivo_huella_excreta", as.data.frame(Archivo_huella_excreta),
  overwrite = FALSE, append = TRUE)

dbWriteTable(base_output, "Camara", as.data.frame(Camara), overwrite = FALSE,
  append = TRUE)
dbWriteTable(base_output, "Archivo_camara", as.data.frame(Archivo_camara),
  overwrite = FALSE, append = TRUE)
dbWriteTable(base_output, "Imagen_referencia_camara", as.data.frame(Imagen_referencia_camara),
  overwrite = FALSE, append = TRUE)

dbWriteTable(base_output, "Grabadora", as.data.frame(Grabadora), overwrite = FALSE,
  append = TRUE)
dbWriteTable(base_output, "Archivo_grabadora", as.data.frame(Archivo_grabadora),
  overwrite = FALSE, append = TRUE)
dbWriteTable(base_output, "Imagen_referencia_microfonos",
  as.data.frame(Imagen_referencia_microfonos), overwrite = FALSE, append = TRUE)

### Leyendo catálogos de la base del cliente:

base_catalogos <- src_sqlite("../datos/aux/base_cliente.db")

Cat_tipo_conglomerado <- tbl(base_catalogos, "Cat_tipo_conglomerado") %>%
  collect()           
Cat_estado_conglomerado <- tbl(base_catalogos, "Cat_estado_conglomerado") %>%
  collect()       
Cat_tenencia_conglomerado <- tbl(base_catalogos, "Cat_tenencia_conglomerado") %>%
  collect()   
Cat_suelo_conglomerado <- tbl(base_catalogos, "Cat_suelo_conglomerado") %>%
  collect()         
Cat_vegetacion_conglomerado <- tbl(base_catalogos, "Cat_vegetacion_conglomerado") %>%
  collect()
Cat_numero_sitio <- tbl(base_catalogos, "Cat_numero_sitio") %>%
  collect()                     
Cat_elipsoide <- tbl(base_catalogos, "Cat_elipsoide") %>%
  collect()                           
Cat_resolucion_camara <- tbl(base_catalogos, "Cat_resolucion_camara") %>%
  collect()           
Cat_sensibilidad_camara <- tbl(base_catalogos, "Cat_sensibilidad_camara") %>%
  collect()       
Cat_numero_transecto <- tbl(base_catalogos, "Cat_numero_transecto") %>%
  collect()             
Cat_numero_individuos <- tbl(base_catalogos, "Cat_numero_individuos") %>%
  collect()           
Cat_conabio_invasoras <- tbl(base_catalogos, "Cat_conabio_invasoras") %>%
  collect()           
Cat_municipio_conglomerado <- tbl(base_catalogos, "Cat_municipio_conglomerado") %>%
  collect() 
Cat_material_carbono <- tbl(base_catalogos, "Cat_material_carbono") %>%
  collect()             
Cat_grado_carbono <- tbl(base_catalogos, "Cat_grado_carbono") %>%
  collect()                   
Cat_transecto_direccion <- tbl(base_catalogos, "Cat_transecto_direccion") %>%
  collect()       
Cat_forma_vida <- tbl(base_catalogos, "Cat_forma_vida") %>%
  collect()                         
Cat_condiciones_ambientales <- tbl(base_catalogos, "Cat_condiciones_ambientales") %>%
  collect()
Cat_tipo_impacto <- tbl(base_catalogos, "Cat_tipo_impacto") %>%
  collect()                     
Cat_severidad_impactos <- tbl(base_catalogos, "Cat_severidad_impactos") %>%
  collect()         
Cat_agente_impactos <- tbl(base_catalogos, "Cat_agente_impactos") %>%
  collect()               
Cat_estatus_impactos <- tbl(base_catalogos, "Cat_estatus_impactos") %>%
  collect()             
Cat_prop_afectacion <- tbl(base_catalogos, "Cat_prop_afectacion") %>%
  collect()               
Cat_incendio <- tbl(base_catalogos, "Cat_incendio") %>%
  collect()                             
Cat_conabio_aves <- tbl(base_catalogos, "Cat_conabio_aves") %>%
  collect()                     

### Escribiendo catálogos en la base de datos SQLite

dbWriteTable(base_output, "Cat_tipo_conglomerado",
  as.data.frame(Cat_tipo_conglomerado), overwrite = FALSE, append = TRUE)
dbWriteTable(base_output, "Cat_estado_conglomerado",
  as.data.frame(Cat_estado_conglomerado), overwrite = FALSE, append = TRUE)
dbWriteTable(base_output, "Cat_tenencia_conglomerado",
  as.data.frame(Cat_tenencia_conglomerado), overwrite = FALSE, append = TRUE)
dbWriteTable(base_output, "Cat_suelo_conglomerado",
  as.data.frame(Cat_suelo_conglomerado), overwrite = FALSE, append = TRUE)
dbWriteTable(base_output, "Cat_vegetacion_conglomerado",
  as.data.frame(Cat_vegetacion_conglomerado), overwrite = FALSE, append = TRUE)
dbWriteTable(base_output, "Cat_numero_sitio",
  as.data.frame(Cat_numero_sitio), overwrite = FALSE, append = TRUE)
dbWriteTable(base_output, "Cat_elipsoide",
  as.data.frame(Cat_elipsoide), overwrite = FALSE, append = TRUE)
dbWriteTable(base_output, "Cat_resolucion_camara",
  as.data.frame(Cat_resolucion_camara), overwrite = FALSE, append = TRUE)
dbWriteTable(base_output, "Cat_sensibilidad_camara",
  as.data.frame(Cat_sensibilidad_camara), overwrite = FALSE, append = TRUE)
dbWriteTable(base_output, "Cat_numero_transecto",
  as.data.frame(Cat_numero_transecto), overwrite = FALSE, append = TRUE)
dbWriteTable(base_output, "Cat_numero_individuos",
  as.data.frame(Cat_numero_individuos), overwrite = FALSE, append = TRUE)
dbWriteTable(base_output, "Cat_conabio_invasoras",
  as.data.frame(Cat_conabio_invasoras), overwrite = FALSE, append = TRUE)
dbWriteTable(base_output, "Cat_municipio_conglomerado",
  as.data.frame(Cat_municipio_conglomerado), overwrite = FALSE, append = TRUE)
dbWriteTable(base_output, "Cat_material_carbono",
  as.data.frame(Cat_material_carbono), overwrite = FALSE, append = TRUE)
dbWriteTable(base_output, "Cat_grado_carbono",
  as.data.frame(Cat_grado_carbono), overwrite = FALSE, append = TRUE)
dbWriteTable(base_output, "Cat_transecto_direccion",
  as.data.frame(Cat_transecto_direccion), overwrite = FALSE, append = TRUE)
dbWriteTable(base_output, "Cat_forma_vida",
  as.data.frame(Cat_forma_vida), overwrite = FALSE, append = TRUE)
dbWriteTable(base_output, "Cat_condiciones_ambientales",
  as.data.frame(Cat_condiciones_ambientales), overwrite = FALSE, append = TRUE)
dbWriteTable(base_output, "Cat_tipo_impacto",
  as.data.frame(Cat_tipo_impacto), overwrite = FALSE, append = TRUE)
dbWriteTable(base_output, "Cat_severidad_impactos",
  as.data.frame(Cat_severidad_impactos), overwrite = FALSE, append = TRUE)
dbWriteTable(base_output, "Cat_agente_impactos",
  as.data.frame(Cat_agente_impactos), overwrite = FALSE, append = TRUE)
dbWriteTable(base_output, "Cat_estatus_impactos",
  as.data.frame(Cat_estatus_impactos), overwrite = FALSE, append = TRUE)
dbWriteTable(base_output, "Cat_prop_afectacion",
  as.data.frame(Cat_prop_afectacion), overwrite = FALSE, append = TRUE)
dbWriteTable(base_output, "Cat_incendio",
  as.data.frame(Cat_incendio), overwrite = FALSE, append = TRUE)
dbWriteTable(base_output, "Cat_conabio_aves",
  as.data.frame(Cat_conabio_aves), overwrite = FALSE, append = TRUE)

#dbCommit(base_output)
dbDisconnect(base_output)

### Escribiendo tablas en la base de datos PostgreSQL

drv <- dbDriver("PostgreSQL")
base_output <- dbConnect(drv = drv, dbname = "snmb_v12", port = 5432,
  host = "localhost", 
  user = "fpardo", password = "")

dbWriteTable(base_output, "conglomerado_muestra", as.data.frame(Conglomerado_muestra),
  overwrite = FALSE, append = TRUE, row.names = 0)

dbWriteTable(base_output, "sitio_muestra", as.data.frame(Sitio_muestra),
  overwrite = FALSE, append = TRUE, row.names = 0)

dbWriteTable(base_output, "transecto_especies_invasoras_muestra",
  as.data.frame(Transecto_especies_invasoras_muestra), overwrite = FALSE, append = TRUE, row.names = 0)
dbWriteTable(base_output, "especie_invasora",
  as.data.frame(Especie_invasora), overwrite = FALSE, append = TRUE, row.names = 0)
dbWriteTable(base_output, "archivo_especie_invasora",
  as.data.frame(Archivo_especie_invasora), overwrite = FALSE, append = TRUE, row.names = 0)

dbWriteTable(base_output, "transecto_huellas_excretas_muestra",
  as.data.frame(Transecto_huellas_excretas_muestra), overwrite = FALSE, append = TRUE, row.names = 0)
dbWriteTable(base_output, "huella_excreta", as.data.frame(Huella_excreta),
  overwrite = FALSE, append = TRUE, row.names = 0)
dbWriteTable(base_output, "archivo_huella_excreta", as.data.frame(Archivo_huella_excreta),
  overwrite = FALSE, append = TRUE, row.names = 0)

dbWriteTable(base_output, "camara", as.data.frame(Camara), overwrite = FALSE,
  append = TRUE, row.names = 0)
dbWriteTable(base_output, "archivo_camara", as.data.frame(Archivo_camara),
  overwrite = FALSE, append = TRUE, row.names = 0)
dbWriteTable(base_output, "imagen_referencia_camara", as.data.frame(Imagen_referencia_camara),
  overwrite = FALSE, append = TRUE, row.names = 0)

dbWriteTable(base_output, "grabadora", as.data.frame(Grabadora), overwrite = FALSE,
  append = TRUE, row.names = 0)
dbWriteTable(base_output, "archivo_grabadora", as.data.frame(Archivo_grabadora),
  overwrite = FALSE, append = TRUE, row.names = 0)
dbWriteTable(base_output, "imagen_referencia_microfonos",
  as.data.frame(Imagen_referencia_microfonos), overwrite = FALSE, append = TRUE, row.names = 0)

# Actualizando secuencias para las id's en la base de datos PostgreSQL:

#-- Login to psql and run the following
#-- What is the result?
dbGetQuery(base_output, "SELECT MAX(id) FROM Conglomerado_muestra;")

#-- Then run...
#-- This should be higher than the last result.
dbGetQuery(base_output, "SELECT nextval('Conglomerado_muestra_id_seq');")

#-- If it's not higher... run this set the sequence last to your highest pid it. 
#-- (wise to run a quick pg_dump first...)

dbGetQuery(base_output,
  "SELECT setval('Conglomerado_muestra_id_seq', (SELECT MAX(id) FROM Conglomerado_muestra));")
dbGetQuery(base_output,
  "SELECT setval('Sitio_muestra_id_seq', (SELECT MAX(id) FROM Sitio_muestra));")

dbGetQuery(base_output,
  "SELECT setval('Transecto_especies_invasoras_muestra_id_seq', (SELECT MAX(id) FROM Transecto_especies_invasoras_muestra));")
dbGetQuery(base_output,
  "SELECT setval('Especie_invasora_id_seq', (SELECT MAX(id) FROM Especie_invasora));")
dbGetQuery(base_output,
  "SELECT setval('Archivo_especie_invasora_id_seq', (SELECT MAX(id) FROM Archivo_especie_invasora));")

dbGetQuery(base_output,
  "SELECT setval('Transecto_huellas_excretas_muestra_id_seq', (SELECT MAX(id) FROM Transecto_huellas_excretas_muestra));")
dbGetQuery(base_output,
  "SELECT setval('Huella_excreta_id_seq', (SELECT MAX(id) FROM Huella_excreta));")
dbGetQuery(base_output,
  "SELECT setval('Archivo_huella_excreta_id_seq', (SELECT MAX(id) FROM Archivo_huella_excreta));")

dbGetQuery(base_output,
  "SELECT setval('Camara_id_seq', (SELECT MAX(id) FROM Camara));")
dbGetQuery(base_output,
  "SELECT setval('Archivo_camara_id_seq', (SELECT MAX(id) FROM Archivo_camara));")
dbGetQuery(base_output,
  "SELECT setval('Imagen_referencia_camara_id_seq', (SELECT MAX(id) FROM Imagen_referencia_camara));")

dbGetQuery(base_output,
  "SELECT setval('Grabadora_id_seq', (SELECT MAX(id) FROM Grabadora));")
dbGetQuery(base_output,
  "SELECT setval('Archivo_grabadora_id_seq', (SELECT MAX(id) FROM Archivo_grabadora));")
dbGetQuery(base_output,
  "SELECT setval('Imagen_referencia_microfonos_id_seq', (SELECT MAX(id) FROM Imagen_referencia_microfonos));")

#-- if your tables might have no rows
#-- false means the set value will be returned by the next nextval() call    
#SELECT setval('your_table_id_seq', COALESCE((SELECT MAX(id)+1 FROM your_table), 1), false);

### Escribiendo catálogos en la base de datos PostgreSQL:

dbWriteTable(base_output, "cat_tipo_conglomerado", as.data.frame(Cat_tipo_conglomerado),
  overwrite = FALSE, append = TRUE, row.names = 0)
dbWriteTable(base_output, "cat_estado_conglomerado", as.data.frame(Cat_estado_conglomerado),
  overwrite = FALSE, append = TRUE, row.names = 0)
dbWriteTable(base_output, "cat_tenencia_conglomerado", as.data.frame(Cat_tenencia_conglomerado),
  overwrite = FALSE, append = TRUE, row.names = 0)
dbWriteTable(base_output, "cat_suelo_conglomerado", as.data.frame(Cat_suelo_conglomerado),
  overwrite = FALSE, append = TRUE, row.names = 0)
dbWriteTable(base_output, "cat_vegetacion_conglomerado", as.data.frame(Cat_vegetacion_conglomerado),
  overwrite = FALSE, append = TRUE, row.names = 0)
dbWriteTable(base_output, "cat_numero_sitio", as.data.frame(Cat_numero_sitio),
  overwrite = FALSE, append = TRUE, row.names = 0)
dbWriteTable(base_output, "cat_elipsoide", as.data.frame(Cat_elipsoide),
  overwrite = FALSE, append = TRUE, row.names = 0)
dbWriteTable(base_output, "cat_resolucion_camara", as.data.frame(Cat_resolucion_camara),
  overwrite = FALSE, append = TRUE, row.names = 0)
dbWriteTable(base_output, "cat_sensibilidad_camara", as.data.frame(Cat_sensibilidad_camara),
  overwrite = FALSE, append = TRUE, row.names = 0)
dbWriteTable(base_output, "cat_numero_transecto", as.data.frame(Cat_numero_transecto),
  overwrite = FALSE, append = TRUE, row.names = 0)
dbWriteTable(base_output, "cat_numero_individuos", as.data.frame(Cat_numero_individuos),
  overwrite = FALSE, append = TRUE, row.names = 0)
dbWriteTable(base_output, "cat_conabio_invasoras", as.data.frame(Cat_conabio_invasoras),
  overwrite = FALSE, append = TRUE, row.names = 0)
dbWriteTable(base_output, "cat_municipio_conglomerado", as.data.frame(Cat_municipio_conglomerado),
  overwrite = FALSE, append = TRUE, row.names = 0)
dbWriteTable(base_output, "cat_material_carbono", as.data.frame(Cat_material_carbono),
  overwrite = FALSE, append = TRUE, row.names = 0)
dbWriteTable(base_output, "cat_grado_carbono", as.data.frame(Cat_grado_carbono),
  overwrite = FALSE, append = TRUE, row.names = 0)
dbWriteTable(base_output, "cat_transecto_direccion", as.data.frame(Cat_transecto_direccion),
  overwrite = FALSE, append = TRUE, row.names = 0)
dbWriteTable(base_output, "cat_forma_vida", as.data.frame(Cat_forma_vida),
  overwrite = FALSE, append = TRUE, row.names = 0)
dbWriteTable(base_output, "cat_condiciones_ambientales", as.data.frame(Cat_condiciones_ambientales),
  overwrite = FALSE, append = TRUE, row.names = 0)
dbWriteTable(base_output, "cat_tipo_impacto", as.data.frame(Cat_tipo_impacto),
  overwrite = FALSE, append = TRUE, row.names = 0)
dbWriteTable(base_output, "cat_severidad_impactos", as.data.frame(Cat_severidad_impactos),
  overwrite = FALSE, append = TRUE, row.names = 0)
dbWriteTable(base_output, "cat_agente_impactos", as.data.frame(Cat_agente_impactos),
  overwrite = FALSE, append = TRUE, row.names = 0)
dbWriteTable(base_output, "cat_estatus_impactos", as.data.frame(Cat_estatus_impactos),
  overwrite = FALSE, append = TRUE, row.names = 0)
dbWriteTable(base_output, "cat_prop_afectacion", as.data.frame(Cat_prop_afectacion),
  overwrite = FALSE, append = TRUE, row.names = 0)
dbWriteTable(base_output, "cat_incendio", as.data.frame(Cat_incendio),
  overwrite = FALSE, append = TRUE, row.names = 0)
dbWriteTable(base_output, "cat_conabio_aves", as.data.frame(Cat_conabio_aves),
  overwrite = FALSE, append = TRUE, row.names = 0)

# Actualizando secuencias para las id's de los catálogos en la base de datos Postgres

dbGetQuery(base_output,
  "SELECT setval('cat_tipo_conglomerado_id_seq', (SELECT MAX(id) FROM cat_tipo_conglomerado));")
dbGetQuery(base_output,
  "SELECT setval('cat_estado_conglomerado_id_seq', (SELECT MAX(id) FROM cat_estado_conglomerado));")
dbGetQuery(base_output,
  "SELECT setval('cat_tenencia_conglomerado_id_seq', (SELECT MAX(id) FROM cat_tenencia_conglomerado));")
dbGetQuery(base_output,
  "SELECT setval('cat_suelo_conglomerado_id_seq', (SELECT MAX(id) FROM cat_suelo_conglomerado));")
dbGetQuery(base_output,
  "SELECT setval('cat_vegetacion_conglomerado_id_seq', (SELECT MAX(id) FROM cat_vegetacion_conglomerado));")
dbGetQuery(base_output,
  "SELECT setval('cat_numero_sitio_id_seq', (SELECT MAX(id) FROM cat_numero_sitio));")
dbGetQuery(base_output,
  "SELECT setval('cat_elipsoide_id_seq', (SELECT MAX(id) FROM cat_elipsoide));")
dbGetQuery(base_output,
  "SELECT setval('cat_resolucion_camara_id_seq', (SELECT MAX(id) FROM cat_resolucion_camara));")
dbGetQuery(base_output,
  "SELECT setval('cat_sensibilidad_camara_id_seq', (SELECT MAX(id) FROM cat_sensibilidad_camara));")
dbGetQuery(base_output,
  "SELECT setval('cat_numero_transecto_id_seq', (SELECT MAX(id) FROM cat_numero_transecto));")
dbGetQuery(base_output,
  "SELECT setval('cat_numero_individuos_id_seq', (SELECT MAX(id) FROM cat_numero_individuos));")
dbGetQuery(base_output,
  "SELECT setval('cat_conabio_invasoras_id_seq', (SELECT MAX(id) FROM cat_conabio_invasoras));")
dbGetQuery(base_output,
  "SELECT setval('cat_municipio_conglomerado_id_seq', (SELECT MAX(id) FROM cat_municipio_conglomerado));")
dbGetQuery(base_output,
  "SELECT setval('cat_material_carbono_id_seq', (SELECT MAX(id) FROM cat_material_carbono));")
dbGetQuery(base_output,
  "SELECT setval('cat_grado_carbono_id_seq', (SELECT MAX(id) FROM cat_grado_carbono));")
dbGetQuery(base_output,
  "SELECT setval('cat_transecto_direccion_id_seq', (SELECT MAX(id) FROM cat_transecto_direccion));")
dbGetQuery(base_output,
  "SELECT setval('cat_forma_vida_id_seq', (SELECT MAX(id) FROM cat_forma_vida));")
dbGetQuery(base_output,
  "SELECT setval('cat_condiciones_ambientales_id_seq', (SELECT MAX(id) FROM cat_condiciones_ambientales));")
dbGetQuery(base_output,
  "SELECT setval('cat_tipo_impacto_id_seq', (SELECT MAX(id) FROM cat_tipo_impacto));")
dbGetQuery(base_output,
  "SELECT setval('cat_severidad_impactos_id_seq', (SELECT MAX(id) FROM cat_severidad_impactos));")
dbGetQuery(base_output,
  "SELECT setval('cat_agente_impactos_id_seq', (SELECT MAX(id) FROM cat_agente_impactos));")
dbGetQuery(base_output,
  "SELECT setval('cat_estatus_impactos_id_seq', (SELECT MAX(id) FROM cat_estatus_impactos));")
dbGetQuery(base_output,
  "SELECT setval('cat_prop_afectacion_id_seq', (SELECT MAX(id) FROM cat_prop_afectacion));")
dbGetQuery(base_output,
  "SELECT setval('cat_incendio_id_seq', (SELECT MAX(id) FROM cat_incendio));")
dbGetQuery(base_output,
  "SELECT setval('cat_conabio_aves_id_seq', (SELECT MAX(id) FROM cat_conabio_aves));")

#dbCommit(base_output)
dbDisconnect(base_output)

### Creando lista con nombres de archivos

#Esta lista se utilizará para buscar los archivos en la estructura de entregas
#guardada en el cluster.

archivos <- c(
  Archivo_especie_invasora$archivo,
  Archivo_huella_excreta$archivo,
  Archivo_camara$archivo,
  Imagen_referencia_camara$archivo,
  Archivo_grabadora$archivo,
  Imagen_referencia_microfonos$archivo)

write.table(archivos, file = "../datos/aux/nombres_archivos_snmb.csv", 
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

archivos_copiar <- read.csv("../datos/aux/nombres_archivos_snmb.csv",
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


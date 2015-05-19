library(plyr)
library(tidyr)
library(stringr)
library(RSQLite)
library(dplyr)

bases_rutas <- list.files(path = "/Volumes/sacmod/data_06_02_2015", 
  recursive = TRUE, full.names = TRUE, pattern = "\\.db$")

sapply(bases_rutas, function(i) grep(".*Thumbs", i))

bases_snmb <- bases_rutas[grep("Thumbs", bases_rutas, invert = TRUE)]
basename(bases_snmb)


bases_rutas <- list.files(path = "/Volumes/sacmod/piloto_faltantes_07052015", 
  recursive = TRUE, full.names = TRUE, pattern = "\\.db$")

sapply(bases_rutas, function(i) grep(".*Thumbs", i))

bases_snmb <- bases_rutas[grep("Thumbs", bases_rutas, invert = TRUE)]
basename(bases_snmb)


conglomerado_tabs <- ldply(bases_rutas, leerDbTab, "conglomerates")


###############################################################################


dir_j = "/Volumes/sacmod"

# Obtenemos rutas de bases de la forma snmb.db localizadas en dir_j
bases_rutas <- list.files(path = dir_j, 
  recursive = TRUE, full.names = TRUE, pattern = "\\.db$")

bases_snmb <- bases_rutas[grep("resources/db/snmb.db", bases_rutas, invert = F)]
basename(bases_snmb)

# copiamos a carpeta local

copiaRenombra <- function(dir_j_archivo, dir_local, id_db){
  # dir_j: directorio (con nombre archivo) donde se ubica la base a copiar
  # dir_local: directorio (sin nombre de archivo) donde se copiará la base
  # id_db: id numérico que se utilizará para distinguir bases en migración
  file.copy(dir_j_archivo, dir_local,  overwrite = FALSE)
  file.rename(from = paste(dir_local, basename(dir_j_archivo), sep = "/"), 
    to = paste(dir_local, "/", id_db, "_", basename(dir_j_archivo), sep = ""))
}

sapply(1:length(bases_snmb), function(i) copiaRenombra(bases_snmb[i], 
  "../datos/bases_snmb", 1000*i))

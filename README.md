# ETL SNMB

Migrar base de datos con esquema de la versión piloto a base de datos con esquema final.

El script *migracion.R* importa las bases sqlite generadas con el cliente piloto, realiza las uniones y transformaciones
necesarias y exporta a una base postgresql y a una base sqlite.

### Supuestos:

1. Los clientes finales están almacenados en carpetas de la forma 
resources/db/snmb.db, esto supone que los clientes en archives/db/snmb.db son
respaldos.

2. Cada vez que encontramos un conglomerado en un cliente final, este 
se ingresó completo, implica que si encontramos un mismo conglomerado en 
más de un cliente estos registros serán idénticos.

### Funcionamiento:

1. Se copian las bases (.db) de la carpeta */Volumes/sacmod* a la carpeta local *bases_snmb*.

2. Se lee la información tabla por tabla uniendo todas las bases encontradas en el paso anterior, para evitar conflictos 
  se crean nuevos ids.

3. Se transforman las tablas para seguir el esquema de la nueva versión del cliente.

4. Se transforman los ids de las tablas para seguir una secuencia de enteros sucesivos.

5. Se conecta a una base de datos postgres que tiene el esquema de la base de datos final, para esto se crea una nueva base de 
datos postgres y se declara el esquema usando el [fusionador](https://github.com/fpardourrutia/fusionador_snmb). Nótese que esta
base no contiene registros, solo el esquema.

6. Se escriben las tablas en la base de datos postgresql.

7. Se actualiza la secuencia generadora de ids ya que R no lo hace automáticamente al insertar los registros, lo que ocasiona
problemas al crear nuevos registros.

8. Se conecta a una base de datos sqlite que tiene el esquema de la base de datos final, para esto se exporta una base de datos usando el [fusionador](https://github.com/fpardourrutia/fusionador_snmb). Análogo al paso 5 esta base no contiene registros, solo el esquema.

9. Se escriben las tablas en la base sqlite.

10. Se exportan los archivos de media (imágenes, videos y grabaciones) a las carpetas ordenadas en */Volumes/sacmod*.

Los pasos 5 a 7 se utilizan para exportar la base postgresql, los pasos 8 y 9 para la sqlite.

### Carpetas y Archivos
La estructura es como sigue.
```
etl_snmb
│   README.md
│
└───datos
|   ├───bases_snmb
|   ├───bases_salida
|   |   |    base_output.db
└───scripts
|   |   migracion.R
|   |   reporte_migracion.Rmd
```

* bases_snmb: aquí se guardan (usando el script) las bases a fusinar.

* bases_salidas: almacena la base sqlite *base_output.db* esta se guarda manualmente siguiendo el paso 8, es decir, 
originalmente es una base sin registros pero con el esquema final, a la que se escriben los registros fusionados usando el script.

* scripts: aquí están los scripts de migración y el script que reporta los resultados de la misma.

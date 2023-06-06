# Práctica 4 - Spark

Integrantes del equipo:

  - Enrique E. de Alvear Doñate
  - Lucía Roldán Rodríguez
  - Laura Cano Gómez

## Introducción
Planteamiento, diseño e implementación de una solución a un problema de análisis de datos utilizando Spark. El dataset sobre el que se trabaja es el que proporciona el ayuntamiento de Madrid del uso del sistema de bicicletas de préstamo BICIMAD. 


## Problema propuesto

Con objetivo de disminuir el número de accidentes derivados de la interacción de las bicis en el tráfico, se quiere construir un carril bici en los trayectos más frecuentados. Además hasta que estos carriles estén construidos en la franja horaria de mayor uso de las bicicletas en estos trayectos, se reservará un carril a la circulación para los ciclos. Por último, de cara a una implementación a largo plazo, se continuará con la construcción del carril bici en las zonas donde residan un mayor número de usuarios de estas biciletas.

Por ello con objetivo dee la resolución de este problema se estudiarán los siguientes datos:

  - Los 100  trayectos más frecuentados

  - La hora más frecuentada en los trayectos anteriores

  - La franja de edad donde más se usan las bicicletas eléctricas.

Cabe destacar que sólo se excluirán los usuarios ocasionales o trabajadores de la empresa en ese estudio.

## Archivos
En este repositorio podemos encontrar varios archivos:
 - **Bicimad.pdf**: donde se encuentra la descripción y motivación del problema, el proceso de diseño e implementación de la solución y su aplicación a los resultados. Además sepuede observar su ejecución en el cluster.
 - **Practica_BiciMad.ipynb**: notebook donde se encuentra el código ya ejecutado. Se recomienda la utilización de este a la hora de estudiar el código puesto que su entendimiento es más claro y completo.
 - **Practica_BiciMad.py**: archivo py que se ha utilizado a la hora de ejecutar en el cluster, contiene el mismo contenido que el notebook excepto por la realización de las gráficas que están únicamente en el notebook. 
 - **201906_Usage_Bicimad.zip**: cuenta con los datos de Junio 2019 comprimidos. (Para ejecutar tanto el notebook como el .py requieren de los datos descomprimidos)
 - **Captura de Pantalla**: se encuentra la imagen de la ejecución del código en el cluster.
 
 ## Ejecución
  - Opción 1: (recomendado para la visualización de los resultados pues cuenta con gráficas y tablas extra)
      - jupyter-notebook
      - Seleccionar Practica_BiciMad.ipynb
  - Opción2: (cuenta con el mismo estudio que en el notebook para varios ficheros usando union de rdd)
      - python3 Practica_BiciMad.py [Nombres_Archivos]*
      
      - ej: python3 Practica_BiciMad.py 201901_Usage_Bicimad.json 201902_Usage_Bicimad.json 201903_Usage_Bicimad.json 201904_Usage_Bicimad.json 201905_Usage_Bicimad.json 201906_Usage_Bicimad.json




 



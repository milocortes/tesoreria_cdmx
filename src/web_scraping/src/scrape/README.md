# Web Scraping

El directorio contiene los programas desarrollados para el scrape de información a Airbnb, Booking y Hoteles.com.

Para realizar solicitudes masivas de información a los Airbnb y Booking se utiliza el paquete ```requests-ip-rotator``` (https://github.com/Ge0rg3/requests-ip-rotator). El paquete utiliza el Gateway API de AWS para generar un pool de IP para construir direcciones IP. Este  mecanismo de rotación de IP evita la negación de recursos y permite realizar solicitudes masivas.

El paquete fue utilizado para realizar web scraping en Airbnb y Booking. Para Hoteles.com, no fue necesario utilizar el paquete. En términos generales, las solicitudes de información se basaron en hacer una consulta de habitaciones disponibles para algún fin de semana para cada colonia de la CDMX.  

## Prerrequisitos para la ejecución del paquete ```requests-ip-rotator```
Tener instalado AWS Software Development Kit (SDK) y contar con una cuenta de acceso a AWS y las respectivas llaves secretas del AMI (access key y secret access key).

Para los detalles de instalación de AWS Software Development Kit (SDK) consulte la liga (https://docs.aws.amazon.com/cli)

Si no tiene disponible llaves secretas,se puede generar un par de llaves al navegar en la consola de administración (https://console.aws.amazon.com/), dar click en el nombre de usuario en la parte superior derecha del menú y elegir la opción "My Security Credentials". Para crear un par nuevo de llaves, dar click en "Create access key".

Una vez que se cuenta con la Access key y Secret access key, ejecuta la instrucción la siguiente instrucción para ingresarlas a la configuración de AWS SDK:

```bash
aws configure
```

Para verificar que la configuración de las llaves ha sido correcta, ejecuta la instrucción

```bash
aws sts get-caller-identity
```

Es necesario que el IAM tenga habilitada la Policy AWSStorageGatewayFullAccess para poder hacer uso de Gateway API de AWS.


## Instalación de paquetes
Para instalar los paquetes necesarios para ejecutar los programas, ejecute la instrucción:

```bash
pip install -r requirements.txt
```

## Ejecución de los programas
Los programas se ejecutan desde linea de comandos. Por ejemplo:
```bash
python AIRBNB_ip_pool_aws.py
```
El scrape de Aribnb lo hace por colonia, el scrape de Booking busca en toda la ciudad de México por fines de semana, las fechas se tienen que cambiar. Hoteles.com busca por alcaldías y es el único código que no necesita que brinque el IP. 

ADVERTENCIA: los tres scrapes corren al día de hoy. Hay que revisarlos periodicamente para asegurarse que la estructura del html no ha cambiado, si esta estructura cambia hay que adaptarlos para que sigan extrayendo la información pertinente. 

## Estimación: 
El archivo que se encuentra en mapas/mapa_folium.py, limpia todos los datos que se obtuvieron del scrape. Posterioremente los junta todos y crea el mapa interactivo, donde se pueden observar los airbnbs, hoteles y bookings. En este mismo archivo hay una estimación simple de la posible recaudación. 



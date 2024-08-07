  # Proyecto Semestral BigData

## Descripción

Este proyecto es una API ~~para mis ex compas del Duoc~~ con Flask que descarga datos desde APIs públicas, los transforma y carga en Google Cloud Storage y BigQuery para su análisis. 

La aplicación maneja datos tanto mensuales como diarios, y utiliza autenticación basada en tokens para asegurar el acceso a los endpoints.

## Funcionalidades

- **Descarga de Datos**: Obtiene datos desde APIs públicas en formato ZIP o JSON.
- **Procesamiento de Datos**: Descomprime archivos, transforma datos y los prepara para la carga.
- **Almacenamiento en la Nube**: Sube los archivos procesados a Google Cloud Storage.
- **Carga en BigQuery**: Carga los datos desde Google Cloud Storage a BigQuery, gestionando tipos de datos y verificando la existencia de tablas y conjuntos de datos.

## Requisitos

- **Python 3.x**
- **Flask**: `pip install flask`
- **Google Cloud SDK**: `pip install google-cloud-storage google-cloud-bigquery`
- **Pandas**: `pip install pandas`
- **Requests**: `pip install requests`

## Configuración

1. **Autenticación**: Configura la autenticación con Google Cloud usando un archivo de clave JSON. Establece la variable de entorno `KEYFILE` con la ruta a este archivo.

2. **Variables de Entorno**: Asegúrate de tener las siguientes variables de entorno configuradas:
   - `KEYFILE`: Ruta al archivo de clave JSON de Google Cloud.
   - `PASSWORD`: Contraseña para la autenticación de tokens.

## Endpoints

### `/cl/red/monthly_data/`

- **Método**: `GET`
- **Descripción**: Descarga y procesa datos mensuales, los sube a Google Cloud Storage y los carga en BigQuery.
- **Parámetros**:
  - `bucket`: Nombre del bucket en Google Cloud Storage.
  - `dataset_id`: ID del conjunto de datos en BigQuery.
- **Token**: Incluye un token en el encabezado de la solicitud para autenticación.

### `/cl/red/daily_data/`

- **Método**: `GET`
- **Descripción**: Obtiene datos diarios, los procesa y carga en Google Cloud Storage y BigQuery.
- **Parámetros**:
  - `bucket`: Nombre del bucket en Google Cloud Storage.
  - `dataset_id`: ID del conjunto de datos en BigQuery.
  - `recorrido_id`: ID del recorrido para obtener los datos.
- **Token**: Incluye un token en el encabezado de la solicitud para autenticación.

## Ejecución

Para ejecutar la aplicación, usa el siguiente comando:

```bash
python app.py

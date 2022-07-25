To install the IWLS PygeoAPI process plugin development server:
    1. Copy content of intall_package to new directory
    2. create and activate  Python virtual environment 
    3. Run install.sh (source ./install.sh)
    4. pygeoapi serve to start server

Server GUI will be available on localhost:5000

Sample syntax for S-104 process query:
    curl -X POST "http://localhost:5000/processes/s100/execution" -H "accept: application/json" -H "Content-Type: application/json" -d "{\"inputs\": {\"layer\":\"S104\",\"bbox\":\"-123.28,49.07,-123.01,49.35\",\"end_time\":\"2021-12-06T23:00:00Z\",\"start_time\":\"2021-12-06T00:00:00Z\"}}" > s104.zip
    - mandatarory input parameters are layer (S104 or S111) bbox (min lon, min lat, max lon, max lat), start_time(ISO 8601), end_time(ISO 8601)
    - Must redirect outputs of curl request to a zip file

On WSL, environment variables must be set every sessions by running the following once the python virtual environment is activated:
    export PYGEOAPI_CONFIG=config_dev.yml
    export PYGEOAPI_OPENAPI=config.yml
    pygeoapi openapi generate $PYGEOAPI_CONFIG > $PYGEOAPI_OPENAPI

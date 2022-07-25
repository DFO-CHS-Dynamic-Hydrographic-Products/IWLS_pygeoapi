mkdir ./http_cache
mkdir ./logs
mkdir ./s100_process
pip install -r requirements.txt
pip install provider_iwls-0.0.1-py3-none-any.whl
export PYGEOAPI_CONFIG=config_dev.yml
export PYGEOAPI_OPENAPI=config.yml
pygeoapi openapi generate $PYGEOAPI_CONFIG > $PYGEOAPI_OPENAPI
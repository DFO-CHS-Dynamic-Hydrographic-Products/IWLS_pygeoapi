# This script should be run from root directory

mkdir -p install/src/provider_iwls
touch install/src/provider_iwls/__init__.py
cp ./utils/compile_wheel/setup.py ./install
cp ./utils/compile_wheel/README.md ./install
rm ./install/src/provider_iwls/*
cp -r ./pygeoapi_iwls/* ./install/src/provider_iwls
pip install wheel
pip wheel ./install
pip install --upgrade --force-reinstall --debug provider_iwls-0.0.1-py3-none-any.whl
export PYGEOAPI_CONFIG=config_dev.yml
export PYGEOAPI_OPENAPI=config.yml
pygeoapi openapi generate $PYGEOAPI_CONFIG > $PYGEOAPI_OPENAPI

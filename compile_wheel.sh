# This script should be run from root directory

mkdir -p install/src/provider_iwls
touch install/src/provider_iwls/__init__.py
cp ./utils/compile_wheel/setup.py ./install
cp ./utils/compile_wheel/README.md ./install
cp ./pygeoapi_iwls/provider_iwls.py ./install/src/provider_iwls
cp ./pygeoapi_iwls/s104.py ./install/src/provider_iwls
cp ./pygeoapi_iwls/s111.py ./install/src/provider_iwls
cp ./pygeoapi_iwls/s100.py ./install/src/provider_iwls
cp ./pygeoapi_iwls/iwls.py ./install/src/provider_iwls
cp ./pygeoapi_iwls/s100_util.py ./install/src/provider_iwls
cp ./pygeoapi_iwls/process_iwls.py ./install/src/provider_iwls
pip install wheel
pip wheel ./install
pip install --upgrade --force-reinstall provider_iwls-0.0.1-py3-none-any.whl

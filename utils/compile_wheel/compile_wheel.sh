# This script should be run from root project directory

# Create directories for wheel and copy over readme and setup.py files
mkdir -p install/src/provider_iwls
cp ./utils/compile_wheel/setup.py ./install
cp ./utils/compile_wheel/README.md ./install

# Clean up from last compile
rm -rf ./install/src/provider_iwls/*

# Copy code over
cp -r ./pygeoapi_iwls/* ./install/src/provider_iwls

# Compile the wheel
pip install wheel
pip wheel ./install
pip install --upgrade --force-reinstall --debug provider_iwls-0.0.1-py3-none-any.whl
mv provider_iwls-0.0.1-py3-none-any.whl utils/provider_iwls-0.0.1-py3-none-any.whl

# Run env var's for pygeoapi
export PYGEOAPI_CONFIG=config_dev.yml
export PYGEOAPI_OPENAPI=config.yml
pygeoapi openapi generate $PYGEOAPI_CONFIG > $PYGEOAPI_OPENAPI

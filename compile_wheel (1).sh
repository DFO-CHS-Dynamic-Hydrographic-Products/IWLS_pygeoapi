
cp ./pygeoapi_iwls/provider_iwls.py ./install/src/provider_iwls
cp ./pygeoapi_iwls/s104.py ./install/src/provider_iwls
cp ./pygeoapi_iwls/s111.py ./install/src/provider_iwls
cp ./pygeoapi_iwls/s100.py ./install/src/provider_iwls
cp ./pygeoapi_iwls/iwls.py ./install/src/provider_iwls
cp ./pygeoapi_iwls/process_iwls.py ./install/src/provider_iwls
cd install
pip3 wheel .
pip3 install --upgrade --force-reinstall provider_iwls-0.0.1-py3-none-any.whl
cd ..

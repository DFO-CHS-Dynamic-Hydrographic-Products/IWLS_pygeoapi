# Packages imports
import numpy as np

class S111Def:
    """ Class to store hardcoded S111 values """
    dataset_names= ('surfaceCurrentSpeed', 'surfaceCurrentDirection')
    dataset_types= (np.float64, np.float64)
    product_id= 'SurfaceCurrent'
    file_type= '111'

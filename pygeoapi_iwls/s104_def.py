# Packages imports
import numpy as np

class S104Def:
    """ Class to store hardcoded S104 values """
    dataset_names= ('waterLevelHeight', 'waterLevelTrend')
    dataset_types= (np.float64, np.int8)
    product_id= 'WaterLevel'
    file_type= '104'

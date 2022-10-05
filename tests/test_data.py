import numpy as np

s104_attr_names = {
    '/': [('verticalCS', np.integer), ('verticalCoordinateBase',np.integer), ('verticalDatum', np.integer), ('verticalDatumReference', np.integer), ('waterLevelTrendThreshold', float), ('eastBoundLongitude', float), ('epoch', str), ('geographicIdentifier', str), ('horizontalCRS', np.integer), ('horizontalDatumReference', str), ('issueDate', str), ('issueTime',str), ('metadata', str), ('northBoundLatitude', float), ('productSpecification', str), ('southBoundLatitude', float), ('westBoundLongitude', float)],

    'WaterLevel': [('commonPointRule', np.integer), ('dataCodingFormat', np.integer), ('dimension', np.integer), ('horizontalPositionUncertainty', float), ('numInstances', np.integer), ('maxDatasetHeight', float), ('methodWaterLevelProduct', str), ('minDatasetHeight', float), ('pickPriorityType', str), ('timeUncertainty', float), ('verticalUncertainty', float)],

    'WaterLevel/WaterLevel.01': [('dateTimeOfFirstRecord', str), ('dateTimeOfLastRecord', str), ('numGRP', np.integer), ('numberOfStations', np.integer), ('numberOfTimes', np.integer), ('timeRecordInterval', np.integer), ('typeOfWaterLevelData', np.integer)],

    'WaterLevel/WaterLevel.01/Group_': [('endDateTime', str), ('numberOfTimes', np.integer), ('startDateTime', str), ('stationIdentification', str), ('stationName', str), ('timeIntervalIndex', np.integer), ('timeRecordInterval', np.integer)]
                }

s111_attr_names = {
     '/': [('depthTypeIndex', np.integer), ('eastBoundLongitude', float), ('epoch', str), ('geographicIdentifier', str), ('horizontalCRS', np.integer), ('horizontalDatumReference', str), ('issueDate', str), ('issueTime',str), ('metadata', str), ('northBoundLatitude', float), ('productSpecification', str), ('southBoundLatitude', float), ('surfaceCurrentDepth', float), ('westBoundLongitude', float), ('verticalDatumReference', np.integer)],

    'SurfaceCurrent': [('commonPointRule', np.integer), ('dataCodingFormat', np.integer), ('dimension', np.integer), ('horizontalPositionUncertainty', float), ('maxDatasetCurrentSpeed', float), ('minDatasetCurrentSpeed', float), ('numInstances', np.integer), ('typeOfCurrentData', np.integer), ('verticalPositionUncertainty', float)],

    'SurfaceCurrent/SurfaceCurrent.01': [('dateTimeOfFirstRecord', str), ('dateTimeOfLastRecord', str), ('numGRP', np.integer), ('numberOfTimes', np.integer), ('timeRecordInterval', np.integer)], # bug fix - numberOfStations

    'SurfaceCurrent/SurfaceCurrent.01/Group_001': [('endDateTime', str), ('numberOfTimes', np.integer), ('startDateTime', str), ('stationIdentification', str), ('stationName', str), ('timeIntervalIndex', np.integer), ('timeRecordInterval', np.integer)]
 }

s104_group_f = {'Group_F/WaterLevel': [['waterLevelHeight', 'Water level height', 'meters', '-9999', 'H5T_FLOAT', '-99.99', '99.99', 'closedInterval'],
 ['waterLevelTrend', 'Water level trend', '', '0', 'H5T_ENUM', '', '', ''],
                                       ['waterLevelTime', 'Water level time', 'DateTime', '', 'H5T_STRING', '19000101T000000Z', '21500101T000000Z', 'closedInterval']]
 }

s111_group_f = {'Group_F/SurfaceCurrent': [['surfaceCurrentSpeed', 'Surface current speed', 'knots', '-1.0', 'H5T_FLOAT', '0.0', '[]', 'geSemiInterval'],
 ['surfaceCurrentDirection', 'Surface current direction', 'arc-degrees', '-1.0', 'H5T_FLOAT', '0.0', '360', 'geLtInterval'],
 ['surfaceCurrentTime', 'Surface current time', 'DateTime', '', 'H5T_STRING', '19000101T000000Z', '21500101T000000Z', 'closedInterval']]
 }

dataset_types = {'H5T_FLOAT': float, 'H5T_ENUM': np.integer}

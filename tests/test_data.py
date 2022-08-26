

s104_attr_names = {
    '/': ['eastBoundLongitude', 'epoch', 'geographicIdentifier', 'horizontalCRS', 'horizontalDatumReference', 'issueDate', 'issueTime', 'metadata', 'northBoundLatitude', 'productSpecification', 'southBoundLatitude', 'verticalCS', 'verticalCoordinateBase', 'verticalDatum', 'verticalDatumReference', 'waterLevelTrendThreshold', 'westBoundLongitude'],

    'WaterLevel': ['commonPointRule', 'dataCodingFormat', 'dimension', 'horizontalPositionUncertainty', 'maxDatasetHeight', 'methodWaterLevelProduct', 'minDatasetHeight', 'numInstances', 'pickPriorityType', 'timeUncertainty', 'verticalUncertainty'],

    'WaterLevel/WaterLevel.01': ['dateTimeOfFirstRecord', 'dateTimeOfLastRecord', 'numGRP', 'numberOfStations', 'numberOfTimes'],

    'WaterLevel/WaterLevel.01/Group_': ['endDateTime', 'numberOfTimes', 'startDateTime', 'stationIdentification', 'stationName', 'timeIntervalIndex', 'timeRecordInterval']
                }

s111_attr_names = {
     '/': ['depthTypeIndex', 'eastBoundLongitude', 'epoch', 'geographicIdentifier', 'horizontalCRS', 'horizontalDatumReference', 'issueDate', 'issueTime', 'metadata', 'northBoundLatitude', 'productSpecification', 'southBoundLatitude', 'surfaceCurrentDepth', 'westBoundLongitude'],

    'SurfaceCurrent': ['commonPointRule', 'dataCodingFormat', 'dimension', 'horizontalPositionUncertainty', 'maxDatasetCurrentSpeed', 'minDatasetCurrentSpeed', 'numInstances', 'typeOfCurrentData', 'verticalPositionUncertainty'],

    'SurfaceCurrent/SurfaceCurrent.01': ['dateTimeOfFirstRecord', 'dateTimeOfLastRecord', 'numGRP', 'numberOfTimes', 'timeRecordInterval'], # numberOfStations

    'SurfaceCurrent/SurfaceCurrent.01/Group_001': ['endDateTime', 'numberOfTimes', 'startDateTime', 'stationIdentification', 'stationName', 'timeIntervalIndex', 'timeRecordInterval']
 }

s104_group_f = {'Group_F/WaterLevel': [['waterLevelHeight', 'Water level height', 'meters', '-9999', 'H5T_FLOAT', '-99.99', '99.99', 'closedInterval'],
 ['waterLevelTrend', 'Water level trend', '', '0', 'H5T_ENUM', '', '', ''],
                                       ['waterLevelTime', 'Water level time', 'DateTime', '', 'H5T_STRING', '19000101T000000Z', '21500101T000000Z', 'closedInterval']]
 }

s111_group_f = {'Group_F/SurfaceCurrent': [['surfaceCurrentSpeed', 'Surface current speed', 'knots', '-1.0', 'H5T_FLOAT', '0.0', '[]', 'geSemiInterval'],
 ['surfaceCurrentDirection', 'Surface current direction', 'arc-degrees', '-1.0', 'H5T_FLOAT', '0.0', '360', 'geLtInterval'],
 ['surfaceCurrentTime', 'Surface current time', 'DateTime', '', 'H5T_STRING', '19000101T000000Z', '21500101T000000Z', 'closedInterval']]
 }

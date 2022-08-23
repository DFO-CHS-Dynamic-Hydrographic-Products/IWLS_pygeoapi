

s104_attr_names = {
    '/': ['eastBoundLongitude', 'epoch', 'geographicIdentifier', 'horizontalCRS', 'horizontalDatumReference', 'issueDate', 'issueTime', 'metadata', 'northBoundLatitude', 'productSpecification', 'southBoundLatitude', 'verticalCS', 'verticalCoordinateBase', 'verticalDatum', 'verticalDatumReference', 'waterLevelTrendThreshold', 'westBoundLongitude'],

    'WaterLevel': ['commonPointRule', 'dataCodingFormat', 'dimension', 'horizontalPositionUncertainty', 'maxDatasetHeight', 'methodWaterLevelProduct', 'minDatasetHeight', 'numInstances', 'pickPriorityType', 'timeUncertainty', 'verticalUncertainty'],

    'WaterLevel/WaterLevel.01': ['dateTimeOfFirstRecord', 'dateTimeOfLastRecord', 'numGRP', 'numberOfStations', 'numberOfTimes'],

    'WaterLevel/WaterLevel.01/Group_': ['endDateTime', 'numberOfTimes', 'startDateTime', 'stationIdentification', 'stationName', 'timeIntervalIndex', 'timeRecordInterval']
                }
s111_attr_names = {
     '/': ['depthTypeIndex', 'eastBoundLongitude', 'epoch', 'geographicIdentifier', 'horizontalCRS', 'horizontalDatumReference', 'issueDate', 'issueTime', 'metadata', 'northBoundLatitude', 'productSpecification', 'southBoundLatitude', 'surfaceCurrentDepth', 'westBoundLongitude'],
    'SurfaceCurrent': ['commonPointRule', 'dataCodingFormat', 'dimension', 'horizontalPositionUncertainty', 'maxDatasetCurrentSpeed', 'minDatasetCurrentSpeed', 'numInstance', 'numInstances', 'typeOfCurrentData', 'verticalPositionUncertainty'],
    'SurfaceCurrent/SurfaceCurrent.01': ['dateTimeOfFirstRecord', 'dateTimeOfLastRecord', 'numGRP', 'numberOfTimes', 'timeRecordInterval'],
    'SurfaceCurrent/SurfaceCurrent.01/Group_': ['endDateTime', 'numberOfTimes', 'startDateTime', 'stationIdentification', 'stationName', 'timeIntervalIndex', 'timeRecordInterval']
 }
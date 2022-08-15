# Standard library imports
import h5py
import numpy as np

# Import local files
from provider_iwls.s100 import S100GeneratorDCF8

class S111GeneratorDCF8(S100GeneratorDCF8):
    """
    class for generating S-111 Data Coding Format 8 (Stationwise arrays)
    files. Inherit from S100GeneratorDCF8
    """

    def __init__(
            self,
            json_path: str,
            folder_path: str,
            template_path: str
    ):
        # Call s100 base class with preconfigured S111 data
        super().__init__(json_path=json_path,
                         folder_path=folder_path,
                         template_path=template_path,
                         dataset_names= ('surfaceCurrentSpeed', 'surfaceCurrentDirection'),
                         dataset_types= (np.float64, np.float64),
                         product_id= 'SurfaceCurrent',
                         file_type= '111')


    def _format_data_arrays(
            self,
            data: list
    ):
        """
        product specific pre formating to convert API response to valid
        data arrays. Must be implemented by child class
        """
        # Convert JSON data to Pandas tables
        wcs = self._gen_data_table(data,'wcs')
        wcd = self._gen_data_table(data,'wcd')

        # Find dataset min and max values
        dataset_max = wcs.max().max()
        dataset_min = wcs.min().min()

        # Replace NaN values
        wcs = wcs.fillna(-1)
        wcd = wcd.fillna(-1)

        position = self._gen_positions(wcs)
        data_arrays = {'wcs':wcs,'wcd':wcd,'position':position,'max':dataset_max,'min':dataset_min}

        return data_arrays

    def _update_product_specific_general_metadata(
            self,
            h5_file: h5py._hl.files.File,
    ):
        """
        Update product specific (S-111) general metadata.
        :param h5_file: h5 file to update
        """
        # Surface Current Depth, No change from template
        # ToDo: surfaceCurrentDepth is proposed for S111 1.1.1,
        # create here for now and move to template when 1.1.1 is finalized
        h5_file.attrs.create('surfaceCurrentDepth',1.0)

    def _update_feature_metadata(
            self,
            h5_file: h5py._hl.files.File,
            data: dict
    ):
        """
        Update feature level metadata (SurfaceCurrent)

        :param h5_file: h5 file to update
        :param data: formatted data arrays generated from _format_data_arrays
        """
        # commonPointRule, No change from template
        # dataCodingFormat, No change from template
        # dimension, No change from template
        # horizontalPositionUncertainty, currently unassessed
        # maxDatasetCurrentSpeed
        h5_file[self.product_id].attrs.modify('maxDatasetCurrentSpeed',data['max'])
        # minDatasetCurrentSpeed
        h5_file[self.product_id].attrs.modify('minDatasetCurrentSpeed',data['min'])
        # typeOfCurrentData, No change from template
        # verticalPositionUncertainty, currently unassessed
        # Number of Feature Instances
        # ToDo: numInstances is proposed for S111 1.1.1,
        # create here for now and move to template when 1.1.1 is finalized
        h5_file[self.product_id].attrs.create('numInstances',data['wcs'].shape[1])


    def _create_groups(
            self,
            h5_file: h5py._hl.files.File,
            data: dict
    ):
        """
        Create data groups for each station

        :param h5_file: h5 file to update
        :param data: formatted data arrays generated from _format_data_arrays
        """

        ### Update Instance Instance Group Metadata ###
        # N/A 1 to 4 bounding box same as feature
        # 5 Number times of records
        instance_group_path = '{product_id}/{product_id}.01'.format(
            product_id=self.product_id
        )

        # Ensure that the group exists
        assert h5_file[self.product_id].__contains__(f'{self.product_id}.01'), f"Group: {instance_group_path} does not exist, cannot write to it"

        # Configure root group to attach datasets for each station
        instance_sc_group = h5_file[instance_group_path]

        # Parse datasets into a tuple
        datasets = (data['wcs'], data['wcd'])

        ### Create and populate data groups ###
        self._create_attributes(h5_file, instance_sc_group, datasets)

        ### Create Positioning Group ###
        self._create_positioning_group(
            h5_file, instance_group_path, data['position']['lat'], data['position']['lon']
        )

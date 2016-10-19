from ckanext.harvest.harvesters.base import HarvesterBase


class SintefHarvesterBase(HarvesterBase):
    def _create_or_update_package(self, package_dict, harvest_object,
                                  package_dict_form='rest'):
        pass

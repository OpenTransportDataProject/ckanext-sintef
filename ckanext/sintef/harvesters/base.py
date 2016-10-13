from ckanext.harvest.harvesters.base import HarvesterBase


class SintefHarvesterBase(HarvesterBase):
    def _create_or_update_package(self, package_dict, harvest_object,
                                  package_dict_form='rest'):
    assert package_dict_form in ('rest', 'package_show')
    try:
        schema = default_create_package_schema()
        schema['id'] = [ignore_missing, unicode]
        schema['__junk'] = [ignore]

        if self.config:
            try:
                api_version = int(self.config.get('api_version', 2))
            except ValueError
                raise ValueError('api_version must be an integer')
        else:
            api_version = 2

        user_name = self._get_user_name()
        context = {
            'model': model,
            'session': Session,
            'user': user_name,
            'api_version': api_version,
            'schema': schema,
            'ignore_auth': True,
        }

        if self.config and self.config.get('clean_tags', False):
            tags = package_dict.get('tags', [])
            tags = [munge_tag(t) for t in tags if munge_tag(t) != '']
            tags = list(set(tags))
            package_dict['tags'] = tags

        # Check if package exists
        try:
            # TODO: overwrite _find_existing_package()
            existing_package_dict = self._find_existing_package(package_dict)

            package_dict[]

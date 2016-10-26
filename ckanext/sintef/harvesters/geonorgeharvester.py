from ckan.plugins.core import SingletonPlugin, implements
from ckanext.harvest.interfaces import IHarvester
from ckan.plugins import toolkit

import urllib
import urllib2
import httplib
import datetime
import socket
import re

from sqlalchemy import exists

from ckan import model
from ckan.logic import ValidationError, NotFound, get_action
from ckan.lib.helpers import json
from ckan.lib.munge import munge_name
from ckan.plugins import toolkit

from ckanext.harvest.model import HarvestJob, HarvestObject, HarvestGatherError

import logging
log = logging.getLogger(__name__)

from ckanext.harvest.harvesters.base import HarvesterBase

class GeonorgeHarvester(HarvesterBase):
    '''
    Geonorge Harvester
    '''
    implements(IHarvester)
    config = None


    def _get_search_api_offset(self):
        return '/api/search/'


    def _get_getdata_api_offset(self):
        return '/api/getdata/'


    def _get_capabilities_api_offset(self):
        return '/api/capabilities/'


    def _get_order_api_offset(self):
        return '/api/order/'


    def _get_geonorge_base_url(self):
        return 'https://kartkatalog.geonorge.no'


    def _get_geonorge_download_url(self):
        return 'http://nedlasting.geonorge.no'


    def info(self):
        '''
        Harvesting implementations must provide this method, which will return
        a dictionary containing different descriptors of the harvester. The
        returned dictionary should contain:

        * name: machine-readable name. This will be the value stored in the
          database, and the one used by ckanext-harvest to call the appropiate
          harvester.
        * title: human-readable name. This will appear in the form's select box
          in the WUI.
        * description: a small description of what the harvester does. This
          will appear on the form as a guidance to the user.

        :returns: A dictionary with the harvester descriptors
        '''
        return {
            'name': 'geonorge',
            'title': 'Geonorge Server',
            'description': 'Harvests from Geonorge instances.'
        }


    def _make_lower_and_alphanumeric(self, string_to_modify):
        '''
        The 'organization_create' method in ckan.logic.action.create requires
        the 'name' parameter to be lowercase and alphanumeric. (It is used in
        the organization's URI.)
        The names of the organizations imported however, contain capitalized
        and letters from the norwegian alphabet. This method is therefore
        needed when creating organizations from imported metadata.

        :param string_to_modify: String that gets modified in this method.
        :returns: A string that is only contains lowercase and alphanumeric
                  letters.
        '''
        # Characters from the norwegian alphabet are replaced. The replacing is
        # needed because words like 'kjøre' and 'kjære' become the same word,
        # 'kjre', if one was to just remove the norwegian characters.
        chars_to_replace = {' ': '-',
                            u'\u00E6': 'ae',
                            u'\u00C6': 'ae',
                            u'\u00F8': 'oe',
                            u'\u00D8': 'oe',
                            u'\u00E5': 'aa',
                            u'\u00C5': 'aa'}

        for char in chars_to_replace:
            string_to_modify = \
                string_to_modify.replace(char, chars_to_replace.get(char))

        string_to_modify = string_to_modify.lower()
        # Removing any other disallowed characters, making the string
        # alphanumeric...
        modified_string = re.sub(r'[^A-Za-z0-9\-\_]+', '', string_to_modify)

        return modified_string


    def _set_config(self, config_str):
        '''
        When creating a harvester, the user has the option of entering further
        configuration in a form. This method sets the global variable 'config'
        to either the configuration, or to an empty dictionary if there is no
        further configuration entered by the user.

        :param config_str: Config string coming from the form.
        :returns: A dictionary containing any user configuration.
        '''
        if config_str:
            self.config = json.loads(config_str)

            log.debug('Using config: %r', self.config)
        else:
            self.config = {}


    def validate_config(self, config):
        '''
        Harvesters can provide this method to validate the configuration
        entered in the form. It should return a single string, which will be
        stored in the database.  Exceptions raised will be shown in the form's
        error messages.

        :param harvest_object_id: Config string coming from the form
        :returns: A string with the validated configuration options
        '''
        if not config:
            return config

        try:
            config_obj = json.loads(config)

            # Check if 'theme' is a list of strings if it is defined
            if 'theme' in config_obj:
                if not isinstance(config_obj['theme'], list):
                    raise ValueError('theme must be a *list* of themes')
                if config_obj['theme'] and \
                        not isinstance(config_obj['theme'][0], basestring):
                    raise ValueError('theme must be a list of strings')

            # Check if 'organization' is a list of strings if it is defined
            if 'organization' in config_obj:
                if not isinstance(config_obj['organization'], list):
                    raise ValueError('organization must be a *list* of organizations')
                if config_obj['organization'] and \
                        not isinstance(config_obj['organization'][0], basestring):
                    raise ValueError('organization must be a list of strings')

            # Check if 'text' is a list of strings if it is defined
            if 'text' in config_obj:
                if not isinstance(config_obj['text'], list):
                    raise ValueError('text must be a *list* of texts')
                if config_obj['text'] and \
                        not isinstance(config_obj['text'][0], basestring):
                    raise ValueError('text must be a list of strings')

            # Check if 'title' is a list of strings if it is defined
            if 'title' in config_obj:
                if not isinstance(config_obj['title'], list):
                    raise ValueError('title must be a *list* of titles')
                if config_obj['title'] and \
                        not isinstance(config_obj['title'][0], basestring):
                    raise ValueError('title must be a list of strings')

            # Check if 'uuid' is a list of strings if it is defined
            if 'uuid' in config_obj:
                if not isinstance(config_obj['uuid'], list):
                    raise ValueError('uuid must be a *list* of uuids')
                if config_obj['title'] and \
                        not isinstance(config_obj['uuid'][0], basestring):
                    raise ValueError('uuid must be a list of strings')

            # Check if 'type' is a list of strings if it is defined
            # Set to 'dataset' if not defined
            if 'type' in config_obj:
                if not isinstance(config_obj['type'], list):
                    raise ValueError('type must be a *list* of types')
                if config_obj['type'] and \
                        not isinstance(config_obj['type'][0], basestring):
                    raise ValueError('type must be a list of strings')
            else:
                config_obj['type'] = 'dataset'

            # Check if 'default_tags' is a list of strings if it is defined
            if 'default_tags' in config_obj:
                if not isinstance(config_obj['default_tags'], list):
                    raise ValueError('default_tags must be a *list* of tags')
                if config_obj['default_tags'] and \
                        not isinstance(config_obj['default_tags'][0], basestring):
                    raise ValueError('default_tags must be a list of strings')

            # Check if 'remote_orgs' is set to 'create' if it is defined
            if 'remote_orgs' in config_obj:
                if not config_obj['remote_orgs'] in ['create']:
                    raise ValueError('remote_orgs can only be set to "create"')

            # Check if 'get_files' is a string that is either 'True' or 'False'
            # Set to False if not defined
            if 'get_files' in config_obj:
                if not config_obj['get_files'] in ['True', 'true', 'False', 'false']:
                    raise ValueError('get_files must be either "True" or "False"')
                if config_obj['get_files'] and \
                        not isinstance(config_obj['get_files'], basestring)
                    raise ValueError('get_files must be a string, either "True" or "False"')
                if config_obj['get_files'] in ['True', 'true']:
                    config_obj['get_files'] = True
                elif config_obj['get_files'] in ['False', 'false']:
                    config_obj['get_files'] = False
            else:
                config_obj['get_files'] = False

            # Check if 'force_all' is a string that is either 'True' or 'False'
            # Set to False if not defined
            if 'force_all' in config_obj:
                if not config_obj['force_all'] in ['True', 'true', 'False', 'false']:
                    raise ValueError('force_all must be either "True" or "False"')
                if config_obj['force_all'] and \
                        not isinstance(config_obj['force_all'], basestring)
                    raise ValueError('force_all must be a string, either "True" or "False"')
                if config_obj['force_all'] in ['True', 'true']:
                    config_obj['force_all'] = True
                elif config_obj['force_all'] in ['False', 'false']:
                    config_obj['force_all'] = False
            else:
                config_obj['force_all'] = False


            config = json.dumps(config_obj)

        except ValueError, e:
            raise e

        return config


    def get_original_url(self, harvest_object_id):
        '''
        This optional but very recommended method allows harvesters to return
        the URL to the original remote document, given a Harvest Object id.
        Note that getting the harvest object you have access to its guid as
        well as the object source, which has the URL.
        This URL will be used on error reports to help publishers link to the
        original document that has the errors. If this method is not provided
        or no URL is returned, only a link to the local copy of the remote
        document will be shown.

        :param harvest_object_id: HarvestObject id
        :returns: A string with the URL to the original document
        '''
        params = {'facets[0]name': 'uuid',
                  'facets[0]value': harvest_object_id}

        metadata_url = self._get_geonorge_base_url() + self._get_search_api_offset() + '?' + urllib.urlencode(params)
        content = self._get_content(metadata_url)
        content_json = json.loads(content)
        try:
            return content_json[u'Results'][0][u'DistributionUrl']
        except Exception as e:
            log.debug('No URL could be found for Harvest Object with ID=\'' + harvest_object_id + '\'')


    @classmethod
    def _last_error_free_job(cls, harvest_job):
        '''
        The harvester uses this method to get the latest job that was completed
        without any errors occuring. The date and time of this job is useful
        when trying to find datasets that were updated after the last successful
        job.

        :param harvest_job: HarvestJob object.
        :returns: The last fully completed job of the harvester.
        '''
        # look for jobs with no gather errors
        jobs = \
            model.Session.query(HarvestJob) \
                 .filter(HarvestJob.source == harvest_job.source) \
                 .filter(HarvestJob.gather_started != None) \
                 .filter(HarvestJob.status == 'Finished') \
                 .filter(HarvestJob.id != harvest_job.id) \
                 .filter(
                     ~exists().where(
                         HarvestGatherError.harvest_job_id == HarvestJob.id)) \
                 .order_by(HarvestJob.gather_started.desc())
        # now check them until we find one with no fetch/import errors
        # (looping rather than doing sql, in case there are lots of objects
        # and lots of jobs)

        for job in jobs:
            for obj in job.objects:
                if obj.current is False and \
                        obj.report_status != 'not modified':
                    # unsuccessful, so go onto the next job
                    break
            else:
                return job


    def _search_for_datasets(self, remote_geonorge_base_url, fq_terms=None):
        '''
        Does a dataset search on Geonorge with specified parameters and returns
        the results.
        Deals with paging to get all the results.

        :param remote_geonorge_base_url: Geonorge base url
        :param fq_terms: Parameters to specify which datasets to search for
        :returns: A list of results from the search, containing dataset-metadata
        '''
        base_search_url = remote_geonorge_base_url + self._get_search_api_offset()
        params = {'offset': 1,
                  'limit': 10,
                  'facets[0]name': 'type',
                  'facets[0]value': 'dataset'}

        # Set the parameters to be readable by geonorge's API
        fq_term_counter = 1
        for fq_term in fq_terms:
            params.update({'facets[' + str(fq_term_counter) + ']name': fq_term})
            params.update({'facets[' + str(fq_term_counter) + ']value': "%s" % (fq_terms[fq_term])})
            fq_term_counter += 1
        param_keys_sorted = sorted(params)
        pkg_dicts = []

        # Goes through each page
        while True:
            url = base_search_url + '?'
            # Add each parameter to the url-string
            for param_key in param_keys_sorted:
                url += urllib.urlencode({param_key: "%s" % (params[param_key])}) + '&'
            url = url[:-1]
            log.debug('Searching for Geonorge datasets: %s', url)
            try:
                # Get the content of the url - this includes the list of results
                content = self._get_content(url)
            except ContentFetchError, e:
                raise SearchError('Error sending request to search remote '
                                  'Geonorge instance %s url %r. Error: %s' %
                                  (remote_geonorge_base_url, url, e))

            try:
                # Load the content as a json (make it a dictionary)
                response_dict = json.loads(content)
            except ValueError:
                raise SearchError('Response from remote Geonorge was not JSON: %r'
                                  % content)

            try:
                # Get the list of results from the response dictionary (content)
                pkg_dicts_page = response_dict.get('Results', [])
            except ValueError:
                raise SearchError('Response JSON did not contain '
                                  'results: %r' % response_dict)
            pkg_dicts.extend(pkg_dicts_page)

            # If paging is at last page, the length is 0 and the search is done
            # for this url
            if len(pkg_dicts_page) == 0:
                break

            # Paging
            params['offset'] += params['limit']

        return pkg_dicts


    def _get_modified_datasets(self, pkg_dicts, base_url, last_harvest):
        '''
        If the harvester has had at least one error-free job in the past, this
        method is used to remove any result in the given dictionary, that has
        not been changed/updated since the last error-free job.

        :param pkg_dicts: Dictionary containing dataset metadata.
        :param base_url: String containing the base URL of the harvesting
                         source.
        :param last_harvest: HarvestJob object.
        :returns: A dictionary that contains only the metadata of the datasets
                  that was updated since last error-free harvesting job.
        '''
        base_getdata_url = base_url + self._get_getdata_api_offset()
        new_pkg_dicts = list(pkg_dicts)

        for pkg_dict in pkg_dicts:
            url = base_getdata_url + pkg_dict['Uuid']
            try:
                content = self._get_content(url)
            except ContentFetchError, e:
                raise SearchError('Error sending request to getdata remote '
                                  'Geonorge instance %s url %r. Error: %s' %
                                  (remote_geonorge_base_url, url, e))
            if content is not None:
                try:
                    response_dict = json.loads(content)
                except ValueError:
                    raise SearchError('Response from remote Geonorge was not JSON: %r'
                                      % content)
            if response_dict.get('DateMetadataUpdated') < last_harvest:
                log.debug('A dataset with ID %s already exists, and is up to date. Removing from job queue...',
                          response_dict.get('Uuid'))
                new_pkg_dicts.remove(pkg_dict)

        return new_pkg_dicts


    def _get_content(self, url, data=None):
        '''
        This methods takes care of any HTTP-request that is made towards the
        API's of Geonorge, either it is the 'kartkatalog' or the 'nedlastning'
        API.

        :param url: String containing the URL to request content from.
        :param data: Dictionary with JSON-data used in POST-requests towards the
                     download API.
        :returns: The content from an HTTP-request.
        '''
        http_request = urllib2.Request(url=url)

        try:
            if not data:
                http_response = urllib2.urlopen(http_request)
            else:
                http_request.add_header('Content-Type', 'application/json')
                params = json.dumps(data)
                http_response = urllib2.urlopen(http_request, data=params)
        except urllib2.HTTPError, e:
            if e.getcode() == 404:
                raise ContentNotFoundError('HTTP error: %s' % e.code)
            else:
                return None
                # raise ContentFetchError('HTTP error: %s' % e.code)
        except urllib2.URLError, e:
            raise ContentFetchError('URL error: %s' % e.reason)
        except httplib.HTTPException, e:
            raise ContentFetchError('HTTP Exception: %s' % e)
        except socket.error, e:
            raise ContentFetchError('HTTP socket error: %s' % e)
        except Exception, e:
            raise ContentFetchError('HTTP general exception: %s' % e)
        return http_response.read()


    def gather_stage(self, harvest_job):
        '''
        The gather stage will receive a HarvestJob object and will be
        responsible for:
            - gathering all the necessary objects to fetch on a later.
              stage (e.g. for a CSW server, perform a GetRecords request)
            - creating the necessary HarvestObjects in the database, specifying
              the guid and a reference to its job. The HarvestObjects need a
              reference date with the last modified date for the resource, this
              may need to be set in a different stage depending on the type of
              source.
            - creating and storing any suitable HarvestGatherErrors that may
              occur.
            - returning a list with all the ids of the created HarvestObjects.
            - to abort the harvest, create a HarvestGatherError and raise an
              exception. Any created HarvestObjects will be deleted.

        :param harvest_job: HarvestJob object
        :returns: A list of HarvestObject ids
        '''
        log.debug('In GeonorgeHarvester gather_stage (%s)',
                  harvest_job.source.url)
        toolkit.requires_ckan_version(min_version='2.0')
        get_all_packages = True

        self._set_config(harvest_job.source.config)

        # Get source URL
        remote_geonorge_base_url = harvest_job.source.url.rstrip('/')

        pkg_dicts = []

        # Filter in/out datasets from particular organizations
        # This makes a list with lists of all possible search-combinations
        # needed to search for everything specified in the config
        def get_item_from_list(_list, index):
            counter = 0
            for item in _list:
                if counter == index:
                    return item
                counter += 1

        filter_include = {}
        fq_terms_list_length = 1
        for filter_item in self.config:
            if filter_item in ['theme', 'organization', 'text', 'title', 'uuid', 'type']:
                filter_include[filter_item] = self.config.get(filter_item, [])
                fq_terms_list_length *= len(filter_include[filter_item])
        fq_terms_list = [{} for i in range(fq_terms_list_length)]

        switchnum_max = 1
        filter_counter = 0
        for filter_item in filter_include:
            switchnum_counter = 0
            search_counter = 0
            for search in range(fq_terms_list_length):
                if switchnum_counter == switchnum_max:
                    search_counter += 1
                    switchnum_counter = 0
                switchnum_counter += 1
                fq_terms_list[search][filter_item] = \
                    filter_include[filter_item][search_counter \
                        % len(filter_include[filter_item])]
            temp_filter_item = get_item_from_list(filter_include, filter_counter)
            if temp_filter_item is not None:
                switchnum_max *= len(filter_include[temp_filter_item])
            filter_counter += 1

        # Ideally we can request from the remote CKAN only those datasets
        # modified since the last completely successful harvest.
        last_error_free_job = self._last_error_free_job(harvest_job)
        log.debug('Last error-free job: %r', last_error_free_job)
        if (last_error_free_job and
                not self.config.get('force_all', False)):
            get_all_packages = False

            # Request only the datasets modified since
            last_time = last_error_free_job.gather_started
            # Note: SOLR works in UTC, and gather_started is also UTC, so
            # this should work as long as local and remote clocks are
            # relatively accurate. Going back a little earlier, just in case.
            get_changes_since = \
                (last_time - datetime.timedelta(hours=1)).isoformat()
            log.info('Searching for datasets modified since: %s UTC',
                     get_changes_since)

            try:
                for fq_terms in fq_terms_list:
                    pkg_dicts.extend(self._search_for_datasets(
                        remote_geonorge_base_url,
                        fq_terms))

                pkg_dicts = \
                    self._get_modified_datasets(pkg_dicts,
                                                    remote_geonorge_base_url,
                                                    get_changes_since)

            except SearchError, e:
                log.info('Searching for datasets changed since last time '
                         'gave an error: %s', e)
                get_all_packages = True

            if not get_all_packages and not pkg_dicts:
                log.info('No datasets have been updated on the remote '
                         'CKAN instance since the last harvest job %s',
                         last_time)
                return None


        # Fall-back option - request all the datasets from the remote CKAN
        if get_all_packages:
            # Request all remote packages
            try:
                for fq_terms in fq_terms_list:
                    pkg_dicts.extend(self._search_for_datasets(remote_geonorge_base_url, fq_terms))
            except SearchError, e:
                log.info('Searching for all datasets gave an error: %s', e)
                self._save_gather_error(
                    'Unable to search remote Geonorge for datasets:%s url:%s'
                    'terms:%s' % (e, remote_geonorge_base_url, fq_terms_list),
                    harvest_job)
                return None
        if not pkg_dicts:
            self._save_gather_error(
                'No datasets found at Geonorge: %s' % remote_geonorge_base_url,
                harvest_job)
            return None

        # Create harvest objects for each dataset
        try:
            package_ids = set()
            object_ids = []
            for pkg_dict in pkg_dicts:
                if pkg_dict['Uuid'] in package_ids:
                    log.info('Discarding duplicate dataset %s - probably due '
                             'to datasets being changed at the same time as '
                             'when the harvester was paging through',
                             pkg_dict['Uuid'])
                    continue
                package_ids.add(pkg_dict['Uuid'])

                log.debug('Creating HarvestObject for %s %s',
                          pkg_dict['Title'], pkg_dict['Uuid'])
                obj = HarvestObject(guid=pkg_dict['Uuid'],
                                    job=harvest_job,
                                    content=json.dumps(pkg_dict))
                obj.save()
                object_ids.append(obj.id)

            return object_ids
        except Exception, e:
            self._save_gather_error('%r' % e.message, harvest_job)


    def fetch_stage(self, harvest_object):
        # Nothing to do here - we got the package dict in the search in the
        # gather stage
        return True


    def import_stage(self, harvest_object):
        '''
        The import stage will receive a HarvestObject object and will be
        responsible for:
            - performing any necessary action with the fetched object (e.g.
              create, update or delete a CKAN package).
              Note: if this stage creates or updates a package, a reference
              to the package should be added to the HarvestObject.
            - setting the HarvestObject.package (if there is one)
            - setting the HarvestObject.current for this harvest:
               - True if successfully created/updated
               - False if successfully deleted
            - setting HarvestObject.current to False for previous harvest
              objects of this harvest source if the action was successful.
            - creating and storing any suitable HarvestObjectErrors that may
              occur.
            - creating the HarvestObject - Package relation (if necessary)
            - returning True if the action was done, "unchanged" if the object
              didn't need harvesting after all or False if there were errors.

        NB You can run this stage repeatedly using 'paster harvest import'.

        :param harvest_object: HarvestObject object
        :returns: True if the action was done, "unchanged" if the object didn't
                  need harvesting after all or False if there were errors.
        '''
        log.debug('In GeonorgeHarvester import_stage')

        base_context = {'model': model, 'session': model.Session,
                        'user': self._get_user_name()}
        if not harvest_object:
            log.error('No harvest object received')
            return False

        if harvest_object.content is None:
            self._save_object_error('Empty content for object %s' %
                                    harvest_object.id,
                                    harvest_object, 'Import')
            return False

        self._set_config(harvest_object.job.source.config)

        try:
            package_dict = json.loads(harvest_object.content)

            if package_dict.get('type') == 'harvest':
                log.warn('Remote dataset is a harvest source, ignoring...')
                return True

            package_dict['id'] = package_dict.pop('Uuid')
            package_dict['title'] = package_dict.pop('Title')
            package_dict['notes'] = package_dict.pop('Abstract')
            package_dict['url'] = package_dict.pop('ShowDetailsUrl')
            package_dict['isopen'] = package_dict.pop('IsOpenData')

            organization_name = package_dict.get('Organization')
            package_dict['owner_org'] = self._make_lower_and_alphanumeric(organization_name)

            package_dict['tags'] = []
            info = {
                    'name': package_dict.pop('Theme')
                    }
            package_dict['tags'].append(info)

            # Set default tags if needed
            default_tags = self.config.get('default_tags', [])
            if default_tags:
                if not 'tags' in package_dict:
                    package_dict['tags'] = []
                package_dict['tags'].extend(
                    [t for t in default_tags if t not in package_dict['tags']])

            # Check if url to every file to the dataset should be added to 'resources'
            if config['get_files']:
                if package_dict.get('DistributionProtocol') == 'WWW:DOWNLOAD-1.0-http--download':
                    package_dict['resources'] = []
                    package_dict['resources'].append({'url': package_dict.get('DistributionUrl'),
                                                      'name': 'Download page',
                                                      'format': 'HTML',
                                                      'mimetype': 'text/html'})
                elif package_dict.get('DistributionProtocol') == 'GEONORGE:DOWNLOAD':
                    try:
                        package_dict['resources'] = []

                        log.info('Making orderdata for dataset %s' % package_dict['id'])
                        payload = {"email": "bruker@epost.no",
                                "orderLines": [{"metadataUuid": package_dict['id']}]}
                        url = self._get_geonorge_download_url() + self._get_capabilities_api_offset() + package_dict['id']
                        capabilities_content = self._get_content(url)
                        capabilities_content_json = json.loads(capabilities_content)
                        resources_orderdata = {}
                        for capability in capabilities_content_json["_links"]:
                            cap_link = capability["href"]
                            capability_description = json.dumps(cap_link).split("/")
                            resource = capability_description[len(capability_description) - 2]
                            if resource in ["area", "format", "projection"]:
                                resources_orderdata["%ss" % resource] = cap_link
                        for rsrc in resources_orderdata:
                            resource_content = self._get_content(resources_orderdata[rsrc])
                            resource_content_json = json.loads(resource_content)
                            if rsrc == "areas":
                                areas_json = []
                                for area in resource_content_json:
                                    last_index = len(areas_json)
                                    areas_json.append({})
                                    for field in area:
                                        if field in ["code", "type", "name"]:
                                            areas_json[last_index][field] = area[field]
                                resource_content_json = areas_json
                            payload['orderLines'][0][rsrc] = resource_content_json
                        log.info('Orderdata was successfully made!')

                        log.info('Using orderdata to get resources.')
                        order_url = self._get_geonorge_download_url() + self._get_order_api_offset()
                        order_content = self._get_content(order_url, payload)
                        order_content_json = json.loads(order_content)
                        log.debug('Inserting files into resources.')
                        for files in order_content_json["files"]:
                            package_dict['resources'].append({'url': files["downloadUrl"],
                                                              'name': files["name"]})
                        log.info('Resources added.')
                    except Exception, e:
                        log.error(e.message)

            # Local harvest source organization
            source_dataset = \
                get_action('package_show')(base_context.copy(),
                                           {'id': harvest_object.source.id})
            local_org = source_dataset.get('owner_org')

            remote_orgs = self.config.get('remote_orgs', None)

            if remote_orgs is not None:
                remote_orgs = self.config.get('remote_orgs', None)

            if not remote_orgs in ('create'):
                # Assign dataset to the source organization
                package_dict['owner_org'] = local_org
            else:
                if not 'owner_org' in package_dict:
                    package_dict['owner_org'] = None

                # check if remote org exist locally, otherwise remove
                validated_org = None
                remote_org = package_dict['owner_org']

                if remote_org:
                    try:
                        data_dict = {'id': remote_org}
                        org = get_action('organization_show')(base_context.copy(), data_dict)
                        if org.get('state') == 'deleted':
                            patch_org = {'id': org.get('id'),
                                         'state': 'active'}
                            get_action('organization_patch')(base_context.copy(), patch_org)
                        validated_org = org['id']
                    except NotFound, e:
                        log.info('Organization %s is not available', remote_org)
                        if remote_orgs == 'create':
                            try:
                                new_org = {'name': package_dict.get('owner_org'),
                                       'title': organization_name,
                                       'image_url': package_dict.get('OrganizationLogo')}

                                org = get_action('organization_create')(base_context.copy(), new_org)

                                log.info('Organization %s has been newly created', remote_org)
                                validated_org = org['id']
                            except (RemoteResourceError, ValidationError):
                                log.error('Could not get remote org %s', remote_org)

                package_dict['owner_org'] = validated_org or local_org

            # # Set default extras if needed
            # default_extras = self.config.get('default_extras', {})
            # def get_extra(key, package_dict):
            #     for extra in package_dict.get('extras', []):
            #         if extra['key'] == key:
            #             return extra
            # if default_extras:
            #     override_extras = self.config.get('override_extras', False)
            #     if not 'extras' in package_dict:
            #         package_dict['extras'] = {}
            #     for key, value in default_extras.iteritems():
            #         existing_extra = get_extra(key, package_dict)
            #         if existing_extra and not override_extras:
            #             continue  # no need for the default
            #         if existing_extra:
            #             package_dict['extras'].remove(existing_extra)
            #         # Look for replacement strings
            #         if isinstance(value, basestring):
            #             value = value.format(
            #                 harvest_source_id=harvest_object.job.source.id,
            #                 harvest_source_url=
            #                 harvest_object.job.source.url.strip('/'),
            #                 harvest_source_title=
            #                 harvest_object.job.source.title,
            #                 harvest_job_id=harvest_object.job.id,
            #                 harvest_object_id=harvest_object.id,
            #                 dataset_id=package_dict['id'])
            #
            #         package_dict['extras'].append({'key': key, 'value': value})
            #
            # for resource in package_dict.get('resources', []):
            #     # Clear remote url_type for resources (eg datastore, upload) as
            #     # we are only creating normal resources with links to the
            #     # remote ones
            #     resource.pop('url_type', None)
            #
            #     # Clear revision_id as the revision won't exist on this CKAN
            #     # and saving it will cause an IntegrityError with the foreign
            #     # key.
            #     resource.pop('revision_id', None)

            result = self._create_or_update_package(
                package_dict, harvest_object, package_dict_form='package_show')

            return result
        except ValidationError, e:
            self._save_object_error('Invalid package with GUID %s: %r' %
                                    (harvest_object.guid, e.error_dict),
                                    harvest_object, 'Import')
            log.error(e.error_dict)
        except Exception, e:
            self._save_object_error('%s' % e, harvest_object, 'Import')

class SearchError(Exception):
    pass

class RemoteResourceError(Exception):
    pass

class ContentFetchError(Exception):
    pass

class ContentNotFoundError(ContentFetchError):
    pass

from ckan.plugins.core import SingletonPlugin, implements
from ckanext.harvest.interfaces import IHarvester

import urllib
import urllib2
import httplib
import datetime
import socket

from sqlalchemy import exists

from ckan import model
from ckan.logic import ValidationError, NotFound, get_action
from ckan.lib.helpers import json
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

    PRINT_OK = '\033[92m'
    PRINT_WARNING = '\033[93m'
    PRINT_ERROR = '\033[91m'
    PRINT_END = '\033[0m'


    def _get_search_api_offset(self):
        return '/api/search/'


    def _get_getdata_api_offset(self):
        return '/api/getdata/'


    def _get_capabilities_api_offset(self):
        return '/api/capabilities/'


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

            # This method is used to check if 'element' is in config_obj
            # and that it is defined as either a string or a list of strings.
            def check_if_element_is_string_or_list_in_config_obj(element):
                if element in config_obj:
                    if not isinstance(config_obj[element], list):
                        if not isinstance(config_obj[element], basestring):
                            raise ValueError('%s must be a string '
                                    'or a list of strings, %s is neither' %
                                    (element, config_obj[element]))
                    else:
                        for item in config_obj[element]:
                            if not isinstance(item, basestring):
                                raise ValueError('%s must be a string '
                                        'or a list of strings, %s is neither' %
                                        (item, config_obj[element][item]))

            # Check if 'filter' is a string or a list of strings if it is defined
            check_if_element_is_string_or_list_in_config_obj('themes')
            check_if_element_is_string_or_list_in_config_obj('organizations')
            check_if_element_is_string_or_list_in_config_obj('text')
            check_if_element_is_string_or_list_in_config_obj('title')
            check_if_element_is_string_or_list_in_config_obj('uuid')
            check_if_element_is_string_or_list_in_config_obj('datatypes')
            check_if_element_is_string_or_list_in_config_obj('default_tags')

            # Check if 'create_orgs' is set to 'create' if it is defined
            if 'create_orgs' in config_obj and not isinstance(config_obj['create_orgs'], bool):
                    raise ValueError('create_orgs must be a boolean, either True or False')

            # Check if 'force_all' is a boolean value
            if 'force_all' in config_obj and not isinstance(config_obj['force_all'], bool):
                    raise ValueError('force_all must be a boolean, either True or False')

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
        obj = model.Session.query(HarvestObject) \
                   .filter(HarvestObject.id==harvest_object_id).first()
        job = model.Session.query(HarvestJob) \
                   .filter(HarvestJob.id==obj.harvest_job_id).first()

        if not obj or not job:
            log.debug('No URL could be found for Harvest Object with ID=\'%s\''
                      % harvest_object_id)
            return None

        return '{base_url}/metadata/uuid/{uuid}'.format(base_url=job.source.url,
                                                        uuid=obj.package_id)


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
        # Initiate the parameters that will be sent with the url
        params = {'offset': 1,
                  'limit': 10}

        # Set the parameters to be readable by geonorge's API
        fq_term_counter = 0
        for fq_term in fq_terms:
            if fq_term == 'text':
                params.update({'text': fq_terms['text']})
                continue
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
                raise SearchError('Response from remote Geonorge was not '
                                  'JSON: %r' % content)

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
                    raise SearchError('Response from remote Geonorge was not '
                                      'JSON: %r' % content)

                # Checking if the dataset is up to date since last error-free
                # harvest.
                if response_dict.get('DateMetadataUpdated') < last_harvest:
                    log.debug('A dataset with ID %s already exists, and is up '
                              'to date. Removing from job queue...'
                              % response_dict.get('Uuid'))
                    new_pkg_dicts.remove(pkg_dict)

        return new_pkg_dicts


    def _get_content(self, url):
        '''
        This methods takes care of any HTTP-request that is made towards
        Geonorges kartkatalog API.

        :param url: String containing the URL to request content from.
        :returns: The content from an HTTP-request.
        '''
        try:
            http_request = urllib2.Request(url=url)
            http_response = urllib2.urlopen(http_request)

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


    def get_metadata_provenance_for_just_this_harvest(self, harvest_object, reharvest=False):
        '''
        This method provides metadata provenance to be used in the 'extras'
        fields in the datasets that get imported.

        :param: HarvestObject object.
        :returns: A dictionary containing the harvest source URL and title.
        '''
        provenance = {
                         'activity_occurred': datetime.datetime.utcnow().isoformat(),
                         'activity': 'harvest',
                         'harvest_source_url': harvest_object.source.url,
                         'harvest_source_title': harvest_object.source.title,
                         'harvest_source_type': harvest_object.source.type,
                         'harvested_guid': harvest_object.guid
                     }
        if reharvest: provenance['activity'] = 'reharvest'
        return provenance


    def get_metadata_provenance(self, harvest_object, harvested_provenance=None):
        '''Returns the metadata_provenance for a dataset, which is the details
        of this harvest added onto any existing metadata_provenance value in
        the dataset. This should be stored in the metadata_provenance extra
        when harvesting.
        Provenance is a record of harvests, imports and perhaps other
        activities of production too, as suggested by W3C PROV.
        This helps keep track when a dataset is created in site A, imported
        into site B, harvested into site C and from there is harvested into
        site D. The metadata_provence will be a list of four dicts with the
        details: [A, B, C, D].
        '''
        reharvest = True
        if isinstance(harvested_provenance, basestring):
            harvested_provenance = json.loads(harvested_provenance)
        elif harvested_provenance is None:
            harvested_provenance = []
            reharvest = False
        metadata_provenance = harvested_provenance + \
            [self.get_metadata_provenance_for_just_this_harvest(
                harvest_object, reharvest
             )]
        return json.dumps(metadata_provenance)


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

        # Retrieves the element at index 'index' from a list '_list'
        def get_item_from_list(_list, index):
            counter = 0
            for item in _list:
                if counter == index:
                    return item
                counter += 1

        '''
        This makes a list with lists of all possible search-combinations
        needed to search for everything specified in the config: '''
        filter_include = {}
        fq_terms_list_length = 1
        for filter_item in self.config:
            if filter_item in ['text', 'title', 'uuid']:
                config_item = self.config[filter_item]
                if isinstance(config_item, basestring): config_item = [config_item]
                filter_include[filter_item] = config_item
                fq_terms_list_length *= len(filter_include[filter_item])
            elif filter_item in ['datatypes', 'organizations', 'themes']:
                # There was a key error when having filter_item = 'type'
                # This is fixed by setting it to 'datatype' and update it
                # to 'type' here (same goes for 'organization' and 'organizations'):
                config_item = self.config[filter_item]
                if isinstance(config_item, basestring): config_item = [config_item]
                if filter_item == 'datatypes':
                    filter_include['type'] = config_item
                    fq_terms_list_length *= len(filter_include['type'])
                elif filter_item == 'organizations':
                    filter_include['organization'] = config_item
                    fq_terms_list_length *= len(filter_include['organization'])
                elif filter_item == 'themes':
                    filter_include['theme'] = config_item
                    fq_terms_list_length *= len(filter_include['theme'])
        # Set type to be 'dataset' by default:
        if not 'type' in filter_include:
            filter_include['type'] = ['dataset']
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
        ''' End of search-combination making.
        All combination of search parameters is now stored in the list:
        fq_terms_list which is a list of dictionaries. Each dictionary in the
        list contains one search to be made.
        '''

        # Ideally we can request from the remote Geonorge only those datasets
        # modified since the last completely successful harvest.
        last_error_free_job = self._last_error_free_job(harvest_job)

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
            log.info('Searching for datasets modified since: %s UTC'
                     % get_changes_since)

            try:
                # For every dictionary of search parameters in fq_terms_list:
                # add the result from the search to pkg_dicts.
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
                         'Geonorge instance since the last harvest job %s',
                         last_time)
                return None


        # Fall-back option - request all the datasets from the remote Geonorge
        if get_all_packages:
            # Request all remote packages
            try:
                for fq_terms in fq_terms_list:
                    pkg_dicts.extend(
                        self._search_for_datasets(remote_geonorge_base_url,
                                                  fq_terms)
                    )
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
                # Create and save the harvest object:
                obj = HarvestObject(guid=pkg_dict['Uuid'],
                                    job=harvest_job,
                                    content=json.dumps(pkg_dict))
                obj.save()
                object_ids.append(obj.id)

            log.info('%sGather stage for job with ID %s was completed '
                     'successfully!%s'
                     % (self.PRINT_OK, harvest_job.source.id, self.PRINT_END))

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
              create, update or delete a Geonorge package).
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

            organization_name = package_dict['Organization']
            package_dict['owner_org'] = self._gen_new_name(organization_name)

            if not 'tags' in package_dict:
                package_dict['tags'] = []

            package_dict['tags'].append({'name': package_dict.pop('Theme')})

            # Set default tags if needed
            default_tags = self.config.get('default_tags', False)
            if default_tags:
                package_dict['tags'].extend(
                    [t for t in default_tags if t not in package_dict['tags']])

            # Check if url to every file to the dataset should be added to 'resources'
            if package_dict.get('DistributionProtocol') == 'GEONORGE:DOWNLOAD':
                # Dataset can be downloaded from Geonorges download API
                try:
                    package_dict['resources'] = []
                    dl_url = '%s%s%s' % (self._get_geonorge_download_url(),
                                         self._get_capabilities_api_offset(),
                                         package_dict.get('id', ''))
                    package_dict['resources'].append(
                        {'url': dl_url,
                        'name': 'Geonorge download API',
                        'format': 'application/json'}
                        )
                except Exception, e:
                    log.error(e.message)
            elif package_dict.get('DistributionUrl'):
                    package_dict['resources'] = []
                    package_dict['resources'].append(
                        {'url': package_dict.get('DistributionUrl'),
                        'name': 'Download page',
                        'format': 'HTML',
                        'mimetype': 'text/html'}
                        )


            # Local harvest source organization
            source_dataset = \
                get_action('package_show')(base_context.copy(),
                                           {'id': harvest_object.source.id})
            local_org = source_dataset.get('owner_org')

            create_orgs = self.config.get('create_orgs', True)

            if not create_orgs:
                # Assign dataset to the source organization
                package_dict['owner_org'] = local_org
            else:
                # check if remote org exist locally, otherwise remove
                validated_org = None
                remote_org = package_dict.get('owner_org', None)

                if remote_org:
                    try:
                        data_dict = {'id': remote_org}
                        org = get_action('organization_show')(base_context.copy(),
                                                              data_dict)
                        if org.get('state') == 'deleted':
                            patch_org = {'id': org.get('id'),
                                         'state': 'active'}
                            get_action('organization_patch')(base_context.copy(),
                                                             patch_org)
                        validated_org = org['id']
                    except NotFound, e:
                        log.info('Organization %s is not available', remote_org)
                        if create_orgs:
                            try:
                                new_org = {
                                    'name': package_dict.get('owner_org'),
                                    'title': organization_name,
                                    'image_url': package_dict.get('OrganizationLogo')
                                }

                                org = get_action('organization_create')(base_context.copy(),
                                                                        new_org)

                                log.info('Organization %s has been newly '
                                         'created', remote_org)
                                validated_org = org['id']
                            except (RemoteResourceError, ValidationError):
                                log.error('Could not get remote org %s', remote_org)

                package_dict['owner_org'] = validated_org or local_org

            if not 'extras' in package_dict:
                package_dict['extras'] = []

            # Make metadata provenance for this package dict.
            data_dict = {'id': package_dict['id']}
            preexisting_provenance = None
            try:
                preexisting_package_dict = \
                    get_action('package_show')(base_context.copy(), data_dict)
                user_prompted = False
                keep_modifications = None

                for extra in preexisting_package_dict['extras']:
                    if extra.get('key') == 'metadata_provenance':
                        preexisting_provenance = extra.get('value')
                    else:
                        if not user_prompted:
                            keep_modifications = raw_input(
                                '%sDataset with ID %s is already imported, but '
                                'contains user modifications. Do you wish to '
                                'keep the modifications to this dataset? '
                                '[y/n] %s'
                                % (self.PRINT_WARNING,
                                   package_dict.get('id', None),
                                   self.PRINT_END)
                            )
                            user_prompted = True
                        if keep_modifications == 'y':
                            package_dict['extras'].append(extra)
                            log.info('Keeping user modifications for dataset '
                                     'with ID %s'
                                     % package_dict.get('id', None))
                        else:
                            log.info('User modifications were discarded for '
                                     'dataset with ID %s.'
                                     % package_dict.get('id', None))
            except Exception as e:
                pass

            metadata_provenance = self.get_metadata_provenance(harvest_object,
                preexisting_provenance)
            package_dict['extras'].append({'key': 'metadata_provenance',
                                           'value': metadata_provenance})

            result = self._create_or_update_package(
                package_dict, harvest_object, package_dict_form='package_show')

            if result is True:
                log.info('%sDataset with ID %s was successfully imported!%s'
                          % (self.PRINT_OK, package_dict.get('id', None),
                             self.PRINT_END))
            else:
                log.error('%sAn error occured while trying to import dataset '
                         'with ID %s%s'
                         % (self.PRINT_ERROR, package_dict.get('id', None),
                             self.PRINT_END))

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

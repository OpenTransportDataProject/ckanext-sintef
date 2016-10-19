from ckan.plugins.core import SingletonPlugin, implements
from ckanext.harvest.interfaces import IHarvester
from ckan.plugins import toolkit

import urllib
import urllib2
import httplib
import datetime
import socket

from sqlalchemy import exists

from ckan import model
from ckan.logic import ValidationError, NotFound, get_action
from ckan.lib.helpers import json
from ckan.lib.munge import munge_name
from ckan.plugins import toolkit

from ckanext.harvest.model import HarvestJob, HarvestObject, HarvestGatherError
from ckanext.sintef.harvesters.base import SintefHarvesterBase

import logging
log = logging.getLogger(__name__)

class GeonorgeHarvester(SintefHarvesterBase):
    '''
    Geonorge Harvester
    '''
    implements(IHarvester)
    config = None

    api_version = 2
    action_api_version = 3

    def _get_search_api_offset(self):
        return '/api/search/'

    def _get_getdata_api_offset(self):
        return '/api/getdata/'

    def _get_geonorge_base_url(self):
        return 'kartkatalog.geonorge.no'

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

        A complete example may be::

            {
                'name': 'csw',
                'title': 'CSW Server',
                'description': 'A server that implements OGC's Catalog Service
                                for the Web (CSW) standard'
            }

        :returns: A dictionary with the harvester descriptors
        '''
        return {
            'name': 'geonorge',
            'title': 'Geonorge Server',
            'description': 'Harvests from Geonorge instances.'
        }

    def _set_config(self, config_str):
        if config_str:
            self.config = json.loads(config_str)
            if 'api_version' in self.config:
                self.api_version = int(self.config['api_version'])

            log.debug('Using config: %r', self.config)
        else:
            self.config = {}

    def validate_config(self, config):
        '''

        [optional]

        Harvesters can provide this method to validate the configuration
        entered in the form. It should return a single string, which will be
        stored in the database.  Exceptions raised will be shown in the form's
        error messages.

        :param harvest_object_id: Config string coming from the form
        :returns: A string with the validated configuration options
        '''

    def get_original_url(self, harvest_object_id):
        '''

        [optional]

        This optional but very recommended method allows harvesters to return
        the URL to the original remote document, given a Harvest Object id.
        Note that getting the harvest object you have access to its guid as
        well as the object source, which has the URL.
        This URL will be used on error reports to help publishers link to the
        original document that has the errors. If this method is not provided
        or no URL is returned, only a link to the local copy of the remote
        document will be shown.

        Examples:
            * For a CKAN record: http://{ckan-instance}/api/rest/{guid}
            * For a WAF record: http://{waf-root}/{file-name}
            * For a CSW record: http://{csw-server}/?Request=GetElementById&Id={guid}&...

        :param harvest_object_id: HarvestObject id
        :returns: A string with the URL to the original document
        '''
        params = {'facets[0]name': 'uuid',
                  'facets[0]value': harvest_object_id,
                  'limit': '1'}
        metadata_url = self._get_geonorge_base_url() + self._get_search_api_offset() + '?' + urllib.urlencode(params)
        content = self._get_content(metadata_url)
        content_json = json.loads(content)
        try:
            return content_json[u'Results'][0][u'DistributionUrl']
        except Exception as e:
            log.debug('No URL could be found for Harvest Object with ID=\'' + harvest_object_id + '\'')


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

        # Filter in/out datasets from particular organizations
        fq_terms = []
        org_filter_include = self.config.get('organizations_filter_include', [])
        org_filter_exclude = self.config.get('organizations_filter_exclude', [])
        if org_filter_include:
            fq_terms.append(' OR '.join(
                'organization:%s' % org_name for org_name in org_filter_include))
        elif org_filter_exclude:
            fq_terms.extend(
                '-organization:%s' % org_name for org_name in org_filter_exclude)

        #Ideally we can request from the remote CKAN only those datasets
        #modified since the last completely successful harvest.
        last_error_free_job = False
        #last_error_free_job = self._last_error_free_job(harvest_job)
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

            fq_since_last_time = 'metadata_modified:[{since}Z TO *]' \
                .format(since=get_changes_since)

            try:
                pkg_dicts = self._search_for_datasets(
                    remote_geonorge_base_url,
                    fq_terms + [fq_since_last_time])
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
                pkg_dicts = self._search_for_datasets(remote_geonorge_base_url)
                                                      #, fq_terms)
            except SearchError, e:
                log.info('Searching for all datasets gave an error: %s', e)
                self._save_gather_error(
                    'Unable to search remote Geonorge for datasets:%s url:%s'
                    'terms:%s' % (e, remote_geonorge_base_url, fq_terms),
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
        '''
        The fetch stage will receive a HarvestObject object and will be
        responsible for:
            - getting the contents of the remote object (e.g. for a CSW server,
              perform a GetRecordById request).
            - saving the content in the provided HarvestObject.
            - creating and storing any suitable HarvestObjectErrors that may
              occur.
            - returning True if everything is ok (ie the object should now be
              imported), "unchanged" if the object didn't need harvesting after
              all (ie no error, but don't continue to import stage) or False if
              there were errors.

        :param harvest_object: HarvestObject object
        :returns: True if successful, 'unchanged' if nothing to import after
                  all, False if not successful
        '''
        # log.debug('In GeonorgeHarvester fetch_stage')
        #
        # remote_geonorge_base_url = harvest_object.job.source.url.rstrip('/')
        # base_getdata_url = remote_geonorge_base_url + self._get_getdata_api_offset()
        # url = base_getdata_url + harvest_object.guid
        #
        # log.debug('Searching for Geonorge dataset: %s', url)
        # try:
        #     content = self._get_content(url)
        # except ContentFetchError, e:
        #     raise SearchError('Error sending request to search remote '
        #                       'Geonorge instance %s url %r. Error: %s' %
        #                       (remote_geonorge_base_url, url, e))
        #
        # try:
        #     response_dict = json.loads(content)
        # except ValueError:
        #     raise SearchError('Response from remote Geonorge was not JSON: %r'
        #                       % content)
        #
        # harvest_object_content = json.loads(harvest_object.content)
        # harvest_object_content.update(response_dict)
        #
        # log.debug('HARVEST_OBJECT_FETCH_UPDATE: %s', harvest_object_content)
        #
        # harvest_object.content = json.dumps(harvest_object_content)

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

        log.debug(harvest_object.job.source.title)

        base_context = {'model': model, 'session': model.Session,
                        'user': None}
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

            package_dict['id'] = package_dict.pop('Uuid')
            package_dict['title'] = package_dict.pop('Title')
            package_dict['notes'] = package_dict.pop('Abstract')
            package_dict['url'] = package_dict.pop('ShowDetailsUrl')
            package_dict['isopen'] = package_dict.pop('IsOpenData')

            package_dict['tags'] = []
            info = {
                    'name': package_dict['Theme'],
                    'display_name': package_dict['Theme']
                    }
            package_dict['tags'].append(info)

            if package_dict.get('type') == 'harvest':
                log.warn('Remote dataset is a harvest source, ignoring...')
                return True

            # Set default tags if needed
            default_tags = self.config.get('default_tags', [])
            if default_tags:
                if not 'tags' in package_dict:
                    package_dict['tags'] = []
                package_dict['tags'].extend(
                    [t for t in default_tags if t not in package_dict['tags']])

            remote_groups = self.config.get('remote_groups', None)
            if not remote_groups in ('only_local', 'create'):
                # Ignore remote groups
                package_dict.pop('groups', None)
            else:
                if not 'groups' in package_dict:
                    package_dict['groups'] = []

                # check if remote groups exist locally, otherwise remove
                validated_groups = []

                for group_ in package_dict['groups']:
                    try:
                        data_dict = {'id': group_['id']}
                        group = get_action('group_show')(base_context.copy(), data_dict)
                        validated_groups.append({'id': group['id'], 'name': group['name']})

                    except NotFound, e:
                        log.info('Group %s is not available', group_)
                        if remote_groups == 'create':
                            try:
                                group = self._get_group(harvest_object.source.url, group_)
                            except RemoteResourceError:
                                log.error('Could not get remote group %s', group_)
                                continue

                            for key in ['packages', 'created', 'users', 'groups', 'tags', 'extras', 'display_name']:
                                group.pop(key, None)

                            get_action('group_create')(base_context.copy(), group)
                            log.info('Group %s has been newly created', group_)
                            validated_groups.append({'id': group['id'], 'name': group['name']})

                package_dict['groups'] = validated_groups

            # Local harvest source organization
            source_dataset = get_action('package_show')(base_context.copy(), {'id': harvest_object.source.id})
            local_org = source_dataset.get('owner_org')

            remote_orgs = self.config.get('remote_orgs', None)

            if not remote_orgs in ('only_local', 'create'):
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
                        validated_org = org['id']
                    except NotFound, e:
                        log.info('Organization %s is not available', remote_org)
                        if remote_orgs == 'create':
                            try:
                                try:
                                    org = self._get_organization(harvest_object.source.url, remote_org)
                                except RemoteResourceError:
                                    # fallback if remote CKAN exposes organizations as groups
                                    # this especially targets older versions of CKAN
                                    org = self._get_group(harvest_object.source.url, remote_org)

                                for key in ['packages', 'created', 'users', 'groups', 'tags', 'extras', 'display_name', 'type']:
                                    org.pop(key, None)
                                get_action('organization_create')(base_context.copy(), org)
                                log.info('Organization %s has been newly created', remote_org)
                                validated_org = org['id']
                            except (RemoteResourceError, ValidationError):
                                log.error('Could not get remote org %s', remote_org)

                package_dict['owner_org'] = validated_org or local_org

            # Set default groups if needed
            default_groups = self.config.get('default_groups', [])
            if default_groups:
                if not 'groups' in package_dict:
                    package_dict['groups'] = []
                existing_group_ids = [g['id'] for g in package_dict['groups']]
                package_dict['groups'].extend(
                    [g for g in self.config['default_group_dicts']
                     if g['id'] not in existing_group_ids])

            # Set default extras if needed
            default_extras = self.config.get('default_extras', {})
            def get_extra(key, package_dict):
                for extra in package_dict.get('extras', []):
                    if extra['key'] == key:
                        return extra
            if default_extras:
                override_extras = self.config.get('override_extras', False)
                if not 'extras' in package_dict:
                    package_dict['extras'] = {}
                for key, value in default_extras.iteritems():
                    existing_extra = get_extra(key, package_dict)
                    if existing_extra and not override_extras:
                        continue  # no need for the default
                    if existing_extra:
                        package_dict['extras'].remove(existing_extra)
                    # Look for replacement strings
                    if isinstance(value, basestring):
                        value = value.format(
                            harvest_source_id=harvest_object.job.source.id,
                            harvest_source_url=
                            harvest_object.job.source.url.strip('/'),
                            harvest_source_title=
                            harvest_object.job.source.title,
                            harvest_job_id=harvest_object.job.id,
                            harvest_object_id=harvest_object.id,
                            dataset_id=package_dict['id'])

                    package_dict['extras'].append({'key': key, 'value': value})

            for resource in package_dict.get('resources', []):
                # Clear remote url_type for resources (eg datastore, upload) as
                # we are only creating normal resources with links to the
                # remote ones
                resource.pop('url_type', None)

                # Clear revision_id as the revision won't exist on this CKAN
                # and saving it will cause an IntegrityError with the foreign
                # key.
                resource.pop('revision_id', None)

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

    def _search_for_datasets(self, remote_geonorge_base_url):
        base_search_url = remote_geonorge_base_url + self._get_search_api_offset()
        params = {'facets[0]name': 'theme',
                  'facets[0]value': 'Samferdsel',
                  'offset': 1,
                  'limit': 10}
        pkg_dicts = []

        while True:
            url = base_search_url + '?' + urllib.urlencode(params)
            log.debug('Searching for Geonorge datasets: %s', url)
            try:
                content = self._get_content(url)
            except ContentFetchError, e:
                raise SearchError('Error sending request to search remote '
                                  'Geonorge instance %s url %r. Error: %s' %
                                  (remote_geonorge_base_url, url, e))

            try:
                response_dict = json.loads(content)
            except ValueError:
                raise SearchError('Response from remote Geonorge was not JSON: %r'
                                  % content)

            try:
                pkg_dicts_page = response_dict.get('Results', [])
            except ValueError:
                raise SearchError('Response JSON did not contain '
                                  'results: %r' % response_dict)
            pkg_dicts.extend(pkg_dicts_page)

            log.debug(len(pkg_dicts_page))

            if len(pkg_dicts_page) == 0:
                break

            params['offset'] += params['limit']

        return pkg_dicts

    def _get_content(self, url):
        http_request = urllib2.Request(url=url)

        api_key = self.config.get('api_key')
        if api_key:
            http_request.add_header('Authorization', api_key)

        try:
            http_response = urllib2.urlopen(http_request)
        except urllib2.HTTPError, e:
            if e.getcode() == 404:
                raise ContentNotFoundError('HTTP error: %s' % e.code)
            else:
                raise ContentFetchError('HTTP error: %s' % e.code)
        except urllib2.URLError, e:
            raise ContentFetchError('URL error: %s' % e.reason)
        except httplib.HTTPException, e:
            raise ContentFetchError('HTTP Exception: %s' % e)
        except socket.error, e:
            raise ContentFetchError('HTTP socket error: %s' % e)
        except Exception, e:
            raise ContentFetchError('HTTP general exception: %s' % e)
        return http_response.read()

class SearchError(Exception):
    pass

class RemoteResourceError(Exception):
    pass

class ContentFetchError(Exception):
    pass

class ContentNotFoundError(ContentFetchError):
    pass

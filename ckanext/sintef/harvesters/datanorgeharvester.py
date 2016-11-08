from ckan.plugins.core import SingletonPlugin, implements
from ckanext.harvest.interfaces import IHarvester
from ckan.plugins import toolkit

import urllib
import urllib2
import httplib
import datetime
import socket
import re
import uuid
from bs4 import BeautifulSoup

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

class DataNorgeHarvester(HarvesterBase):
    '''
    Data Norge Harvester
    '''
    implements(IHarvester)
    config = None


    def _get_datanorge_base_url(self):
        return 'http://data.norge.no/'


    def _get_datanorge_api_offset(self):
        return '/api/dcat/data.json'


    def _get_new_uuid_from_id(self, guid):
        return str(uuid.uuid3(uuid.NAMESPACE_URL, guid))


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
            'name': 'datanorge',
            'title': 'Data Norge Server',
            'description': 'Harvests from Data Norge instances.'
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
        # needed because words like 'kjoere' and 'kjaere' become the same word,
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

            # Check if 'publisher' is a string or a list of strings
            if 'publisher' in config_obj:
                if not isinstance(config_obj['publisher'], list):
                    if not isinstance(config_obj['publisher'], basestring):
                        raise ValueError('publisher must be a string '
                        'or a list of strings, %s is neither' % config_obj['publisher'])
                else:
                    for publisher in config_obj['publisher']:
                        if not isinstance(publisher, basestring):
                            raise ValueError('publisher must be a string '
                            'or a list of strings, %s is neither' % publisher)

            # Check if 'publisher' is a string or a list of strings
            if 'keyword' in config_obj:
                if not isinstance(config_obj['keyword'], list):
                    if not isinstance(config_obj['keyword'], basestring):
                        raise ValueError('keyword must be a string '
                        'or a list of strings, %s is neither' % config_obj['keyword'])
                else:
                    for keyword in config_obj['keyword']:
                        if not isinstance(keyword, basestring):
                            raise ValueError('keyword must be a string '
                            'or a list of strings, %s is neither' % keyword)

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
        try:
            return ''
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


    def _search_for_datasets(self, remote_datanorge_base_url, modified_since=None):
        '''
        Does a dataset search on Datanorge with specified parameters and returns
        the results.
        Deals with paging to get all the results.

        :param remote_geonorge_base_url: Geonorge base url
        :param modified_since: Search only for datasets modified since this date
                               format: 'yyyy-mm-dd' as a string.
        :returns: A list of results from the search, containing dataset-metadata
        '''
        page = 1

        base_search_url = remote_datanorge_base_url + self._get_datanorge_api_offset() + '?'

        if modified_since:
            base_search_url += urllib.urlencode({'modified_since': modified_since}) + '&'

        pkg_dicts = []

        while True:
            url = base_search_url + urllib.urlencode({'page': page})

            try:
                content = self._get_content(url)
                response_dict = json.loads(content)
                package_dict_datasets = response_dict.get('datasets', [])
            except ContentFetchError, e:
                raise SearchError('Error sending request to search remote '
                                  'Datanorge instance %s url %r. Error: %s' %
                                  (remote_datanorge_base_url, url, e))
            except ValueError:
                raise SearchError('Response from remote Datanorge was not JSON: %r'
                                  % content)
            except ValueError:
                raise SearchError('Response JSON did not contain '
                                  'results: %r' % response_dict)

            if len(package_dict_datasets) == 0:
                break

            pkg_dicts.extend(package_dict_datasets)
            page += 1

        return pkg_dicts


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
        log.debug('In DataNorgeHarvester gather_stage (%s)',
                  harvest_job.source.url)
        toolkit.requires_ckan_version(min_version='2.0')
        get_all_packages = True

        self._set_config(harvest_job.source.config)

        # Get source URL
        remote_datanorge_base_url = harvest_job.source.url.rstrip('/')

        pkg_dicts = []

        # Ideally we can request from the remote Geonorge only those datasets
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
                (str(last_time - datetime.timedelta(hours=1)).split(' '))[0]
            log.info('Searching for datasets modified since: %s UTC',
                     get_changes_since)

            try:
                # Add the result from the search to pkg_dicts.
                pkg_dicts.extend(self._search_for_datasets(
                    remote_datanorge_base_url, get_changes_since))

            except SearchError, e:
                log.info('Searching for datasets changed since last time '
                         'gave an error: %s', e)
                get_all_packages = True

            if not get_all_packages and not pkg_dicts:
                log.info('No datasets have been updated on the remote '
                         'DataNorge instance since the last harvest job %s',
                         last_time)
                return None

        # Fall-back option - request all the datasets from the remote DataNorge
        if get_all_packages:
            # Request all remote packages
            try:
                pkg_dicts.extend(self._search_for_datasets(
                    remote_datanorge_base_url))
            except SearchError, e:
                log.info('Searching for all datasets gave an error: %s', e)
                self._save_gather_error(
                    'Unable to search remote DataNorge for datasets:%s url:%s'
                    % (e, remote_datanorge_base_url),
                    harvest_job)
                return None
        if not pkg_dicts:
            self._save_gather_error(
                'No datasets found at DataNorge: %s' % remote_datanorge_base_url,
                harvest_job)
            return None

        # Create harvest objects for each dataset
        try:
            package_ids = set()
            object_ids = []

            for pkg_dict in pkg_dicts:
                publisher_filter = self.config.get('publisher', None)
                keyword_filter = self.config.get('keyword', None)
                passed_filter = True
                this_publisher = pkg_dict.get('publisher').get('name')
                this_keywords = pkg_dict.get('keyword')

                if not publisher_filter == None:
                    # If this publisher is unwanted, continue.
                    if not this_publisher in publisher_filter:
                        continue

                if not keyword_filter == None:
                    # If none of the keywords match, continue.
                    if not [kw for kw in this_keywords if kw in keyword_filter]:
                        continue


                # Set URL to the DataNorge dataset's ID, which is the dataset's
                # URL. Then create a new UUID based on the URL.
                pkg_dict['url'] = pkg_dict.get('id')
                pkg_dict['id'] = self._get_new_uuid_from_id(pkg_dict.get('url'))

                if pkg_dict['id'] in package_ids:
                    log.info('Discarding duplicate dataset %s - probably due '
                             'to datasets being changed at the same time as '
                             'when the harvester was paging through',
                             pkg_dict['id'])
                    continue
                package_ids.add(pkg_dict['id'])

                log.debug('Creating HarvestObject for %s %s',
                          pkg_dict['title'], pkg_dict['id'])
                # Create and save the harvest object:
                obj = HarvestObject(guid=pkg_dict['id'],
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
              create, update or delete a DataNorge package).
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
        log.debug('In DataNorgeHarvester import_stage')

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
            if package_dict.get('type', '') == 'harvest':
                log.warn('Remote dataset is a harvest source, ignoring...')
                return True

            organization_name = package_dict['publisher'].get('name')
            package_dict['owner_org'] = self._make_lower_and_alphanumeric(organization_name)

            if not 'tags' in package_dict:
                package_dict['tags'] = []

            for keyword in package_dict.get('keyword'):
                package_dict['tags'].append({'name': keyword})
            # Set default tags if needed
            default_tags = self.config.get('default_tags', False)
            if default_tags:
                package_dict['tags'].extend(
                    [t for t in default_tags if t not in package_dict['tags']])

            # Sets a description to the dataset.
            descriptions = package_dict.pop('description')
            notes = None
            for item in descriptions:
                if item.get('language') == 'nb':
                    notes = item.get('value')
            if notes:
                package_dict['notes'] = notes

            if not 'resources' in package_dict:
                package_dict['resources'] = []
            for resource in package_dict.get('distribution'):
                for item in resource.get('description'):
                    if item.get('language') == 'nb':
                        name = item.get('value')
                package_dict['resources'].append({'url': resource.get('accessURL'),
                                                  'name': name,
                                                  'format': resource.get('format')})

            source_dataset = \
                get_action('package_show')(base_context.copy(),
                                           {'id': harvest_object.source.id})

            # Local harvest source organization
            source_dataset = \
                get_action('package_show')(base_context.copy(),
                                           {'id': harvest_object.source.id})
            local_org = source_dataset.get('owner_org')

            remote_orgs = self.config.get('remote_orgs', None)

            if not remote_orgs == 'create':
                # Assign dataset to the source organization
                package_dict['owner_org'] = local_org
            else:
                # check if remote org exist locally, otherwise remove
                validated_org = None
                remote_org = package_dict.get('owner_org', None)

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
                                           'title': organization_name}

                                try:
                                    html_source = BeautifulSoup(
                                        urllib.urlopen(package_dict.get('url')).read())
                                    img_source = html_source.body.find(
                                        'div', attrs={'class': 'logo'}).img.get('src')
                                except AttributeError, e:
                                    img_source = None
                                    log.debug('No logo was found for remote org %s.', remote_org)

                                if img_source:
                                    new_org['image_url'] = img_source

                                org = get_action('organization_create')(base_context.copy(), new_org)

                                log.info('Organization %s has been newly created', remote_org)
                                validated_org = org['id']
                            except (RemoteResourceError, ValidationError):
                                log.error('Could not get remote org %s', remote_org)

                package_dict['owner_org'] = validated_org or local_org


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

        return True

class SearchError(Exception):
    pass

class RemoteResourceError(Exception):
    pass

class ContentFetchError(Exception):
    pass

class ContentNotFoundError(ContentFetchError):
    pass

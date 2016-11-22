=============
ckanext-sintef
=============

A CKAN plugin made for SINTEF ICT that extends ckanext-harvest. This plugin enables harvesting from the sources Geonorge and Difi.


------------
Installation
------------

To install ckanext-sintef:

1. Activate your CKAN virtual environment, for example::

     . /usr/lib/ckan/default/bin/activate

2. Install the ckanext-sintef Python package into your virtual environment::

     pip install ckanext-sintef

3. Add ``sintef`` to the ``ckan.plugins`` setting in your CKAN
   config file (by default the config file is located at
   ``/etc/ckan/default/production.ini``).

4. Restart CKAN. For example if you've deployed CKAN with Apache on Ubuntu::

     sudo service apache2 reload


------------------------
Development Installation
------------------------

To install ckanext-sintef for development, activate your CKAN virtualenv and
do::

    git clone https://github.com/NTNU-SINTEF-Project-Group/ckanext-sintef.git
    cd ckanext-sintef
    python setup.py develop
    pip install -r dev-requirements.txt

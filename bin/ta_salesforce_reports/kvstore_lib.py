import re
import json
import urllib
from xml.etree import cElementTree as et
from traceback import format_exc
from httplib2 import Http

class KVException(Exception):
    pass


class KVAlreadyExists(KVException):
    pass


class KVNotExists(KVException):
    pass


class KVClient(object):

    def __init__(self, splunkd_host, session_key, service):
        self.service = service
        self._splunkd_host = splunkd_host
        self._session_key = session_key

    def create_collection(self, collection, app, owner="nobody"):
        """
        :collection: collection name
        :return: None if successful otherwise KV exception thrown
        """

        assert collection
        assert app

        uri = self._get_config_endpoint(app, owner)
        data = {
            "name": collection,
            "replicate": "true"
        }
        self._do_request(uri, "POST", data)

    def config_collection(self, collection, app, data=None, owner="nobody"):
        """
        :collection: collection name
        :return: None if successful otherwise KV exception thrown
        """

        assert collection
        assert app

        uri = self._get_config_endpoint(app, owner, collection)

        self._do_request(uri, "POST", data)

    def list_collection(self, collection=None, app=None, owner="nobody"):
        """
        :collection: collection name. When euqals "None", return all
        collections in the system.
        :return: a list containing the connection names if successful, throws
        KVNotExists if no such colection or other exception if other error
        happened
        """

        uri = self._get_config_endpoint(app, owner, collection)

        content = self._do_request(uri, method="GET")
        m = re.search(r'xmlns="([^"]+)"', content)
        path = "./entry/title"
        if m:
            ns = m.group(1)
            path = "./{%s}entry/{%s}title" % (ns, ns)

        collections = et.fromstring(content)
        return [node.text for node in collections.iterfind(path)]

    def delete_collection(self, collection, app, owner="nobody"):
        """
        :collection: collection name to be deleted
        :return: None if successful otherwise throw KVNotExists exception if
        the collection doesn't exist in the system or other exception if other
        error happened
        """

        assert collection

        uri = self._get_config_endpoint(app, owner, collection)
        self._do_request(uri, method="DELETE")

    def insert_collection_data(self, collection, data, app, owner="nobody"):
        """
        :collection: collection name
        :data: dict like key values to be inserted and attached to
        this collection
        :return: {"_key": "key_id"} when successful, clients can use this
        key to do query/delete/update, throws KV exceptions when failed
        """

        assert collection
        assert data is not None
        assert app

        uri = self._get_data_endpoint(app, owner, collection)
        key = self._do_request(uri, "POST", data,
                               content_type="application/json")
        return json.loads(key)

    def delete_collection_data(self, collection, key_id, app, owner="nobody"):
        """
        :collection: collection name
        :key_id: key id returned when creation. If None, delete all data
        associated with this collection
        :return: None if successful otherwise throws KV exception
        """

        assert collection

        uri = self._get_data_endpoint(app, owner, collection, key_id)
        self._do_request(uri, "DELETE", content_type="application/json")

    def update_collection_data(self, collection, key_id, data,
                               app, owner="nobody"):
        """
        :collection: collection name
        :key_id: key id returned when creation
        :return: key id if successful otherwise throws KV exception
        """

        assert collection
        assert key_id
        assert app

        uri = self._get_data_endpoint(app, owner, collection, key_id)
        k = self._do_request(uri, "POST", data,
                             content_type="application/json")
        return json.loads(k)

    def get_collection_data(self, collection, key_id, app, owner="nobody"):
        """
        :collection: collection name
        :key_id: key id returned when creation. If None, get all data
        associated with this collection
        :return: when key_id is not None, return key values if
        successful. when key_id is None, return a list of key values if
        sucessful. Throws KV exception if failure
        """

        assert collection

        uri = self._get_data_endpoint(app, owner, collection, key_id)
        k = self._do_request(uri, "GET")
        return json.loads(k)

    def _do_request(self, uri, method, data=None,
                    content_type="application/x-www-form-urlencoded", timeout=None):

        header = {}
        header['Content-Type'] = content_type
        header['Authorization'] = "Splunk " + self._session_key

        if data is not None:
            if content_type == "application/json":
                data = json.dumps(data)
            else:
                data = urllib.urlencode(data)

        http = Http(timeout=timeout, disable_ssl_certificate_validation=True)
        resp, content = http.request(uri, method=method, headers=header, body=data)
        
        if resp is None and content is None:
            return KVException("Failed uri={0}, data={1}".format(uri, data))
        if resp.status in (200, 201):
            return content
        elif resp.status == 409:
            return KVAlreadyExists("{0}-{1} already exists".format(uri, data))
        elif resp.status == 404:
            raise KVNotExists("{0}-{1} not exists".format(uri, data))
        else:
            raise KVException("Failed to {0} {1}, reason={2}".format(method, uri, resp.reason))

    def _get_config_endpoint(self, app, owner, collection=None):
        uri = "{0}/servicesNS/{1}/{2}/storage/collections/config"
        return self._do_get_endpoint(app, owner, collection, None, uri)

    def _get_data_endpoint(self, app, owner, collection, key_id=None):
        uri = "{0}/servicesNS/{1}/{2}/storage/collections/data"
        return self._do_get_endpoint(app, owner, collection, key_id, uri)

    def _get_lookup_endpoint(self, app, owner, lookup):
        uri = "{0}/servicesNS/{1}/{2}/data/transforms/lookups"
        return self._do_get_endpoint(app, owner, lookup, None, uri)

    def _do_get_endpoint(self, app, owner, collection, key_id, uri_template):
        if not app:
            app = "-"

        if not owner:
            owner = "-"

        uri = uri_template.format(self._splunkd_host, owner, app)

        if collection is not None:
            uri += "/{0}".format(collection)
            if key_id is not None:
                uri += "/{0}".format(key_id)
        return uri

    def new_collection(self, collection, app):
        not_exists = False
        try:
            res = self.list_collection(collection, app)
        except KVNotExists:
            not_exists = True
        except Exception:
            not_exists = True

        if not_exists or not res:
            for i in xrange(3):
                try:
                    self.create_collection(collection, app)
                except KVAlreadyExists:
                    return
                except Exception as e:
                    ex = e
                else:
                    return
            else:
                raise ex

    def config_lookup(self, lookup, app, fields, owner="nobody"):
        """
        :lookup: lookup name
        :return: None if successful otherwise KV exception thrown
        """

        assert lookup
        assert app
        assert fields

        uri = self._get_lookup_endpoint(app, owner, lookup)
        data = {
            "name": lookup,
            "collection": lookup,
            "external_type": "kvstore",
            "fields_list": fields
        }
        self._do_request(uri, "POST", data)

    def delete_collection_query(self, collection, app, query, owner="nobody"):
        """
        :collection: collection name
        :return: None if successful otherwise KV exception thrown
        """

        assert collection
        assert app
        assert query

        uri = self._get_data_endpoint(app, owner, collection)
        qs = {"query": query}
        uri += "?" + urllib.urlencode(qs)
        
        self._do_request(uri, "DELETE")
        
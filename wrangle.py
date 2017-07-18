# -*- coding: utf-8 -*-

#In this program we parse the OSM XML file , audit the various tag elements when needed and then write the audited data into csv files.

'''
The process for this transformation is as follows:
- Use iterparse to iteratively step through each top level element in the XML
- Shape each element into several data structures using a custom function and audit the city names and street names .
- Utilize a schema and validation library to ensure the transformed data is in the correct format
- Write each data structure to the appropriate .csv files

'''

import csv
import codecs
import pprint
import re
import xml.etree.cElementTree as eT
import string
import cerberus
import schema

OSM_PATH = "data/test_osm.osm"

NODES_PATH = "data/csv/nodes.csv"
NODE_TAGS_PATH = "data/csv/nodes_tags.csv"
WAYS_PATH = "data/csv/ways.csv"
WAY_NODES_PATH = "data/csv/ways_nodes.csv"
WAY_TAGS_PATH = "data/csv/ways_tags.csv"

LOWER_COLON = re.compile(r'(^([A-Z]|[a-z]|_)+):(([A-Z]|[a-z]|_|[0-9])+)')
PROBLEMCHARS = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')
street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)  #regex to extract the last part of the street name
postcode_format_re= re.compile(r'^\d{5}(?:[-\s]\d{4})?$')

SCHEMA = schema.schema

# The following are the fielnames we are interested in
NODE_FIELDS = ['id', 'lat', 'lon', 'user', 'uid', 'version', 'changeset', 'timestamp']
NODE_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_FIELDS = ['id', 'user', 'uid', 'version', 'changeset', 'timestamp']
WAY_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_NODES_FIELDS = ['id', 'node_id', 'position']

#function to audit city names. Eg: seattle, WA becomes Seattle

def update_city_name(name):
    """Remove the State abbrevation if any and format the city name to Abc format"""
    if ', WA' or ',WA' in name:
        name = name.rstrip (', WA')
    return string.capwords(name)

#mapping shows the change required in last part of street name abbrevations and what they need to be changed to

mapping = {"St": "Street",
           "St.": "Street",
           "Rd.": "Road",
           "Ave": "Avenue",
           "NE" : "Northeast",
           "NW" : "Northwest",
           "SE" : "Southeast",
           "SW" : "Southwest",
           "WY" : "Way",
           "Ln" : "Lane",
           "ln" : "Lane"
           }
# This function takes a street name, extracts the last part of street name and changes it as specified in 'mapping'
def update_street_name(name, mapping):
    """Return the street name after auditing it as specified in mapping"""
    m = street_type_re.search(name)
    if m:
        street_type = m.group()
        if street_type in list(mapping.keys()):
            better_street_type = mapping[street_type]
            name = street_type_re.sub(better_street_type, name)
    return name

def update_postcode(postcode, invalid = True):
    """Check if postcode is of standard format 12345 or 12345-6789. If it neither then mark invalid as TRUE. If valid, Return just the first 5 digits"""
    m = postcode_format_re.search(postcode)
    if m:
        invalid = False
        postcode= postcode[:5]
    return (invalid, postcode)

#The function that takes a XML element and then shapes it based on the required conditions specified above.
#This also calls the functions to audit street name and city name before adding the element to the python dict

def extract_node(element, node_attr_fields = NODE_FIELDS, problem_chars=PROBLEMCHARS, default_tag_type='regular') :
    """ Extract node attributes from osm xml element and return a dictionary representing the node and related attributes"""
    attribs = {}
    tags = []

    """ Extraction Routine"""
    for key in node_attr_fields:
        attribs[key] = element.attrib[key]
    for tag in element.iter("tag"):
        node_tag = {}
        node_tag["type"] = default_tag_type
        node_tag["id"] = attribs["id"]
        node_tag["value"] = tag.attrib["v"]

        k = tag.attrib["k"]

        if problem_chars.search(k):
            continue
        elif ":" in k:
            node_tag["key"] = k.split(":", 1)[1]
            node_tag["type"] = k.split(":", 1)[0]
        else:
            node_tag["key"] = k

        # Update city name , if any, before appending the dictionary in list

        if node_tag["key"] == "city":
            node_tag["value"] = update_city_name(node_tag["value"])

        # Update street name, if any , as per mapping

        if node_tag["key"] == "street" or "street:name":
            node_tag["value"] = update_street_name(node_tag["value"], mapping)

        # Check if postcode is valid, if invalid prefix the postcode value with 'fixme:'

        if node_tag["key"] == "postcode":
            invalid, node_tag["value"] = update_postcode(node_tag["value"])
            if invalid:
                node_tag["value"] = 'fixme:' + node_tag["value"]


        tags.append(node_tag)

    return {'node': attribs, 'node_tags': tags}


def extract_way(element, way_attr_fields = WAY_FIELDS, problem_chars=PROBLEMCHARS, default_tag_type='regular') :
    """ Extract node attributes from osm xml element and return a dictionary representing the node and related attributes"""
    attribs = {}
    nodes = []
    tags =[]

    for key in way_attr_fields:
        attribs[key] = element.attrib[key]
    for tag in element.iter("tag"):
        way_tag = {}
        way_tag["type"] = default_tag_type
        way_tag["id"] = attribs["id"]
        way_tag["value"] = tag.attrib["v"]

        k = tag.attrib["k"]
        if PROBLEMCHARS.search(k):
            continue
        elif ":" in k:
            way_tag["key"] = k.split(":", 1)[1]
            way_tag["type"] = k.split(":", 1)[0]
        else:
            way_tag["key"] = k

        # Audit city name , if any, before appending the dictionary in list

        if way_tag["key"] == "city":
            way_tag["value"] = update_city_name(way_tag["value"])

        # Audit street name, if any , as per mapping

        if way_tag["key"] == "street" or "street:name":
            way_tag["value"] = update_street_name(way_tag["value"], mapping)

        # Check if postcode is valid, if invalid prefix the postcode value with 'fixme:'

        if way_tag["key"] == "postcode":
            invalid, way_tag["value"] = update_postcode(way_tag["value"])
            if invalid:
                way_tag["value"]='fixme:'+ way_tag["value"]

        tags.append(way_tag)

    for counter, nd in enumerate(element.iter("nd")):
        nd_tags = {}
        nd_tags["id"] = attribs["id"]
        nd_tags["node_id"] = nd.attrib["ref"]
        nd_tags["position"] = counter

        nodes.append(nd_tags)

    return {'way': attribs, 'way_nodes': nodes, 'way_tags': tags}


def extract_element(element,default_tag_type='regular'):
    """ Check Element type and return the extracted dictionary based on element tag  """
    if element.tag == 'node':
        return extract_node(element,NODE_FIELDS,PROBLEMCHARS,default_tag_type)
    if element.tag == 'way':
        return extract_way(element,WAY_FIELDS,PROBLEMCHARS,default_tag_type)



# ================================================== #
#               Helper Functions                     #
# ================================================== #
def get_element(osm_file, tags=('node', 'way', 'relation')):
    """Yield element if it is the right type of tag"""

    context = eT.iterparse(osm_file, events=('start', 'end'))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()


def validate_element(element, validator, schema=SCHEMA):
    """Raise ValidationError if element does not match schema"""
    if validator.validate(element, schema) is not True:
        print(validator.errors)
        for field, errors in validator.errors.items() :
            message_string = "\nElement of type '{0}' has the following errors:\n{1}"
            error_string = pprint.pformat(errors)

        raise Exception(message_string.format(field, error_string))


class UnicodeDictWriter(csv.DictWriter, object):
    """Extend csv.DictWriter to handle Unicode input"""

    def writerow(self, row):
        super(UnicodeDictWriter, self).writerow(row)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


# ================================================== #
#               Main Function                        #
# ================================================== #
def process_map(file_in, validate):
    """Iteratively process each XML element and write to csv(s)"""

    with codecs.open(NODES_PATH, 'w',encoding='utf8') as nodes_file, \
            codecs.open(NODE_TAGS_PATH, 'w',encoding='utf8') as nodes_tags_file, \
            codecs.open(WAYS_PATH, 'w',encoding='utf8') as ways_file, \
            codecs.open(WAY_NODES_PATH, 'w',encoding='utf8') as way_nodes_file, \
            codecs.open(WAY_TAGS_PATH, 'w',encoding='utf8') as way_tags_file:

        nodes_writer = UnicodeDictWriter(nodes_file, NODE_FIELDS)
        node_tags_writer = UnicodeDictWriter(nodes_tags_file, NODE_TAGS_FIELDS)
        ways_writer = UnicodeDictWriter(ways_file, WAY_FIELDS)
        way_nodes_writer = UnicodeDictWriter(way_nodes_file, WAY_NODES_FIELDS)
        way_tags_writer = UnicodeDictWriter(way_tags_file, WAY_TAGS_FIELDS)

        nodes_writer.writeheader()
        node_tags_writer.writeheader()
        ways_writer.writeheader()
        way_nodes_writer.writeheader()
        way_tags_writer.writeheader()

        validator = cerberus.Validator()

        for element in get_element(file_in, tags=('node', 'way')):
            el = extract_element(element)
            if el:
                if validate is True:
                    validate_element(el, validator)

                if element.tag == 'node':
                    nodes_writer.writerow(el['node'])
                    node_tags_writer.writerows(el['node_tags'])

                elif element.tag == 'way':
                    ways_writer.writerow(el['way'])
                    way_nodes_writer.writerows(el['way_nodes'])
                    way_tags_writer.writerows(el['way_tags'])




if __name__ == '__main__':
    print("Starting wrangling on Greater Seattle OSM data from data folder....")
    print("Have a cup of Coffee ! It takes a while..")
    process_map(OSM_PATH, validate=True)
    print("Completed wrangling OSM data, output CSV available in data/csv folder")

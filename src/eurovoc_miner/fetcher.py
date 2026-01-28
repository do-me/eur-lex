import datetime
import requests
import jinja2
import xmltodict
import logging
from joblib import expires_after
from .config import USER_AGENT, SPARQL_ENDPOINT, EUROVOC_XML_URL, TEMPLATES_DIR, CACHE_DIR
from joblib import Memory

log = logging.getLogger(__name__)
memory = Memory(CACHE_DIR, verbose=0)

@memory.cache(cache_validation_callback=expires_after(minutes=120))
def get_eurovoc_terms_and_id():
    """Fetch and parse the Eurovoc taxonomy."""
    eurovoc_terms_and_id = {}
    response = requests.get(
        EUROVOC_XML_URL,
        headers={'Accept': 'application/xml', 'Accept-Language': 'en', 'User-Agent': USER_AGENT}
    )
    data = xmltodict.parse(response.content)
    
    # Documentation suggests it's under xs:enumeration
    try:
        nodes = data['xs:schema']['xs:simpleType']['xs:restriction']['xs:enumeration']
        for term in nodes:
            try:
                name = term['xs:annotation']['xs:documentation'].split('/')[0].strip()
                eurovoc_id = term['@value'].split(':')[1]
                eurovoc_terms_and_id[name.lower()] = eurovoc_id
            except (KeyError, IndexError):
                continue
    except KeyError:
        log.error("Failed to parse Eurovoc XML structure")
        
    return eurovoc_terms_and_id

def get_sparql_query(d, lang=None):
    """Generate SPARQL query from template."""
    start = d.strftime('%Y-%m-%d')
    end = (d + datetime.timedelta(days=2)).strftime('%Y-%m-%d')
    
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(TEMPLATES_DIR))
    template = env.get_template('query.j2')
    return template.render(start=start, end=end, lang=lang)

def get_json_response(d, lang=None):
    """Execute SPARQL query and return JSON response."""
    headers = {'User-Agent': USER_AGENT}
    query = get_sparql_query(d, lang=lang)
    params = {
        "default-graph-uri": "",
        "query": query,
        "format": "application/sparql-results+json",
        "timeout": "0",
        "debug": "on",
        "run": "Run Query"
    }

    response = requests.get(SPARQL_ENDPOINT, headers=headers, params=params)
    response.raise_for_status()
    return response.json()

def get_concepts_id(list_of_eurovoc_terms):
    """Map terms to IDs."""
    terms_map = get_eurovoc_terms_and_id()
    seen = set()
    for e in list_of_eurovoc_terms:
        term_clean = e.strip().lower()
        if term_clean in terms_map:
            cid = terms_map[term_clean]
            if cid not in seen:
                seen.add(cid)
                yield cid
        else:
            log.warning(f"Eurovoc term not found: {e}")

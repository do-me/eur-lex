import datetime
import requests
import jinja2
import xmltodict
import logging
from joblib import expires_after
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from .config import USER_AGENT, SPARQL_ENDPOINT, EUROVOC_XML_URL, TEMPLATES_DIR, CACHE_DIR
from joblib import Memory

log = logging.getLogger(__name__)
memory = Memory(CACHE_DIR, verbose=0)

def get_session():
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

@memory.cache(cache_validation_callback=expires_after(minutes=120))
def get_eurovoc_terms_and_id():
    """Fetch and parse the Eurovoc taxonomy."""
    eurovoc_terms_and_id = {}
    session = get_session()
    response = session.get(
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

def get_sparql_query(d, lang=None, days=1):
    """Render the SPARQL query template."""
    start = d.strftime('%Y-%m-%d')
    end = (d + datetime.timedelta(days=days)).strftime('%Y-%m-%d')
    environment = jinja2.Environment(loader=jinja2.FileSystemLoader(TEMPLATES_DIR))
    template = environment.get_template("query.j2")
    return template.render(start=start, end=end, lang=lang)

def get_json_response(d, lang=None, days=1):
    """Execute SPARQL query and return JSON response."""
    headers = {'User-Agent': USER_AGENT}
    query = get_sparql_query(d, lang=lang, days=days)
    params = {
        "default-graph-uri": "",
        "query": query,
        "format": "application/sparql-results+json",
        "timeout": "0",
        "debug": "on",
        "run": "Run Query"
    }

    session = get_session()
    response = session.get(SPARQL_ENDPOINT, headers=headers, params=params)
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

import datetime
import logging

import jinja2
import requests
import xmltodict
from joblib import Memory, expires_after
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from .config import (
    CACHE_DIR,
    EUROVOC_XML_URL,
    SPARQL_ENDPOINT,
    TEMPLATES_DIR,
    USER_AGENT,
)

log = logging.getLogger(__name__)
memory = Memory(CACHE_DIR, verbose=0)
SPARQL_PAGE_SIZE = 1000

def get_session():
    session = requests.Session()
    retry = Retry(
        total=3,
        connect=3,
        # Don't retry on read timeouts. If the server accepted the request but
        # never responded, retrying the same expensive SPARQL query just burns
        # the same time again — observed to multiply a 180s timeout into 15min
        # of compounded retries during a slow Publications Office period.
        read=False,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        # Avoid raising for retried status codes; let the caller see the final
        # response and decide.
        raise_on_status=False,
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
        headers={'Accept': 'application/xml', 'Accept-Language': 'en', 'User-Agent': USER_AGENT},
        # (connect, read) seconds. Bounds an individual hung request so a slow
        # Publications Office endpoint cannot stall the whole job indefinitely.
        timeout=(10, 120),
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

def get_sparql_query(d, lang=None, days=1, *, offset=0, page_size=SPARQL_PAGE_SIZE):
    """Render the SPARQL query template."""
    if not 1 <= page_size <= SPARQL_PAGE_SIZE or offset < 0:
        raise ValueError("SPARQL pages must contain 1..1000 rows with a non-negative offset")
    start = d.strftime('%Y-%m-%d')
    end = (d + datetime.timedelta(days=days)).strftime('%Y-%m-%d')
    environment = jinja2.Environment(loader=jinja2.FileSystemLoader(TEMPLATES_DIR))
    template = environment.get_template("query.j2")
    return template.render(start=start, end=end, lang=lang,
                           page_size=page_size, offset=offset)

def get_json_response(d, lang=None, days=1):
    """Fetch every final-result page; fail before writing on partial/error responses.

    Cellar blocks public queries with LIMIT > 999999. Limiting the inner
    work-selection subquery also silently drops candidates before language
    and EuroVoc filtering. Page only the final grouped results, in a stable
    order, and keep every request comfortably below the service's limit.
    """
    headers = {'User-Agent': USER_AGENT}
    params = {
        "default-graph-uri": "",
        "format": "application/sparql-results+json",
        "timeout": "0",
        "debug": "on",
        "run": "Run Query"
    }

    session = get_session()
    combined = None
    offset = 0
    previous_page = None
    with session:
        while True:
            params['query'] = get_sparql_query(d, lang=lang, days=days, offset=offset)
            response = session.get(SPARQL_ENDPOINT, headers=headers, params=params, timeout=(10, 90))
            response.raise_for_status()
            state = response.headers.get('X-SQL-State', '00000')
            if state != '00000' or response.headers.get('X-SPARQL-MaxRows'):
                raise RuntimeError(f"Cellar returned partial results (SQL state {state}); retry the date")
            page = response.json()
            bindings = page['results']['bindings']
            if len(bindings) > SPARQL_PAGE_SIZE:
                raise RuntimeError("Cellar ignored the final-result page size")
            if bindings and bindings == previous_page:
                raise RuntimeError("Cellar repeated a page; refusing incomplete results")
            if combined is None:
                combined = page
                combined['results']['bindings'] = list(bindings)
            else:
                combined['results']['bindings'].extend(bindings)
            if len(bindings) < SPARQL_PAGE_SIZE:
                return combined
            previous_page = bindings
            offset += SPARQL_PAGE_SIZE

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

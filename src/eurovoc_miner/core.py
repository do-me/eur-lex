import logging
import datetime
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm
from .fetcher import get_json_response, get_concepts_id
from .parsers import get_body
from .config import MAX_WORKERS

log = logging.getLogger(__name__)

def get_docs(d, lang=None, days=1):
    """Yield document metadata from SPARQL results."""
    try:
        results = get_json_response(d, lang=lang, days=days)
        for r in results['results']['bindings']:
            subjects = r.get('subjects', {}).get('value', '').replace('\xa0', ' ').split('|||')
            terms = [t.strip() for t in subjects if t.strip()]
            if not terms:
                continue
                
            concept_ids = list(get_concepts_id(terms))
            if not concept_ids:
                continue
                
            # Flatten dictionary and set core fields
            doc = {
                'url': r['cellarURIs']['value'].split('|||')[0],  # Take first if multiple
                'celex': r.get('celexIds', {}).get('value', '').split('|||')[0],
                'eli': r.get('eliIds', {}).get('value', '').split('|||')[0],
                'title': r['title']['value'].split('|||')[0],
                'date': r['date']['value'],
                'lang': r['langIdentifier']['value'].lower(),
                'institutions': [t.strip() for t in r.get('authors', {}).get('value', '').split('|||') if t.strip()],
                'work_types': [t.strip() for t in r.get('workTypes', {}).get('value', '').split('|||') if t.strip()],
                'procedure_ids': [t.strip() for t in r.get('procedureIds', {}).get('value', '').split('|||') if t.strip()],
                'directory_codes': [t.strip() for t in r.get('directoryCodes', {}).get('value', '').split('|||') if t.strip()],
                'formats': [t.strip() for t in r['mtypes']['value'].split('|||')],
                'eurovoc_concepts': terms,
                'eurovoc_concepts_ids': concept_ids
            }
            yield doc
    except Exception as e:
        log.error(f"Error fetching docs for {d}: {e}")

def get_docs_text(d, lang=None, days=1):
    """Fetch and parse all documents for a given date or range in parallel."""
    docs = list(get_docs(d, lang=lang, days=days))
    if not docs:
        return
        
    date_range = f"{d}" if days == 1 else f"{d} to {d + datetime.timedelta(days=days-1)}"
    log.info(f"Processing {len(docs)} documents for {date_range}")
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = tqdm(executor.map(get_body, docs), total=len(docs), desc=date_range, colour='green')
        for doc in results:
            if doc:
                yield doc

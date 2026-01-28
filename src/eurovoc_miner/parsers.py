import logging
from io import BytesIO
from bs4 import BeautifulSoup
import docx2txt
from pdfminer.high_level import extract_text
from .config import USER_AGENT, CACHE_DIR
from .fetcher import get_session
from joblib import Memory

log = logging.getLogger(__name__)
memory = Memory(CACHE_DIR, verbose=0)

@memory.cache()
def get_pdf_body(r):
    url, lang = r['url'], r['lang']
    session = get_session()
    response = session.get(url, headers={'Accept': 'application/pdf', 'Accept-Language': lang, 'User-Agent': USER_AGENT})
    if response.status_code == 200:
        return extract_text(BytesIO(response.content))
    return ""

@memory.cache()
def get_html_content(r, accept='text/html'):
    url, lang = r['url'], r['lang']
    session = get_session()
    response = session.get(url, headers={'Accept': accept, 'Accept-Language': lang, 'User-Agent': USER_AGENT})
    if response.status_code == 200:
        return BeautifulSoup(response.content, 'html.parser').get_text()
    return ""

@memory.cache()
def get_doc_content(url, accept, lang='en'):
    session = get_session()
    response = session.get(url, headers={'Accept': accept, 'Accept-Language': lang, 'User-Agent': USER_AGENT})
    if response.status_code == 200:
        return docx2txt.process(BytesIO(response.content))
    return ""

def get_body(r):
    """Factory function for document parsing."""
    try:
        formats = r.get('formats', [])
        if 'pdf' in formats:
            text = get_pdf_body(r)
        elif any(f in formats for f in ['xhtml', 'html']):
            text = get_html_content(r, 'application/xhtml+xml' if 'xhtml' in formats else 'text/html')
        elif any(f in formats for f in ['docx', 'doc']):
            accept = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml' if 'docx' in formats else 'application/msword'
            text = get_doc_content(r['url'], accept, r['lang'])
        else:
            return None

        if not text or not text.strip():
            return None
            
        r['text'] = text
        return r
    except Exception as e:
        log.error(f"Error parsing {r.get('url')}: {e}")
        return None

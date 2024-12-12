import logging
import json
import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from typing import Optional, Dict

logger = logging.getLogger(__name__)

class ContentExtractor:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
        }
        self.site_structures = self._load_site_structures()

    def _load_site_structures(self) -> Dict:
        try:
            base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))))
            structure_path = os.path.join(base_path, 'resources', 'news_site_structures.json')
            
            with open(structure_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load news site structures: {str(e)}")
            raise

    def extract_content(self, url: str) -> str:
        try:
            logger.info(f"Extracting content from: {url}")
            
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # 언론사별 선택자로 컨텐츠 추출 시도
            content = self._extract_by_selector(soup, url)
            
            # 실패시 폴백: 전체 텍스트 추출
            if not content:
                logger.warning(f"Falling back to basic text extraction for: {url}")
                content = self._extract_fallback(soup)
            
            if not content:
                raise ValueError("No content could be extracted")
                
            return self._clean_content(content)
            
        except requests.RequestException as e:
            logger.error(f"Request failed for {url}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Content extraction failed for {url}: {str(e)}")
            raise

    def _extract_by_selector(self, soup: BeautifulSoup, url: str) -> Optional[str]:
        try:
            domain = urlparse(url).netloc
            
            # news_site_structures.json의 구조에 따라 적절한 선택자 찾기
            for site_name, site_info in self.site_structures.items():
                if isinstance(site_info["domain"], list):
                    # 여러 도메인을 가진 사이트 처리 (예: 매일경제)
                    if domain in site_info["domain"]:
                        selectors = site_info["selectors"]
                        break
                elif site_info["domain"] in domain:
                    selectors = site_info["selectors"]
                    break
            else:
                logger.warning(f"No matching site structure for domain: {domain}")
                return None

            # 선택자가 딕셔너리인 경우 (예: 서울경제)
            if isinstance(selectors, dict):
                if domain in selectors:
                    selector = selectors[domain]
                else:
                    # 부분 매치 시도
                    selector = next(
                        (s for d, s in selectors.items() if d in domain),
                        None
                    )
            else:
                selector = selectors

            if not selector:
                return None

            content_element = soup.select_one(selector)
            return content_element.get_text(strip=True) if content_element else None
            
        except Exception as e:
            logger.warning(f"Selector-based extraction failed: {str(e)}")
            return None

    def _extract_fallback(self, soup: BeautifulSoup) -> str:
        for tag in soup(['script', 'style', 'header', 'footer', 'nav']):
            tag.decompose()
        return soup.get_text(strip=True)

    def _clean_content(self, content: str) -> str:
        return ' '.join(content.split())
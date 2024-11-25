import logging
import feedparser
from typing import Dict, List
from datetime import datetime
from urllib.parse import urlparse
from googlenewsdecoder import new_decoderv1

logger = logging.getLogger(__name__)

class NewsFetcher:
    def __init__(self):
        self.press_mapping = {
            '한국경제': 'hankyung.com',
            '매일경제': 'mk.co.kr',
            '서울경제': 'sedaily.com'
        }
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
        }

    def fetch_news(self, period: int, keyword: str, press: str, total_article_num: int = 6) -> Dict:
        """뉴스 피드 수집"""
        try:
            logger.info(f"{press}를 위한 뉴스 수집, 사용한 키워드: {keyword}")
            
            domain = self.press_mapping.get(press)
            if not domain:
                raise ValueError(f"Invalid press: {press}")
                
            rss_url = f"https://news.google.com/rss/search?q={keyword}%20site%3A{domain}%20when%3A{period}d&hl=ko&gl=KR&ceid=KR:ko"
            feed = feedparser.parse(rss_url)
            
            articles, cnt = [], 0
            for entry in feed.entries:
                try:
                    if cnt >= total_article_num:
                        break
                    decoded_url = self._decode_url(entry.link)
                    if self._is_valid_url(decoded_url, domain):
                        articles.append({
                            'title': entry.title,
                            'url': decoded_url,
                            'published_date': self._parse_date(entry.published)
                        })
                    cnt += 1
                except Exception as e:
                    logger.warning(f"entry 프로세싱 실패: {entry.link}, Error: {str(e)}")
                    continue
            
            logger.info(f"{len(articles)} 기사 찾음: {press}-{keyword}")
            return {
                'articles': articles,
                'press': press,
                'keyword': keyword,
                'count': len(articles),
                'fetch_time': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"뉴스 수집에 실패한 지점은 {press}-{keyword}: {str(e)}")
            raise
    
    def _decode_url(self, google_news_url: str) -> str:
        """Google News URL을 원본 URL로 디코딩"""
        try:
            decoded_result = new_decoderv1(google_news_url)
            if decoded_result.get("status"):
                return decoded_result["decoded_url"]
            else:
                logger.warning(f"URL 디코딩 실패: {google_news_url}\nError: {decoded_result}")
                return google_news_url
        except Exception as e:
            logger.error(f"URL 디코딩 좌절: {str(e)}")
            return google_news_url

    def _is_valid_url(self, url: str, domain: str) -> bool:
        """URL이 해당 언론사의 것인지 확인"""
        try:
            parsed_url = urlparse(url)
            return domain in parsed_url.netloc
        except Exception:
            return False

    def _parse_date(self, date_str: str) -> str:
        """날짜 문자열 파싱"""
        try:
            dt = datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %Z")
            return dt.isoformat()
        except Exception as e:
            logger.warning(f"날짜 파싱 좌절: {str(e)}")
            return datetime.now().isoformat()
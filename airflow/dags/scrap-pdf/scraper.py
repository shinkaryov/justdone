import requests
import xml.etree.ElementTree as ET


class Scraper:
    """
    Base class for scrapers.

    Attributes:
        query (str): The search query for fetching data.
        namespaces (dict): XML namespaces used for parsing.
    """

    def __init__(self, query: str):
        """
        Initializes the Scraper with a search query.

        Args:
            query (str): The search term to be used in the scraper.
        """
        self.query = query
        self.namespaces = {"atom": "http://www.w3.org/2005/Atom"}

    def get_papers(self, max_results: int = 10):
        """
        Abstract method to fetch papers.

        Args:
            max_results (int, optional): The maximum number of results to retrieve. Defaults to 10.

        Raises:
            NotImplementedError: Must be implemented by subclasses.
        """
        raise NotImplementedError("Subclasses must implement this method")


class ArxivScraper(Scraper):
    """
    A scraper for fetching academic papers from the arXiv API.

    Attributes:
        base_url (str): The API endpoint for querying arXiv papers.
    """

    def __init__(self, base_url: str, query: str):
        """
        Initializes the ArxivScraper with a base URL and search query.

        Args:
            base_url (str): The base URL of the arXiv API.
            query (str): The search query for retrieving papers.
        """
        super().__init__(query)
        self.base_url = base_url

    def get_papers(self, max_results: int = 10) -> list[dict]:
        """
        Fetches academic papers from the arXiv API.

        Args:
            max_results (int, optional): The maximum number of papers to fetch. Defaults to 10.

        Returns:
            list[dict]: A list of dictionaries, each containing details about a paper.

        Raises:
            Exception: If the API request fails or the XML response cannot be parsed.
        """
        params = {
            "search_query": self.query,
            "start": 0,
            "max_results": max_results,
            "sortBy": "submittedDate",
            "sortOrder": "descending",
        }

        try:
            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()  # Raise an HTTPError for bad responses
        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to fetch papers: {e}")

        try:
            root = ET.fromstring(response.content)
        except ET.ParseError as e:
            raise Exception(f"Failed to parse XML response: {e}")

        papers = []
        for entry in root.findall("atom:entry", self.namespaces):
            try:
                paper_id = entry.find("atom:id", self.namespaces)
                title = entry.find("atom:title", self.namespaces)
                summary = entry.find("atom:summary", self.namespaces)
                published = entry.find("atom:published", self.namespaces)

                if None in (paper_id, title, summary, published):
                    continue  # Skip entries with missing fields

                authors = [
                    author.find("atom:name", self.namespaces).text
                    for author in entry.findall("atom:author", self.namespaces)
                    if author.find("atom:name", self.namespaces) is not None
                ]

                categories = [
                    category.get("term") for category in entry.findall("atom:category", self.namespaces)
                ]

                links = {
                    link.get("title", "html"): link.get("href")
                    for link in entry.findall("atom:link", self.namespaces)
                    if link.get("rel") in ["alternate", "related"] and link.get("href") is not None
                }

                paper = {
                    "Id": paper_id.text,
                    "Source": "arXiv",
                    "Title": title.text,
                    "ShortDescriptionEng": summary.text,
                    "PublishedAt": published.text,
                    "Authors": authors,
                    "Categories": categories,
                    "Links": {"HTML": links["html"], "PDF": links["pdf"]}
                }
                papers.append(paper)

            except AttributeError as e:
                print(f"Skipping an entry due to missing data: {e}")

        return papers

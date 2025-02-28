import aiohttp
import asyncio
import fitz  # PyMuPDF
from io import BytesIO
import re


class AsyncPDFScraper:
    @staticmethod
    async def download_pdf(url):
        """
        Downloads a PDF file asynchronously.

        Args:
            url (str): The URL of the PDF file.

        Returns:
            BytesIO or None: The PDF content as a BytesIO object, or None if an error occurs.
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        return BytesIO(await response.read())
                    else:
                        print(f"Error loading PDF: {url} (Status Code: {response.status})")
        except aiohttp.ClientError as e:
            print(f"Request error while downloading PDF: {url} - {e}")
        return None

    @staticmethod
    async def extract_text(pdf_bytes):
        """
        Extracts text from a PDF asynchronously.

        Args:
            pdf_bytes (BytesIO): The PDF content.

        Returns:
            str or None: Extracted text, or None if an error occurs.
        """
        if pdf_bytes is None:
            return None
        return await asyncio.to_thread(AsyncPDFScraper._sync_extract_text, pdf_bytes)

    @staticmethod
    def _sync_extract_text(pdf_bytes):
        """
        Synchronously extracts text from a PDF.

        Args:
            pdf_bytes (BytesIO): The PDF content.

        Returns:
            str or None: Extracted text, or None if an error occurs.
        """
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            text = "\n".join([page.get_text("text") for page in doc]).strip()
            return AsyncPDFScraper.clean_text(text)
        except Exception as e:
            print(f"Error processing PDF: {e}")
            return None

    @staticmethod
    def clean_text(text):
        """
        Cleans extracted text by removing excessive whitespace and encoding issues.

        Args:
            text (str): The raw extracted text.

        Returns:
            str: Cleaned text.
        """
        text = re.sub(r"\s+", " ", text)
        text = text.encode("utf-8", "ignore").decode("utf-8")
        return text.strip()

    async def get_paper_text(self, pdf_url):
        """
        Downloads and extracts text from a PDF.

        Args:
            pdf_url (str): The URL of the PDF file.

        Returns:
            str or None: Extracted text, or None if an error occurs.
        """
        pdf_bytes = await self.download_pdf(pdf_url)
        return await self.extract_text(pdf_bytes)

"""
Module for language translation using Google's Vertex AI and Gemini models.

This module defines an abstract base class `LLMTranslator` and a concrete implementation
`GeminiTranslator`, which leverages Vertex AI's GenerativeModel for translation.

Features:
- Uses Google's Gemini AI model (`gemini-1.5-flash-002`) for translation.
- Supports asynchronous translation with error handling and retries.
- Implements exponential backoff for robustness against transient errors.

Classes:
1. **LLMTranslator** (Abstract Base Class)
    - Defines the `translate` method that must be implemented by subclasses.

2. **GeminiTranslator** (Concrete Implementation)
    - Uses Vertex AI's GenerativeModel for translation.
    - Initializes with a project ID and location.
    - Implements retries with exponential backoff in case of API failures.
"""

import abc
import asyncio
import vertexai
from vertexai.generative_models import GenerativeModel
import google.api_core.exceptions


class LLMTranslator(abc.ABC):
    """
    Abstract base class for an LLM-based translator.

    Subclasses must implement the `translate` method.
    """

    @abc.abstractmethod
    async def translate(self, text: str, source_lang: str, target_lang: str) -> str:
        """
        Translates text from source language to target language.

        Args:
            text (str): The text to translate.
            source_lang (str): The source language code (e.g., "en" for English).
            target_lang (str): The target language code (e.g., "uk" for Ukrainian).

        Returns:
            str: The translated text.
        """
        pass


class GeminiTranslator(LLMTranslator):
    """
    A translator implementation using Google's Vertex AI Gemini model.

    This class initializes the Gemini model and supports translation between languages.
    It includes error handling and retries to ensure robustness.

    Attributes:
        project_id (str): The GCP project ID.
        location (str): The Vertex AI model location.
        model (GenerativeModel): The loaded Gemini AI model.
    """

    def __init__(self, project_id: str, location: str = "europe-west1"):
        """
        Initializes the GeminiTranslator with a Vertex AI model.

        Args:
            project_id (str): The Google Cloud project ID.
            location (str, optional): The location for the Vertex AI service. Defaults to "europe-west1".
        """
        vertexai.init(project=project_id, location=location)
        self.model = GenerativeModel("gemini-1.5-flash-002")

    async def translate(self, text: str, source_lang="en", target_lang="uk", max_retries=3, delay=2) -> str:
        """
        Translates text from source language to target language using Gemini AI.

        Implements retries with exponential backoff in case of API failures.

        Args:
            text (str): The text to translate.
            source_lang (str, optional): The source language code. Defaults to "en".
            target_lang (str, optional): The target language code. Defaults to "uk".
            max_retries (int, optional): Maximum number of retry attempts. Defaults to 3.
            delay (int, optional): Initial delay in seconds before retrying. Defaults to 2.

        Returns:
            str: The translated text, or None if translation fails after retries.
        """
        prompt = f"Translate the following text from {source_lang} to {target_lang}:\n{text}"

        for attempt in range(1, max_retries + 1):
            try:
                response = await asyncio.to_thread(self.model.generate_content, [prompt])
                if response and response.text:
                    return response.text.replace("'", '"').strip()
                else:
                    print(f"⚠️ Empty response received on attempt {attempt}. Retrying...")
            except google.api_core.exceptions.GoogleAPIError as e:
                print(f"⚠️ API Error on attempt {attempt}: {e}")
            except Exception as e:
                print(f"⚠️ Unexpected error on attempt {attempt}: {e}")

            if attempt < max_retries:
                await asyncio.sleep(delay * (2 ** (attempt - 1)))  # Exponential backoff

        print(f"❌ Translation failed after {max_retries} attempts.")
        return None

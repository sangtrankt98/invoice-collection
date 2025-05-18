import os
import re
import json
import base64
import warnings
import gc
import time
from typing import Dict, Any, Optional, Union, List
import io
from PIL import Image
import torch
import psutil

# For PyTorch and Transformers
from transformers import pipeline, AutoModelForCausalLM, AutoTokenizer

# PDF processing libraries
import fitz  # PyMuPDF
import pdfplumber
from utils.logger_setup import setup_logger

logger = setup_logger()

# Ignore PDF-related warnings
warnings.filterwarnings("ignore", category=UserWarning, module="pdfplumber")
warnings.filterwarnings("ignore", category=UserWarning, module="pdfminer")

# Default output dictionary structure
DEFAULT_DICT = {
    "document_type": None,
    "document_number": None,
    "date": None,
    "entity_name": None,
    "entity_tax_number": None,
    "counterparty_name": None,
    "counterparty_tax_number": None,
    "payment_method": None,
    "amount_before_tax": None,
    "tax_rate": None,
    "tax_amount": None,
    "total_amount": None,
    "direction": None,
    "description": None,
}


class InvoiceExtractor:
    """
    A class to extract information from PDF invoices using Hugging Face Transformers.
    With memory management for processing large batches of documents.
    """

    def __init__(
        self,
        model_id: str,
        device_map: str = "auto",
        torch_dtype=torch.bfloat16,
        memory_threshold_gb: float = 0.7,
        batch_size: int = 10,
        output_dir: Optional[str] = None,
    ):
        """
        Initialize the InvoiceExtractor with a Hugging Face model.

        Args:
            model_id (str): Hugging Face model ID or local path
            device_map (str): Device mapping strategy (auto, cpu, cuda, etc.)
            torch_dtype: Data type for model weights (e.g., torch.bfloat16, torch.float16)
            memory_threshold_gb (float): Memory threshold to trigger cleanup (0.8 = 80% of available RAM)
            batch_size (int): Number of documents to process before forced memory cleanup
            output_dir (str, optional): Directory to save intermediate results
        """
        self.model_id = model_id
        self.device_map = device_map
        self.torch_dtype = torch_dtype
        self.memory_threshold_gb = memory_threshold_gb
        self.batch_size = batch_size
        self.output_dir = output_dir

        # Create output directory if specified
        if self.output_dir and not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        # Load the model
        logger.info(f"Loading model: {model_id}")
        self._load_model()

    def _load_model(self):
        """Load the model using the Hugging Face pipeline"""
        try:
            # Clear memory before loading
            self._clear_memory()

            logger.info(f"Initializing text-generation pipeline with {self.model_id}")

            # Create a custom generation config with your preferred settings
            from transformers import GenerationConfig

            # Create custom generation config with YOUR preferred settings
            generation_config = GenerationConfig(
                do_sample=False,  # Always select highest probability tokens
                max_new_tokens=1000,
                # You can add other parameters here
            )

            # Load tokenizer first (smaller memory footprint)
            if os.path.exists(self.model_id):
                # For local models
                logger.info("Loading tokenizer...")
                self.tokenizer = AutoTokenizer.from_pretrained(self.model_id)

                # Set the pad_token_id in both tokenizer and generation config
                if self.tokenizer.pad_token_id is None:
                    logger.info("Setting pad_token_id to eos_token_id")
                    self.tokenizer.pad_token_id = self.tokenizer.eos_token_id
                    generation_config.pad_token_id = self.tokenizer.eos_token_id

                # Load model with CPU optimizations
                logger.info("Loading model (this may take some time)...")
                self.model = AutoModelForCausalLM.from_pretrained(
                    self.model_id,
                    torch_dtype=torch.float32,  # Use float32 for CPU
                    device_map="cpu",  # Force CPU
                    low_cpu_mem_usage=True,
                    offload_folder="offload_folder",  # Enable disk offloading if needed
                )

                # Create pipeline with explicit generation_config
                logger.info("Creating pipeline with custom generation parameters...")
                self.pipe = pipeline(
                    "text-generation",
                    model=self.model,
                    tokenizer=self.tokenizer,
                    device_map="cpu",
                    generation_config=generation_config,  # Use our custom config
                )
            else:
                # For HuggingFace Hub models
                logger.info("Loading model from HuggingFace Hub...")

                # First, get the tokenizer to set pad_token_id
                self.tokenizer = AutoTokenizer.from_pretrained(self.model_id)
                if self.tokenizer.pad_token_id is None:
                    logger.info("Setting pad_token_id to eos_token_id")
                    self.tokenizer.pad_token_id = self.tokenizer.eos_token_id
                    generation_config.pad_token_id = self.tokenizer.eos_token_id

                self.pipe = pipeline(
                    "text-generation",
                    model=self.model_id,
                    tokenizer=self.tokenizer,
                    torch_dtype=torch.float32,
                    device_map="cpu",
                    generation_config=generation_config,  # Use our custom config
                )

            logger.info("Model loaded successfully with custom generation parameters")

        except Exception as e:
            logger.error(f"Error loading model: {e}")
            raise

    def _clear_memory(self):
        """Clear memory cache and run garbage collection"""
        logger.info("Clearing memory...")

        # For CPU models, we need to explicitly delete tensors
        if hasattr(self, "last_inputs"):
            del self.last_inputs

        # Reset CUDA cache even if using CPU (helps with hybrid operations)
        if torch.cuda.is_available():
            torch.cuda.empty_cache()

        # Force garbage collection multiple times
        for _ in range(5):  # More iterations for better cleanup
            gc.collect()

        # Report memory after cleanup
        ram = psutil.virtual_memory()
        logger.info(f"Memory cleared. Current RAM usage: {ram.percent}%")

    def _check_memory(self) -> bool:
        """
        Check memory usage and clear if above threshold

        Returns:
            bool: True if memory was cleared, False otherwise
        """
        # Focus on system RAM for CPU-only environments
        ram = psutil.virtual_memory()
        ram_used_gb = ram.used / (1024**3)
        ram_total_gb = ram.total / (1024**3)
        ram_percent = ram.percent / 100.0

        logger.info(
            f"System RAM: {ram_used_gb:.2f}GB / {ram_total_gb:.2f}GB ({ram_percent:.2%})"
        )

        if ram_percent > self.memory_threshold_gb:
            logger.warning(
                f"System RAM threshold exceeded ({ram_percent:.2%} > {self.memory_threshold_gb:.2%}). Clearing cache..."
            )
            self._clear_memory()
            return True

        return False

    def normalize_vietnamese_text(self, text: str) -> str:
        """
        Normalize company names by removing Vietnamese diacritical marks,
        standardizing case and spacing, and removing special characters.

        Args:
            text (str): Text to normalize

        Returns:
            str: Normalized text
        """
        if not isinstance(text, str):
            return text

        # Vietnamese character mappings
        vietnamese_map = {
            "à": "a",
            "á": "a",
            "ả": "a",
            "ã": "a",
            "ạ": "a",
            "ă": "a",
            "ằ": "a",
            "ắ": "a",
            "ẳ": "a",
            "ẵ": "a",
            "ặ": "a",
            "â": "a",
            "ầ": "a",
            "ấ": "a",
            "ẩ": "a",
            "ẫ": "a",
            "ậ": "a",
            "đ": "d",
            "è": "e",
            "é": "e",
            "ẻ": "e",
            "ẽ": "e",
            "ẹ": "e",
            "ê": "e",
            "ề": "e",
            "ế": "e",
            "ể": "e",
            "ễ": "e",
            "ệ": "e",
            "ì": "i",
            "í": "i",
            "ỉ": "i",
            "ĩ": "i",
            "ị": "i",
            "ò": "o",
            "ó": "o",
            "ỏ": "o",
            "õ": "o",
            "ọ": "o",
            "ô": "o",
            "ồ": "o",
            "ố": "o",
            "ổ": "o",
            "ỗ": "o",
            "ộ": "o",
            "ơ": "o",
            "ờ": "o",
            "ớ": "o",
            "ở": "o",
            "ỡ": "o",
            "ợ": "o",
            "ù": "u",
            "ú": "u",
            "ủ": "u",
            "ũ": "u",
            "ụ": "u",
            "ư": "u",
            "ừ": "u",
            "ứ": "u",
            "ử": "u",
            "ữ": "u",
            "ự": "u",
            "ỳ": "y",
            "ý": "y",
            "ỷ": "y",
            "ỹ": "y",
            "ỵ": "y",
        }

        # Step 1: Remove Vietnamese diacritical marks
        result = ""
        for char in text:
            lower_char = char.lower()
            if lower_char in vietnamese_map:
                # Preserve the original case
                if char.isupper():
                    result += vietnamese_map[lower_char].upper()
                else:
                    result += vietnamese_map[lower_char]
            else:
                result += char

        # Step 2: Remove special characters and punctuation (keep alphanumeric and spaces)
        result = re.sub(r"[^\w\s]", "", result)

        # Step 3: Convert to uppercase and standardize spacing
        result = " ".join(result.upper().split())

        return result

    def process_text_with_model(self, text: str) -> Dict[str, Any]:
        """
        Process text with Transformers model to extract invoice data.
        Includes memory management.

        Args:
            text (str): Text to process

        Returns:
            dict: Extracted invoice data
        """
        self.last_inputs = text
        # Check memory before processing
        self._check_memory()

        # Create system message and user message with the prompt
        user_prompt = f"""Extract structured financial data from this Vietnamese/English document.

                Document text:
                {text}

                Extract the following information into JSON format:
                - document_type: INVOICE, BANK_TRANSACTION, TAX_DOCUMENT, or OTHER_FINANCIAL
                - document_number: ID number (only get information after 'Số'/'Number'/'No')
                - date: Format yyyy-mm-dd
                - entity_name: Main company/person
                - entity_tax_number: Their tax ID
                - counterparty_name: Other transaction party
                - counterparty_tax_number: Their tax ID
                - payment_method: "bank_transfer", "cash", or "others"
                - amount_before_tax: Number without formatting
                - tax_rate: Number only (e.g., 10 for 10%)
                - tax_amount: Number without formatting
                - total_amount: Number without formatting
                - description: Brief context (50-60 chars) for invoices only
                """

        messages = [
            {
                "role": "system",
                "content": "You extract structured financial data from documents into JSON format. Always respond with valid JSON only.",
            },
            {"role": "user", "content": user_prompt},
        ]

        # Process with model
        try:
            logger.info("Processing text with Transformers model...")

            # Generate response
            output = self.pipe(
                messages,
            )

            # Extract the response
            response = output[0]["generated_text"]

            # The last message should contain our result
            if isinstance(response, list) and len(response) > 0:
                if isinstance(response[-1], dict) and "content" in response[-1]:
                    result_str = response[-1]["content"]
                elif isinstance(response[-1], str):
                    result_str = response[-1]
                else:
                    logger.warning(f"Unexpected response format: {type(response[-1])}")
                    result_str = str(response[-1])
            else:
                logger.warning(f"Unexpected response format: {type(response)}")
                result_str = str(response)

            logger.info("Processing completed successfully")

            # Extract JSON from the response
            json_str = self._extract_json(result_str)

            # Parse the extracted JSON
            extracted_data = json.loads(json_str)

            # Apply post-processing to normalize fields
            result_dict = self._post_process_extraction(extracted_data)

            # Check memory after processing
            self._check_memory()
            # After processing, explicitly delete intermediates
            if hasattr(self, "last_inputs"):
                del self.last_inputs

            return result_dict

        except Exception as e:
            logger.error(f"Error processing text with model: {e}")
            logger.error(f"Attempted to process text: {text[:100]}...")

            # Always clear memory on error
            self._clear_memory()

            return DEFAULT_DICT.copy()

    def _extract_json(self, text: str) -> str:
        """
        Extract JSON from a text that might contain additional content.

        Args:
            text (str): Text containing JSON

        Returns:
            str: Extracted JSON string
        """
        # First try to find JSON between curly braces
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            return match.group(0)

        # If no JSON found, return the original text
        return text

    def _post_process_extraction(
        self, extracted_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Post-process the extracted data to normalize fields.

        Args:
            extracted_data (dict): Extracted data from model

        Returns:
            dict: Normalized data
        """
        result_dict = DEFAULT_DICT.copy()

        # Process each field
        for key, value in extracted_data.items():
            if key in result_dict:
                if isinstance(value, str):
                    if value.strip() == "":
                        result_dict[key] = None
                    else:
                        # Normalize Vietnamese text fields
                        if key in ["entity_name", "counterparty_name"]:
                            result_dict[key] = self.normalize_vietnamese_text(value)
                        else:
                            result_dict[key] = value
                elif value == 0 and key in [
                    "amount_before_tax",
                    "tax_rate",
                    "tax_amount",
                    "total_amount",
                ]:
                    # Keep numeric zeros as they are
                    result_dict[key] = value
                else:
                    result_dict[key] = value

        return result_dict

    def extract_text_from_pdf(self, pdf_path: str) -> str:
        """
        Extract text from a PDF file using pdfplumber.

        Args:
            pdf_path (str): Path to the PDF file

        Returns:
            str: Extracted text
        """
        try:
            logger.info(f"Extracting text from PDF: {pdf_path}")
            with pdfplumber.open(pdf_path) as pdf:
                text = "\n".join(page.extract_text() or "" for page in pdf.pages)

            if text.strip():
                logger.info(f"Successfully extracted {len(text)} characters of text")
                return text
            else:
                logger.warning("No text extracted from PDF")
                return ""
        except Exception as e:
            logger.error(f"Error extracting text with pdfplumber: {e}")
            return ""

    def extract_data_from_pdf(self, pdf_path: str) -> Dict[str, Any]:
        """
        Extract invoice data from a PDF, handling multi-page documents with various extraction methods.

        Args:
            pdf_path (str): Path to the PDF file

        Returns:
            dict: Extracted invoice data
        """
        # First try to extract text using pdfplumber
        text = self.extract_text_from_pdf(pdf_path)

        # If we got text, use the text-based extraction
        if text.strip():
            logger.info("Using text-based extraction")
            return self.process_text_with_model(text)

        # If no text was extracted, fall back to image-based extraction
        logger.info("No text extracted, falling back to image-based extraction")

        # Convert PDF to image
        image_data = self._get_pdf_as_image(pdf_path)
        if image_data:
            return self.extract_data_from_image(image_data)
        else:
            logger.error("Could not convert PDF to image")
            return DEFAULT_DICT.copy()

    def extract_data_from_image(self, image_input: Union[str, bytes]) -> Dict[str, Any]:
        """
        Extract invoice data from an image.

        Args:
            image_input (Union[str, bytes]): Path to the image file or base64 string or bytes

        Returns:
            dict: Extracted invoice data
        """
        # First, convert the image to a format we can use for OCR
        image_bytes = self._prepare_image(image_input)
        if not image_bytes:
            logger.error("Failed to prepare image for processing")
            return DEFAULT_DICT.copy()

        # Perform OCR on the image
        text = self._perform_ocr(image_bytes)
        if not text:
            logger.error("OCR failed to extract text from image")
            return DEFAULT_DICT.copy()

        # Process the extracted text with model
        return self.process_text_with_model(text)

    def _prepare_image(self, image_input: Union[str, bytes]) -> Optional[bytes]:
        """
        Prepare an image for processing, handling various input formats.

        Args:
            image_input: Path to image file, base64 string, or bytes

        Returns:
            bytes: Image bytes or None if preparation failed
        """
        try:
            # Handle file path
            if isinstance(image_input, str) and os.path.exists(image_input):
                logger.info(f"Reading image from file: {image_input}")
                with open(image_input, "rb") as f:
                    return f.read()

            # Handle base64 string
            elif isinstance(image_input, str):
                # Remove data URL prefix if present
                if image_input.startswith("data:image"):
                    image_input = image_input.split(",", 1)[1]

                logger.info("Decoding base64 image")
                return base64.b64decode(image_input)

            # Handle bytes directly
            elif isinstance(image_input, bytes):
                return image_input

            else:
                logger.error(f"Unsupported image input type: {type(image_input)}")
                return None

        except Exception as e:
            logger.error(f"Error preparing image: {e}")
            return None

    def _perform_ocr(self, image_bytes: bytes) -> str:
        """
        Perform OCR on an image to extract text.

        Args:
            image_bytes: Image data as bytes

        Returns:
            str: Extracted text
        """
        try:
            # Import tesseract here to avoid dependency if not used
            import pytesseract
            from PIL import Image

            logger.info("Performing OCR on image")
            pytesseract.pytesseract.tesseract_cmd = (
                r"C:\Users\NJV\AppData\Local\Programs\Tesseract-OCR"  # Windows example
            )

            # Convert bytes to PIL Image
            image = Image.open(io.BytesIO(image_bytes))

            # Perform OCR with Vietnamese language support
            text = pytesseract.image_to_string(
                image,
                lang="vie+eng",  # Vietnamese + English
                config="--psm 1",  # Automatic page segmentation with OSD
            )

            logger.info(f"OCR extracted {len(text)} characters")
            return text

        except ImportError:
            logger.error(
                "pytesseract not installed. Install with: pip install pytesseract"
            )
            logger.error("Also ensure Tesseract OCR is installed on your system")
            return ""

        except Exception as e:
            logger.error(f"OCR error: {e}")
            return ""

    def _get_pdf_as_image(self, pdf_path: str) -> Optional[str]:
        """
        Convert a PDF to an image for processing.
        Handles multiple pages by concatenating them vertically.

        Args:
            pdf_path (str): Path to the PDF file

        Returns:
            Optional[str]: Base64-encoded image data or None if conversion failed
        """
        # Try pdf2image first
        try:
            from pdf2image import convert_from_path

            logger.info("Converting PDF to image using pdf2image")

            # Convert all pages of the PDF to images
            images = convert_from_path(pdf_path)

            if not images:
                logger.warning("No images extracted from PDF using pdf2image")
                return None

            # If there's only one page, use it directly
            if len(images) == 1:
                img = images[0]
                img_byte_arr = io.BytesIO()
                img.save(img_byte_arr, format="JPEG", quality=85)
                img_byte_arr = img_byte_arr.getvalue()
                return base64.b64encode(img_byte_arr).decode("utf-8")

            # If there are multiple pages, concatenate them vertically
            total_height = sum(img.height for img in images)
            max_width = max(img.width for img in images)

            # Create a new image with the combined dimensions
            combined_img = Image.new("RGB", (max_width, total_height))

            # Paste each page image into the combined image
            y_offset = 0
            for img in images:
                combined_img.paste(img, (0, y_offset))
                y_offset += img.height

            # Convert the combined image to bytes
            img_byte_arr = io.BytesIO()
            combined_img.save(img_byte_arr, format="JPEG", quality=85)
            img_byte_arr = img_byte_arr.getvalue()

            # Convert to base64
            return base64.b64encode(img_byte_arr).decode("utf-8")

        except Exception as e:
            logger.warning(f"pdf2image conversion failed: {e}")

        # Try PyMuPDF as a fallback
        try:
            logger.info("Converting PDF to image using PyMuPDF")

            doc = fitz.open(pdf_path)

            if doc.page_count == 0:
                logger.warning("PDF has no pages according to PyMuPDF")
                return None

            # If there's only one page
            if doc.page_count == 1:
                page = doc.load_page(0)
                pix = page.get_pixmap(
                    matrix=fitz.Matrix(2, 2)
                )  # 2x scaling for better quality

                img_data = pix.samples
                img = Image.frombytes("RGB", [pix.width, pix.height], img_data)

                img_byte_arr = io.BytesIO()
                img.save(img_byte_arr, format="JPEG", quality=85)
                img_byte_arr = img_byte_arr.getvalue()

                return base64.b64encode(img_byte_arr).decode("utf-8")

            # If multiple pages, combine them
            else:
                # Create list to hold individual page images
                page_images = []
                total_height = 0
                max_width = 0

                # Convert each page to an image
                for page_num in range(doc.page_count):
                    page = doc.load_page(page_num)
                    pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))

                    img_data = pix.samples
                    img = Image.frombytes("RGB", [pix.width, pix.height], img_data)

                    page_images.append(img)
                    total_height += img.height
                    max_width = max(max_width, img.width)

                # Create a new image with the combined dimensions
                combined_img = Image.new("RGB", (max_width, total_height))

                # Paste each page image into the combined image
                y_offset = 0
                for img in page_images:
                    combined_img.paste(img, (0, y_offset))
                    y_offset += img.height

                # Convert the combined image to bytes
                img_byte_arr = io.BytesIO()
                combined_img.save(img_byte_arr, format="JPEG", quality=85)
                img_byte_arr = img_byte_arr.getvalue()

                # Convert to base64
                return base64.b64encode(img_byte_arr).decode("utf-8")

        except Exception as e:
            logger.warning(f"PyMuPDF conversion failed: {e}")

        # If all methods fail
        logger.error("All PDF to image conversion methods failed")
        return None

    def extract_data_from_text(self, text: str) -> Dict[str, Any]:
        """
        Extract data directly from text.

        Args:
            text (str): Text to process

        Returns:
            dict: Extracted data
        """
        return self.process_text_with_model(text)

    def batch_process_pdfs(
        self, pdf_directory: str, save_interval: int = 20
    ) -> List[Dict[str, Any]]:
        """
        Process multiple PDF files in a directory with memory management.

        Args:
            pdf_directory (str): Directory containing PDF files
            save_interval (int): How often to save intermediate results

        Returns:
            List[Dict[str, Any]]: List of extraction results
        """
        logger.info(f"Starting batch processing of PDFs in {pdf_directory}")

        # Get list of PDF files
        pdf_files = [
            os.path.join(pdf_directory, f)
            for f in os.listdir(pdf_directory)
            if f.lower().endswith(".pdf")
        ]
        logger.info(f"Found {len(pdf_files)} PDF files to process")

        # Initialize results list
        all_results = []

        # Clear memory before starting batch processing
        self._clear_memory()

        try:
            # Process files in batches
            for i, pdf_path in enumerate(pdf_files):
                try:
                    # Extract filename for reporting
                    filename = os.path.basename(pdf_path)
                    logger.info(f"Processing file {i+1}/{len(pdf_files)}: {filename}")

                    # Extract data from PDF
                    result = self.extract_data_from_pdf(pdf_path)

                    # Add filename to result for reference
                    result["filename"] = filename

                    # Add to results list
                    all_results.append(result)

                    # Log progress
                    logger.info(f"Completed processing {filename}")

                    # Save intermediate results
                    if self.output_dir and (i + 1) % save_interval == 0:
                        self._save_intermediate_results(all_results, i + 1)

                    # Force memory cleanup after every batch_size files
                    if (i + 1) % self.batch_size == 0:
                        logger.info(
                            f"Completed batch of {self.batch_size} files. Clearing memory..."
                        )
                        self._clear_memory()

                    # Also check memory usage regardless of batch
                    self._check_memory()

                    # Small pause to allow system to stabilize
                    time.sleep(0.1)

                except Exception as e:
                    logger.error(f"Error processing {pdf_path}: {e}")
                    # Add error entry to results
                    all_results.append(
                        {
                            "filename": os.path.basename(pdf_path),
                            "error": str(e),
                            **DEFAULT_DICT,
                        }
                    )
                    # Clear memory on error
                    self._clear_memory()

            # Save final results
            if self.output_dir:
                self._save_final_results(all_results)

            logger.info(
                f"Batch processing complete. Processed {len(all_results)} files."
            )

            return all_results

        except Exception as e:
            logger.error(f"Fatal error in batch processing: {e}")

            # Save whatever results we have in case of fatal error
            if self.output_dir and all_results:
                self._save_intermediate_results(all_results, "error")

            return all_results

        finally:
            # Always clean up at the end
            self._clear_memory()

    def _save_intermediate_results(
        self, results: List[Dict[str, Any]], batch_num: Union[int, str]
    ):
        """Save intermediate results to file"""
        if not self.output_dir:
            return

        output_path = os.path.join(
            self.output_dir, f"extraction_results_batch_{batch_num}.json"
        )
        try:
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved intermediate results to {output_path}")
        except Exception as e:
            logger.error(f"Error saving intermediate results: {e}")

    def _save_final_results(self, results: List[Dict[str, Any]]):
        """Save final results to file"""
        if not self.output_dir:
            return

        timestamp = time.strftime("%Y%m%d-%H%M%S")
        output_path = os.path.join(
            self.output_dir, f"extraction_results_final_{timestamp}.json"
        )
        try:
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved final results to {output_path}")
        except Exception as e:
            logger.error(f"Error saving final results: {e}")

    def close(self):
        """
        Properly close and clean up resources.
        Call this method when you're done using the extractor.
        """
        logger.info("Closing InvoiceExtractor and cleaning up resources")

        try:
            # Delete large objects explicitly
            if hasattr(self, "pipe"):
                del self.pipe
            if hasattr(self, "model"):
                del self.model
            if hasattr(self, "tokenizer"):
                del self.tokenizer

            # Final memory cleanup
            self._clear_memory()

            logger.info("Resources cleaned up successfully")

        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

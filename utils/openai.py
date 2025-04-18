import openai
import base64
import pandas as pd
import fitz  # PyMuPDF
import os
from PIL import Image


class PDFInvoiceExtractor:
    """
    A class to extract information from PDF invoices using OpenAI API.
    """

    def __init__(self, api_key):
        """
        Initialize the PDFInvoiceExtractor with OpenAI API key.

        Args:
            api_key (str): OpenAI API key
        """
        self.api_key = api_key
        openai.api_key = api_key
        self.temp_folder = "temp_images"

        # Create temp folder if it doesn't exist
        if not os.path.exists(self.temp_folder):
            os.makedirs(self.temp_folder)

    def convert_pdf_to_images(self, pdf_path, merge=True):
        """
        Convert PDF pages to images. Can merge all pages into a single image.

        Args:
            pdf_path (str): Path to the PDF file
            merge (bool): Whether to merge all pages into a single image

        Returns:
            list: List of paths to the generated images (single item if merge=True)
        """
        doc = fitz.open(pdf_path)
        image_paths = []

        # First save individual page images temporarily
        temp_page_images = []
        for page_num in range(len(doc)):
            page = doc.load_page(page_num)
            pix = page.get_pixmap()
            temp_image_path = os.path.join(
                self.temp_folder, f"temp_page_{page_num + 1}.png"
            )
            pix.save(temp_image_path)
            temp_page_images.append(temp_image_path)

        # Merge all pages into one image
        merged_image_path = os.path.join(self.temp_folder, "merged_pages.jpg")
        self._merge_images(temp_page_images, merged_image_path)

        # Clean up temporary individual page images
        for temp_path in temp_page_images:
            if os.path.exists(temp_path):
                os.remove(temp_path)

        image_paths.append(merged_image_path)

        return image_paths

    def _merge_images(self, image_paths, output_path):
        """
        Merge multiple images into a single vertical image.

        Args:
            image_paths (list): List of image paths to merge
            output_path (str): Path to save the merged image
        """
        # Open images and calculate total dimensions
        images = [Image.open(path) for path in image_paths]
        width = max(img.width for img in images)
        height = sum(img.height for img in images)

        # Create a new image with the total size
        merged_image = Image.new("RGB", (width, height), color="white")

        # Paste each image
        y_offset = 0
        for img in images:
            merged_image.paste(img, (0, y_offset))
            y_offset += img.height
            img.close()

        # Save the merged image
        merged_image.save(output_path, "JPEG", quality=90)
        merged_image.close()

    def extract_data_from_image(self, image_path):
        """
        Extract invoice data from an image using OpenAI API.

        Args:
            image_path (str): Path to the image file

        Returns:
            dict: Extracted invoice data
        """
        with open(image_path, "rb") as f:
            image_data = base64.b64encode(f.read()).decode("utf-8")

        response = openai.chat.completions.create(
            model="gpt-4-turbo",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": "Extract the invoice number, date, vendor name, and total amount from this invoice. Display as a dict without any output comment",
                        },
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/jpeg;base64,{image_data}"
                            },
                        },
                    ],
                }
            ],
            max_tokens=500,
        )

        # Parse the response to get a dictionary
        result_str = response.choices[0].message.content

        # Basic cleaning to convert string representation to dict
        # Note: This is a simple approach - consider using ast.literal_eval for more complex outputs
        result_str = result_str.strip("`")
        if result_str.startswith("```python"):
            result_str = result_str[10:]  # Remove ```python
        if result_str.startswith("```"):
            result_str = result_str[3:]  # Remove ```
        if result_str.endswith("```"):
            result_str = result_str[:-3]  # Remove trailing ```

        # Use eval with caution - in production code, consider safer alternatives
        try:
            result_dict = eval(result_str)
            if not isinstance(result_dict, dict):
                raise ValueError("Result is not a dictionary")
        except Exception as e:
            print(f"Error parsing result: {e}")
            print(f"Original result: {result_str}")
            result_dict = {"error": "Failed to parse result"}

        return result_dict

    def process_pdf(self, pdf_path, merge=True):
        """
        Process a PDF file and extract invoice data.

        Args:
            pdf_path (str): Path to the PDF file
            merge (bool): Whether to merge all pages into a single image

        Returns:
            dict or list: Dictionary with extracted data if merge=True,
                         list of dictionaries for each page if merge=False
        """
        image_paths = self.convert_pdf_to_images(pdf_path, merge=merge)

        if merge:
            # Single image with all pages merged
            data = self.extract_data_from_image(image_paths[0])
            return data
        else:
            # Multiple images (one per page)
            results = []
            for image_path in image_paths:
                data = self.extract_data_from_image(image_path)
                results.append(data)
            return results

    def to_dataframe(self, results):
        """
        Convert list of dictionaries to pandas DataFrame.

        Args:
            results (list or dict): List of dictionaries or single dictionary with extracted data

        Returns:
            pandas.DataFrame: DataFrame containing the extracted data
        """
        if isinstance(results, dict):
            results = [results]

        df = pd.DataFrame(results)
        return df

    def cleanup(self):
        """
        Clean up temporary image files.
        """
        for file in os.listdir(self.temp_folder):
            file_path = os.path.join(self.temp_folder, file)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
            except Exception as e:
                print(f"Error deleting {file_path}: {e}")

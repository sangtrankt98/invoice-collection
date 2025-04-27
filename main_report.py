try:
    # Create sample data
    data = {
        "document_type": ["PKT"] * 5,
        "document_number": ["128/01", "129/01", "133/01", "134/01", "130/01"],
        "document_date": [
            "03/10/24",
            "07/10/24",
            "07/10/24",
            "07/10/24",
            "07/10/24",
        ],
        "invoice_series": ["1C24TVT", "1K24DAD", "1K24DAD", "1K24DAD", "1K24DAD"],
        "invoice_number": ["13149", "6305445", "6316216", "6317242", "6319144"],
        "invoice_date": [
            "03/10/24",
            "07/10/24",
            "07/10/24",
            "07/10/24",
            "07/10/24",
        ],
        "seller_name": [
            "CÔNG TY CỔ PHẦN VẬN TẢI HÀNG KHÔNG MIỀN NAM",
            "CÔNG TY DỊCH VỤ MOBIFONE KHU VỰC 2 - CHI NHÁNH TỔNG CÔNG TY VIỄN THÔNG MOBIFONE",
            "CÔNG TY DỊCH VỤ MOBIFONE KHU VỰC 2 - CHI NHÁNH TỔNG CÔNG TY VIỄN THÔNG MOBIFONE",
            "CÔNG TY DỊCH VỤ MOBIFONE KHU VỰC 2 - CHI NHÁNH TỔNG CÔNG TY VIỄN THÔNG MOBIFONE",
            "CÔNG TY DỊCH VỤ MOBIFONE KHU VỰC 2 - CHI NHÁNH TỔNG CÔNG TY VIỄN THÔNG MOBIFONE",
        ],
        "tax_code": [
            "0310422869",
            "0100686209-002",
            "0100686209-002",
            "0100686209-002",
            "0100686209-002",
        ],
        "product_description": [
            "Bảo dưỡng ô tô",
            "Cước viễn thông",
            "Cước viễn thông",
            "Cước viễn thông",
            "Cước viễn thông",
        ],
        "amount_before_tax": [10358750, 146843, 217273, 81548, 80909],
        "tax_rate": [10, 10, 10, 10, 10],
        "vat_amount": [1035875, 14684, 21727, 8155, 8091],
        "notes": ["", "", "", "", ""],
    }

    df = pd.DataFrame(data)
    logger.debug(f"Sample DataFrame created with {len(df)} rows")

    # Create Excel file
    generator = InvoiceExcelGenerator(
        company_name="CÔNG TY TNHH DV VIỆT LUẬT",
        tax_code="0312426354",
        address="2 Hoa Phượng, Phường 02, Quận Phú Nhuận, TP.HCM",
        period="q4",
        year=2024,
    )

    output_file = generator.generate_excel(df, "BKMV_Output.xlsx")
    logger.info(f"Excel file generated: {output_file}")
    print(f"Excel file generated: {output_file}")
except Exception as e:
    logger.error(f"Error in example script: {str(e)}")
    print(f"Error generating Excel file: {str(e)}")

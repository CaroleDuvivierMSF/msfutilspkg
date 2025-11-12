import pandas as pd
import datetime
import numpy as np
import os, shutil
from deltalake import DeltaTable
from deltalake.writer import write_deltalake

def write_delta_lake_table(df: pd.DataFrame, table_path: str, schema_dtype: dict, mode: str = 'append'):
    """
    Ajoute les métadonnées du job à la table Delta spécifiée par table_path.
    """
    df_new_row = df.copy()

    # Application du schéma et nettoyage
    try:
        df_new_row = df_new_row.astype(schema_dtype)
    except Exception as e:
        # Ceci est très important: si une clé manque, le DataFrame sera créé avec le mauvais type.
        print(f"Erreur de conversion de type. Vérifiez que toutes les clés du schéma sont dans le dictionnaire de métadonnées: {e}")
        raise 

    if os.path.exists(table_path) and mode == 'overwrite':
        dt = DeltaTable(table_path)
        dt.delete()  # this logically deletes data files

    # df_new_row['error_message'] = df_new_row['error_message'].fillna('')

    # Écriture transactionnelle en mode 'append'
    write_deltalake(
        table_or_uri=table_path, 
        data=df_new_row, 
        mode = mode
    )
    
    print(f"Statut du job '{df.get('job_name')}' ajouté à la table Delta à {table_path}")

def write_excel_2003_xml_from_df(df, filename, sheet_name="Sheet1"):
    """
    Write a pandas DataFrame to Excel 2003 XML (.xls) with:
      - Dates displayed as DD/MM/YYYY
      - NaT, NaN, None handled as blank cells
      - Array-like cells skipped safely
    """
    xml_header = f"""<?xml version="1.0"?>
<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet"
          xmlns:o="urn:schemas-microsoft-com:office:office"
          xmlns:x="urn:schemas-microsoft-com:office:excel"
          xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet">
  <Styles>
    <Style ss:ID="sDate">
      <NumberFormat ss:Format="DD/MM/YYYY"/>
    </Style>
  </Styles>
  <Worksheet ss:Name="{sheet_name}">
    <Table>
"""
    xml_footer = """    </Table>
  </Worksheet>
</Workbook>"""

    # Headers
    xml_rows = "      <Row>\n"
    for header in df.columns:
        xml_rows += f'        <Cell><Data ss:Type="String">{header}</Data></Cell>\n'
    xml_rows += "      </Row>\n"

    # Data rows
    for _, row in df.iterrows():
        xml_rows += "      <Row>\n"
        for col, value in row.items():
            # Skip missing values and array-like objects
            if isinstance(value, (list, np.ndarray, pd.Series)):
                value = str(value)  # Convert to string representation

            # Now safe to check pd.isna()
            if pd.isna(value):
                continue

            # Numeric
            if isinstance(value, (int, float)):
                xml_rows += f'        <Cell><Data ss:Type="Number">{value}</Data></Cell>\n'

            # Dates
            elif isinstance(value, (datetime.date, datetime.datetime, pd.Timestamp)):
                if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
                    value = datetime.datetime.combine(value, datetime.time(0, 0, 0))
                date_value = value.strftime("%Y-%m-%dT%H:%M:%S.000")
                xml_rows += f'        <Cell ss:StyleID="sDate"><Data ss:Type="DateTime">{date_value}</Data></Cell>\n'

            # Strings / other
            else:
                xml_rows += f'        <Cell><Data ss:Type="String">{value}</Data></Cell>\n'

        xml_rows += "      </Row>\n"

    # Save XML file
    with open(filename, "w", encoding="utf-8") as f:
        f.write(xml_header + xml_rows + xml_footer)

    print(f"✅ File saved as '{filename}'.")

    
def write_excel_xlsx(df: pd.DataFrame, filename: str, sheet_name: str = "Sheet1"):
    """
    Write a pandas DataFrame to a simple Excel .xlsx file.

    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame to write.
    filename : str
        Output file path, e.g., "output.xlsx".
    sheet_name : str, optional
        Worksheet name, default is "Sheet1".
    """
    # Use pandas built-in Excel writer
    df.to_excel(filename, sheet_name=sheet_name, index=False)
    print(f"✅ File saved as '{filename}' in .xlsx format.")
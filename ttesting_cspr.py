import wrds
import pandas as pd
from pathlib import Path


def create_company_info(path):
    """
    Extracts company names, CIKs, and years from text files in the given path.
    """
    info_list = []

    for file_path in path.glob("*.txt"):
        with file_path.open("r", encoding="utf-8") as file:
            content = file.read().splitlines()
            name, cik, year = None, None, file_path.stem[-4:]

            for line in content:
                if "COMPANY CONFORMED NAME:" in line:
                    name = line.replace("COMPANY CONFORMED NAME:", "").strip()
                elif "CENTRAL INDEX KEY:" in line:
                    cik = line.replace("CENTRAL INDEX KEY:", "").strip()
                if name and cik:
                    break

            if name and cik:
                info_list.append({"Companies": name, "CIK": cik, "Year": year})

    return pd.DataFrame(info_list).drop_duplicates()


def query_wrds(cik_list):
    """
    Queries WRDS for company information and returns data.
    """
    with wrds.Connection() as db:
        cik_params = {"cik_list": tuple(cik_list)}
        CIK_query = """
            SELECT cik, cusip, lpermno, linkdt 
            FROM crsp_a_ccm.ccm_lookup
            WHERE cik IN %(cik_list)s
        """
        cusip_data = db.raw_sql(CIK_query, params=cik_params)

        permno_params = {"permno_list": tuple(cusip_data["lpermno"])}
        Returns_Query = """
            SELECT permno, yyyy, cusip, annret, anncap  
            FROM crsp_a_stock.stkannsecuritydata
            WHERE permno IN %(permno_list)s
        """
        returns_data = db.raw_sql(Returns_Query, params=permno_params)

    return cusip_data, returns_data


output_dir = Path.cwd() / "SAMPLE_10Ks"
output_dir.mkdir(parents=True, exist_ok=True)
company_info = create_company_info(output_dir)

cik_list = company_info["CIK"].unique().tolist()
CUSIP, RETURNS = query_wrds(cik_list)

print(CUSIP.head())
print(RETURNS.head())

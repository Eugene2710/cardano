import pandas as pd


def get_github_profile(url: str) -> str:
    """
    Responsible for getting only the github profile from a github url link
    e.g."github.com/pacu/developer-portfolio" -> "pacu"
    "github.com/pacu" -> "pacu",
    "github.com/pacu/" -> "pacu"
    """
    if not isinstance(url, str) or not url.strip():
        return ""
    url = url.strip()

    prefix: str = "github.com/"
    len_prefix: int = len(prefix)
    if url.startswith(prefix):
        url = url[len_prefix:]
    return url.split("/")[0]


if __name__ == "__main__":
    df = pd.read_csv("/Users/eugeneleejunping/Documents/cardano_grants/extracted_companies_from_ideascale.csv")
    company_list: list[str] = df['Company'].to_list()
    company_list = [get_github_profile(company) for company in company_list]
    df['Company'] = company_list

    df.to_csv("updated_company_with_extracted_github.csv")
    print("get github profile complete")
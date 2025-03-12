import pandas as pd

def extract_company(url: str) -> str:
    """
    responsible for:
    1) remove prefix: "https://"
    2) removing subsequent possible chars, in list, from url if it exists

    note: startswith(), endswith(), removeprefix, removesuffix are written in C so they are optimized for use
    """
    if not isinstance(url, str) or not url.strip():
        return ""
    url = url.strip()

    prefix_list: list[str] = [
        "https://www.",
        "https://"

    ]
    suffix_list: list[str] = [
        ".com",
        ".com/",
        ".io",
        ".io/",
        ".fi",
        ".fi/",
        ".rest",
        ".rest/",
        ".net",
        ".net/",
        ".city",
        ".city/",
        ".org",
        ".org/",
        ".co.jp/",
        ".co.jp",
        ".id",
        ".id/",
        ".vn",
        ".vn/",
        ".dev",
        ".dev/",
        ".network",
        ".network/",
        ".xyz",
        ".xyz/"
        ".earth",
        ".earth/",
        ".art",
        ".art/",
        ".co",
        ".co/",
        ".de",
        ".de/",
        ".com/en/",
        ".com.br/",
        ".com.br",
        ".ca",
        ".ca/",
        ".ac.in/",
        ".ch",
        ".ch/",
        ".ae",
        ".ae/",
        ".li",
        ".li/",
        ".jp",
        ".jp/",
        ".me",
        ".me/",
        ".gg",
        ".gg/",
        ".pt",
        ".pt/",
        ".ai",
        ".ai/"
    ]
    company: str = url
    for prefix in prefix_list:
        if company.startswith(prefix):
            company = company.removeprefix(prefix)
            break

    for suffix in suffix_list:
        if company.endswith(suffix):
            company = company.removesuffix(suffix)
            break

    return company


if __name__ == "__main__":
    # read CSV
    df = pd.read_csv("/Users/eugeneleejunping/Documents/cardano_grants/company_links_scraped_ideascale.csv")
    # parse the "Company Link" column into a list
    company_links: list[str] = df['Company List'].to_list()
    company_list: list[str] = []

    for link in company_links:
        if not isinstance(link, str):
            company_list.append("")
        else:
            company_list.append(extract_company(link))

    df['Company'] = company_list

    df.to_csv("extracted_companies_from_ideascale.csv", index=False)
    print("extraction complete")


"""
totest: 
https://github.com/Anastasia-Labs/
https://www.scaleway.com/en/
https://www.adapix.com.br/

"""
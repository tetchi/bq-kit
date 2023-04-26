import pydata_google_auth

from .config import DEFAULT_SCOPE


def get_credentials(scopes: list | None = None):
    if scopes is None:
        scopes = DEFAULT_SCOPE
    return pydata_google_auth.get_user_credentials(scopes=scopes)

def get_table_id(project:str, dataset:str, table:str):
    return "{}.{}.{}".format(project, dataset, table)